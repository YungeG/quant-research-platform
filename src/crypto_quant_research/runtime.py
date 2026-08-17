from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from crypto_quant_domain import (
    ArtifactEnvelope,
    ArtifactRef,
    canonical_bytes,
    canonical_sha256,
)
from crypto_quant_foundation import LocalFoundation, LogEntryRef
from crypto_quant_validation import SampleConsumptionLedger, SampleConsumptionRecord

from .integration import (
    RESEARCH_EXECUTION_LOG,
    AnalysisTask,
    CandidateFamily,
    DependencyBlock,
    ExecutionEntry,
    ExperimentExecutionManifest,
    ExperimentSpec,
    LocalFailure,
    NoSelection,
    ResearchCoreError,
    SelectionDeclaration,
    SelectionPolicy,
    TaskAttemptClosed,
    TaskAttemptStarted,
    TaskOutcome,
    TaskRef,
    TrialCompletedPublication,
    TrialDeclaration,
    VerifiedAnalysis,
    block_analysis_from_upstream,
    build_candidate_family,
    build_execution_manifest,
    build_task_universe,
    build_trial_declarations,
    map_backtest_observation,
    select_candidate,
    validate_execution_prefix,
)

_RESEARCH_ARTIFACTS_LOG = "research.artifacts.v1"


class _RuntimeFailure(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _plain(value: object) -> Any:
    try:
        return json.loads(canonical_bytes(value).decode("utf-8"))
    except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("value must be canonical JSON") from error


def _wire(value: object) -> str:
    return canonical_bytes(value).decode("utf-8")


def _ref_payload(ref: ArtifactRef) -> dict[str, object]:
    return ref.to_canonical_dict()


def _artifact_event_id(log_name: str, ref: ArtifactRef) -> str:
    return canonical_sha256(("artifact-publication-v1", log_name, ref))


def _translate(value: object, refs: Mapping[str, ArtifactRef]) -> object:
    def replace(item: object) -> object:
        if type(item) is str and item in refs:
            return _ref_payload(refs[item])
        if type(item) is dict:
            return {key: replace(child) for key, child in item.items()}
        if type(item) is list:
            return [replace(child) for child in item]
        return item

    return replace(_plain(value))


def _reverse(value: object, refs: Mapping[str, str]) -> object:
    if type(value) is dict:
        wire = _wire(value)
        if wire in refs:
            return refs[wire]
        return {key: _reverse(child, refs) for key, child in value.items()}
    if type(value) is list:
        return [_reverse(child, refs) for child in value]
    return value


def _publish(
    foundation: LocalFoundation,
    log_name: str,
    artifact_type: str,
    payload: object,
):
    envelope = ArtifactEnvelope.create(artifact_type, 1, payload)
    ref = foundation.put(envelope=envelope)
    receipt = foundation.append(
        log_name,
        _artifact_event_id(log_name, ref),
        canonical_bytes(envelope),
    )
    return ref, receipt


def _published_entries(
    foundation: LocalFoundation, log_name: str
) -> tuple[tuple[object, ArtifactRef, dict[str, object]], ...]:
    result: list[tuple[object, ArtifactRef, dict[str, object]]] = []
    for entry in foundation.entries(log_name):
        try:
            decoded = json.loads(entry.payload.decode("utf-8"))
            envelope = ArtifactEnvelope(
                decoded["artifact_type"],
                decoded["schema_version"],
                decoded["payload"],
                decoded["content_hash"],
            )
            if canonical_bytes(envelope) != entry.payload:
                raise ValueError("entry is not canonical")
            ref = ArtifactRef.from_envelope(envelope)
            if entry.event_id != _artifact_event_id(log_name, ref):
                raise ValueError("entry has the wrong event id")
            payload = _plain(envelope.payload)
            if type(payload) is not dict:
                raise ValueError("artifact payload is not an object")
        except (KeyError, TypeError, UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
            raise ResearchCoreError("MANIFEST_CUTOFF_INVALID") from error
        result.append((entry, ref, payload))
    return tuple(result)


def _failure_code(error: Exception) -> str:
    code = getattr(error, "code", None)
    return code if type(code) is str and code else "BACKTEST_OPERATION_FAILED"


def _require_backtest(backtest: object) -> None:
    if not all(
        callable(getattr(backtest, name, None))
        for name in ("run", "derive", "load_completed", "load_terminal", "load_analysis")
    ):
        raise TypeError("backtest must expose the frozen BT-PORT operations")


@dataclass(frozen=True, slots=True)
class TrialExecution:
    """One opaque fixture request and its caller-supplied request ref."""

    trial_declaration_ref: object
    request_spec: dict[str, object]
    backtest_request_ref: object
    resolved_model_refs: tuple[object, ...] = ()

    def __post_init__(self) -> None:
        trial_ref = _plain(self.trial_declaration_ref)
        request = _plain(self.request_spec)
        request_ref = _plain(self.backtest_request_ref)
        if type(trial_ref) is not str or type(request) is not dict or type(request_ref) is not dict:
            raise ValueError("trial execution must contain canonical opaque references")
        if type(self.resolved_model_refs) is not tuple:
            raise ValueError("resolved_model_refs must be a tuple")
        object.__setattr__(self, "trial_declaration_ref", trial_ref)
        object.__setattr__(self, "request_spec", request)
        object.__setattr__(self, "backtest_request_ref", request_ref)
        object.__setattr__(
            self,
            "resolved_model_refs",
            tuple(_plain(ref) for ref in self.resolved_model_refs),
        )


@dataclass(frozen=True, slots=True)
class FrozenExperimentInputs:
    experiment_spec: ExperimentSpec
    selection_policy: SelectionPolicy
    selection_declared_by_ref: object
    trial_executions: tuple[TrialExecution, ...]
    reservation_at: str
    max_attempts: int = 1

    def __post_init__(self) -> None:
        if type(self.experiment_spec) is not ExperimentSpec:
            raise TypeError("experiment_spec must be an ExperimentSpec")
        if type(self.selection_policy) is not SelectionPolicy:
            raise TypeError("selection_policy must be a SelectionPolicy")
        if type(self.trial_executions) is not tuple or any(
            type(item) is not TrialExecution for item in self.trial_executions
        ):
            raise TypeError("trial_executions must be a tuple of TrialExecution")
        if type(self.max_attempts) is not int or self.max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        object.__setattr__(self, "selection_declared_by_ref", _plain(self.selection_declared_by_ref))

        trials = build_trial_declarations(self.experiment_spec)
        expected = {trial.ref for trial in trials}
        supplied = tuple(item.trial_declaration_ref for item in self.trial_executions)
        if len(supplied) != len(expected) or set(supplied) != expected:
            raise ResearchCoreError("TASK_REF_FOREIGN")
        if len({_wire(item.backtest_request_ref) for item in self.trial_executions}) != len(supplied):
            raise ValueError("TRIAL_REQUEST_COLLISION")
        if not any(
            _wire(self.selection_policy.metric_profile_ref) == _wire(profile_ref)
            for profile_ref in self.experiment_spec.metric_profile_refs
        ):
            raise ResearchCoreError("SELECTION_POLICY_MISMATCH")

        declaration = SelectionDeclaration(
            self.experiment_spec.ref,
            self.selection_policy.ref,
            "candidate_trial_declarations_v1",
            self.selection_declared_by_ref,
        )
        first_slice = self.experiment_spec.data_slices[0]
        record = SampleConsumptionRecord(
            first_slice.dataset_revision,
            first_slice.interval_start,
            first_slice.interval_end,
            "discovery",
            "preflight",
            self.reservation_at,
        )
        object.__setattr__(self, "reservation_at", record.consumed_at)
        # Constructing this before I/O validates the predeclared selection wire.
        object.__setattr__(self, "selection_declared_by_ref", declaration.declared_by_ref)


@dataclass(frozen=True, slots=True)
class PublishedStrategyCandidate:
    strategy_candidate_ref: ArtifactRef
    candidate_family_ref: ArtifactRef
    execution_manifest_ref: ArtifactRef
    manifest_cutoff: LogEntryRef


@dataclass(frozen=True, slots=True)
class PublishedNoSelection:
    candidate_family_ref: ArtifactRef
    execution_manifest_ref: ArtifactRef
    manifest_cutoff: LogEntryRef
    reason_code: str


@dataclass
class _PublishedBase:
    inputs: FrozenExperimentInputs
    trials: tuple[TrialDeclaration, ...]
    universe: tuple[TaskRef, ...]
    trial_tasks: dict[str, TaskRef]
    refs: dict[str, ArtifactRef]
    selection: SelectionDeclaration
    selection_ref: ArtifactRef
    selection_ledger_sequence: int
    trial_specs: dict[str, ArtifactRef]

    @property
    def experiment_ref(self) -> ArtifactRef:
        return self.refs[self.inputs.experiment_spec.ref]


@dataclass
class _RecoveredExecution:
    entries: list[ExecutionEntry]
    outcomes: dict[TaskRef, TaskOutcome]
    starts: dict[str, TaskAttemptStarted]
    open_attempts: dict[TaskRef, TaskAttemptStarted]
    last_closes: dict[TaskRef, TaskAttemptClosed]


def _normal_inputs(value: object) -> FrozenExperimentInputs:
    if type(value) is not FrozenExperimentInputs:
        raise TypeError("frozen_inputs must be FrozenExperimentInputs")
    executions = tuple(
        TrialExecution(
            item.trial_declaration_ref,
            item.request_spec,
            item.backtest_request_ref,
            item.resolved_model_refs,
        )
        for item in value.trial_executions
    )
    return FrozenExperimentInputs(
        value.experiment_spec,
        value.selection_policy,
        value.selection_declared_by_ref,
        executions,
        value.reservation_at,
        value.max_attempts,
    )


def _publish_base(
    inputs: FrozenExperimentInputs, foundation: LocalFoundation
) -> _PublishedBase:
    spec = inputs.experiment_spec
    trials = build_trial_declarations(spec)
    universe = build_task_universe(spec)
    trial_tasks = {
        task.task_artifact_ref: task for task in universe if task.kind == "TRIAL"
    }
    refs: dict[str, ArtifactRef] = {}

    experiment_ref, _ = _publish(
        foundation, _RESEARCH_ARTIFACTS_LOG, "experiment_spec", spec.payload
    )
    refs[spec.ref] = experiment_ref

    for trial in trials:
        ref, _ = _publish(
            foundation,
            _RESEARCH_ARTIFACTS_LOG,
            "trial_declaration",
            _translate(trial.payload, refs),
        )
        refs[trial.ref] = ref

    for task in universe:
        if task.kind != "ANALYSIS":
            continue
        analysis = task.artifact
        if type(analysis) is not AnalysisTask:
            raise ResearchCoreError("TASK_OUTCOME_INVALID")
        ref, _ = _publish(
            foundation,
            _RESEARCH_ARTIFACTS_LOG,
            "analysis_task",
            _translate(analysis.payload, refs),
        )
        refs[analysis.ref] = ref

    policy_ref, _ = _publish(
        foundation,
        _RESEARCH_ARTIFACTS_LOG,
        "selection_policy",
        inputs.selection_policy.payload,
    )
    refs[inputs.selection_policy.ref] = policy_ref
    selection = SelectionDeclaration(
        spec.ref,
        inputs.selection_policy.ref,
        "candidate_trial_declarations_v1",
        inputs.selection_declared_by_ref,
    )
    selection_ref, selection_receipt = _publish(
        foundation,
        _RESEARCH_ARTIFACTS_LOG,
        "selection_declaration",
        _translate(selection.payload, refs),
    )
    refs[selection.ref] = selection_ref

    executions = {item.trial_declaration_ref: item for item in inputs.trial_executions}
    trial_specs: dict[str, ArtifactRef] = {}
    for trial in trials:
        execution = executions[trial.ref]
        ref, _ = _publish(
            foundation,
            _RESEARCH_ARTIFACTS_LOG,
            "backtest_trial_spec",
            {
                "trial_declaration_ref": _ref_payload(refs[trial.ref]),
                "resolved_model_refs": list(execution.resolved_model_refs),
                "backtest_request_ref": execution.backtest_request_ref,
            },
        )
        trial_specs[trial.ref] = ref

    return _PublishedBase(
        inputs,
        trials,
        universe,
        trial_tasks,
        refs,
        selection,
        selection_ref,
        selection_receipt.ledger_sequence,
        trial_specs,
    )


def _reserve(
    ledger: SampleConsumptionLedger,
    producer_ref: ArtifactRef,
    trial: TrialDeclaration,
    purpose: str,
    reservation_at: str,
) -> None:
    data_slice = trial.data_slice
    ledger.reserve(
        SampleConsumptionRecord(
            data_slice.dataset_revision,
            data_slice.interval_start,
            data_slice.interval_end,
            purpose,
            canonical_sha256(("sample-consumer-v1", producer_ref)),
            reservation_at,
        ),
        producer_ref,
    )


def _reserve_selection(base: _PublishedBase, ledger: SampleConsumptionLedger) -> None:
    for data_slice in base.inputs.experiment_spec.data_slices:
        ledger.reserve(
            SampleConsumptionRecord(
                data_slice.dataset_revision,
                data_slice.interval_start,
                data_slice.interval_end,
                "selection",
                canonical_sha256(("sample-consumer-v1", base.selection_ref)),
                base.inputs.reservation_at,
            ),
            base.selection_ref,
        )


def _publish_execution(
    base: _PublishedBase,
    foundation: LocalFoundation,
    artifact_type: str,
    value: object,
):
    ref, receipt = _publish(
        foundation,
        RESEARCH_EXECUTION_LOG,
        artifact_type,
        _translate(value.payload, base.refs),
    )
    base.refs[value.ref] = ref
    return receipt


def _append_start(
    base: _PublishedBase,
    foundation: LocalFoundation,
    entries: list[ExecutionEntry],
    task: TaskRef,
    ordinal: int,
    parent_closed_attempt_ref: object | None,
) -> TaskAttemptStarted:
    started = TaskAttemptStarted(
        task,
        ordinal,
        parent_closed_attempt_ref,
        (base.selection.ref,),
    )
    _publish_execution(base, foundation, "task_attempt_started", started)
    entries.append(ExecutionEntry(len(entries) + 1, started))
    return started


def _append_close(
    base: _PublishedBase,
    foundation: LocalFoundation,
    entries: list[ExecutionEntry],
    closed: TaskAttemptClosed,
) -> None:
    _publish_execution(base, foundation, "task_attempt_closed", closed)
    entries.append(ExecutionEntry(len(entries) + 1, closed))


def _append_terminal(
    base: _PublishedBase,
    foundation: LocalFoundation,
    entries: list[ExecutionEntry],
    started: TaskAttemptStarted,
    outcome: TaskOutcome,
) -> None:
    _publish_execution(base, foundation, "task_outcome", outcome)
    entries.append(ExecutionEntry(len(entries) + 1, outcome))
    _append_close(
        base,
        foundation,
        entries,
        TaskAttemptClosed(started.ref, "TERMINAL", outcome.ref, None),
    )


def _trial_observation(
    backtest: object,
    request_spec: dict[str, object],
    trial_ref: ArtifactRef,
) -> Mapping[str, object]:
    request = dict(request_spec)
    request["experiment_id"] = canonical_bytes(trial_ref).decode("utf-8")
    run_ref = backtest.run(request)
    try:
        return backtest.load_completed(run_ref)
    except Exception as error:
        if _failure_code(error) != "PORT_REF_TYPE_MISMATCH":
            raise
    return backtest.load_terminal(run_ref)


def _verified_analysis(
    analysis_task: TaskRef,
    trial_outcome: TaskOutcome,
    completed: Mapping[str, object],
    analysis: Mapping[str, object],
) -> VerifiedAnalysis:
    task = analysis_task.artifact
    if (
        type(task) is not AnalysisTask
        or type(trial_outcome.witness) is not TrialCompletedPublication
        or _wire(analysis.get("source_publication_ref"))
        != _wire(trial_outcome.witness.publication_ref)
        or analysis.get("source_execution_result_hash")
        != completed.get("execution_result_hash")
        or _wire(analysis.get("metric_profile_ref")) != _wire(task.metric_profile_ref)
        or analysis.get("result_grade") != completed.get("result_grade")
    ):
        raise _RuntimeFailure("ANALYSIS_LINK_INVALID")
    try:
        return VerifiedAnalysis.from_record(analysis)
    except (TypeError, ValueError, ResearchCoreError) as error:
        raise _RuntimeFailure("ANALYSIS_LINK_INVALID") from error


def _load_verified_analyses(
    manifest: ExperimentExecutionManifest, backtest: object
) -> tuple[VerifiedAnalysis, ...]:
    outcomes = manifest.outcome_map
    completed: dict[str, Mapping[str, object]] = {}
    trial_outcomes: dict[str, TaskOutcome] = {}
    for task, outcome in outcomes.items():
        if task.kind != "TRIAL" or outcome.state != "COMPLETED":
            continue
        if type(outcome.witness) is not TrialCompletedPublication:
            raise ResearchCoreError("SELECTION_INPUT_INCOMPLETE")
        record = backtest.load_completed(_plain(outcome.witness.publication_ref))
        try:
            mapped = map_backtest_observation(task, record)
        except (TypeError, ValueError, ResearchCoreError) as error:
            raise _RuntimeFailure("ANALYSIS_LINK_INVALID") from error
        if _wire(mapped.payload) != _wire(outcome.payload):
            raise _RuntimeFailure("ANALYSIS_LINK_INVALID")
        trial = task.artifact
        if type(trial) is not TrialDeclaration:
            raise ResearchCoreError("SELECTION_INPUT_INCOMPLETE")
        completed[trial.ref] = record
        trial_outcomes[trial.ref] = outcome

    verified: list[VerifiedAnalysis] = []
    for task, outcome in outcomes.items():
        if task.kind != "ANALYSIS" or outcome.state != "COMPLETED":
            continue
        analysis_task = task.artifact
        if type(analysis_task) is not AnalysisTask:
            raise ResearchCoreError("SELECTION_INPUT_INCOMPLETE")
        trial_outcome = trial_outcomes.get(analysis_task.trial_declaration_ref)
        record = backtest.load_analysis(_plain(outcome.witness.analysis_ref))  # type: ignore[union-attr]
        if trial_outcome is None or analysis_task.trial_declaration_ref not in completed:
            raise _RuntimeFailure("ANALYSIS_LINK_INVALID")
        mapped = map_backtest_observation(task, record)
        if _wire(mapped.payload) != _wire(outcome.payload):
            raise _RuntimeFailure("ANALYSIS_LINK_INVALID")
        verified.append(
            _verified_analysis(
                task,
                trial_outcome,
                completed[analysis_task.trial_declaration_ref],
                record,
            )
        )
    return tuple(verified)


def _family(
    base: _PublishedBase,
    foundation: LocalFoundation,
    manifest: ExperimentExecutionManifest,
    manifest_ref: ArtifactRef,
) -> tuple[CandidateFamily, ArtifactRef]:
    family = build_candidate_family(base.inputs.experiment_spec.ref, manifest)
    base.refs[manifest.ref] = manifest_ref
    payload = _translate(
        {
            "experiment_ref": family.experiment_ref,
            "execution_manifest_ref": family.execution_manifest_ref,
        },
        base.refs,
    )
    expected = ArtifactRef.from_envelope(
        ArtifactEnvelope.create("candidate_family", 1, payload)
    )
    families = [
        (ref, candidate)
        for _, ref, candidate in _published_entries(foundation, _RESEARCH_ARTIFACTS_LOG)
        if ref.artifact_type == "candidate_family"
        and candidate.get("experiment_ref") == _ref_payload(base.experiment_ref)
    ]
    if any(candidate != payload for _, candidate in families) or len(families) > 1:
        raise ResearchCoreError("EXPERIMENT_REOPENED_AFTER_CLOSE")
    matches = [ref for ref, candidate in families if candidate == payload]
    if matches:
        if matches[0] != expected:
            raise ResearchCoreError("TASK_OUTCOME_INVALID")
        family_ref = matches[0]
    else:
        family_ref, _ = _publish(
            foundation, _RESEARCH_ARTIFACTS_LOG, "candidate_family", payload
        )
    return family, family_ref


def _candidate_payload(
    base: _PublishedBase,
    family_ref: ArtifactRef,
    selected: object,
) -> dict[str, object]:
    trial_ref = selected.trial_declaration_ref
    if type(trial_ref) is not str or trial_ref not in base.refs or trial_ref not in base.trial_specs:
        raise ResearchCoreError("SELECTION_INPUT_INCOMPLETE")
    return {
        "candidate_family_ref": _ref_payload(family_ref),
        "selection_declaration_ref": _ref_payload(base.selection_ref),
        "selected_trial_declaration_ref": _ref_payload(base.refs[trial_ref]),
        "selected_trial_spec_ref": _ref_payload(base.trial_specs[trial_ref]),
        "selected_publication_ref": _plain(selected.trial_publication_ref),
        "selected_analysis_ref": _plain(selected.analysis_ref),
        "selection_rank": selected.selection_rank,
        "validated": False,
    }


def _select_and_publish(
    base: _PublishedBase,
    foundation: LocalFoundation,
    ledger: SampleConsumptionLedger,
    backtest: object,
    manifest: ExperimentExecutionManifest,
    manifest_ref: ArtifactRef,
    manifest_cutoff: LogEntryRef,
) -> PublishedStrategyCandidate | PublishedNoSelection:
    family, family_ref = _family(base, foundation, manifest, manifest_ref)
    _reserve_selection(base, ledger)
    selected = select_candidate(
        family,
        manifest,
        base.inputs.selection_policy,
        _load_verified_analyses(manifest, backtest),
    )
    candidates = [
        (ref, payload)
        for _, ref, payload in _published_entries(foundation, _RESEARCH_ARTIFACTS_LOG)
        if ref.artifact_type == "strategy_candidate"
        and payload.get("candidate_family_ref") == _ref_payload(family_ref)
    ]
    if type(selected) is NoSelection:
        if candidates:
            raise ResearchCoreError("SELECTION_INPUT_INCOMPLETE")
        return PublishedNoSelection(
            family_ref, manifest_ref, manifest_cutoff, selected.reason_code
        )

    payload = _candidate_payload(base, family_ref, selected)
    expected = ArtifactRef.from_envelope(
        ArtifactEnvelope.create("strategy_candidate", 1, payload)
    )
    if len(candidates) > 1 or (candidates and candidates[0][1] != payload):
        raise ResearchCoreError("SELECTION_INPUT_INCOMPLETE")
    if candidates:
        candidate_ref = candidates[0][0]
        if candidate_ref != expected:
            raise ResearchCoreError("SELECTION_INPUT_INCOMPLETE")
    else:
        candidate_ref, _ = _publish(
            foundation, _RESEARCH_ARTIFACTS_LOG, "strategy_candidate", payload
        )
    return PublishedStrategyCandidate(
        candidate_ref, family_ref, manifest_ref, manifest_cutoff
    )


def _task_ref_wire(payload: Mapping[str, object]) -> str | None:
    task_ref = payload.get("task_ref")
    if type(task_ref) is not dict or set(task_ref) != {"kind", "task_artifact_ref"}:
        return None
    return _wire(task_ref["task_artifact_ref"])


def _foreign_task_for_experiment(
    foundation: LocalFoundation, payload: Mapping[str, object], experiment_ref: ArtifactRef
) -> bool:
    task_ref = payload.get("task_ref")
    if type(task_ref) is not dict:
        return False
    candidate = task_ref.get("task_artifact_ref")
    if type(candidate) is not dict or set(candidate) != {
        "type",
        "artifact_type",
        "schema_version",
        "content_hash",
    }:
        return False
    try:
        ref = ArtifactRef(
            candidate["artifact_type"],
            candidate["schema_version"],
            candidate["content_hash"],
        )
        if candidate["type"] != "artifact_ref" or ref.artifact_type not in {
            "trial_declaration",
            "analysis_task",
        }:
            return False
        source = foundation.read(ref=ref).source_bytes
        decoded = json.loads(source.decode("utf-8"))
        envelope = ArtifactEnvelope(
            decoded["artifact_type"],
            decoded["schema_version"],
            decoded["payload"],
            decoded["content_hash"],
        )
        artifact = _plain(envelope.payload)
        if (
            canonical_bytes(envelope) != source
            or envelope.artifact_type != ref.artifact_type
            or type(artifact) is not dict
        ):
            raise ValueError("invalid task artifact")
    except (KeyError, TypeError, UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
        raise ResearchCoreError("TASK_REF_FOREIGN") from error
    return artifact.get("experiment_ref") == _ref_payload(experiment_ref)


def _core_execution_value(artifact_type: str, payload: dict[str, object]) -> object:
    try:
        if artifact_type == "task_attempt_started":
            task_payload = payload["task_ref"]
            return TaskAttemptStarted(
                TaskRef(task_payload["kind"], task_payload["task_artifact_ref"]),
                payload["ordinal"],
                payload["parent_closed_attempt_ref"],
                tuple(payload["selection_declaration_refs"]),
                payload["dispatch_ref"],
            )
        if artifact_type == "task_attempt_closed":
            return TaskAttemptClosed(
                payload["started_attempt_ref"],
                payload["disposition"],
                payload["task_outcome_ref"],
                payload["failure_code"],
            )
        if artifact_type == "task_outcome":
            task_payload = payload["task_ref"]
            return TaskOutcome(
                TaskRef(task_payload["kind"], task_payload["task_artifact_ref"]),
                payload["state"],
                payload["witness"],
            )
        if artifact_type == "experiment_execution_manifest":
            return ExperimentExecutionManifest(
                payload["experiment_ref"], tuple(payload["task_outcome_refs"])
            )
    except (KeyError, TypeError, ValueError, ResearchCoreError) as error:
        raise ResearchCoreError("TASK_OUTCOME_INVALID") from error
    raise AssertionError("unreachable execution artifact type")


def _replay_execution_entries(
    base: _PublishedBase,
    foundation: LocalFoundation,
    through_log_sequence: int | None = None,
) -> tuple[list[ExecutionEntry], int | None]:
    entries = _published_entries(foundation, RESEARCH_EXECUTION_LOG)
    actual_to_core = {
        _wire(_ref_payload(ref)): local_ref for local_ref, ref in base.refs.items()
    }
    known_task_refs = {
        _wire(_ref_payload(base.refs[task.task_artifact_ref])) for task in base.universe
    }
    known_starts: set[str] = set()
    core_entries: list[ExecutionEntry] = [ExecutionEntry(1, base.selection)]
    first_target_ledger_sequence: int | None = None

    for entry, ref, payload in entries:
        artifact_type = ref.artifact_type
        task_wire = _task_ref_wire(payload)
        is_target = False
        if artifact_type in {"task_attempt_started", "task_outcome"}:
            is_target = task_wire in known_task_refs
            if not is_target and _foreign_task_for_experiment(
                foundation, payload, base.experiment_ref
            ):
                raise ResearchCoreError("TASK_REF_FOREIGN")
        elif artifact_type == "task_attempt_closed":
            is_target = _wire(payload.get("started_attempt_ref")) in known_starts
        elif artifact_type == "experiment_execution_manifest":
            is_target = payload.get("experiment_ref") == _ref_payload(
                base.experiment_ref
            )

        if (
            through_log_sequence is not None
            and entry.log_sequence > through_log_sequence
        ):
            if is_target:
                raise ResearchCoreError("EXPERIMENT_REOPENED_AFTER_CLOSE")
            continue
        if not is_target:
            continue
        if first_target_ledger_sequence is None:
            first_target_ledger_sequence = entry.ledger_sequence
        converted = _reverse(payload, actual_to_core)
        if type(converted) is not dict:
            raise ResearchCoreError("TASK_OUTCOME_INVALID")
        value = _core_execution_value(artifact_type, converted)
        actual_to_core[_wire(_ref_payload(ref))] = value.ref
        base.refs[value.ref] = ref
        if artifact_type == "task_attempt_started":
            known_starts.add(_wire(_ref_payload(ref)))
        core_entries.append(ExecutionEntry(len(core_entries) + 1, value))

    return core_entries, first_target_ledger_sequence


def _replay_existing(
    base: _PublishedBase,
    foundation: LocalFoundation,
    ledger: SampleConsumptionLedger,
    backtest: object,
) -> PublishedStrategyCandidate | PublishedNoSelection | None:
    entries = _published_entries(foundation, RESEARCH_EXECUTION_LOG)
    matching_manifests = [
        (entry, ref, payload)
        for entry, ref, payload in entries
        if ref.artifact_type == "experiment_execution_manifest"
        and payload.get("experiment_ref") == _ref_payload(base.experiment_ref)
    ]
    if not matching_manifests:
        return None
    if len(matching_manifests) != 1:
        raise ResearchCoreError("EXPERIMENT_REOPENED_AFTER_CLOSE")
    manifest_entry, manifest_ref, manifest_payload = matching_manifests[0]
    core_entries, first_target_ledger_sequence = _replay_execution_entries(
        base, foundation, manifest_entry.log_sequence
    )
    manifests = [
        entry.payload
        for entry in core_entries
        if type(entry.payload) is ExperimentExecutionManifest
    ]
    if (
        len(manifests) != 1
        or first_target_ledger_sequence is None
        or base.selection_ledger_sequence >= first_target_ledger_sequence
    ):
        raise ResearchCoreError("SELECTION_PRECOMMIT_MISSING")
    cutoff = {
        "log_name": RESEARCH_EXECUTION_LOG,
        "log_sequence": len(core_entries),
        "receipt_hash": manifest_entry.receipt_hash,
    }
    projection = validate_execution_prefix(
        base.inputs.experiment_spec, core_entries, cutoff
    )
    manifest = build_execution_manifest(base.inputs.experiment_spec, projection, cutoff)
    if _translate(manifest.payload, base.refs) != manifest_payload:
        raise ResearchCoreError("MANIFEST_CUTOFF_INVALID")
    base.refs[manifest.ref] = manifest_ref
    return _select_and_publish(
        base,
        foundation,
        ledger,
        backtest,
        manifest,
        manifest_ref,
        manifest_entry.entry_ref,
    )


def _recover_execution(
    base: _PublishedBase, foundation: LocalFoundation
) -> _RecoveredExecution:
    entries, _ = _replay_execution_entries(base, foundation)
    outcomes: dict[TaskRef, TaskOutcome] = {}
    starts: dict[str, TaskAttemptStarted] = {}
    open_attempts: dict[TaskRef, TaskAttemptStarted] = {}
    last_closes: dict[TaskRef, TaskAttemptClosed] = {}
    canonical_tasks = {task.canonical_wire: task for task in base.universe}

    for index, entry in enumerate(entries[1:], start=1):
        value = entry.payload
        if type(value) is TaskAttemptStarted:
            task = canonical_tasks.get(value.task_ref.canonical_wire)
            if task is None:
                raise ResearchCoreError("TASK_REF_FOREIGN")
            if task is not value.task_ref:
                value = TaskAttemptStarted(
                    task,
                    value.ordinal,
                    value.parent_closed_attempt_ref,
                    value.selection_declaration_refs,
                    value.dispatch_ref,
                )
                entries[index] = ExecutionEntry(entry.log_sequence, value)
            if task in outcomes or task in open_attempts:
                raise ResearchCoreError("ATTEMPT_CHAIN_INVALID")
            previous = last_closes.get(task)
            if previous is None:
                if value.ordinal != 1 or value.parent_closed_attempt_ref is not None:
                    raise ResearchCoreError("ATTEMPT_CHAIN_INVALID")
            else:
                previous_start = starts.get(_wire(previous.started_attempt_ref))
                if (
                    previous_start is None
                    or previous.disposition == "TERMINAL"
                    or value.ordinal != previous_start.ordinal + 1
                    or value.parent_closed_attempt_ref is None
                    or _wire(value.parent_closed_attempt_ref) != _wire(previous.ref)
                ):
                    raise ResearchCoreError("ATTEMPT_CHAIN_INVALID")
            wire = _wire(value.ref)
            if wire in starts:
                raise ResearchCoreError("ATTEMPT_CHAIN_INVALID")
            starts[wire] = value
            open_attempts[task] = value
            continue
        if type(value) is TaskOutcome:
            task = canonical_tasks.get(value.task_ref.canonical_wire)
            if task is None:
                raise ResearchCoreError("TASK_REF_FOREIGN")
            if task is not value.task_ref:
                value = TaskOutcome(task, value.state, value.witness)
                entries[index] = ExecutionEntry(entry.log_sequence, value)
            if task in outcomes or task not in open_attempts:
                raise ResearchCoreError("TASK_OUTCOME_MISSING_OR_DUPLICATE")
            outcomes[task] = value
            continue
        if type(value) is TaskAttemptClosed:
            started = starts.get(_wire(value.started_attempt_ref))
            if started is None or open_attempts.get(started.task_ref) is not started:
                raise ResearchCoreError("ATTEMPT_CHAIN_INVALID")
            task = started.task_ref
            if value.disposition == "TERMINAL":
                outcome = outcomes.get(task)
                if (
                    outcome is None
                    or _wire(value.task_outcome_ref) != _wire(outcome.ref)
                ):
                    raise ResearchCoreError("ATTEMPT_CHAIN_INVALID")
            elif task in outcomes:
                raise ResearchCoreError("ATTEMPT_CHAIN_INVALID")
            del open_attempts[task]
            last_closes[task] = value
            continue
        raise ResearchCoreError("TASK_OUTCOME_INVALID")

    for task in outcomes:
        if task not in open_attempts:
            closed = last_closes.get(task)
            if closed is None or closed.disposition != "TERMINAL":
                raise ResearchCoreError("ATTEMPT_CHAIN_INVALID")
    return _RecoveredExecution(entries, outcomes, starts, open_attempts, last_closes)


def _close_persisted_outcomes(
    base: _PublishedBase, foundation: LocalFoundation, recovery: _RecoveredExecution
) -> None:
    for task, outcome in tuple(recovery.outcomes.items()):
        started = recovery.open_attempts.get(task)
        if started is None:
            continue
        closed = TaskAttemptClosed(started.ref, "TERMINAL", outcome.ref, None)
        _append_close(base, foundation, recovery.entries, closed)
        del recovery.open_attempts[task]
        recovery.last_closes[task] = closed


def _resume_attempt(
    base: _PublishedBase,
    foundation: LocalFoundation,
    recovery: _RecoveredExecution,
    task: TaskRef,
) -> TaskAttemptStarted:
    started = recovery.open_attempts.get(task)
    if started is not None:
        if started.ordinal > base.inputs.max_attempts:
            raise ResearchCoreError("ATTEMPT_CHAIN_INVALID")
        return started

    previous = recovery.last_closes.get(task)
    if previous is None:
        ordinal = 1
        parent: object | None = None
    else:
        previous_start = recovery.starts.get(_wire(previous.started_attempt_ref))
        if previous_start is None or previous.disposition == "TERMINAL":
            raise ResearchCoreError("ATTEMPT_CHAIN_INVALID")
        ordinal = previous_start.ordinal + 1
        parent = previous.ref
    if ordinal > base.inputs.max_attempts:
        raise ResearchCoreError("ATTEMPT_CHAIN_INVALID")
    started = _append_start(base, foundation, recovery.entries, task, ordinal, parent)
    recovery.starts[_wire(started.ref)] = started
    recovery.open_attempts[task] = started
    return started


def _append_recovered_terminal(
    base: _PublishedBase,
    foundation: LocalFoundation,
    recovery: _RecoveredExecution,
    started: TaskAttemptStarted,
    outcome: TaskOutcome,
) -> None:
    task = started.task_ref
    if recovery.open_attempts.get(task) is not started:
        raise ResearchCoreError("ATTEMPT_CHAIN_INVALID")
    _append_terminal(base, foundation, recovery.entries, started, outcome)
    closed = recovery.entries[-1].payload
    if type(closed) is not TaskAttemptClosed:
        raise ResearchCoreError("ATTEMPT_CHAIN_INVALID")
    recovery.outcomes[task] = outcome
    del recovery.open_attempts[task]
    recovery.last_closes[task] = closed


def _append_retryable_close(
    base: _PublishedBase,
    foundation: LocalFoundation,
    recovery: _RecoveredExecution,
    started: TaskAttemptStarted,
    failure_code: str,
) -> None:
    closed = TaskAttemptClosed(started.ref, "RETRYABLE_FAILURE", None, failure_code)
    _append_close(base, foundation, recovery.entries, closed)
    del recovery.open_attempts[started.task_ref]
    recovery.last_closes[started.task_ref] = closed


def _completed_record(
    task: TaskRef, outcome: TaskOutcome, backtest: object
) -> Mapping[str, object]:
    if type(outcome.witness) is not TrialCompletedPublication:
        raise _RuntimeFailure("ANALYSIS_LINK_INVALID")
    record = backtest.load_completed(_plain(outcome.witness.publication_ref))
    mapped = map_backtest_observation(task, record)
    if _wire(mapped.payload) != _wire(outcome.payload):
        raise _RuntimeFailure("ANALYSIS_LINK_INVALID")
    return record


def _execute_new(
    base: _PublishedBase,
    foundation: LocalFoundation,
    ledger: SampleConsumptionLedger,
    backtest: object,
) -> PublishedStrategyCandidate | PublishedNoSelection:
    executions = {
        item.trial_declaration_ref: item for item in base.inputs.trial_executions
    }
    recovery = _recover_execution(base, foundation)
    _close_persisted_outcomes(base, foundation, recovery)
    completed_records: dict[str, Mapping[str, object]] = {}

    for trial in base.trials:
        task = base.trial_tasks[trial.ref]
        if task in recovery.outcomes:
            continue
        execution = executions[trial.ref]
        while True:
            started = _resume_attempt(base, foundation, recovery, task)
            try:
                _reserve(
                    ledger,
                    base.refs[trial.ref],
                    trial,
                    "discovery",
                    base.inputs.reservation_at,
                )
            except Exception:  # noqa: BLE001 - reservation boundary blocks the read
                outcome = TaskOutcome(
                    task,
                    "BLOCKED",
                    DependencyBlock("SAMPLE_RESERVATION_FAILED"),
                )
            else:
                try:
                    record = _trial_observation(
                        backtest, execution.request_spec, base.refs[trial.ref]
                    )
                    outcome = map_backtest_observation(task, record)
                    if outcome.state == "COMPLETED":
                        completed_records[trial.ref] = record
                except Exception as error:  # noqa: BLE001 - frozen provider boundary
                    if started.ordinal < base.inputs.max_attempts:
                        _append_retryable_close(
                            base,
                            foundation,
                            recovery,
                            started,
                            _failure_code(error),
                        )
                        continue
                    outcome = TaskOutcome(
                        task, "FAILED", LocalFailure(_failure_code(error))
                    )
            _append_recovered_terminal(base, foundation, recovery, started, outcome)
            break

    for task in base.universe:
        if task.kind != "ANALYSIS" or task in recovery.outcomes:
            continue
        analysis_task = task.artifact
        if type(analysis_task) is not AnalysisTask:
            raise ResearchCoreError("TASK_OUTCOME_INVALID")
        trial_task = base.trial_tasks[analysis_task.trial_declaration_ref]
        trial_outcome = recovery.outcomes[trial_task]
        if trial_outcome.state != "COMPLETED":
            started = _resume_attempt(base, foundation, recovery, task)
            _append_recovered_terminal(
                base,
                foundation,
                recovery,
                started,
                block_analysis_from_upstream(task, trial_outcome),
            )
            continue

        while True:
            started = _resume_attempt(base, foundation, recovery, task)
            try:
                record = completed_records.get(analysis_task.trial_declaration_ref)
                if record is None:
                    record = _completed_record(trial_task, trial_outcome, backtest)
                    completed_records[analysis_task.trial_declaration_ref] = record
                publication_ref = (  # type: ignore[union-attr]
                    trial_outcome.witness.publication_ref
                )
                analysis_ref = backtest.derive(
                    _plain(publication_ref), _plain(analysis_task.metric_profile_ref)
                )
                analysis = backtest.load_analysis(analysis_ref)
                _verified_analysis(task, trial_outcome, record, analysis)
                outcome = map_backtest_observation(task, analysis)
            except Exception as error:  # noqa: BLE001 - frozen provider boundary
                if started.ordinal < base.inputs.max_attempts:
                    _append_retryable_close(
                        base,
                        foundation,
                        recovery,
                        started,
                        _failure_code(error),
                    )
                    continue
                outcome = TaskOutcome(
                    task, "FAILED", LocalFailure(_failure_code(error))
                )
            _append_recovered_terminal(base, foundation, recovery, started, outcome)
            break

    draft = ExperimentExecutionManifest(
        base.inputs.experiment_spec.ref,
        tuple(recovery.outcomes[task].ref for task in base.universe),
    )
    preflight_entries = recovery.entries + [
        ExecutionEntry(len(recovery.entries) + 1, draft)
    ]
    preflight_cutoff = {
        "log_name": RESEARCH_EXECUTION_LOG,
        "log_sequence": len(preflight_entries),
        "receipt_hash": "sha256:" + "0" * 64,
    }
    validate_execution_prefix(
        base.inputs.experiment_spec, preflight_entries, preflight_cutoff
    )
    manifest_receipt = _publish_execution(
        base, foundation, "experiment_execution_manifest", draft
    )
    recovery.entries.append(ExecutionEntry(len(recovery.entries) + 1, draft))
    cutoff = {
        "log_name": RESEARCH_EXECUTION_LOG,
        "log_sequence": len(recovery.entries),
        "receipt_hash": manifest_receipt.receipt_hash,
    }
    projection = validate_execution_prefix(
        base.inputs.experiment_spec, recovery.entries, cutoff
    )
    manifest = build_execution_manifest(base.inputs.experiment_spec, projection, cutoff)
    if manifest.ref != draft.ref:
        raise ResearchCoreError("MANIFEST_CUTOFF_INVALID")
    return _select_and_publish(
        base,
        foundation,
        ledger,
        backtest,
        manifest,
        base.refs[draft.ref],
        manifest_receipt.entry_ref,
    )


def execute_experiment(
    frozen_inputs: FrozenExperimentInputs,
    foundation: LocalFoundation,
    sample_ledger: SampleConsumptionLedger,
    backtest: object,
) -> PublishedStrategyCandidate | PublishedNoSelection:
    """Publish one fixture-backed Research Experiment without a provider seam."""

    inputs = _normal_inputs(frozen_inputs)
    if type(foundation) is not LocalFoundation:
        raise TypeError("foundation must be a LocalFoundation")
    if type(sample_ledger) is not SampleConsumptionLedger:
        raise TypeError("sample_ledger must be a SampleConsumptionLedger")
    _require_backtest(backtest)
    base = _publish_base(inputs, foundation)
    existing = _replay_existing(base, foundation, sample_ledger, backtest)
    if existing is not None:
        return existing
    return _execute_new(base, foundation, sample_ledger, backtest)


__all__ = [
    "FrozenExperimentInputs",
    "PublishedNoSelection",
    "PublishedStrategyCandidate",
    "TrialExecution",
    "execute_experiment",
]
