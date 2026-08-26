from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
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
    DataSlice,
    DependencyBlock,
    ExecutionEntry,
    ExperimentExecutionManifest,
    ExperimentSpec,
    FeatureBuildTask,
    FeatureDatasetManifest,
    FeatureDatasetPublication,
    FeatureRecipe,
    LocalFailure,
    ModelBuildEvidence,
    ModelBuildPlan,
    ModelBuildPublication,
    ModelTrainingTask,
    NoSelection,
    ResearchCoreError,
    SelectionDeclaration,
    SelectionPolicy,
    TaskAttemptClosed,
    TaskAttemptStarted,
    TaskOutcome,
    TaskRef,
    TargetBuildPublication,
    TargetBuildTask,
    TargetMaterializationEvidence,
    TargetRecipe,
    TrainerRecipe,
    TrialCompletedPublication,
    TrialDeclaration,
    UpstreamTaskOutcome,
    VerifiedAnalysis,
    _backtest_ref_version,
    _canonical_build_artifact,
    _canonical_target_stream,
    _content_hash,
    block_analysis_from_upstream,
    build_candidate_family,
    build_execution_manifest,
    build_task_universe,
    build_trial_declarations,
    map_backtest_observation,
    select_candidate,
    validate_execution_prefix,
    validate_model_build,
)

_RESEARCH_ARTIFACTS_LOG = "research.artifacts.v1"
_SAMPLE_CONSUMPTION_LOG = "validation.sample-consumption.v1"


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


def _artifact_schema_versions(artifact_type: str) -> set[int]:
    if artifact_type == "experiment_spec":
        return {1, 2}
    if artifact_type == "strategy_candidate":
        return {1, 2, 3}
    return {1}


def _publish(
    foundation: LocalFoundation,
    log_name: str,
    artifact_type: str,
    payload: object,
    *,
    schema_version: int = 1,
):
    if schema_version not in _artifact_schema_versions(artifact_type):
        raise ValueError(f"{artifact_type}@{schema_version} is not publishable")
    envelope = ArtifactEnvelope.create(artifact_type, schema_version, payload)
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
            if envelope.schema_version not in _artifact_schema_versions(
                envelope.artifact_type
            ):
                raise ValueError("artifact schema version is not dispatchable")
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
    if type(code) is str and code:
        return code
    value = getattr(code, "value", None)
    return value if type(value) is str and value else "BACKTEST_OPERATION_FAILED"


def _require_backtest(backtest: object) -> None:
    if not all(
        callable(getattr(backtest, name, None))
        for name in ("run", "derive", "load_completed", "load_terminal", "load_analysis")
    ):
        raise TypeError("backtest must expose the frozen BT-PORT operations")


def _load_completed(backtest: object, ref: object) -> Mapping[str, object]:
    try:
        version = _backtest_ref_version(ref, "completed")
    except ValueError as error:
        raise _RuntimeFailure("PORT_REF_TYPE_MISMATCH") from error
    operation = "load_completed" if version == 1 else "load_completed_v3"
    loader = getattr(backtest, operation, None)
    if not callable(loader):
        raise _RuntimeFailure("PORT_REF_TYPE_MISMATCH")
    return loader(_plain(ref))


def _load_analysis(backtest: object, ref: object) -> Mapping[str, object]:
    try:
        version = _backtest_ref_version(ref, "analysis")
    except ValueError as error:
        raise _RuntimeFailure("PORT_REF_TYPE_MISMATCH") from error
    operation = "load_analysis" if version == 1 else "load_analysis_v2"
    loader = getattr(backtest, operation, None)
    if not callable(loader):
        raise _RuntimeFailure("PORT_REF_TYPE_MISMATCH")
    return loader(_plain(ref))


def _is_terminal_ref(value: object) -> bool:
    plain = _plain(value)
    return (
        type(plain) is dict
        and set(plain) == {"type", "artifact_type", "schema_version", "content_hash"}
        and plain.get("type") == "artifact_ref"
    )


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
        if self.experiment_spec.schema_version != 1:
            raise ResearchCoreError("EXPERIMENT_SPEC_INVALID")
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
class FrozenModelExperimentInputs:
    experiment_spec: ExperimentSpec
    feature_recipe: FeatureRecipe
    trainer_recipe: TrainerRecipe
    model_build_plan: ModelBuildPlan
    selection_policy: SelectionPolicy
    selection_declared_by_ref: object
    reservation_at: str
    max_attempts: int = 1

    def __post_init__(self) -> None:
        expected = (
            ("experiment_spec", self.experiment_spec, ExperimentSpec),
            ("feature_recipe", self.feature_recipe, FeatureRecipe),
            ("trainer_recipe", self.trainer_recipe, TrainerRecipe),
            ("model_build_plan", self.model_build_plan, ModelBuildPlan),
            ("selection_policy", self.selection_policy, SelectionPolicy),
        )
        for name, value, expected_type in expected:
            if type(value) is not expected_type:
                raise TypeError(f"{name} must be exact {expected_type.__name__}")
        if type(self.max_attempts) is not int or self.max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        if self.experiment_spec.schema_version != 1:
            raise ResearchCoreError("MODEL_BUILD_PLAN_INVALID")
        if (
            self.experiment_spec.model_build_plan != self.model_build_plan.ref
            or self.model_build_plan.feature_recipe_ref != self.feature_recipe.ref
            or self.model_build_plan.trainer_recipe_ref != self.trainer_recipe.ref
        ):
            raise ResearchCoreError("MODEL_BUILD_PLAN_INVALID")
        if not any(
            _wire(self.selection_policy.metric_profile_ref) == _wire(profile_ref)
            for profile_ref in self.experiment_spec.metric_profile_refs
        ):
            raise ResearchCoreError("SELECTION_POLICY_MISMATCH")
        declaration = SelectionDeclaration(
            self.experiment_spec.ref,
            self.selection_policy.ref,
            "candidate_trial_declarations_v1",
            _plain(self.selection_declared_by_ref),
        )
        record = SampleConsumptionRecord(
            self.model_build_plan.training_slice.dataset_revision,
            self.model_build_plan.training_slice.interval_start,
            self.model_build_plan.training_slice.interval_end,
            "feature_build",
            "preflight",
            self.reservation_at,
        )
        object.__setattr__(self, "selection_declared_by_ref", declaration.declared_by_ref)
        object.__setattr__(self, "reservation_at", record.consumed_at)


@dataclass(frozen=True, slots=True)
class FrozenTargetExperimentInputs:
    experiment_spec: ExperimentSpec
    target_recipe: TargetRecipe
    selection_policy: SelectionPolicy
    selection_declared_by_ref: object
    reservation_at: str
    max_attempts: int = 1

    def __post_init__(self) -> None:
        expected = (
            ("experiment_spec", self.experiment_spec, ExperimentSpec),
            ("target_recipe", self.target_recipe, TargetRecipe),
            ("selection_policy", self.selection_policy, SelectionPolicy),
        )
        for name, value, expected_type in expected:
            if type(value) is not expected_type:
                raise TypeError(f"{name} must be exact {expected_type.__name__}")
        if type(self.max_attempts) is not int or self.max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        if (
            self.experiment_spec.schema_version != 2
            or self.experiment_spec.target_recipe_ref != self.target_recipe.ref
            or self.experiment_spec.model_build_plan is not None
        ):
            raise ResearchCoreError("TARGET_RECIPE_INVALID")
        if not any(
            _wire(self.selection_policy.metric_profile_ref) == _wire(profile_ref)
            for profile_ref in self.experiment_spec.metric_profile_refs
        ):
            raise ResearchCoreError("SELECTION_POLICY_MISMATCH")
        declaration = SelectionDeclaration(
            self.experiment_spec.ref,
            self.selection_policy.ref,
            "candidate_trial_declarations_v1",
            _plain(self.selection_declared_by_ref),
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
        object.__setattr__(self, "selection_declared_by_ref", declaration.declared_by_ref)
        object.__setattr__(self, "reservation_at", record.consumed_at)


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
    inputs: FrozenExperimentInputs | FrozenModelExperimentInputs | FrozenTargetExperimentInputs
    trials: tuple[TrialDeclaration, ...]
    universe: tuple[TaskRef, ...]
    trial_tasks: dict[str, TaskRef]
    refs: dict[str, ArtifactRef]
    selection: SelectionDeclaration
    selection_ref: ArtifactRef
    selection_ledger_sequence: int
    trial_specs: dict[str, ArtifactRef]
    model_build_evidence_ref: ArtifactRef | None = None
    target_evidence_refs: dict[str, ArtifactRef] = field(default_factory=dict)
    target_trial_executions: dict[str, TrialExecution] = field(default_factory=dict)
    trial_preparation_failures: dict[str, str] = field(default_factory=dict)

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


def _normal_model_inputs(value: object) -> FrozenModelExperimentInputs:
    if type(value) is not FrozenModelExperimentInputs:
        raise TypeError("frozen_inputs must be FrozenModelExperimentInputs")
    return FrozenModelExperimentInputs(
        value.experiment_spec,
        value.feature_recipe,
        value.trainer_recipe,
        value.model_build_plan,
        value.selection_policy,
        value.selection_declared_by_ref,
        value.reservation_at,
        value.max_attempts,
    )


def _normal_target_inputs(value: object) -> FrozenTargetExperimentInputs:
    if type(value) is not FrozenTargetExperimentInputs:
        raise TypeError("frozen_inputs must be FrozenTargetExperimentInputs")
    return FrozenTargetExperimentInputs(
        value.experiment_spec,
        value.target_recipe,
        value.selection_policy,
        value.selection_declared_by_ref,
        value.reservation_at,
        value.max_attempts,
    )


def _require_target_materializer(materializer: object, recipe: TargetRecipe) -> None:
    if not callable(getattr(materializer, "materialize_target", None)):
        raise TypeError("materializer must expose materialize_target")
    try:
        artifact = _canonical_build_artifact(
            getattr(materializer, "strategy_artifact")
        )
    except (AttributeError, ValueError) as error:
        raise TypeError(
            "materializer must expose exact immutable strategy_artifact"
        ) from error
    if _wire(artifact) != _wire(recipe.strategy_artifact):
        raise ResearchCoreError("TARGET_RECIPE_INVALID")


def _require_target_backtest(backtest: object) -> None:
    _require_backtest(backtest)
    if not all(
        callable(getattr(backtest, name, None))
        for name in ("publish_target", "load_target", "prepare_trials")
    ):
        raise TypeError(
            "backtest must expose publish_target, load_target, and prepare_trials"
        )


def _require_model_builder(builder: object) -> None:
    if not all(
        callable(getattr(builder, name, None))
        for name in ("build_features", "train_model")
    ):
        raise TypeError("builder must expose build_features and train_model")


def _require_model_backtest(backtest: object) -> None:
    _require_backtest(backtest)
    if not callable(getattr(backtest, "prepare_trials", None)):
        raise TypeError("backtest must expose prepare_trials for model experiments")


def _publish_base(
    inputs: FrozenExperimentInputs | FrozenModelExperimentInputs | FrozenTargetExperimentInputs,
    foundation: LocalFoundation,
    *,
    initial_refs: Mapping[str, ArtifactRef] | None = None,
    publish_trial_specs: bool = True,
) -> _PublishedBase:
    spec = inputs.experiment_spec
    trials = build_trial_declarations(spec)
    universe = build_task_universe(spec)
    trial_tasks = {
        task.task_artifact_ref: task for task in universe if task.kind == "TRIAL"
    }
    refs: dict[str, ArtifactRef] = dict(initial_refs or {})

    experiment_ref, _ = _publish(
        foundation,
        _RESEARCH_ARTIFACTS_LOG,
        "experiment_spec",
        _translate(spec.payload, refs),
        schema_version=spec.schema_version,
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

    task_types = {
        "ANALYSIS": (AnalysisTask, "analysis_task"),
        "FEATURE_BUILD": (FeatureBuildTask, "feature_build_task"),
        "MODEL_TRAINING": (ModelTrainingTask, "model_training_task"),
        "TARGET_BUILD": (TargetBuildTask, "target_build_task"),
    }
    for task in universe:
        if task.kind not in task_types:
            continue
        expected_type, artifact_type = task_types[task.kind]
        artifact = task.artifact
        if type(artifact) is not expected_type:
            raise ResearchCoreError("TASK_OUTCOME_INVALID")
        ref, _ = _publish(
            foundation,
            _RESEARCH_ARTIFACTS_LOG,
            artifact_type,
            _translate(artifact.payload, refs),
        )
        refs[artifact.ref] = ref

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

    base = _PublishedBase(
        inputs,
        trials,
        universe,
        trial_tasks,
        refs,
        selection,
        selection_ref,
        selection_receipt.ledger_sequence,
        {},
    )
    if publish_trial_specs:
        if type(inputs) is not FrozenExperimentInputs:
            raise TypeError("trial specs require FrozenExperimentInputs")
        _publish_trial_specs(base, foundation, inputs.trial_executions)
    return base


def _publish_trial_specs(
    base: _PublishedBase,
    foundation: LocalFoundation,
    executions: tuple[TrialExecution, ...],
) -> None:
    by_trial = {item.trial_declaration_ref: item for item in executions}
    expected = {trial.ref for trial in base.trials}
    if not set(by_trial).issubset(expected) or (
        type(base.inputs) is not FrozenTargetExperimentInputs
        and set(by_trial) != expected
    ):
        raise ResearchCoreError("TASK_REF_FOREIGN")
    for trial in base.trials:
        execution = by_trial.get(trial.ref)
        if execution is None:
            continue
        ref, _ = _publish(
            foundation,
            _RESEARCH_ARTIFACTS_LOG,
            "backtest_trial_spec",
            {
                "trial_declaration_ref": _ref_payload(base.refs[trial.ref]),
                "resolved_model_refs": list(execution.resolved_model_refs),
                "backtest_request_ref": execution.backtest_request_ref,
            },
        )
        base.trial_specs[trial.ref] = ref


def _recover_model_publications(
    base: _PublishedBase,
    foundation: LocalFoundation,
) -> tuple[FeatureDatasetManifest | None, ModelBuildEvidence | None]:
    actual_to_core = {
        _wire(_ref_payload(ref)): local_ref for local_ref, ref in base.refs.items()
    }
    feature_manifest: FeatureDatasetManifest | None = None
    model_evidence: ModelBuildEvidence | None = None
    plan_ref = base.inputs.experiment_spec.model_build_plan
    if type(plan_ref) is not str:
        raise ResearchCoreError("MODEL_BUILD_PLAN_INVALID")
    plan_wire = _wire(_ref_payload(base.refs[plan_ref]))
    trial_ref_by_wire = {
        _wire(_ref_payload(base.refs[trial.ref])): trial.ref for trial in base.trials
    }
    for _, ref, payload in _published_entries(foundation, _RESEARCH_ARTIFACTS_LOG):
        if ref.artifact_type == "backtest_trial_spec":
            trial_ref = trial_ref_by_wire.get(_wire(payload.get("trial_declaration_ref")))
            if trial_ref is not None:
                existing = base.trial_specs.get(trial_ref)
                if existing is not None and existing != ref:
                    raise ResearchCoreError("MODEL_BINDING_INVALID")
                base.trial_specs[trial_ref] = ref
            continue
        if ref.artifact_type not in {
            "feature_dataset_manifest",
            "model_build_evidence",
        }:
            continue
        if _wire(payload.get("model_build_plan_ref")) != plan_wire:
            continue
        converted = _reverse(payload, actual_to_core)
        if type(converted) is not dict:
            raise ResearchCoreError("MODEL_BINDING_INVALID")
        if ref.artifact_type == "feature_dataset_manifest":
            value = FeatureDatasetManifest(
                converted["model_build_plan_ref"],
                converted["dataset_revision"],
                converted["interval_start"],
                converted["interval_end"],
                converted["feature_schema_hash"],
                converted["training_data_hash"],
                converted["row_count"],
            )
            if value.model_build_plan_ref != base.inputs.experiment_spec.model_build_plan:
                continue
            if feature_manifest is not None and feature_manifest != value:
                raise ResearchCoreError("MODEL_BINDING_INVALID")
            feature_manifest = value
        else:
            value = ModelBuildEvidence(
                converted["model_build_plan_ref"],
                converted["feature_dataset_manifest_ref"],
                converted["model_artifact"],
            )
            if value.model_build_plan_ref != base.inputs.experiment_spec.model_build_plan:
                continue
            if model_evidence is not None and model_evidence != value:
                raise ResearchCoreError("MODEL_BINDING_INVALID")
            model_evidence = value
            base.model_build_evidence_ref = ref
        actual_to_core[_wire(_ref_payload(ref))] = value.ref
        base.refs[value.ref] = ref
    return feature_manifest, model_evidence


def _publish_target_base(
    inputs: FrozenTargetExperimentInputs,
    foundation: LocalFoundation,
) -> _PublishedBase:
    recipe_ref, _ = _publish(
        foundation,
        _RESEARCH_ARTIFACTS_LOG,
        "target_recipe",
        inputs.target_recipe.payload,
    )
    return _publish_base(
        inputs,
        foundation,
        initial_refs={inputs.target_recipe.ref: recipe_ref},
        publish_trial_specs=False,
    )


def _target_reservation_bytes(
    base: _PublishedBase, trial: TrialDeclaration
) -> bytes:
    producer_ref = base.refs[trial.ref]
    record = SampleConsumptionRecord(
        trial.data_slice.dataset_revision,
        trial.data_slice.interval_start,
        trial.data_slice.interval_end,
        "discovery",
        canonical_sha256(("sample-consumer-v1", producer_ref)),
        base.inputs.reservation_at,
    )
    return canonical_bytes(
        ArtifactEnvelope.create(
            "sample_consumption_append",
            1,
            {
                "record": {
                    "dataset_revision": record.dataset_revision,
                    "interval_start": record.interval_start,
                    "interval_end": record.interval_end,
                    "purpose": record.purpose,
                    "consumer_id": record.consumer_id,
                    "consumed_at": record.consumed_at,
                },
                "producer_ref": producer_ref,
            },
        )
    )


def _require_target_reservation(
    base: _PublishedBase,
    foundation: LocalFoundation,
    trial: TrialDeclaration,
    evidence_ledger_sequence: int,
) -> None:
    matches = tuple(
        entry
        for entry in foundation.entries(_SAMPLE_CONSUMPTION_LOG)
        if entry.payload == _target_reservation_bytes(base, trial)
    )
    expected_event_id = canonical_sha256(
        (
            "sample-consumption-append-v1",
            base.refs[trial.ref],
            trial.data_slice.dataset_revision,
            trial.data_slice.interval_start,
            trial.data_slice.interval_end,
            "discovery",
        )
    )
    if (
        len(matches) != 1
        or matches[0].event_id != expected_event_id
        or base.inputs.reservation_at > matches[0].accepted_at
        or matches[0].ledger_sequence >= evidence_ledger_sequence
    ):
        raise ResearchCoreError("TARGET_MATERIALIZATION_INVALID")


def _verified_target(
    backtest: object,
    target_ref: object,
    producer_context_ref: ArtifactRef,
    *,
    expected_digest: str,
    expected_event_count: int,
    expected_stream: object | None = None,
) -> Mapping[str, object]:
    try:
        loaded = _plain(backtest.load_target(_plain(target_ref)))
        if type(loaded) is not dict or set(loaded) != {
            "ref",
            "producer_context_ref",
            "target_stream",
            "digest",
        }:
            raise ValueError("load_target returned the wrong record")
        stream = _canonical_target_stream(loaded["target_stream"])
        digest = canonical_sha256(stream)
        if (
            _wire(loaded["ref"]) != _wire(target_ref)
            or _wire(loaded["producer_context_ref"])
            != _wire(_ref_payload(producer_context_ref))
            or loaded["digest"] != digest
            or loaded["digest"] != expected_digest
            or len(stream["events"]) != expected_event_count
            or (
                expected_stream is not None
                and _wire(stream) != _wire(expected_stream)
            )
        ):
            raise ValueError("loaded target does not exactly bind its evidence")
        return loaded
    except Exception as error:  # noqa: BLE001 - target repository boundary
        raise _RuntimeFailure("TARGET_STORE_INVALID") from error


def _recover_target_publications(
    base: _PublishedBase,
    foundation: LocalFoundation,
    backtest: object,
) -> dict[str, TargetMaterializationEvidence]:
    actual_to_core = {
        _wire(_ref_payload(ref)): local_ref for local_ref, ref in base.refs.items()
    }
    task_by_ref = {
        task.task_artifact_ref: task
        for task in base.universe
        if task.kind == "TARGET_BUILD"
    }
    trial_by_ref = {trial.ref: trial for trial in base.trials}
    recovered: dict[str, TargetMaterializationEvidence] = {}
    trial_ref_by_wire = {
        _wire(_ref_payload(base.refs[trial.ref])): trial.ref for trial in base.trials
    }
    for entry, ref, payload in _published_entries(foundation, _RESEARCH_ARTIFACTS_LOG):
        if ref.artifact_type == "backtest_trial_spec":
            trial_ref = trial_ref_by_wire.get(
                _wire(payload.get("trial_declaration_ref"))
            )
            if trial_ref is not None:
                existing = base.trial_specs.get(trial_ref)
                if existing is not None and existing != ref:
                    raise ResearchCoreError("TARGET_MATERIALIZATION_INVALID")
                base.trial_specs[trial_ref] = ref
            continue
        if ref.artifact_type != "target_materialization_evidence":
            continue
        converted = _reverse(payload, actual_to_core)
        if type(converted) is not dict:
            raise ResearchCoreError("TARGET_MATERIALIZATION_INVALID")
        try:
            evidence = TargetMaterializationEvidence(
                converted["target_build_task_ref"],
                converted["trial_declaration_ref"],
                converted["target_recipe_ref"],
                converted["materialization_request_hash"],
                converted["input_data_hash"],
                converted["target_stream_ref"],
                converted["target_stream_digest"],
                converted["event_count"],
            )
        except (KeyError, TypeError, ValueError, ResearchCoreError) as error:
            raise ResearchCoreError("TARGET_MATERIALIZATION_INVALID") from error
        task = task_by_ref.get(evidence.target_build_task_ref)
        trial = trial_by_ref.get(evidence.trial_declaration_ref)
        if task is None or trial is None or type(task.artifact) is not TargetBuildTask:
            continue
        if (
            task.artifact.trial_declaration_ref != trial.ref
            or task.artifact.target_recipe_ref
            != base.inputs.experiment_spec.target_recipe_ref
            or evidence.target_recipe_ref
            != base.inputs.experiment_spec.target_recipe_ref
            or evidence.materialization_request_hash
            != canonical_sha256(_target_request(base, trial))
        ):
            raise ResearchCoreError("TARGET_MATERIALIZATION_INVALID")
        _require_target_reservation(
            base, foundation, trial, entry.ledger_sequence
        )
        try:
            _verified_target(
                backtest,
                evidence.target_stream_ref,
                base.refs[trial.ref],
                expected_digest=evidence.target_stream_digest,
                expected_event_count=evidence.event_count,
            )
        except _RuntimeFailure as error:
            raise ResearchCoreError("TARGET_MATERIALIZATION_INVALID") from error
        existing = recovered.get(trial.ref)
        if existing is not None and existing != evidence:
            raise ResearchCoreError("TARGET_MATERIALIZATION_INVALID")
        recovered[trial.ref] = evidence
        base.refs[evidence.ref] = ref
        base.target_evidence_refs[trial.ref] = ref
        actual_to_core[_wire(_ref_payload(ref))] = evidence.ref
    return recovered


def _reserve_slice(
    ledger: SampleConsumptionLedger,
    producer_ref: ArtifactRef,
    data_slice: DataSlice,
    purpose: str,
    reservation_at: str,
) -> None:
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


def _reserve(
    ledger: SampleConsumptionLedger,
    producer_ref: ArtifactRef,
    trial: TrialDeclaration,
    purpose: str,
    reservation_at: str,
) -> None:
    _reserve_slice(
        ledger, producer_ref, trial.data_slice, purpose, reservation_at
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
    if _is_terminal_ref(run_ref):
        return backtest.load_terminal(run_ref)
    return _load_completed(backtest, run_ref)


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
        record = _load_completed(backtest, outcome.witness.publication_ref)
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
        record = _load_analysis(backtest, outcome.witness.analysis_ref)  # type: ignore[union-attr]
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
    payload = {
        "candidate_family_ref": _ref_payload(family_ref),
        "selection_declaration_ref": _ref_payload(base.selection_ref),
        "selected_trial_declaration_ref": _ref_payload(base.refs[trial_ref]),
        "selected_trial_spec_ref": _ref_payload(base.trial_specs[trial_ref]),
        "selected_publication_ref": _plain(selected.trial_publication_ref),
        "selected_analysis_ref": _plain(selected.analysis_ref),
        "selection_rank": selected.selection_rank,
        "validated": False,
    }
    if base.model_build_evidence_ref is not None:
        payload["model_build_evidence_ref"] = _ref_payload(
            base.model_build_evidence_ref
        )
    target_evidence_ref = base.target_evidence_refs.get(trial_ref)
    if base.inputs.experiment_spec.target_recipe_ref is not None:
        if target_evidence_ref is None:
            raise ResearchCoreError("SELECTION_INPUT_INCOMPLETE")
        payload["selected_target_materialization_evidence_ref"] = _ref_payload(
            target_evidence_ref
        )
    return payload


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
    candidate_version = (
        3
        if base.inputs.experiment_spec.target_recipe_ref is not None
        else 2
        if base.model_build_evidence_ref is not None
        else 1
    )
    expected = ArtifactRef.from_envelope(
        ArtifactEnvelope.create("strategy_candidate", candidate_version, payload)
    )
    if len(candidates) > 1 or (candidates and candidates[0][1] != payload):
        raise ResearchCoreError("SELECTION_INPUT_INCOMPLETE")
    if candidates:
        candidate_ref = candidates[0][0]
        if candidate_ref != expected:
            raise ResearchCoreError("SELECTION_INPUT_INCOMPLETE")
    else:
        candidate_ref, _ = _publish(
            foundation,
            _RESEARCH_ARTIFACTS_LOG,
            "strategy_candidate",
            payload,
            schema_version=candidate_version,
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
            "target_build_task",
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
    record = _load_completed(backtest, outcome.witness.publication_ref)
    mapped = map_backtest_observation(task, record)
    if _wire(mapped.payload) != _wire(outcome.payload):
        raise _RuntimeFailure("ANALYSIS_LINK_INVALID")
    return record


def _target_request(
    base: _PublishedBase,
    trial: TrialDeclaration,
) -> dict[str, object]:
    target_recipe_ref = base.inputs.experiment_spec.target_recipe_ref
    if type(target_recipe_ref) is not str:
        raise ResearchCoreError("TARGET_RECIPE_INVALID")
    return {
        "type": "target_materialization_request",
        "schema_version": 1,
        "consumer_ref": _ref_payload(base.refs[trial.ref]),
        "target_recipe_ref": _ref_payload(base.refs[target_recipe_ref]),
        "market_bundle_ref": _plain(trial.data_slice.market_bundle_ref),
        "dataset_revision": _plain(trial.data_slice.dataset_revision),
        "interval_start": _plain(trial.data_slice.interval_start),
        "interval_end": _plain(trial.data_slice.interval_end),
        "parameter_values": _plain(trial.parameter_values.payload),
        "seed": trial.seed,
    }


def _materialize_target(
    base: _PublishedBase,
    materializer: object,
    backtest: object,
    task: TaskRef,
    trial: TrialDeclaration,
) -> TargetMaterializationEvidence:
    if type(base.inputs) is not FrozenTargetExperimentInputs:
        raise TypeError("target materialization requires FrozenTargetExperimentInputs")
    request = _target_request(base, trial)
    request_hash = canonical_sha256(request)
    materializer_request = _plain(request)
    try:
        result_value = materializer.materialize_target(materializer_request)
    except Exception as error:  # noqa: BLE001 - materializer boundary
        raise _RuntimeFailure("TARGET_MATERIALIZATION_INVALID") from error
    if _wire(materializer_request) != _wire(request):
        raise _RuntimeFailure("TARGET_MATERIALIZATION_INVALID")
    result = _plain(result_value)
    if type(result) is not dict or set(result) != {
        "type",
        "schema_version",
        "request_hash",
        "strategy_artifact",
        "input_data_hash",
        "target_stream",
    }:
        raise _RuntimeFailure("TARGET_MATERIALIZATION_INVALID")
    try:
        strategy_artifact = _canonical_build_artifact(result["strategy_artifact"])
        target_stream = _canonical_target_stream(result["target_stream"])
    except ValueError as error:
        raise _RuntimeFailure("TARGET_MATERIALIZATION_INVALID") from error
    if (
        result["type"] != "target_materialization_result"
        or result["schema_version"] != 1
        or result["request_hash"] != request_hash
        or _wire(strategy_artifact) != _wire(base.inputs.target_recipe.strategy_artifact)
        or _wire(strategy_artifact)
        != _wire(_canonical_build_artifact(materializer.strategy_artifact))
    ):
        raise _RuntimeFailure("TARGET_MATERIALIZATION_INVALID")
    input_data_hash = result["input_data_hash"]
    try:
        _content_hash(input_data_hash, "input_data_hash")
    except ValueError as error:
        raise _RuntimeFailure("TARGET_MATERIALIZATION_INVALID") from error
    try:
        target_ref = _plain(
            backtest.publish_target(
                _ref_payload(base.refs[trial.ref]),
                _plain(target_stream),
            )
        )
    except Exception as error:  # noqa: BLE001 - target repository boundary
        raise _RuntimeFailure("TARGET_STORE_INVALID") from error
    expected_digest = canonical_sha256(target_stream)
    _verified_target(
        backtest,
        target_ref,
        base.refs[trial.ref],
        expected_digest=expected_digest,
        expected_event_count=len(target_stream["events"]),
        expected_stream=target_stream,
    )
    return TargetMaterializationEvidence(
        task.task_artifact_ref,
        trial.ref,
        base.inputs.target_recipe.ref,
        request_hash,
        input_data_hash,
        target_ref,
        expected_digest,
        len(target_stream["events"]),
    )


def _execute_target_builds(
    base: _PublishedBase,
    foundation: LocalFoundation,
    ledger: SampleConsumptionLedger,
    materializer: object,
    backtest: object,
    evidences: dict[str, TargetMaterializationEvidence],
) -> dict[str, TargetMaterializationEvidence]:
    recovery = _recover_execution(base, foundation)
    _close_persisted_outcomes(base, foundation, recovery)
    target_tasks = {
        task.artifact.trial_declaration_ref: task
        for task in base.universe
        if task.kind == "TARGET_BUILD" and type(task.artifact) is TargetBuildTask
    }
    for trial in base.trials:
        task = target_tasks[trial.ref]
        evidence = evidences.get(trial.ref)
        if task in recovery.outcomes:
            outcome = recovery.outcomes[task]
            if (
                outcome.state == "COMPLETED"
                and (
                    evidence is None
                    or type(outcome.witness) is not TargetBuildPublication
                    or outcome.witness.target_materialization_evidence_ref != evidence.ref
                )
            ) or (outcome.state != "COMPLETED" and evidence is not None):
                raise ResearchCoreError("TARGET_MATERIALIZATION_INVALID")
            continue
        started = _resume_attempt(base, foundation, recovery, task)
        if evidence is not None:
            outcome = TaskOutcome(
                task,
                "COMPLETED",
                TargetBuildPublication(evidence.ref),
            )
            _append_recovered_terminal(
                base, foundation, recovery, started, outcome
            )
            continue
        try:
            _reserve(
                ledger,
                base.refs[trial.ref],
                trial,
                "discovery",
                base.inputs.reservation_at,
            )
        except Exception:  # noqa: BLE001 - reservation boundary blocks materialization
            outcome = TaskOutcome(
                task,
                "BLOCKED",
                DependencyBlock("SAMPLE_RESERVATION_FAILED"),
            )
            _append_recovered_terminal(
                base, foundation, recovery, started, outcome
            )
            continue
        while True:
            try:
                evidence = _materialize_target(
                    base, materializer, backtest, task, trial
                )
                try:
                    ref, _ = _publish(
                        foundation,
                        _RESEARCH_ARTIFACTS_LOG,
                        "target_materialization_evidence",
                        _translate(evidence.payload, base.refs),
                    )
                except Exception as error:
                    raise _RuntimeFailure(
                        "TARGET_EVIDENCE_PUBLICATION_FAILED"
                    ) from error
                base.refs[evidence.ref] = ref
                base.target_evidence_refs[trial.ref] = ref
                evidences[trial.ref] = evidence
                outcome = TaskOutcome(
                    task,
                    "COMPLETED",
                    TargetBuildPublication(evidence.ref),
                )
            except Exception as error:  # noqa: BLE001 - materializer/storage boundary
                committed = _recover_target_publications(
                    base, foundation, backtest
                ).get(trial.ref)
                if committed is not None:
                    evidence = committed
                    evidences[trial.ref] = committed
                    outcome = TaskOutcome(
                        task,
                        "COMPLETED",
                        TargetBuildPublication(committed.ref),
                    )
                elif started.ordinal < base.inputs.max_attempts:
                    _append_retryable_close(
                        base,
                        foundation,
                        recovery,
                        started,
                        _failure_code(error),
                    )
                    started = _resume_attempt(base, foundation, recovery, task)
                    continue
                else:
                    outcome = TaskOutcome(
                        task,
                        "FAILED",
                        LocalFailure(_failure_code(error)),
                    )
            _append_recovered_terminal(
                base, foundation, recovery, started, outcome
            )
            break
    return evidences


def _prepare_target_trials(
    base: _PublishedBase,
    foundation: LocalFoundation,
    backtest: object,
    evidences: Mapping[str, TargetMaterializationEvidence],
) -> None:
    executions: list[TrialExecution] = []
    recovery = _recover_execution(base, foundation)
    target_outcomes = {
        task.artifact.trial_declaration_ref: recovery.outcomes[task]
        for task in base.universe
        if task.kind == "TARGET_BUILD"
        and type(task.artifact) is TargetBuildTask
        and task in recovery.outcomes
    }
    for trial in base.trials:
        outcome = target_outcomes[trial.ref]
        if outcome.state != "COMPLETED":
            continue
        evidence = evidences.get(trial.ref)
        if evidence is None:
            raise ResearchCoreError("TARGET_MATERIALIZATION_INVALID")
        try:
            prepared = backtest.prepare_trials(
                (trial,), _plain(evidence.target_stream_ref)
            )
            if (
                type(prepared) is not tuple
                or len(prepared) != 1
                or type(prepared[0]) is not TrialExecution
                or prepared[0].trial_declaration_ref != trial.ref
                or prepared[0].resolved_model_refs
            ):
                raise TypeError(
                    "prepare_trials must return one exact target TrialExecution"
                )
            executions.append(prepared[0])
            base.target_trial_executions[trial.ref] = prepared[0]
        except Exception:  # noqa: BLE001 - preparation boundary
            base.trial_preparation_failures[trial.ref] = (
                "TARGET_PREPARATION_FAILED"
            )
    request_wires: dict[str, list[str]] = {}
    for execution in executions:
        request_wires.setdefault(
            _wire(execution.backtest_request_ref), []
        ).append(execution.trial_declaration_ref)
    collisions = {
        trial_ref
        for trial_refs in request_wires.values()
        if len(trial_refs) > 1
        for trial_ref in trial_refs
    }
    if collisions:
        executions = [
            execution
            for execution in executions
            if execution.trial_declaration_ref not in collisions
        ]
        for trial_ref in collisions:
            base.target_trial_executions.pop(trial_ref, None)
            base.trial_preparation_failures[trial_ref] = "TRIAL_REQUEST_COLLISION"
    if executions:
        _publish_trial_specs(base, foundation, tuple(executions))


def _execute_new(
    base: _PublishedBase,
    foundation: LocalFoundation,
    ledger: SampleConsumptionLedger,
    backtest: object,
) -> PublishedStrategyCandidate | PublishedNoSelection:
    executions = {
        item.trial_declaration_ref: item
        for item in getattr(base.inputs, "trial_executions", ())
    }
    executions.update(base.target_trial_executions)
    recovery = _recover_execution(base, foundation)
    _close_persisted_outcomes(base, foundation, recovery)
    completed_records: dict[str, Mapping[str, object]] = {}
    training_outcome = next(
        (
            recovery.outcomes[task]
            for task in base.universe
            if task.kind == "MODEL_TRAINING" and task in recovery.outcomes
        ),
        None,
    )
    target_outcomes = {
        task.artifact.trial_declaration_ref: recovery.outcomes[task]
        for task in base.universe
        if task.kind == "TARGET_BUILD"
        and type(task.artifact) is TargetBuildTask
        and task in recovery.outcomes
    }

    for trial in base.trials:
        task = base.trial_tasks[trial.ref]
        if task in recovery.outcomes:
            continue
        upstream = training_outcome or target_outcomes.get(trial.ref)
        if upstream is not None and upstream.state != "COMPLETED":
            started = _resume_attempt(base, foundation, recovery, task)
            _append_recovered_terminal(
                base,
                foundation,
                recovery,
                started,
                TaskOutcome(
                    task,
                    "BLOCKED",
                    UpstreamTaskOutcome(upstream.ref),
                ),
            )
            continue
        preparation_failure = base.trial_preparation_failures.get(trial.ref)
        if preparation_failure is not None:
            started = _resume_attempt(base, foundation, recovery, task)
            _append_recovered_terminal(
                base,
                foundation,
                recovery,
                started,
                TaskOutcome(task, "FAILED", LocalFailure(preparation_failure)),
            )
            continue
        execution = executions.get(trial.ref)
        if execution is None:
            raise ResearchCoreError(
                "TARGET_MATERIALIZATION_INVALID"
                if target_outcomes
                else "MODEL_BINDING_INVALID"
            )
        while True:
            started = _resume_attempt(base, foundation, recovery, task)
            try:
                if trial.ref not in target_outcomes:
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
                analysis = _load_analysis(backtest, analysis_ref)
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


def _publish_model_base(
    inputs: FrozenModelExperimentInputs,
    foundation: LocalFoundation,
) -> tuple[_PublishedBase, FeatureDatasetManifest | None, ModelBuildEvidence | None]:
    refs: dict[str, ArtifactRef] = {}
    feature_ref, _ = _publish(
        foundation,
        _RESEARCH_ARTIFACTS_LOG,
        "feature_recipe",
        inputs.feature_recipe.payload,
    )
    refs[inputs.feature_recipe.ref] = feature_ref
    trainer_ref, _ = _publish(
        foundation,
        _RESEARCH_ARTIFACTS_LOG,
        "trainer_recipe",
        inputs.trainer_recipe.payload,
    )
    refs[inputs.trainer_recipe.ref] = trainer_ref
    plan_ref, _ = _publish(
        foundation,
        _RESEARCH_ARTIFACTS_LOG,
        "model_build_plan",
        _translate(inputs.model_build_plan.payload, refs),
    )
    refs[inputs.model_build_plan.ref] = plan_ref
    base = _publish_base(
        inputs,
        foundation,
        initial_refs=refs,
        publish_trial_specs=False,
    )
    feature_manifest, model_evidence = _recover_model_publications(base, foundation)
    return base, feature_manifest, model_evidence


def _execute_model_build(
    base: _PublishedBase,
    foundation: LocalFoundation,
    ledger: SampleConsumptionLedger,
    builder: object,
    feature_manifest: FeatureDatasetManifest | None,
    model_evidence: ModelBuildEvidence | None,
) -> ModelBuildEvidence | None:
    if type(base.inputs) is not FrozenModelExperimentInputs:
        raise TypeError("model build requires FrozenModelExperimentInputs")
    inputs = base.inputs
    recovery = _recover_execution(base, foundation)
    _close_persisted_outcomes(base, foundation, recovery)
    feature_task = next(task for task in base.universe if task.kind == "FEATURE_BUILD")
    training_task = next(task for task in base.universe if task.kind == "MODEL_TRAINING")

    if feature_task not in recovery.outcomes:
        while True:
            started = _resume_attempt(base, foundation, recovery, feature_task)
            try:
                _reserve_slice(
                    ledger,
                    base.refs[feature_task.task_artifact_ref],
                    inputs.model_build_plan.training_slice,
                    "feature_build",
                    inputs.reservation_at,
                )
            except Exception:  # noqa: BLE001 - reservation failure blocks the read
                outcome = TaskOutcome(
                    feature_task,
                    "BLOCKED",
                    DependencyBlock("SAMPLE_RESERVATION_FAILED"),
                )
            else:
                try:
                    candidate = builder.build_features(inputs.model_build_plan)
                    if type(candidate) is not FeatureDatasetManifest or not (
                        candidate.model_build_plan_ref == inputs.model_build_plan.ref
                        and candidate.dataset_revision
                        == inputs.model_build_plan.training_slice.dataset_revision
                        and candidate.interval_start
                        == inputs.model_build_plan.training_slice.interval_start
                        and candidate.interval_end
                        == inputs.model_build_plan.training_slice.interval_end
                        and candidate.feature_schema_hash
                        == inputs.feature_recipe.feature_schema_hash
                    ):
                        raise ResearchCoreError("MODEL_BINDING_INVALID")
                    feature_manifest = candidate
                    ref, _ = _publish(
                        foundation,
                        _RESEARCH_ARTIFACTS_LOG,
                        "feature_dataset_manifest",
                        _translate(candidate.payload, base.refs),
                    )
                    base.refs[candidate.ref] = ref
                    outcome = TaskOutcome(
                        feature_task,
                        "COMPLETED",
                        FeatureDatasetPublication(candidate.ref),
                    )
                except Exception as error:  # noqa: BLE001 - builder boundary
                    if started.ordinal < inputs.max_attempts:
                        _append_retryable_close(
                            base,
                            foundation,
                            recovery,
                            started,
                            _failure_code(error),
                        )
                        continue
                    outcome = TaskOutcome(
                        feature_task, "FAILED", LocalFailure(_failure_code(error))
                    )
            _append_recovered_terminal(
                base, foundation, recovery, started, outcome
            )
            break

    feature_outcome = recovery.outcomes[feature_task]
    if training_task not in recovery.outcomes:
        started = _resume_attempt(base, foundation, recovery, training_task)
        if feature_outcome.state != "COMPLETED":
            outcome = TaskOutcome(
                training_task,
                "BLOCKED",
                UpstreamTaskOutcome(feature_outcome.ref),
            )
            _append_recovered_terminal(
                base, foundation, recovery, started, outcome
            )
        else:
            if feature_manifest is None:
                raise ResearchCoreError("MODEL_BINDING_INVALID")
            try:
                _reserve_slice(
                    ledger,
                    base.refs[training_task.task_artifact_ref],
                    inputs.model_build_plan.training_slice,
                    "model_training",
                    inputs.reservation_at,
                )
            except Exception:  # noqa: BLE001 - reservation failure blocks the read
                outcome = TaskOutcome(
                    training_task,
                    "BLOCKED",
                    DependencyBlock("SAMPLE_RESERVATION_FAILED"),
                )
                _append_recovered_terminal(
                    base, foundation, recovery, started, outcome
                )
            else:
                while True:
                    try:
                        artifact = builder.train_model(
                            inputs.model_build_plan, feature_manifest
                        )
                        model_evidence = validate_model_build(
                            inputs.model_build_plan,
                            inputs.feature_recipe,
                            inputs.trainer_recipe,
                            feature_manifest,
                            artifact,
                        )
                        ref, _ = _publish(
                            foundation,
                            _RESEARCH_ARTIFACTS_LOG,
                            "model_build_evidence",
                            _translate(model_evidence.payload, base.refs),
                        )
                        base.refs[model_evidence.ref] = ref
                        base.model_build_evidence_ref = ref
                        outcome = TaskOutcome(
                            training_task,
                            "COMPLETED",
                            ModelBuildPublication(model_evidence.ref),
                        )
                    except Exception as error:  # noqa: BLE001 - builder boundary
                        if started.ordinal < inputs.max_attempts:
                            _append_retryable_close(
                                base,
                                foundation,
                                recovery,
                                started,
                                _failure_code(error),
                            )
                            started = _resume_attempt(
                                base, foundation, recovery, training_task
                            )
                            continue
                        outcome = TaskOutcome(
                            training_task,
                            "FAILED",
                            LocalFailure(_failure_code(error)),
                        )
                    _append_recovered_terminal(
                        base, foundation, recovery, started, outcome
                    )
                    break

    training_outcome = recovery.outcomes[training_task]
    if training_outcome.state != "COMPLETED":
        return None
    if model_evidence is None:
        raise ResearchCoreError("MODEL_BINDING_INVALID")
    return model_evidence


def execute_target_experiment(
    frozen_inputs: FrozenTargetExperimentInputs,
    foundation: LocalFoundation,
    sample_ledger: SampleConsumptionLedger,
    materializer: object,
    backtest: object,
) -> PublishedStrategyCandidate | PublishedNoSelection:
    """Publish one reserved target materialization per Trial and execute it."""

    inputs = _normal_target_inputs(frozen_inputs)
    if type(foundation) is not LocalFoundation:
        raise TypeError("foundation must be a LocalFoundation")
    if type(sample_ledger) is not SampleConsumptionLedger:
        raise TypeError("sample_ledger must be a SampleConsumptionLedger")
    _require_target_materializer(materializer, inputs.target_recipe)
    _require_target_backtest(backtest)
    base = _publish_target_base(inputs, foundation)
    evidences = _recover_target_publications(base, foundation, backtest)
    existing = _replay_existing(base, foundation, sample_ledger, backtest)
    if existing is not None:
        return existing
    evidences = _execute_target_builds(
        base,
        foundation,
        sample_ledger,
        materializer,
        backtest,
        evidences,
    )
    _prepare_target_trials(base, foundation, backtest, evidences)
    return _execute_new(base, foundation, sample_ledger, backtest)


def execute_model_experiment(
    frozen_inputs: FrozenModelExperimentInputs,
    foundation: LocalFoundation,
    sample_ledger: SampleConsumptionLedger,
    builder: object,
    backtest: object,
) -> PublishedStrategyCandidate | PublishedNoSelection:
    """Publish one immutable model build and its exact model-bound Experiment."""

    inputs = _normal_model_inputs(frozen_inputs)
    if type(foundation) is not LocalFoundation:
        raise TypeError("foundation must be a LocalFoundation")
    if type(sample_ledger) is not SampleConsumptionLedger:
        raise TypeError("sample_ledger must be a SampleConsumptionLedger")
    _require_model_builder(builder)
    _require_model_backtest(backtest)
    base, feature_manifest, model_evidence = _publish_model_base(inputs, foundation)
    existing = _replay_existing(base, foundation, sample_ledger, backtest)
    if existing is not None:
        return existing
    model_evidence = _execute_model_build(
        base,
        foundation,
        sample_ledger,
        builder,
        feature_manifest,
        model_evidence,
    )
    if model_evidence is not None:
        executions = backtest.prepare_trials(base.trials, model_evidence)
        if type(executions) is not tuple or any(
            type(item) is not TrialExecution for item in executions
        ):
            raise TypeError("prepare_trials must return tuple[TrialExecution, ...]")
        expected_model = _wire(model_evidence.model_artifact)
        if any(
            len(item.resolved_model_refs) != 1
            or _wire(item.resolved_model_refs[0]) != expected_model
            for item in executions
        ):
            raise ResearchCoreError("MODEL_BINDING_INVALID")
        normal = FrozenExperimentInputs(
            inputs.experiment_spec,
            inputs.selection_policy,
            inputs.selection_declared_by_ref,
            executions,
            inputs.reservation_at,
            inputs.max_attempts,
        )
        base.inputs = normal
        _publish_trial_specs(base, foundation, executions)
    return _execute_new(base, foundation, sample_ledger, backtest)


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
    "FrozenModelExperimentInputs",
    "FrozenTargetExperimentInputs",
    "PublishedNoSelection",
    "PublishedStrategyCandidate",
    "TrialExecution",
    "execute_experiment",
    "execute_model_experiment",
    "execute_target_experiment",
]
