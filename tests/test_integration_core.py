from __future__ import annotations

import ast
from copy import deepcopy
from dataclasses import fields
import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

from crypto_quant_research.integration import (
    AnalysisTask,
    CandidateFamily,
    DataSlice,
    ExecutionEntry,
    ExperimentSpec,
    HardFilter,
    LocalFailure,
    NoSelection,
    OrderingCriterion,
    ParameterCombination,
    ResearchCoreError,
    Selected,
    SelectionDeclaration,
    SelectionPolicy,
    TaskAttemptClosed,
    TaskAttemptStarted,
    TaskOutcome,
    TaskRef,
    TrialDeclaration,
    VerifiedAnalysis,
    block_analysis_from_upstream,
    build_candidate_family,
    build_execution_manifest,
    build_task_universe,
    map_backtest_observation,
    select_candidate,
    validate_execution_prefix,
)

_PORT_HELPER_PATH = (
    Path(__file__).resolve().parents[2] / "tests/support/backtest_consumer_port.py"
)
_PORT_SPEC = importlib.util.spec_from_file_location(
    "backtest_consumer_port", _PORT_HELPER_PATH
)
assert _PORT_SPEC is not None and _PORT_SPEC.loader is not None
_PORT_MODULE = importlib.util.module_from_spec(_PORT_SPEC)
_PORT_SPEC.loader.exec_module(_PORT_MODULE)
InMemoryBacktestConsumerPort = _PORT_MODULE.InMemoryBacktestConsumerPort
PortFailure = _PORT_MODULE.PortFailure


def _plain(value: object) -> object:
    if isinstance(value, dict) or hasattr(value, "items"):
        return {key: _plain(item) for key, item in value.items()}  # type: ignore[union-attr]
    if type(value) is tuple:
        return [_plain(item) for item in value]
    return value


def _wire(value: object) -> str:
    return json.dumps(_plain(value), sort_keys=True, separators=(",", ":"))


def _artifact_ref(artifact_type: str, marker: str) -> dict[str, object]:
    return {
        "type": "artifact_ref",
        "artifact_type": artifact_type,
        "schema_version": 1,
        "content_hash": "sha256:" + marker * 64,
    }


def _tagged_ref(tag: str, artifact_type: str, marker: str) -> dict[str, object]:
    return {"type": tag, "artifact_ref": _artifact_ref(artifact_type, marker)}


def _cutoff(sequence: int) -> dict[str, object]:
    return {
        "log_name": "research.execution.v1",
        "log_sequence": sequence,
        "receipt_hash": "sha256:" + "f" * 64,
    }


def _spec(*, marker: str = "1") -> ExperimentSpec:
    port = InMemoryBacktestConsumerPort()
    profile_ref = port.case("adverse_completed")["derive"]["metric_profile_ref"]
    return ExperimentSpec(
        hypothesis_ref=_artifact_ref("hypothesis", marker),
        strategy_definition_ref=_artifact_ref("strategy_definition", marker),
        data_slices=(
            DataSlice(
                _tagged_ref(
                    "backtest_market_bundle_ref", "backtest_market_bundle", marker
                ),
                f"dataset-{marker}",
                "2026-01-01T00:00:00.000000Z",
                "2026-02-01T00:00:00.000000Z",
            ),
        ),
        parameter_combinations=(
            ParameterCombination((("lookback", "10"),)),
            ParameterCombination((("lookback", "20"),)),
        ),
        seeds=(1, 2),
        scenario_refs=(_artifact_ref("scenario", marker),),
        backtest_template_ref=_artifact_ref("backtest_template", marker),
        model_build_plan=None,
        metric_profile_refs=(profile_ref,),
        budget={"max_trials": 4},
    )


def _trial_key(task: TaskRef) -> tuple[str, int]:
    assert task.kind == "TRIAL"
    declaration = task.artifact
    assert type(declaration) is TrialDeclaration
    return declaration.parameter_values.values[0][1], declaration.seed  # type: ignore[return-value]


def _records_for_golden(
    spec: ExperimentSpec,
) -> tuple[dict[TaskRef, TaskOutcome], list[VerifiedAnalysis]]:
    port = InMemoryBacktestConsumerPort()
    completed_case = port.case("adverse_completed")
    completed_ref = port.run(completed_case["request_spec"])
    completed_record = port.load_completed(completed_ref)
    analysis_ref = port.derive(
        completed_ref, completed_case["derive"]["metric_profile_ref"]
    )
    analysis_record = port.load_analysis(analysis_ref)
    blocked_case = port.case("terminal_blocked")
    blocked_ref = port.run(blocked_case["request_spec"])
    blocked_record = port.load_terminal(blocked_ref)

    universe = build_task_universe(spec)
    trial_tasks = {_trial_key(task): task for task in universe if task.kind == "TRIAL"}
    analyses_by_trial: dict[tuple[str, int], TaskRef] = {}
    trial_by_ref = {
        _wire(task.task_artifact_ref): key for key, task in trial_tasks.items()
    }
    for task in universe:
        if task.kind == "ANALYSIS":
            artifact = task.artifact
            assert type(artifact) is AnalysisTask
            analyses_by_trial[trial_by_ref[_wire(artifact.trial_declaration_ref)]] = (
                task
            )

    outcomes: dict[TaskRef, TaskOutcome] = {}
    verified: list[VerifiedAnalysis] = []
    completed_keys = (("10", 1), ("10", 2), ("20", 1))
    returns = {("10", 1): "-0.1", ("10", 2): "-0.2", ("20", 1): "-0.3"}
    for index, key in enumerate(completed_keys):
        completed = deepcopy(completed_record)
        analysis = deepcopy(analysis_record)
        if index:
            marker = str(index + 2)
            completed["publication_ref"] = _tagged_ref(
                "backtest_canonical_publication_ref",
                "canonical_publication_manifest",
                marker,
            )
            completed["execution_result_hash"] = "sha256:" + marker * 64
            analysis["analysis_ref"] = _tagged_ref(
                "analysis_artifact_ref", "backtest_analysis", marker
            )
            analysis["source_publication_ref"] = completed["publication_ref"]
            analysis["source_execution_result_hash"] = completed[
                "execution_result_hash"
            ]
        analysis["simple_period_return"] = returns[key]
        trial_outcome = map_backtest_observation(trial_tasks[key], completed)
        analysis_outcome = map_backtest_observation(analyses_by_trial[key], analysis)
        outcomes[trial_tasks[key]] = trial_outcome
        outcomes[analyses_by_trial[key]] = analysis_outcome
        verified.append(VerifiedAnalysis.from_record(analysis))

    blocked_key = ("20", 2)
    blocked_outcome = map_backtest_observation(trial_tasks[blocked_key], blocked_record)
    outcomes[trial_tasks[blocked_key]] = blocked_outcome
    outcomes[analyses_by_trial[blocked_key]] = block_analysis_from_upstream(
        analyses_by_trial[blocked_key], blocked_outcome
    )
    return outcomes, verified


def _policy(spec: ExperimentSpec, **overrides: object) -> SelectionPolicy:
    values: dict[str, object] = {
        "metric_profile_ref": spec.metric_profile_refs[0],
        "eligible_trial_statuses": ("COMPLETED",),
        "accepted_backtest_grades": ("development",),
        "hard_filters": (HardFilter("trade_count", "gte", 1),),
        "ordering": (
            OrderingCriterion("simple_period_return", "descending"),
            OrderingCriterion("trade_count", "descending"),
        ),
        "max_selections": 1,
        "tie_break": "trial_declaration_ref_ascending",
    }
    values.update(overrides)
    return SelectionPolicy(**values)  # type: ignore[arg-type]


def _closed_entries(
    spec: ExperimentSpec,
    outcomes: dict[TaskRef, TaskOutcome],
    policy: SelectionPolicy,
    *,
    include_precommit: bool = True,
    unrelated: bool = True,
    retry_first: bool = False,
) -> tuple[list[ExecutionEntry], object, SelectionDeclaration]:
    declaration = SelectionDeclaration(
        spec.ref,
        policy.ref,
        "candidate_trial_declarations_v1",
        _artifact_ref("actor", "a"),
    )
    entries: list[ExecutionEntry] = []
    sequence = 1
    if include_precommit:
        entries.append(ExecutionEntry(sequence, declaration))
        sequence += 1

    if unrelated:
        other = _spec(marker="9")
        other_task = next(
            task for task in build_task_universe(other) if task.kind == "TRIAL"
        )
        entries.append(
            ExecutionEntry(
                sequence,
                TaskAttemptStarted(other_task, 1, None, (declaration.ref,)),
            )
        )
        sequence += 1

    for index, task in enumerate(build_task_universe(spec)):
        selection_refs = (declaration.ref,) if include_precommit else ()
        start = TaskAttemptStarted(task, 1, None, selection_refs)
        outcome = outcomes[task]
        if retry_first and index == 0:
            retry_close = TaskAttemptClosed(
                start.ref, "RETRYABLE_FAILURE", None, "PORT_RETENTION_UNAVAILABLE"
            )
            retry = TaskAttemptStarted(task, 2, retry_close.ref, selection_refs)
            terminal_close = TaskAttemptClosed(retry.ref, "TERMINAL", outcome.ref, None)
            entries.extend(
                (
                    ExecutionEntry(sequence, start),
                    ExecutionEntry(sequence + 1, retry_close),
                    ExecutionEntry(sequence + 2, retry),
                    ExecutionEntry(sequence + 3, outcome),
                    ExecutionEntry(sequence + 4, terminal_close),
                )
            )
            sequence += 5
            continue
        close = TaskAttemptClosed(start.ref, "TERMINAL", outcome.ref, None)
        entries.extend(
            (
                ExecutionEntry(sequence, start),
                ExecutionEntry(sequence + 1, outcome),
                ExecutionEntry(sequence + 2, close),
            )
        )
        sequence += 3

    cutoff = _cutoff(sequence)
    direct_manifest = build_execution_manifest(spec, outcomes, cutoff)
    entries.append(ExecutionEntry(sequence, direct_manifest))
    return entries, cutoff, declaration


def _golden() -> tuple[
    ExperimentSpec,
    dict[TaskRef, TaskOutcome],
    list[VerifiedAnalysis],
    SelectionPolicy,
    list[ExecutionEntry],
    object,
]:
    spec = _spec()
    outcomes, verified = _records_for_golden(spec)
    policy = _policy(spec)
    entries, cutoff, _ = _closed_entries(spec, outcomes, policy)
    return spec, outcomes, verified, policy, entries, cutoff


def _assert_code(code: str, action: Any) -> None:
    with pytest.raises(ResearchCoreError) as caught:
        action()
    assert caught.value.code == code


def test_deterministic_four_trial_eight_task_universe_and_axis_failures() -> None:
    spec = _spec()
    first = build_task_universe(spec)
    second = build_task_universe(spec)

    assert first == second
    assert len(first) == 8
    assert sum(task.kind == "TRIAL" for task in first) == 4
    assert sum(task.kind == "ANALYSIS" for task in first) == 4
    assert all(_wire(task.experiment_ref) == _wire(spec.ref) for task in first)
    assert [task.canonical_wire for task in first] == sorted(
        task.canonical_wire for task in first
    )

    values = {field.name: getattr(spec, field.name) for field in fields(ExperimentSpec)}
    values["seeds"] = (1, 1)
    _assert_code("TASK_AXIS_DUPLICATE", lambda: ExperimentSpec(**values))
    values["seeds"] = (2, 1)
    _assert_code("EXPERIMENT_SPEC_INVALID", lambda: ExperimentSpec(**values))
    values["seeds"] = spec.seeds
    values["parameter_combinations"] = tuple(reversed(spec.parameter_combinations))
    _assert_code("EXPERIMENT_SPEC_INVALID", lambda: ExperimentSpec(**values))


def test_exact_manifest_cover_interleaving_and_deterministic_selection() -> None:
    spec, _, verified, policy, entries, cutoff = _golden()

    projection = validate_execution_prefix(spec, entries, cutoff)
    manifest = build_execution_manifest(spec, projection, cutoff)
    family = build_candidate_family(spec.ref, manifest)
    selected = select_candidate(family, manifest, policy, tuple(verified))

    assert len(projection) == 8
    assert len(manifest.task_outcome_refs) == 8
    assert {field.name for field in fields(type(manifest))} == {
        "experiment_ref",
        "task_outcome_refs",
    }
    assert {field.name for field in fields(CandidateFamily)} == {
        "experiment_ref",
        "execution_manifest_ref",
    }
    assert type(selected) is Selected
    selected_task = next(
        task
        for task in build_task_universe(spec)
        if task.kind == "TRIAL"
        and _wire(task.task_artifact_ref) == _wire(selected.trial_declaration_ref)
    )
    assert _trial_key(selected_task) == ("10", 1)
    assert selected.selection_rank == 1


def test_retry_chain_is_append_only_and_contiguous() -> None:
    spec = _spec()
    outcomes, _ = _records_for_golden(spec)
    policy = _policy(spec)
    entries, cutoff, _ = _closed_entries(
        spec, outcomes, policy, unrelated=False, retry_first=True
    )
    projection = validate_execution_prefix(spec, entries, cutoff)
    assert len(projection) == 8

    second_start_index = next(
        index
        for index, entry in enumerate(entries)
        if type(entry.payload) is TaskAttemptStarted and entry.payload.ordinal == 2
    )
    second_start = entries[second_start_index].payload
    assert type(second_start) is TaskAttemptStarted
    entries[second_start_index] = ExecutionEntry(
        entries[second_start_index].log_sequence,
        TaskAttemptStarted(
            second_start.task_ref,
            3,
            second_start.parent_closed_attempt_ref,
            second_start.selection_declaration_refs,
        ),
    )
    _assert_code(
        "ATTEMPT_CHAIN_INVALID",
        lambda: validate_execution_prefix(spec, entries, cutoff),
    )


def test_completed_terminal_and_port_failure_mapping_are_distinct() -> None:
    spec = _spec()
    trial = next(task for task in build_task_universe(spec) if task.kind == "TRIAL")
    port = InMemoryBacktestConsumerPort()
    completed_case = port.case("adverse_completed")
    completed_ref = port.run(completed_case["request_spec"])
    completed = map_backtest_observation(trial, port.load_completed(completed_ref))
    assert completed.state == "COMPLETED"

    for case_id, expected in (
        ("terminal_blocked", "BLOCKED"),
        ("terminal_failed", "FAILED"),
        ("terminal_cancelled", "CANCELLED"),
    ):
        case = port.case(case_id)
        terminal_ref = port.run(case["request_spec"])
        outcome = map_backtest_observation(trial, port.load_terminal(terminal_ref))
        assert outcome.state == expected
        assert type(outcome.witness).__name__ == "BacktestTerminal"

    failure_case = port.case("provider_failure")
    with pytest.raises(PortFailure) as caught:
        port.run(failure_case["request_spec"])
    failed = map_backtest_observation(trial, caught.value)
    assert failed.state == "FAILED"
    assert failed.witness == LocalFailure("PORT_RETENTION_UNAVAILABLE")


def test_hidden_duplicate_foreign_unmatched_and_reopened_evidence_fail_closed() -> None:
    spec, outcomes, _, policy, entries, cutoff = _golden()

    hidden = entries.copy()
    hidden.pop(3)  # one target TaskOutcome; its close remains
    _assert_code(
        "TASK_OUTCOME_MISSING_OR_DUPLICATE",
        lambda: validate_execution_prefix(spec, hidden, cutoff),
    )

    duplicate = entries.copy()
    outcome_entry = next(
        entry for entry in entries if type(entry.payload) is TaskOutcome
    )
    duplicate.insert(
        -1, ExecutionEntry(outcome_entry.log_sequence + 100, outcome_entry.payload)
    )
    duplicate[-1] = ExecutionEntry(
        duplicate[-1].log_sequence + 101, duplicate[-1].payload
    )
    duplicate_cutoff = _cutoff(duplicate[-1].log_sequence)
    _assert_code(
        "TASK_OUTCOME_MISSING_OR_DUPLICATE",
        lambda: validate_execution_prefix(spec, duplicate, duplicate_cutoff),
    )

    trial = next(task for task in build_task_universe(spec) if task.kind == "TRIAL")
    foreign_ref = "rp-core:trial_declaration@1:sha256:" + "e" * 64
    foreign = TaskRef("TRIAL", foreign_ref, _experiment_ref=spec.ref)
    foreign_outcome = TaskOutcome(foreign, "FAILED", LocalFailure("LOCAL"))
    foreign_entries = entries.copy()
    foreign_entries.insert(
        -1, ExecutionEntry(entries[-1].log_sequence - 1, foreign_outcome)
    )
    _assert_code(
        "TASK_REF_FOREIGN",
        lambda: validate_execution_prefix(spec, foreign_entries, cutoff),
    )

    reopened = entries + [
        ExecutionEntry(
            entries[-1].log_sequence + 1,
            TaskAttemptStarted(trial, 2, None, ()),
        )
    ]
    _assert_code(
        "EXPERIMENT_REOPENED_AFTER_CLOSE",
        lambda: validate_execution_prefix(spec, reopened, cutoff),
    )

    missing = dict(outcomes)
    missing.pop(next(iter(missing)))
    _assert_code(
        "TASK_OUTCOME_MISSING_OR_DUPLICATE",
        lambda: build_execution_manifest(spec, missing, cutoff),
    )


def test_selection_requires_precommit_and_verified_completed_analysis_only() -> None:
    spec, outcomes, verified, policy, _, _ = _golden()
    entries, cutoff, _ = _closed_entries(
        spec,
        outcomes,
        policy,
        include_precommit=False,
        unrelated=False,
    )
    projection = validate_execution_prefix(spec, entries, cutoff)
    manifest = build_execution_manifest(spec, projection, cutoff)
    family = build_candidate_family(spec.ref, manifest)
    _assert_code(
        "SELECTION_PRECOMMIT_MISSING",
        lambda: select_candidate(family, manifest, policy, verified),
    )

    entries, cutoff, _ = _closed_entries(spec, outcomes, policy, unrelated=False)
    projection = validate_execution_prefix(spec, entries, cutoff)
    manifest = build_execution_manifest(spec, projection, cutoff)
    family = build_candidate_family(spec.ref, manifest)
    _assert_code(
        "SELECTION_INPUT_INCOMPLETE",
        lambda: select_candidate(family, manifest, policy, verified[:-1]),
    )

    extra = VerifiedAnalysis(
        _tagged_ref("analysis_artifact_ref", "backtest_analysis", "d"),
        verified[0].trial_publication_ref,
        verified[0].metric_profile_ref,
        "0",
        1,
        "development",
    )
    _assert_code(
        "SELECTION_INPUT_INCOMPLETE",
        lambda: select_candidate(family, manifest, policy, verified + [extra]),
    )

    no_grade = _policy(spec, accepted_backtest_grades=("decision_grade",))
    no_grade_entries, no_grade_cutoff, _ = _closed_entries(
        spec, outcomes, no_grade, unrelated=False
    )
    no_grade_projection = validate_execution_prefix(
        spec, no_grade_entries, no_grade_cutoff
    )
    no_grade_manifest = build_execution_manifest(
        spec, no_grade_projection, no_grade_cutoff
    )
    no_grade_family = build_candidate_family(spec.ref, no_grade_manifest)
    result = select_candidate(no_grade_family, no_grade_manifest, no_grade, verified)
    assert type(result) is NoSelection
    assert result.reason_code == "NO_ELIGIBLE_TRIAL"


def test_forged_analysis_link_and_wrong_policy_fail_closed() -> None:
    spec, outcomes, verified, policy, entries, cutoff = _golden()
    completed_analysis = next(
        outcome
        for outcome in outcomes.values()
        if outcome.task_ref.kind == "ANALYSIS" and outcome.state == "COMPLETED"
    )
    forged = VerifiedAnalysis(
        completed_analysis.witness.analysis_ref,  # type: ignore[union-attr]
        _tagged_ref(
            "backtest_canonical_publication_ref",
            "canonical_publication_manifest",
            "c",
        ),
        spec.metric_profile_refs[0],
        "0",
        1,
        "development",
    )
    projection = validate_execution_prefix(spec, entries, cutoff)
    manifest = build_execution_manifest(spec, projection, cutoff)
    family = build_candidate_family(spec.ref, manifest)
    replaced = [
        forged if _wire(item.analysis_ref) == _wire(forged.analysis_ref) else item
        for item in verified
    ]
    _assert_code(
        "SELECTION_INPUT_INCOMPLETE",
        lambda: select_candidate(family, manifest, policy, replaced),
    )

    later_policy = _policy(spec, hard_filters=())
    _assert_code(
        "SELECTION_POLICY_MISMATCH",
        lambda: select_candidate(family, manifest, later_policy, verified),
    )


def test_pure_core_import_boundary_and_no_duplicate_reference_types() -> None:
    source_path = (
        Path(__file__).resolve().parents[1] / "src/crypto_quant_research/integration.py"
    )
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(source_path))
    imports = {
        name
        for node in ast.walk(tree)
        for name in (
            [alias.name for alias in node.names]
            if isinstance(node, ast.Import)
            else [node.module or ""]
            if isinstance(node, ast.ImportFrom)
            else []
        )
    }
    class_names = {
        node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)
    }

    assert not any(name.startswith("crypto_quant_backtest") for name in imports)
    assert not any(name.startswith("crypto_quant_domain") for name in imports)
    assert not any(name.startswith("crypto_quant_foundation") for name in imports)
    assert "ArtifactRef" not in class_names
    assert "LogEntryRef" not in class_names
    assert "def _artifact_ref" not in source
    assert "Protocol" not in source
