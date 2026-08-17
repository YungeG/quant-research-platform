from __future__ import annotations

import ast
import importlib.util
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import crypto_quant_research.runtime as research_runtime
import pytest
from crypto_quant_domain import ArtifactEnvelope, canonical_bytes, canonical_sha256
from crypto_quant_foundation import FoundationFailure, LocalFoundation
from crypto_quant_research import (
    FrozenExperimentInputs,
    PublishedNoSelection,
    PublishedStrategyCandidate,
    TrialExecution,
    execute_experiment,
)
from crypto_quant_research.integration import (
    DataSlice,
    ExperimentSpec,
    HardFilter,
    OrderingCriterion,
    ParameterCombination,
    ResearchCoreError,
    SelectionPolicy,
    TrialDeclaration,
    build_trial_declarations,
)
from crypto_quant_validation import SampleConsumptionLedger

_PORT_PATH = Path(__file__).resolve().parents[2] / "tests/support/backtest_consumer_port.py"
_PORT_SPEC = importlib.util.spec_from_file_location("backtest_consumer_port", _PORT_PATH)
assert _PORT_SPEC is not None and _PORT_SPEC.loader is not None
_PORT_MODULE = importlib.util.module_from_spec(_PORT_SPEC)
_PORT_SPEC.loader.exec_module(_PORT_MODULE)
InMemoryBacktestConsumerPort = _PORT_MODULE.InMemoryBacktestConsumerPort
load_contract_fixture = _PORT_MODULE.load_contract_fixture

ARTIFACT_LOG = "research.artifacts.v1"
EXECUTION_LOG = "research.execution.v1"
SAMPLE_LOG = "validation.sample-consumption.v1"
RESERVED_AT = "2026-02-01T00:00:00.000000Z"
RECEIVED_AT = "2026-02-02T00:00:00.000000Z"


def _artifact_ref(artifact_type: str, marker: str) -> dict[str, object]:
    return {
        "type": "artifact_ref",
        "artifact_type": artifact_type,
        "schema_version": 1,
        "content_hash": "sha256:" + marker * 64,
    }


def _tagged_ref(tag: str, artifact_type: str, marker: str) -> dict[str, object]:
    return {"type": tag, "artifact_ref": _artifact_ref(artifact_type, marker)}


def _spec() -> ExperimentSpec:
    profile = load_contract_fixture()["cases"][0]["derive"]["metric_profile_ref"]
    return ExperimentSpec(
        hypothesis_ref=_artifact_ref("hypothesis", "1"),
        strategy_definition_ref=_artifact_ref("strategy_definition", "2"),
        data_slices=(
            DataSlice(
                _tagged_ref("backtest_market_bundle_ref", "backtest_market_bundle", "3"),
                "fixture-v1",
                "2026-01-01T00:00:00.000000Z",
                "2026-02-01T00:00:00.000000Z",
            ),
        ),
        parameter_combinations=(
            ParameterCombination((("lookback", "10"),)),
            ParameterCombination((("lookback", "20"),)),
        ),
        seeds=(1, 2),
        scenario_refs=(_artifact_ref("scenario", "4"),),
        backtest_template_ref=_artifact_ref("backtest_template", "5"),
        model_build_plan=None,
        metric_profile_refs=(profile,),
        budget={"max_trials": 4},
    )


def _policy(
    spec: ExperimentSpec, *, grades: tuple[str, ...] = ("development",)
) -> SelectionPolicy:
    return SelectionPolicy(
        metric_profile_ref=spec.metric_profile_refs[0],
        eligible_trial_statuses=("COMPLETED",),
        accepted_backtest_grades=grades,
        hard_filters=(HardFilter("trade_count", "gte", 1),),
        ordering=(
            OrderingCriterion("simple_period_return", "descending"),
            OrderingCriterion("trade_count", "descending"),
        ),
        max_selections=1,
        tie_break="trial_declaration_ref_ascending",
    )


def _trial_key(trial: TrialDeclaration) -> tuple[str, int]:
    return trial.parameter_values.values[0][1], trial.seed  # type: ignore[return-value]


def _completed_case(case_id: str, marker: str, result: str) -> dict[str, Any]:
    contract = load_contract_fixture()
    case = deepcopy(contract["cases"][0])
    case["case_id"] = case_id
    case["request_spec"] = {"fixture_case": case_id}
    publication = _tagged_ref(
        "backtest_canonical_publication_ref", "canonical_publication_manifest", marker
    )
    analysis = _tagged_ref("analysis_artifact_ref", "backtest_analysis", marker)
    case["run"] = {"kind": "completed", "ref": deepcopy(publication)}
    case["completed"]["publication_ref"] = deepcopy(publication)
    case["completed"]["execution_result_hash"] = "sha256:" + marker * 64
    case["derive"]["analysis_ref"] = deepcopy(analysis)
    case["analysis"]["analysis_ref"] = deepcopy(analysis)
    case["analysis"]["source_publication_ref"] = deepcopy(publication)
    case["analysis"]["source_execution_result_hash"] = "sha256:" + marker * 64
    case["analysis"]["simple_period_return"] = result
    return case


def _contract() -> dict[str, Any]:
    contract = load_contract_fixture()
    contract["cases"].extend(
        (
            _completed_case("completed-10-1", "1", "-0.1"),
            _completed_case("completed-10-2", "2", "-0.2"),
            _completed_case("completed-20-1", "3", "-0.3"),
        )
    )
    return contract


class RecordingPort(InMemoryBacktestConsumerPort):
    def __init__(self, foundation: LocalFoundation, contract: dict[str, Any]) -> None:
        super().__init__(contract)
        self._foundation = foundation
        self.run_requests: list[dict[str, object]] = []
        self.run_reservation_counts: list[int] = []
        self.derive_calls: list[object] = []

    def run(self, request_spec: dict[str, object]) -> dict[str, object]:
        self.run_requests.append(deepcopy(request_spec))
        self.run_reservation_counts.append(len(self._foundation.entries(SAMPLE_LOG)))
        return super().run(request_spec)

    def derive(
        self, completed_ref: dict[str, object], metric_profile_ref: dict[str, object]
    ) -> dict[str, object]:
        self.derive_calls.append(deepcopy(completed_ref))
        return super().derive(completed_ref, metric_profile_ref)


def _inputs(
    *,
    terminal_case: str = "terminal_blocked",
    failure_key: tuple[str, int] | None = None,
    max_attempts: int = 1,
    grades: tuple[str, ...] = ("development",),
) -> FrozenExperimentInputs:
    spec = _spec()
    cases = {
        ("10", 1): "completed-10-1",
        ("10", 2): "completed-10-2",
        ("20", 1): "completed-20-1",
        ("20", 2): terminal_case,
    }
    if failure_key is not None:
        cases[failure_key] = "provider_failure"
    executions = tuple(
        TrialExecution(
            trial.ref,
            {"fixture_case": cases[_trial_key(trial)]},
            {"type": "backtest_request_ref", "id": f"fixture-{_trial_key(trial)[0]}-{_trial_key(trial)[1]}"},
        )
        for trial in build_trial_declarations(spec)
    )
    return FrozenExperimentInputs(
        spec,
        _policy(spec, grades=grades),
        {"type": "actor_ref", "actor_id": "research"},
        executions,
        RESERVED_AT,
        max_attempts,
    )


def _payload(foundation: LocalFoundation, ref: object) -> dict[str, object]:
    return json.loads(foundation.read(ref=ref).source_bytes)["payload"]


def _log_payloads(foundation: LocalFoundation, log_name: str) -> list[dict[str, object]]:
    return [json.loads(entry.payload) for entry in foundation.entries(log_name)]


def _outcomes(foundation: LocalFoundation) -> list[dict[str, object]]:
    return [
        envelope["payload"]
        for envelope in _log_payloads(foundation, EXECUTION_LOG)
        if envelope["artifact_type"] == "task_outcome"
    ]


def _runtime(tmp_path: Path) -> tuple[LocalFoundation, SampleConsumptionLedger, RecordingPort]:
    foundation = LocalFoundation(tmp_path, clock=lambda: RECEIVED_AT)
    ledger = SampleConsumptionLedger(foundation)
    return foundation, ledger, RecordingPort(foundation, _contract())


def test_fixture_shell_publishes_exact_closure_and_replays_without_a_second_run(
    tmp_path: Path,
) -> None:
    foundation, ledger, port = _runtime(tmp_path)
    result = execute_experiment(_inputs(), foundation, ledger, port)

    assert type(result) is PublishedStrategyCandidate
    assert port.run_reservation_counts == [1, 2, 3, 4]
    context_refs = [json.loads(request["experiment_id"]) for request in port.run_requests]
    assert len({item["content_hash"] for item in context_refs}) == 4
    assert {item["artifact_type"] for item in context_refs} == {"trial_declaration"}
    assert len(port.derive_calls) == 3
    assert len(foundation.entries(SAMPLE_LOG)) == 5
    assert len(_outcomes(foundation)) == 8

    manifest_entries = [
        entry
        for entry in foundation.entries(EXECUTION_LOG)
        if json.loads(entry.payload)["artifact_type"] == "experiment_execution_manifest"
    ]
    assert len(manifest_entries) == 1
    assert manifest_entries[0].entry_ref == result.manifest_cutoff
    manifest = _payload(foundation, result.execution_manifest_ref)
    assert set(manifest) == {"experiment_ref", "task_outcome_refs"}
    assert len(manifest["task_outcome_refs"]) == 8

    family = _payload(foundation, result.candidate_family_ref)
    assert family == {
        "experiment_ref": manifest["experiment_ref"],
        "execution_manifest_ref": result.execution_manifest_ref.to_canonical_dict(),
    }
    candidate = _payload(foundation, result.strategy_candidate_ref)
    assert set(candidate) == {
        "candidate_family_ref",
        "selection_declaration_ref",
        "selected_trial_declaration_ref",
        "selected_trial_spec_ref",
        "selected_publication_ref",
        "selected_analysis_ref",
        "selection_rank",
        "validated",
    }
    assert candidate["candidate_family_ref"] == result.candidate_family_ref.to_canonical_dict()
    assert candidate["selection_rank"] == 1
    assert candidate["validated"] is False

    trial_specs = [
        envelope["payload"]
        for envelope in _log_payloads(foundation, ARTIFACT_LOG)
        if envelope["artifact_type"] == "backtest_trial_spec"
    ]
    assert len(trial_specs) == 4
    assert {item["backtest_request_ref"]["type"] for item in trial_specs} == {
        "backtest_request_ref"
    }

    before = (
        len(port.run_requests),
        len(port.derive_calls),
        len(foundation.entries(ARTIFACT_LOG)),
        len(foundation.entries(EXECUTION_LOG)),
        len(foundation.entries(SAMPLE_LOG)),
    )
    replay = execute_experiment(_inputs(), foundation, ledger, port)
    assert replay == result
    assert before == (
        len(port.run_requests),
        len(port.derive_calls),
        len(foundation.entries(ARTIFACT_LOG)),
        len(foundation.entries(EXECUTION_LOG)),
        len(foundation.entries(SAMPLE_LOG)),
    )


@pytest.mark.parametrize("interrupt_after", range(1, 26))
def test_partial_replay_never_reexecutes_a_durable_task_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, interrupt_after: int
) -> None:
    foundation, ledger, port = _runtime(tmp_path)
    publish_execution = research_runtime._publish_execution
    published = 0

    def interrupt_after_commit(*args: object, **kwargs: object):
        nonlocal published
        receipt = publish_execution(*args, **kwargs)
        published += 1
        if published == interrupt_after:
            raise RuntimeError("interrupted after durable execution commit")
        return receipt

    monkeypatch.setattr(research_runtime, "_publish_execution", interrupt_after_commit)
    with pytest.raises(
        RuntimeError, match="interrupted after durable execution commit"
    ):
        execute_experiment(_inputs(), foundation, ledger, port)
    monkeypatch.setattr(research_runtime, "_publish_execution", publish_execution)

    result = execute_experiment(_inputs(), foundation, ledger, port)

    assert type(result) is PublishedStrategyCandidate
    assert len(port.run_requests) == 4
    assert len(port.derive_calls) == 3
    assert len(_outcomes(foundation)) == 8


@pytest.mark.parametrize(
    ("terminal_case", "state"),
    (
        ("terminal_blocked", "BLOCKED"),
        ("terminal_failed", "FAILED"),
        ("terminal_cancelled", "CANCELLED"),
    ),
)
def test_terminal_trials_do_not_derive_and_block_their_analysis(
    tmp_path: Path, terminal_case: str, state: str
) -> None:
    foundation, ledger, port = _runtime(tmp_path)
    result = execute_experiment(
        _inputs(terminal_case=terminal_case), foundation, ledger, port
    )

    assert type(result) is PublishedStrategyCandidate
    assert len(port.derive_calls) == 3
    outcomes = _outcomes(foundation)
    terminal = [
        item
        for item in outcomes
        if item["task_ref"]["kind"] == "TRIAL" and item["state"] == state
    ]
    assert len(terminal) == 1
    assert set(terminal[0]["witness"]) == {"backtest_terminal"}
    blocked = [
        item
        for item in outcomes
        if item["task_ref"]["kind"] == "ANALYSIS" and item["state"] == "BLOCKED"
    ]
    assert len(blocked) == 1
    assert set(blocked[0]["witness"]) == {"upstream_task_outcome"}


def test_exhausted_provider_failure_is_local_and_reuses_the_same_request(
    tmp_path: Path,
) -> None:
    foundation, ledger, port = _runtime(tmp_path)
    result = execute_experiment(
        _inputs(failure_key=("20", 2), max_attempts=2), foundation, ledger, port
    )

    assert type(result) is PublishedStrategyCandidate
    failed = [
        item
        for item in _outcomes(foundation)
        if item["task_ref"]["kind"] == "TRIAL" and item["state"] == "FAILED"
    ]
    assert len(failed) == 1
    assert failed[0]["witness"] == {
        "local_failure": {"failure_code": "PORT_RETENTION_UNAVAILABLE"}
    }
    failed_requests = [
        request
        for request in port.run_requests
        if request["fixture_case"] == "provider_failure"
    ]
    assert len(failed_requests) == 2
    assert failed_requests[0] == failed_requests[1]
    assert json.loads(failed_requests[0]["experiment_id"])["artifact_type"] == (
        "trial_declaration"
    )
    blocked = [
        item
        for item in _outcomes(foundation)
        if item["task_ref"]["kind"] == "ANALYSIS" and item["state"] == "BLOCKED"
    ]
    assert len(blocked) == 1


def test_reservation_failure_blocks_before_the_port_is_called(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    foundation, ledger, port = _runtime(tmp_path)
    reserve = ledger.reserve
    failed_once = False

    def fail_first_trial(record: object, producer_ref: object):
        nonlocal failed_once
        if not failed_once and getattr(producer_ref, "artifact_type", None) == "trial_declaration":
            failed_once = True
            raise FoundationFailure("WRITE_LOCK_UNAVAILABLE")
        return reserve(record, producer_ref)

    monkeypatch.setattr(ledger, "reserve", fail_first_trial)
    result = execute_experiment(
        _inputs(terminal_case="terminal_cancelled"), foundation, ledger, port
    )

    assert type(result) is PublishedStrategyCandidate
    assert len(port.run_requests) == 3
    blocked_trials = [
        item
        for item in _outcomes(foundation)
        if item["task_ref"]["kind"] == "TRIAL" and item["state"] == "BLOCKED"
    ]
    assert len(blocked_trials) == 1
    assert blocked_trials[0]["witness"] == {
        "dependency_block": {
            "reason_code": "SAMPLE_RESERVATION_FAILED",
            "dependency_ref": None,
        }
    }


def test_invalid_frozen_inputs_touch_no_foundation_log(tmp_path: Path) -> None:
    foundation, ledger, port = _runtime(tmp_path)
    inputs = _inputs()
    object.__setattr__(inputs, "trial_executions", inputs.trial_executions[:-1])

    with pytest.raises(ResearchCoreError) as raised:
        execute_experiment(inputs, foundation, ledger, port)
    assert raised.value.code == "TASK_REF_FOREIGN"
    assert foundation.entries(ARTIFACT_LOG) == ()
    assert foundation.entries(EXECUTION_LOG) == ()
    assert foundation.entries(SAMPLE_LOG) == ()


def test_forged_analysis_link_fails_the_analysis_task_and_publishes_no_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    foundation, ledger, port = _runtime(tmp_path)
    load_analysis = port.load_analysis
    calls = 0

    def forged_analysis(ref: dict[str, object]) -> dict[str, object]:
        nonlocal calls
        calls += 1
        record = load_analysis(ref)
        if calls == 2:
            record["source_execution_result_hash"] = "sha256:" + "f" * 64
        return record

    monkeypatch.setattr(port, "load_analysis", forged_analysis)
    with pytest.raises(ResearchCoreError) as raised:
        execute_experiment(_inputs(), foundation, ledger, port)
    assert raised.value.code == "SELECTION_INPUT_INCOMPLETE"
    assert any(
        item["task_ref"]["kind"] == "ANALYSIS"
        and item["state"] == "FAILED"
        and item["witness"]
        == {"local_failure": {"failure_code": "ANALYSIS_LINK_INVALID"}}
        for item in _outcomes(foundation)
    )
    assert not any(
        envelope["artifact_type"] == "strategy_candidate"
        for envelope in _log_payloads(foundation, ARTIFACT_LOG)
    )


def test_replay_rejects_a_task_after_the_manifest_cutoff(tmp_path: Path) -> None:
    foundation, ledger, port = _runtime(tmp_path)
    result = execute_experiment(_inputs(), foundation, ledger, port)
    assert type(result) is PublishedStrategyCandidate
    candidate = _payload(foundation, result.strategy_candidate_ref)
    envelope = ArtifactEnvelope.create(
        "task_attempt_started",
        1,
        {
            "task_ref": {
                "kind": "TRIAL",
                "task_artifact_ref": candidate["selected_trial_declaration_ref"],
            },
            "ordinal": 2,
            "parent_closed_attempt_ref": None,
            "selection_declaration_refs": [candidate["selection_declaration_ref"]],
            "dispatch_ref": None,
        },
    )
    ref = foundation.put(envelope=envelope)
    foundation.append(
        EXECUTION_LOG,
        canonical_sha256(("artifact-publication-v1", EXECUTION_LOG, ref)),
        canonical_bytes(envelope),
    )

    with pytest.raises(ResearchCoreError) as raised:
        execute_experiment(_inputs(), foundation, ledger, port)
    assert raised.value.code == "EXPERIMENT_REOPENED_AFTER_CLOSE"


def test_replay_rejects_a_foreign_task_that_claims_the_experiment(tmp_path: Path) -> None:
    foundation, ledger, port = _runtime(tmp_path)
    result = execute_experiment(_inputs(), foundation, ledger, port)
    manifest = _payload(foundation, result.execution_manifest_ref)
    foreign_envelope = ArtifactEnvelope.create(
        "trial_declaration", 1, {"experiment_ref": manifest["experiment_ref"]}
    )
    foreign_ref = foundation.put(envelope=foreign_envelope)
    foundation.append(
        ARTIFACT_LOG,
        canonical_sha256(("artifact-publication-v1", ARTIFACT_LOG, foreign_ref)),
        canonical_bytes(foreign_envelope),
    )
    outcome = ArtifactEnvelope.create(
        "task_outcome",
        1,
        {
            "task_ref": {
                "kind": "TRIAL",
                "task_artifact_ref": foreign_ref.to_canonical_dict(),
            },
            "state": "FAILED",
            "witness": {"local_failure": {"failure_code": "FOREIGN"}},
        },
    )
    outcome_ref = foundation.put(envelope=outcome)
    foundation.append(
        EXECUTION_LOG,
        canonical_sha256(("artifact-publication-v1", EXECUTION_LOG, outcome_ref)),
        canonical_bytes(outcome),
    )

    with pytest.raises(ResearchCoreError) as raised:
        execute_experiment(_inputs(), foundation, ledger, port)
    assert raised.value.code == "TASK_REF_FOREIGN"


def test_no_selection_is_explicit_and_publishes_no_candidate(tmp_path: Path) -> None:
    foundation, ledger, port = _runtime(tmp_path)
    result = execute_experiment(
        _inputs(grades=("decision_grade",)), foundation, ledger, port
    )

    assert type(result) is PublishedNoSelection
    assert result.reason_code == "NO_ELIGIBLE_TRIAL"
    assert not any(
        envelope["artifact_type"] == "strategy_candidate"
        for envelope in _log_payloads(foundation, ARTIFACT_LOG)
    )
    runs = len(port.run_requests)
    assert execute_experiment(
        _inputs(grades=("decision_grade",)), foundation, ledger, port
    ) == result
    assert len(port.run_requests) == runs


def test_runtime_uses_only_public_sibling_roots_and_no_provider_adapter() -> None:
    path = Path(__file__).resolve().parents[1] / "src/crypto_quant_research/runtime.py"
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
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

    assert not any(name.startswith("crypto_quant_backtest") for name in imports)
    assert not any(name.startswith("tests") for name in imports)
    assert "Protocol" not in source
    assert "InMemoryBacktestConsumerPort" not in source
