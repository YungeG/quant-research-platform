from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import crypto_quant_backtest as backtest
import pytest
from crypto_quant_domain import (
    ArtifactIntegrityError,
    ArtifactRef,
    ArtifactRetentionUnavailableError,
    canonical_bytes,
)
from crypto_quant_foundation import LocalFoundation
from crypto_quant_research import (
    FrozenExperimentInputs,
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
    SelectionPolicy,
    build_trial_declarations,
)
from crypto_quant_validation import SampleConsumptionLedger

_PLATFORM_ROOT = Path(__file__).resolve().parents[2]
_BINDING_PATH = _PLATFORM_ROOT / "tests/integration/test_backtest_public_binding.py"
_BINDING_SPEC = importlib.util.spec_from_file_location("platform_public_binding", _BINDING_PATH)
assert _BINDING_SPEC is not None and _BINDING_SPEC.loader is not None
_BINDING_MODULE = importlib.util.module_from_spec(_BINDING_SPEC)
sys.modules[_BINDING_SPEC.name] = _BINDING_MODULE
_BINDING_SPEC.loader.exec_module(_BINDING_MODULE)
_prepare_with = _BINDING_MODULE._prepare_with

_SAMPLE_LOG = "validation.sample-consumption.v1"
_RESERVED_AT = "2026-08-18T00:00:00.000000Z"
_RECEIVED_AT = "2026-08-18T00:00:01.000000Z"


def _plain(value: object) -> Any:
    return json.loads(canonical_bytes(value))


def _artifact_ref(artifact_type: str, marker: str) -> dict[str, object]:
    return {
        "type": "artifact_ref",
        "artifact_type": artifact_type,
        "schema_version": 1,
        "content_hash": "sha256:" + marker * 64,
    }


def _spec(profile_ref: ArtifactRef) -> ExperimentSpec:
    return ExperimentSpec(
        hypothesis_ref=_artifact_ref("hypothesis", "1"),
        strategy_definition_ref=_artifact_ref("strategy_definition", "2"),
        data_slices=(
            DataSlice(
                {
                    "type": "backtest_market_bundle_ref",
                    "artifact_ref": _artifact_ref("backtest_market_bundle", "3"),
                },
                "cash-development-v1",
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
        metric_profile_refs=(_plain(profile_ref),),
        budget={"max_trials": 4},
    )


def _policy(spec: ExperimentSpec) -> SelectionPolicy:
    return SelectionPolicy(
        metric_profile_ref=spec.metric_profile_refs[0],
        eligible_trial_statuses=("COMPLETED",),
        accepted_backtest_grades=("development",),
        hard_filters=(HardFilter("trade_count", "gte", 1),),
        ordering=(
            OrderingCriterion("simple_period_return", "descending"),
            OrderingCriterion("trade_count", "descending"),
        ),
        max_selections=1,
        tie_break="trial_declaration_ref_ascending",
    )


class _PublicBacktestOperations:
    """Integrated-test composition of accepted public Backtest roots."""

    def __init__(
        self,
        foundation: LocalFoundation,
        prepared: dict[str, backtest.PreparedBacktestExecution],
        *,
        repository_failure: str | None = None,
    ) -> None:
        self._foundation = foundation
        self._prepared = prepared
        self._refs: dict[str, object] = {}
        self._repository_failure = repository_failure
        self.run_calls = 0
        self.derive_calls = 0
        self.reservations_before_run: list[int] = []

    def _remember(self, ref: object) -> object:
        self._refs[canonical_bytes(ref).decode()] = ref
        return ref

    def _nominal(self, ref: object) -> object:
        return self._refs[canonical_bytes(ref).decode()]

    def _repository(self, target: object | None = None) -> backtest.BacktestEvidenceRepository:
        if self._repository_failure is None or target is None:
            return backtest.BacktestEvidenceRepository(self._foundation)

        failure = self._repository_failure
        foundation = self._foundation

        target_ref = getattr(target, "artifact_ref", target)

        class _Reader:
            def read(self, *, ref: ArtifactRef):
                if ref == target_ref:
                    if failure == "tamper":
                        raise ArtifactIntegrityError("injected Research tamper")
                    raise ArtifactRetentionUnavailableError(
                        "injected Research retention failure"
                    )
                return foundation.read(ref=ref)

        return backtest.BacktestEvidenceRepository(_Reader())

    def run(self, request_spec: dict[str, object]) -> dict[str, object]:
        self.run_calls += 1
        self.reservations_before_run.append(
            len(self._foundation.entries(_SAMPLE_LOG))
        )
        prepared = self._prepared[request_spec["binding_key"]]  # type: ignore[index]
        return _plain(self._remember(prepared.runtime.run(prepared.execution_request)))

    def load_completed(self, ref: object) -> dict[str, object]:
        nominal = self._nominal(ref)
        record = self._repository(nominal).load_completed(nominal)
        return {
            "publication_ref": _plain(record.source_publication_ref),
            "semantic_run_id": record.semantic_run_id,
            "execution_result_hash": record.source_execution_result_hash,
            "result_grade": record.result_grade.value,
        }

    def load_terminal(self, ref: object) -> dict[str, object]:
        nominal = self._nominal(ref)
        record = self._repository(nominal).load_terminal(nominal)
        return {
            "status": record.status.value,
            "durable_evidence_ref": _plain(record.durable_evidence_ref),
        }

    def derive(self, completed_ref: object, metric_profile_ref: object) -> dict[str, object]:
        self.derive_calls += 1
        nominal = self._nominal(completed_ref)
        completed = self._repository().load_completed(nominal)
        profile = ArtifactRef(
            metric_profile_ref["artifact_type"],  # type: ignore[index,arg-type]
            metric_profile_ref["schema_version"],  # type: ignore[index,arg-type]
            metric_profile_ref["content_hash"],  # type: ignore[index,arg-type]
        )
        return _plain(
            self._remember(
                backtest.BacktestAnalysisRuntime(self._foundation).derive(
                    completed, profile
                )
            )
        )

    def load_analysis(self, ref: object) -> dict[str, object]:
        nominal = self._nominal(ref)
        record = self._repository(nominal).load_analysis(nominal)
        analysis = record.analysis
        return {
            "analysis_ref": _plain(record.analysis_ref),
            "metric_profile_ref": _plain(analysis.metric_profile_ref),
            "source_publication_ref": _plain(analysis.source_publication_ref),
            "source_execution_result_hash": analysis.source_execution_result_hash,
            "simple_period_return": analysis.simple_period_return,
            "trade_count": analysis.trade_count,
            "result_grade": analysis.result_grade.value,
        }


def _runtime(
    tmp_path: Path,
    *,
    repository_failure: str | None = None,
) -> tuple[
    LocalFoundation,
    SampleConsumptionLedger,
    _PublicBacktestOperations,
    FrozenExperimentInputs,
]:
    foundation = LocalFoundation(tmp_path / "foundation", clock=lambda: _RECEIVED_AT)
    profile_ref = backtest.BacktestAnalysisRuntime(foundation).publish_metric_profile()
    spec = _spec(profile_ref)
    trials = build_trial_declarations(spec)
    prepared: dict[str, backtest.PreparedBacktestExecution] = {}
    executions: list[TrialExecution] = []
    for index, trial in enumerate(trials):
        binding_key = trial.ref
        execution = _prepare_with(
            foundation,
            tmp_path / "publications",
            experiment_id=f"research:{trial.ref}",
            market=index < 3,
        )
        prepared[binding_key] = execution
        executions.append(
            TrialExecution(
                trial.ref,
                {"binding_key": binding_key},
                _plain(execution.request_ref),
            )
        )
    inputs = FrozenExperimentInputs(
        spec,
        _policy(spec),
        {"type": "actor_ref", "actor_id": "research"},
        tuple(executions),
        _RESERVED_AT,
    )
    return (
        foundation,
        SampleConsumptionLedger(foundation),
        _PublicBacktestOperations(
            foundation,
            prepared,
            repository_failure=repository_failure,
        ),
        inputs,
    )


def test_real_research_golden_replays_without_a_second_economic_run(tmp_path: Path) -> None:
    foundation, ledger, provider, inputs = _runtime(tmp_path)

    first = execute_experiment(inputs, foundation, ledger, provider)
    attempts = tuple(
        (tmp_path / "publications").rglob("attempt-execution-record.json")
    )
    second = execute_experiment(inputs, foundation, ledger, provider)

    assert type(first) is PublishedStrategyCandidate
    assert second == first
    assert provider.run_calls == 4
    assert provider.derive_calls == 3
    assert provider.reservations_before_run == [1, 2, 3, 4]
    execution = [
        json.loads(entry.payload)["payload"]
        for entry in foundation.entries("research.execution.v1")
        if json.loads(entry.payload)["artifact_type"] == "task_outcome"
    ]
    assert [item["state"] for item in execution].count("COMPLETED") == 6
    assert [item["state"] for item in execution].count("BLOCKED") == 2
    assert len(attempts) == 7
    assert tuple(
        (tmp_path / "publications").rglob("attempt-execution-record.json")
    ) == attempts


@pytest.mark.parametrize("failure", ("tamper", "retention"))
def test_real_repository_failure_remains_local_and_publishes_no_candidate(
    tmp_path: Path,
    failure: str,
) -> None:
    foundation, ledger, provider, inputs = _runtime(
        tmp_path,
        repository_failure=failure,
    )

    result = execute_experiment(inputs, foundation, ledger, provider)

    assert type(result).__name__ == "PublishedNoSelection"
    outcomes = [
        json.loads(entry.payload)["payload"]
        for entry in foundation.entries("research.execution.v1")
        if json.loads(entry.payload)["artifact_type"] == "task_outcome"
    ]
    assert all("backtest_terminal" not in item["witness"] for item in outcomes)
    assert any("local_failure" in item["witness"] for item in outcomes)
