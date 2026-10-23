from __future__ import annotations

import hashlib
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
    SimulationInstant,
    SourceSequence,
    TimelinePhase,
    UtcInstant,
    canonical_bytes,
    canonical_sha256,
)
from crypto_quant_foundation import LocalFoundation
from crypto_quant_research import (
    FeatureDatasetManifest,
    FeatureRecipe,
    FrozenExperimentInputs,
    FrozenModelExperimentInputs,
    ModelBuildPlan,
    PublishedStrategyCandidate,
    TrainerRecipe,
    TrialExecution,
    execute_experiment,
    execute_model_experiment,
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


def _outcome_payloads(foundation: LocalFoundation) -> list[dict[str, Any]]:
    outcomes: list[dict[str, Any]] = []
    for entry in foundation.entries("research.execution.v1"):
        try:
            envelope = json.loads(entry.payload)
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as error:
            pytest.fail(f"Research execution entry is not canonical JSON: {error}")
        if envelope["artifact_type"] == "task_outcome":
            outcomes.append(envelope["payload"])
    return outcomes


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
        payload = {
            "publication_ref": _plain(record.source_publication_ref),
            "semantic_run_id": record.semantic_run_id,
            "execution_result_hash": record.source_execution_result_hash,
            "result_grade": record.result_grade.value,
        }
        if record.engine_context.model_binding is not None:
            payload["model_binding"] = _plain(record.engine_context.model_binding)
        return payload

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


def _content_hash(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


class _FixtureModelBuilder:
    def __init__(
        self,
        foundation: LocalFoundation,
        feature_manifest: FeatureDatasetManifest,
        model_artifact: backtest.ModelArtifactRef,
        *,
        fail_feature: bool = False,
        fail_training_once: bool = False,
    ) -> None:
        self.training_reader, self.training_rows = _BINDING_MODULE._model_training_source()
        self.foundation = foundation
        self.feature_manifest = feature_manifest
        self.model_artifact = model_artifact
        self.fail_feature = fail_feature
        self.fail_training_once = fail_training_once
        self.feature_calls = 0
        self.training_calls = 0
        self.reservations_before_read: list[int] = []

    def build_features(self, plan: ModelBuildPlan) -> FeatureDatasetManifest:
        self.feature_calls += 1
        self.reservations_before_read.append(
            len(self.foundation.entries(_SAMPLE_LOG))
        )
        if self.fail_feature:
            raise RuntimeError("fixture feature build failed")
        assert plan.ref == self.feature_manifest.model_build_plan_ref
        assert plan.training_slice.market_bundle_ref == self.training_reader.bundle_ref.to_canonical_dict()
        assert plan.training_slice.dataset_revision == "model-evaluation-synthetic-training-v1"
        assert self.training_reader.manifest.coverage_start == UtcInstant(0)
        assert self.training_reader.manifest.coverage_end_exclusive == UtcInstant(1000)
        assert self.feature_manifest.row_count == len(self.training_rows) == 2
        assert self.feature_manifest.training_data_hash == canonical_sha256(self.training_rows)
        return self.feature_manifest

    def train_model(
        self, plan: ModelBuildPlan, feature_manifest: FeatureDatasetManifest
    ) -> dict[str, object]:
        self.training_calls += 1
        self.reservations_before_read.append(
            len(self.foundation.entries(_SAMPLE_LOG))
        )
        assert plan.ref == feature_manifest.model_build_plan_ref
        if self.fail_training_once and self.training_calls == 1:
            raise RuntimeError("fixture transient training failure")
        return self.model_artifact.to_canonical_dict()


class _ReservationCheckedReader:
    def __init__(self, reader, check, observed_read):
        self._reader, self._check = reader, check
        self._observed_read = observed_read

    @property
    def bundle_ref(self):
        return self._reader.bundle_ref

    @property
    def manifest(self):
        return self._reader.manifest

    def validate_requirements(self, **kwargs):
        return self._reader.validate_requirements(**kwargs)

    def resume_cursor(self, *args, **kwargs):
        self._check()
        self._observed_read()
        return self._reader.resume_cursor(*args, **kwargs)

    def open_cursor(self, *args, **kwargs):
        self._check()
        self._observed_read()
        return self._reader.open_cursor(*args, **kwargs)

    def read_batch(self, *args, **kwargs):
        self._check()
        self._observed_read()
        return self._reader.read_batch(*args, **kwargs)


class _ModelBacktestOperations(_PublicBacktestOperations):
    def __init__(
        self,
        foundation: LocalFoundation,
        publication_root: Path,
        timeline: backtest.ModelRevisionTimeline,
    ) -> None:
        super().__init__(foundation, {})
        self._publication_root = publication_root
        self._timeline = timeline
        self.prepare_calls = 0
        self.oos_prepare_calls = 0
        self.source_read_calls = 0
        self.reservations_before_prepare: list[int] = []
        self.outcomes_before_prepare: list[int] = []
        self.prepared_bindings: list[backtest.ModelRequestBinding] = []

    def _observed_read(self):
        self.source_read_calls += 1

    def run(self, request_spec):
        key = request_spec["binding_key"]
        config = self._prepared[key]
        if type(config) is dict:
            case_ref = json.loads(request_spec["experiment_id"])

            def check():
                reservations = [json.loads(entry.payload)["payload"]
                                for entry in self._foundation.entries(_SAMPLE_LOG)]
                assert any(
                    item["producer_ref"] == case_ref
                    and item["record"]["purpose"] == "validation"
                    and item["record"]["dataset_revision"] == "model-evaluation-synthetic-oos-v1"
                    and item["record"]["interval_start"] == "1970-01-01T00:00:00.000004Z"
                    and item["record"]["interval_end"] == "1970-01-01T00:00:00.000005Z"
                    for item in reservations
                )

            check()
            intent, inputs, reader = _BINDING_MODULE._model_case(request_spec["experiment_id"], scope="oos")
            self.oos_prepare_calls += 1
            self._prepared[key] = backtest.prepare_model_bound_cash_development_backtest(
                request_intent=intent, provider_inputs=inputs, model_timeline=self._timeline,
                expected_model_key=config["model_key"], expected_artifact_ref_hash=config["artifact_ref_hash"],
                artifact_reader=self._foundation, artifact_publisher=self._foundation,
                market_reader=_ReservationCheckedReader(reader, check, self._observed_read),
                publication_root=Path(config["publication_root"]),
            )
        return super().run(request_spec)

    def prepare_trials(self, trials, evidence) -> tuple[TrialExecution, ...]:
        self.reservations_before_prepare.append(
            len(self._foundation.entries(_SAMPLE_LOG))
        )
        self.outcomes_before_prepare.append(len(_outcome_payloads(self._foundation)))
        executions: list[TrialExecution] = []
        for index, trial in enumerate(trials):
            intent, provider_inputs, reader = _BINDING_MODULE._model_case(f"research:{trial.ref}", market=index < 3)
            assert trial.data_slice.market_bundle_ref == reader.bundle_ref.to_canonical_dict()
            assert trial.data_slice.interval_start == "1970-01-01T00:00:00.000002Z"
            assert trial.data_slice.interval_end == "1970-01-01T00:00:00.000003Z"
            assert trial.data_slice.dataset_revision == reader.bundle_ref.bundle_key
            def check():
                payloads = [json.loads(entry.payload)["payload"] for entry in self._foundation.entries(_SAMPLE_LOG)]
                matching = [p for p in payloads if p["record"]["purpose"] == "discovery"
                            and p["producer_ref"]["artifact_type"] == "trial_declaration"
                            and p["record"]["dataset_revision"] == trial.data_slice.dataset_revision
                            and p["record"]["interval_start"] == trial.data_slice.interval_start
                            and p["record"]["interval_end"] == trial.data_slice.interval_end]
                assert len(matching) == 4
            check()
            prepared = backtest.prepare_model_bound_cash_development_backtest(
                request_intent=intent,
                provider_inputs=provider_inputs,
                model_timeline=self._timeline,
                expected_model_key=evidence.model_artifact["model_key"],
                expected_artifact_ref_hash=evidence.model_artifact[
                    "artifact_ref_hash"
                ],
                artifact_reader=self._foundation,
                artifact_publisher=self._foundation,
                market_reader=_ReservationCheckedReader(reader, check, self._observed_read),
                publication_root=self._publication_root,
            )
            self.prepare_calls += 1
            self.prepared_bindings.append(prepared.model_binding)
            self._prepared[trial.ref] = prepared
            executions.append(
                TrialExecution(
                    trial.ref,
                    {"binding_key": trial.ref},
                    _plain(prepared.request_ref),
                    (evidence.model_artifact,),
                )
            )
        return tuple(executions)


def _model_runtime(
    tmp_path: Path,
    *,
    fail_feature: bool = False,
    fail_training_once: bool = False,
    max_attempts: int = 1,
):
    foundation = LocalFoundation(tmp_path / "foundation", clock=lambda: _RECEIVED_AT)
    profile_ref = backtest.BacktestAnalysisRuntime(foundation).publish_metric_profile()
    training_reader, rows = _BINDING_MODULE._model_training_source()
    _, _, evaluation_reader = _BINDING_MODULE._model_case("research:evaluation")
    training_slice = DataSlice(
        training_reader.bundle_ref.to_canonical_dict(),
        "model-evaluation-synthetic-training-v1",
        "1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000001Z",
    )
    evaluation_slice = DataSlice(
        evaluation_reader.bundle_ref.to_canonical_dict(),
        "model-evaluation-synthetic-evaluation-v1",
        "1970-01-01T00:00:00.000002Z", "1970-01-01T00:00:00.000003Z",
    )
    feature = FeatureRecipe(
        "returns-v1",
        _content_hash("feature-code"),
        _content_hash("feature-schema"),
        ("open_price_units",),
    )
    trainer = TrainerRecipe(
        "linear-v1",
        _content_hash("training-code"),
        "alpha.primary",
        {"ridge": "0.1"},
    )
    plan = ModelBuildPlan(feature.ref, trainer.ref, training_slice, 7)
    spec = ExperimentSpec(
        hypothesis_ref=_artifact_ref("hypothesis", "1"),
        strategy_definition_ref=_artifact_ref("strategy_definition", "2"),
        data_slices=tuple(sorted((training_slice, evaluation_slice), key=lambda item: item.canonical_wire)),
        evaluation_slices=(evaluation_slice,),
        parameter_combinations=(
            ParameterCombination((("lookback", "10"),)),
            ParameterCombination((("lookback", "20"),)),
        ),
        seeds=(1, 2),
        scenario_refs=(_artifact_ref("scenario", "4"),),
        backtest_template_ref=_artifact_ref("backtest_template", "5"),
        model_build_plan=plan,
        metric_profile_refs=(_plain(profile_ref),),
        budget={"max_trials": 4},
    )
    manifest = FeatureDatasetManifest(
        plan.ref,
        training_slice.dataset_revision,
        training_slice.interval_start,
        training_slice.interval_end,
        feature.feature_schema_hash,
        canonical_sha256(rows),
        len(rows),
    )
    artifact = backtest.ModelArtifactRef(
        model_key=trainer.model_key,
        model_hash=_content_hash("model-artifact"),
        training_data_hash=manifest.training_data_hash,
        training_start=UtcInstant(0),
        training_end=UtcInstant(1_000),
        training_code_hash=trainer.training_code_hash,
        feature_schema_hash=feature.feature_schema_hash,
        available_at=SimulationInstant(
            UtcInstant(1_000),
            TimelinePhase(70, "model_availability"),
            SourceSequence(1),
        ),
        revision_id="genesis",
        supersedes_revision_id=None,
    )
    timeline = backtest.ModelRevisionTimeline(
        model_key=artifact.model_key,
        decision_instant=SimulationInstant(
            UtcInstant(1_000),
            TimelinePhase(70, "model_availability"),
            SourceSequence(2),
        ),
        artifacts=(artifact,),
    )
    inputs = FrozenModelExperimentInputs(
        spec,
        feature,
        trainer,
        plan,
        _policy(spec),
        {"type": "actor_ref", "actor_id": "research"},
        _RESERVED_AT,
        max_attempts,
    )
    return (
        foundation,
        SampleConsumptionLedger(foundation),
        _FixtureModelBuilder(
            foundation,
            manifest,
            artifact,
            fail_feature=fail_feature,
            fail_training_once=fail_training_once,
        ),
        _ModelBacktestOperations(
            foundation,
            tmp_path / "publications",
            timeline,
        ),
        inputs,
        artifact,
    )


def test_real_model_research_golden_replays_exact_ten_task_candidate(
    tmp_path: Path,
) -> None:
    foundation, ledger, builder, provider, inputs, artifact = _model_runtime(tmp_path)

    first = execute_model_experiment(inputs, foundation, ledger, builder, provider)
    source_reads = provider.source_read_calls
    assert source_reads > 0
    ledger_before_replay = foundation.entries(_SAMPLE_LOG)
    second = execute_model_experiment(inputs, foundation, ledger, builder, provider)

    assert type(first) is PublishedStrategyCandidate
    assert second == first
    assert provider.source_read_calls == source_reads
    assert foundation.entries(_SAMPLE_LOG) == ledger_before_replay
    assert builder.feature_calls == 1
    assert builder.training_calls == 1
    assert builder.reservations_before_read == [1, 2]
    assert provider.prepare_calls == 4
    assert provider.reservations_before_prepare == [6]
    assert provider.outcomes_before_prepare == [2]
    assert provider.run_calls == 4
    assert provider.derive_calls == 3
    assert all(
        binding.artifact_ref_hash == artifact.artifact_ref_hash
        for binding in provider.prepared_bindings
    )
    outcomes = _outcome_payloads(foundation)
    assert len(outcomes) == 10
    assert [item["state"] for item in outcomes].count("COMPLETED") == 8
    assert [item["state"] for item in outcomes].count("BLOCKED") == 2
    purposes = [
        json.loads(entry.payload)["payload"]["record"]["purpose"]
        for entry in foundation.entries(_SAMPLE_LOG)
    ]
    assert purposes == ["feature_build", "model_training"] + ["discovery"] * 4 + ["selection"] * 2
    reservations = [json.loads(entry.payload)["payload"] for entry in foundation.entries(_SAMPLE_LOG)]
    assert [item["producer_ref"]["artifact_type"] for item in reservations] == ["feature_build_task", "model_training_task"] + ["trial_declaration"] * 4 + ["selection_declaration"] * 2
    assert len({canonical_bytes(item["producer_ref"]) for item in reservations[2:6]}) == 4
    records = [item["record"] for item in reservations]
    assert len(records) == 8
    assert all(r["dataset_revision"] == "model-evaluation-synthetic-training-v1" for r in records[:2])
    assert all(r["dataset_revision"] == "model-evaluation-synthetic-evaluation-v1" for r in records[2:6])
    assert {r["dataset_revision"] for r in records[6:]} == {"model-evaluation-synthetic-training-v1", "model-evaluation-synthetic-evaluation-v1"}
    from crypto_quant_research import verify_candidate_provenance
    verified = verify_candidate_provenance(first.strategy_candidate_ref, foundation, provider)
    assert verified.experiment_ref["schema_version"] == 3
    assert len(foundation.entries(_SAMPLE_LOG)) == 8
    candidates = [
        json.loads(entry.payload)
        for entry in foundation.entries("research.artifacts.v1")
        if json.loads(entry.payload)["artifact_type"] == "strategy_candidate"
    ]
    assert len(candidates) == 1
    assert candidates[0]["schema_version"] == 2
    assert "model_build_evidence_ref" in candidates[0]["payload"]


def test_transient_model_training_failure_retries_without_duplicate_reservations(
    tmp_path: Path,
) -> None:
    foundation, ledger, builder, provider, inputs, _ = _model_runtime(
        tmp_path,
        fail_training_once=True,
        max_attempts=2,
    )

    result = execute_model_experiment(inputs, foundation, ledger, builder, provider)

    assert type(result) is PublishedStrategyCandidate
    assert builder.feature_calls == 1
    assert builder.training_calls == 2
    assert builder.reservations_before_read == [1, 2, 2]
    assert len(foundation.entries(_SAMPLE_LOG)) == 8
    assert provider.prepare_calls == 4


def test_feature_build_failure_blocks_training_and_all_trials(
    tmp_path: Path,
) -> None:
    foundation, ledger, builder, provider, inputs, _ = _model_runtime(
        tmp_path, fail_feature=True
    )

    result = execute_model_experiment(inputs, foundation, ledger, builder, provider)

    assert type(result).__name__ == "PublishedNoSelection"
    assert builder.feature_calls == 1
    assert builder.training_calls == 0
    assert provider.prepare_calls == 0
    assert provider.run_calls == 0
    outcomes = _outcome_payloads(foundation)
    assert len(outcomes) == 10
    assert [item["state"] for item in outcomes].count("FAILED") == 1
    assert [item["state"] for item in outcomes].count("BLOCKED") == 9


def test_feature_reservation_failure_blocks_every_model_consumer(
    tmp_path: Path,
) -> None:
    foundation, ledger, builder, provider, inputs, _ = _model_runtime(tmp_path)
    reserve = ledger.reserve

    def fail_feature_reservation(record, producer_ref):
        if record.purpose == "feature_build":
            raise RuntimeError("injected reservation failure")
        return reserve(record, producer_ref)

    ledger.reserve = fail_feature_reservation  # type: ignore[method-assign]
    result = execute_model_experiment(inputs, foundation, ledger, builder, provider)

    assert type(result).__name__ == "PublishedNoSelection"
    assert builder.feature_calls == 0
    assert builder.training_calls == 0
    assert provider.prepare_calls == 0
    outcomes = _outcome_payloads(foundation)
    assert len(outcomes) == 10
    assert [item["state"] for item in outcomes].count("BLOCKED") == 10
    assert outcomes[0]["witness"]["dependency_block"]["reason_code"] == (
        "SAMPLE_RESERVATION_FAILED"
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
    execution = _outcome_payloads(foundation)
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
    outcomes = _outcome_payloads(foundation)
    assert all("backtest_terminal" not in item["witness"] for item in outcomes)
    assert any("local_failure" in item["witness"] for item in outcomes)


@pytest.mark.parametrize('same_path', [False, True])
def test_schema3_model_guard_precedes_io(tmp_path, monkeypatch, same_path):
    from crypto_quant_validation import ValidationCoreFailure
    foundation, _, builder, provider, inputs, _ = _model_runtime(tmp_path)
    foreign = LocalFoundation(tmp_path / ('foundation' if same_path else 'foreign'))
    ledger = SampleConsumptionLedger(foreign)
    def forbidden(*args, **kwargs):
        pytest.fail('foreign ledger must fail before any I/O')
    for store in (foundation, foreign):
        for name in ('read', 'entries', 'put', 'append', 'checkpoint', '_clock'):
            monkeypatch.setattr(store, name, forbidden)
    with pytest.raises(ValidationCoreFailure, match='SAMPLE_LEDGER_CONFLICT'):
        execute_model_experiment(inputs, foundation, ledger, builder, provider)
    assert builder.feature_calls == provider.prepare_calls == 0


@pytest.mark.parametrize('failure_index', [1, 3])
def test_discovery_reservation_failure_prevents_all_preparation(tmp_path, failure_index):
    foundation, ledger, builder, provider, inputs, _ = _model_runtime(tmp_path)
    original = ledger.reserve
    calls = 0
    error = RuntimeError('discovery reservation failure')
    def reserve(record, producer):
        nonlocal calls
        if record.purpose == 'discovery':
            calls += 1
            if calls == failure_index:
                raise error
        return original(record, producer)
    ledger.reserve = reserve
    with pytest.raises(RuntimeError) as caught:
        execute_model_experiment(inputs, foundation, ledger, builder, provider)
    assert caught.value is error
    assert provider.prepare_calls == provider.run_calls == provider.source_read_calls == 0
    assert len(foundation.entries(_SAMPLE_LOG)) == 2 + failure_index - 1
    assert not any(json.loads(e.payload)['artifact_type'] == 'strategy_candidate' for e in foundation.entries('research.artifacts.v1'))


def test_legacy_model_also_reserves_all_trials_before_prepare(tmp_path):
    from dataclasses import fields

    foundation, ledger, builder, provider, inputs, _ = _model_runtime(tmp_path)
    values = {field.name: getattr(inputs.experiment_spec, field.name)
              for field in fields(inputs.experiment_spec)}
    values.update(model_build_plan=inputs.model_build_plan, evaluation_slices=None,
                  data_slices=(inputs.model_build_plan.training_slice,))
    spec = ExperimentSpec(**values)
    legacy = FrozenModelExperimentInputs(
        spec, inputs.feature_recipe, inputs.trainer_recipe, inputs.model_build_plan,
        _policy(spec), inputs.selection_declared_by_ref, inputs.reservation_at,
    )
    sentinel = RuntimeError('stop before legacy future-model SDK preparation')

    def prepare(trials, evidence):
        reservations = [json.loads(entry.payload)['payload']
                        for entry in foundation.entries(_SAMPLE_LOG)]
        assert len(reservations) == 6
        assert [item['record']['purpose'] for item in reservations] == [
            'feature_build', 'model_training', *(['discovery'] * 4)
        ]
        assert all(item['record']['dataset_revision'] == inputs.model_build_plan.training_slice.dataset_revision
                   for item in reservations)
        raise sentinel

    provider.prepare_trials = prepare
    with pytest.raises(RuntimeError) as caught:
        execute_model_experiment(legacy, foundation, ledger, builder, provider)
    assert caught.value is sentinel
    assert provider.source_read_calls == provider.run_calls == 0
