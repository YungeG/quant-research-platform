from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import fields
from pathlib import Path
from typing import Any

import pytest
import crypto_quant_research.runtime as research_runtime
from crypto_quant_domain import (
    ArtifactEnvelope,
    ArtifactRef,
    canonical_bytes,
    canonical_sha256,
)
from crypto_quant_foundation import LocalFoundation
from crypto_quant_research import (
    ExperimentParameterCombination as ParameterCombination,
    ExperimentSelectionPolicy as SelectionPolicy,
    ExperimentSpec,
    FeatureRecipe,
    FrozenExperimentInputs,
    FrozenModelExperimentInputs,
    FrozenTargetExperimentInputs,
    HardFilter,
    ModelBuildPlan,
    OrderingCriterion,
    PublishedStrategyCandidate,
    TargetBuildTask,
    TargetMaterializationEvidence,
    TargetRecipe,
    TrainerRecipe,
    TrialExecution,
    execute_target_experiment,
)
from crypto_quant_research.integration import (
    DataSlice,
    ResearchCoreError,
    TargetBuildPublication,
    TaskOutcome,
    TaskRef,
    build_task_universe,
    build_trial_declarations,
)
from crypto_quant_validation import SampleConsumptionLedger

ARTIFACT_LOG = "research.artifacts.v1"
EXECUTION_LOG = "research.execution.v1"
SAMPLE_LOG = "validation.sample-consumption.v1"
RESERVED_AT = "2026-08-26T00:00:00.000000Z"
RECEIVED_AT = "2026-08-26T00:00:01.000000Z"


def _hash(marker: str) -> str:
    return "sha256:" + marker * 64


def _artifact_ref(artifact_type: str, marker: str) -> dict[str, object]:
    return {
        "type": "artifact_ref",
        "artifact_type": artifact_type,
        "schema_version": 1,
        "content_hash": _hash(marker),
    }


def _tagged_ref(tag: str, artifact_type: str, marker: str) -> dict[str, object]:
    return {"type": tag, "artifact_ref": _artifact_ref(artifact_type, marker)}


def _strategy(marker: str = "a") -> dict[str, object]:
    return {
        "type": "build_artifact_ref",
        "role": "decision_source",
        "artifact_key": "fixed-targets",
        "artifact_version": "1.0.0",
        "install_mode": "wheel",
        "source_tree_state": "clean",
        "content_hash": _hash(marker),
        "source_snapshot_hash": None,
    }


def _target_stream(marker: str = "b") -> dict[str, object]:
    instant = {"type": "utc_instant", "epoch_nanoseconds": 100}
    return {
        "type": "precomputed_target_stream",
        "schema_version": 1,
        "stream_key": "targets",
        "events": [
            {
                "type": "market_event",
                "event_id": f"target-{marker}",
                "stream_key": "targets",
                "event_type": "strategy_decision_candidate",
                "capability": {
                    "type": "market_bundle_capability",
                    "key": "precomputed_target_stream",
                    "version": 1,
                },
                "instrument_id": None,
                "event_time": instant,
                "available_time": instant,
                "phase": {
                    "type": "timeline_phase",
                    "rank": 50,
                    "code": "decision",
                },
                "source_sequence": {"type": "source_sequence", "value": 1},
                "revision_id": "genesis",
                "supersedes_revision_id": None,
                "source_key": "fixture",
                "source_hash": _hash(marker),
                "payload": {"schema_version": 1, "candidate": {}},
            }
        ],
    }


def _recipe() -> TargetRecipe:
    return TargetRecipe("fixed-targets", _strategy(), _hash("c"), ("bars.open",))


def _ordinary_spec() -> ExperimentSpec:
    return ExperimentSpec(
        hypothesis_ref=_artifact_ref("hypothesis", "1"),
        strategy_definition_ref=_artifact_ref("strategy_definition", "2"),
        data_slices=(
            DataSlice(
                _tagged_ref(
                    "backtest_market_bundle_ref", "backtest_market_bundle", "3"
                ),
                "fixture-v1",
                "2026-01-01T00:00:00.000000Z",
                "2026-02-01T00:00:00.000000Z",
            ),
        ),
        parameter_combinations=(ParameterCombination((("lookback", "10"),)),),
        seeds=(1,),
        scenario_refs=(_artifact_ref("scenario", "4"),),
        backtest_template_ref=_artifact_ref("backtest_template", "5"),
        model_build_plan=None,
        metric_profile_refs=(_artifact_ref("backtest_metric_profile", "6"),),
        budget={"max_trials": 1},
    )


def _spec(recipe: TargetRecipe | None = None) -> ExperimentSpec:
    ordinary = _ordinary_spec()
    values = {field.name: getattr(ordinary, field.name) for field in fields(ordinary)}
    values["target_recipe_ref"] = (recipe or _recipe()).ref
    return ExperimentSpec(**values)


def _policy(spec: ExperimentSpec) -> SelectionPolicy:
    return SelectionPolicy(
        metric_profile_ref=spec.metric_profile_refs[0],
        eligible_trial_statuses=("COMPLETED",),
        accepted_backtest_grades=("development",),
        hard_filters=(HardFilter("trade_count", "gte", 1),),
        ordering=(OrderingCriterion("simple_period_return", "descending"),),
        max_selections=1,
        tie_break="trial_declaration_ref_ascending",
    )


def _inputs(recipe: TargetRecipe | None = None) -> FrozenTargetExperimentInputs:
    recipe = recipe or _recipe()
    spec = _spec(recipe)
    return FrozenTargetExperimentInputs(
        spec,
        recipe,
        _policy(spec),
        {"type": "actor_ref", "actor_id": "research"},
        RESERVED_AT,
    )


def _payload(foundation: LocalFoundation, ref: object) -> dict[str, object]:
    return json.loads(foundation.read(ref=ref).source_bytes)["payload"]


def _outcomes(foundation: LocalFoundation) -> list[dict[str, object]]:
    return [
        json.loads(entry.payload)["payload"]
        for entry in foundation.entries(EXECUTION_LOG)
        if json.loads(entry.payload)["artifact_type"] == "task_outcome"
    ]


class _Materializer:
    strategy_artifact = _strategy()

    def __init__(self, foundation: LocalFoundation) -> None:
        self.foundation = foundation
        self.calls = 0
        self.requests: list[dict[str, object]] = []
        self.mutate: str | None = None

    def materialize_target(self, request: dict[str, object]) -> dict[str, object]:
        self.calls += 1
        self.requests.append(deepcopy(request))
        assert len(self.foundation.entries(SAMPLE_LOG)) >= 1
        result = {
            "type": "target_materialization_result",
            "schema_version": 1,
            "request_hash": canonical_sha256(request),
            "strategy_artifact": self.strategy_artifact,
            "input_data_hash": _hash("d"),
            "target_stream": _target_stream(),
        }
        if self.mutate == "extra":
            result["extra"] = True
        elif self.mutate == "artifact":
            result["strategy_artifact"] = _strategy("f")
        elif self.mutate == "request":
            result["request_hash"] = _hash("f")
        elif self.mutate == "failure":
            raise RuntimeError("materializer failed")
        return result


class _Backtest:
    def __init__(self) -> None:
        self.store_calls = 0
        self.load_target_calls = 0
        self.prepare_calls = 0
        self.run_calls = 0
        self.economic_run_calls = 0
        self.cache_calls = 0
        self.derive_calls = 0
        self.store_mutation: str | None = None
        self.load_mutation: str | None = None
        self.prepare_failure = False
        self.terminal_status: str | None = None
        self._targets: dict[str, dict[str, object]] = {}
        self._runs: dict[str, object] = {}
        self._publication = _tagged_ref(
            "backtest_canonical_publication_ref",
            "canonical_publication_manifest",
            "7",
        )
        self._analysis = _tagged_ref(
            "analysis_artifact_ref", "backtest_analysis", "8"
        )

    def publish_target(
        self, producer_context_ref: dict[str, object], target_stream: dict[str, object]
    ) -> dict[str, object]:
        self.store_calls += 1
        if self.store_mutation == "failure":
            raise RuntimeError("store failed")
        envelope = ArtifactEnvelope.create(
            "backtest_target_stream",
            1,
            {
                "producer_context_ref": producer_context_ref,
                "target_stream": target_stream,
            },
        )
        artifact_ref = ArtifactRef.from_envelope(envelope)
        ref = {
            "type": "backtest_target_stream_ref",
            "artifact_ref": artifact_ref.to_canonical_dict(),
        }
        if self.store_mutation == "ref":
            ref = _tagged_ref("analysis_artifact_ref", "backtest_analysis", "f")
        self._targets[canonical_bytes(ref).decode()] = {
            "ref": ref,
            "producer_context_ref": producer_context_ref,
            "target_stream": target_stream,
            "digest": canonical_sha256(target_stream),
        }
        return ref

    def load_target(self, ref: dict[str, object]) -> dict[str, object]:
        self.load_target_calls += 1
        if self.load_mutation == "failure":
            raise RuntimeError("load failed")
        loaded = deepcopy(self._targets[canonical_bytes(ref).decode()])
        mutation = self.load_mutation or self.store_mutation
        if mutation == "producer":
            loaded["producer_context_ref"] = _artifact_ref("trial_declaration", "f")
        elif mutation == "stream":
            loaded["target_stream"] = _target_stream("e")
        elif mutation == "digest":
            loaded["digest"] = _hash("f")
        elif mutation == "count":
            loaded["event_count"] = 2
        return loaded

    def prepare_trials(self, trials, target_ref) -> tuple[TrialExecution, ...]:
        self.prepare_calls += 1
        if self.prepare_failure:
            raise RuntimeError("prepare failed")
        trial = trials[0]
        assert canonical_bytes(target_ref).decode() in self._targets
        return (
            TrialExecution(
                trial.ref,
                {"trial": trial.ref},
                {"type": "backtest_request_ref", "id": trial.ref},
            ),
        )

    def run(self, request: dict[str, object]) -> dict[str, object]:
        self.run_calls += 1
        key = canonical_bytes(request).decode()
        if key in self._runs:
            self.cache_calls += 1
            return deepcopy(self._runs[key])
        self.economic_run_calls += 1
        result = (
            _artifact_ref("blocked_attempt_report", "e")
            if self.terminal_status is not None
            else self._publication
        )
        self._runs[key] = deepcopy(result)
        return result

    def load_completed(self, ref: object) -> dict[str, object]:
        return {
            "publication_ref": self._publication,
            "semantic_run_id": "run_fixture",
            "execution_result_hash": _hash("a"),
            "result_grade": "development",
        }

    def load_terminal(self, ref: object) -> dict[str, object]:
        return {
            "status": self.terminal_status,
            "durable_evidence_ref": _artifact_ref("blocked_attempt_report", "e"),
        }

    def derive(self, publication_ref: object, metric_profile_ref: object) -> object:
        self.derive_calls += 1
        return self._analysis

    def load_analysis(self, ref: object) -> dict[str, object]:
        return {
            "analysis_ref": self._analysis,
            "metric_profile_ref": _artifact_ref("backtest_metric_profile", "6"),
            "source_publication_ref": self._publication,
            "source_execution_result_hash": _hash("a"),
            "simple_period_return": "0.1",
            "trade_count": 1,
            "result_grade": "development",
        }


def _runtime(tmp_path: Path):
    foundation = LocalFoundation(tmp_path, clock=lambda: RECEIVED_AT)
    return (
        foundation,
        SampleConsumptionLedger(foundation),
        _Materializer(foundation),
        _Backtest(),
    )


def test_target_spec_is_additive_and_build_task_exact_cover() -> None:
    ordinary = _ordinary_spec()
    target = _spec()
    tasks = build_task_universe(target)

    assert ordinary.schema_version == 1
    assert "target_recipe_ref" not in ordinary.payload
    assert target.schema_version == 2
    assert set(target.payload) == set(ordinary.payload) | {"target_recipe_ref"}
    assert [task.kind for task in tasks] == ["TARGET_BUILD", "ANALYSIS", "TRIAL"]
    assert type(tasks[0].artifact) is TargetBuildTask
    assert tasks[0].artifact.trial_declaration_ref == build_trial_declarations(target)[0].ref


def test_target_recipe_and_evidence_are_exact_structural_wires() -> None:
    recipe = _recipe()
    task = next(task for task in build_task_universe(_spec(recipe)) if task.kind == "TARGET_BUILD")
    evidence = TargetMaterializationEvidence(
        task.task_artifact_ref,
        task.artifact.trial_declaration_ref,
        recipe.ref,
        _hash("1"),
        _hash("2"),
        _tagged_ref("backtest_target_stream_ref", "backtest_target_stream", "3"),
        _hash("4"),
        1,
    )
    outcome = TaskOutcome(task, "COMPLETED", TargetBuildPublication(evidence.ref))

    assert set(recipe.payload) == {
        "target_key",
        "strategy_artifact",
        "target_schema_hash",
        "input_names",
    }
    assert set(evidence.payload) == {
        "target_build_task_ref",
        "trial_declaration_ref",
        "target_recipe_ref",
        "materialization_request_hash",
        "input_data_hash",
        "target_stream_ref",
        "target_stream_digest",
        "event_count",
    }
    assert outcome.task_ref == TaskRef("TARGET_BUILD", task.task_artifact_ref)


def test_target_shell_orders_reservation_materialization_store_prepare_and_candidate(
    tmp_path: Path,
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)

    result = execute_target_experiment(
        _inputs(), foundation, ledger, materializer, backtest
    )

    assert type(result) is PublishedStrategyCandidate
    assert materializer.calls == backtest.store_calls == backtest.prepare_calls == 1
    assert backtest.run_calls == backtest.derive_calls == 1
    assert len(foundation.entries(SAMPLE_LOG)) == 2
    assert set(materializer.requests[0]) == {
        "type",
        "schema_version",
        "consumer_ref",
        "target_recipe_ref",
        "market_bundle_ref",
        "dataset_revision",
        "interval_start",
        "interval_end",
        "parameter_values",
        "seed",
    }
    candidate = _payload(foundation, result.strategy_candidate_ref)
    assert result.strategy_candidate_ref.schema_version == 3
    assert "selected_target_materialization_evidence_ref" in candidate
    evidence_ref = candidate["selected_target_materialization_evidence_ref"]
    evidence = _payload(
        foundation,
        type(result.strategy_candidate_ref)(
            evidence_ref["artifact_type"],
            evidence_ref["schema_version"],
            evidence_ref["content_hash"],
        ),
    )
    assert evidence["trial_declaration_ref"] == candidate["selected_trial_declaration_ref"]


def test_evidence_commit_recovery_never_rematerializes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    publish = _interrupt_after_evidence(monkeypatch)
    with pytest.raises(KeyboardInterrupt):
        execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)
    monkeypatch.setattr(research_runtime, "_publish", publish)

    result = execute_target_experiment(
        _inputs(), foundation, ledger, materializer, backtest
    )

    assert type(result) is PublishedStrategyCandidate
    assert materializer.calls == backtest.store_calls == backtest.prepare_calls == 1
    assert backtest.run_calls == backtest.economic_run_calls == 1
    assert backtest.cache_calls == 0


def test_closed_target_replay_never_calls_external_target_or_run_operations(
    tmp_path: Path,
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    inputs = _inputs()
    first = execute_target_experiment(inputs, foundation, ledger, materializer, backtest)
    counters = (
        materializer.calls,
        backtest.store_calls,
        backtest.prepare_calls,
        backtest.run_calls,
        backtest.derive_calls,
    )

    second = execute_target_experiment(inputs, foundation, ledger, materializer, backtest)

    assert second == first
    assert counters == (
        materializer.calls,
        backtest.store_calls,
        backtest.prepare_calls,
        backtest.run_calls,
        backtest.derive_calls,
    )


def test_target_cas_orphan_retries_materialization_but_only_one_economic_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    publish_target = backtest.publish_target
    interrupted = False

    def orphan(context: dict[str, object], stream: dict[str, object]):
        nonlocal interrupted
        ref = publish_target(context, stream)
        if not interrupted:
            interrupted = True
            raise KeyboardInterrupt("after target CAS")
        return ref

    monkeypatch.setattr(backtest, "publish_target", orphan)
    with pytest.raises(KeyboardInterrupt):
        execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)
    monkeypatch.setattr(backtest, "publish_target", publish_target)
    execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)

    assert materializer.calls == backtest.store_calls == 2
    assert backtest.prepare_calls == 1
    assert backtest.run_calls == backtest.economic_run_calls == 1
    assert backtest.cache_calls == 0
    assert len(foundation.entries(SAMPLE_LOG)) == 2


def test_interruption_after_preparation_repeats_only_idempotent_preparation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    prepare = backtest.prepare_trials
    interrupted = False

    def interrupt(trials: object, target_ref: object):
        nonlocal interrupted
        execution = prepare(trials, target_ref)
        if not interrupted:
            interrupted = True
            raise KeyboardInterrupt("after preparation")
        return execution

    monkeypatch.setattr(backtest, "prepare_trials", interrupt)
    with pytest.raises(KeyboardInterrupt):
        execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)
    execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)

    assert materializer.calls == backtest.store_calls == 1
    assert backtest.prepare_calls == 2
    assert backtest.run_calls == backtest.economic_run_calls == 1
    assert backtest.cache_calls == 0


def test_interruption_after_run_return_uses_durable_backtest_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    run = backtest.run
    interrupted = False

    def interrupt(request: dict[str, object]):
        nonlocal interrupted
        result = run(request)
        if not interrupted:
            interrupted = True
            raise KeyboardInterrupt("after run return")
        return result

    monkeypatch.setattr(backtest, "run", interrupt)
    with pytest.raises(KeyboardInterrupt):
        execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)
    execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)

    assert materializer.calls == backtest.store_calls == 1
    assert backtest.prepare_calls == 2
    assert backtest.run_calls == 2
    assert backtest.economic_run_calls == backtest.cache_calls == 1


def test_interruption_after_trial_outcome_does_not_rerun(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    publish = research_runtime._publish
    interrupted = False

    def interrupt(*args: object, **kwargs: object):
        nonlocal interrupted
        result = publish(*args, **kwargs)
        payload = args[3] if len(args) > 3 else kwargs.get("payload")
        if (
            args[2] == "task_outcome"
            and isinstance(payload, dict)
            and payload.get("task_ref", {}).get("kind") == "TRIAL"
            and not interrupted
        ):
            interrupted = True
            raise KeyboardInterrupt("after trial outcome")
        return result

    monkeypatch.setattr(research_runtime, "_publish", interrupt)
    with pytest.raises(KeyboardInterrupt):
        execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)
    monkeypatch.setattr(research_runtime, "_publish", publish)
    execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)

    assert materializer.calls == backtest.store_calls == 1
    assert backtest.prepare_calls == 2
    assert backtest.run_calls == backtest.economic_run_calls == 1
    assert backtest.cache_calls == 0


def test_interruption_after_manifest_replays_without_preparation_or_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    publish = research_runtime._publish
    interrupted = False

    def interrupt(*args: object, **kwargs: object):
        nonlocal interrupted
        result = publish(*args, **kwargs)
        if args[2] == "experiment_execution_manifest" and not interrupted:
            interrupted = True
            raise KeyboardInterrupt("after manifest")
        return result

    monkeypatch.setattr(research_runtime, "_publish", interrupt)
    with pytest.raises(KeyboardInterrupt):
        execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)
    counters = (
        materializer.calls,
        backtest.store_calls,
        backtest.prepare_calls,
        backtest.run_calls,
        backtest.economic_run_calls,
    )
    monkeypatch.setattr(research_runtime, "_publish", publish)
    execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)
    assert counters == (
        materializer.calls,
        backtest.store_calls,
        backtest.prepare_calls,
        backtest.run_calls,
        backtest.economic_run_calls,
    )


@pytest.mark.parametrize("mutation", ("extra", "artifact", "request", "failure"))
def test_materializer_malformed_failure_and_artifact_mismatch_are_terminal(
    tmp_path: Path, mutation: str
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    materializer.mutate = mutation

    result = execute_target_experiment(
        _inputs(), foundation, ledger, materializer, backtest
    )

    assert type(result).__name__ == "PublishedNoSelection"
    assert backtest.store_calls == backtest.prepare_calls == backtest.run_calls == 0


@pytest.mark.parametrize(
    "mutation", ("ref", "producer", "stream", "digest", "failure")
)
def test_target_store_substitution_digest_and_failure_block_preparation(
    tmp_path: Path, mutation: str
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    backtest.store_mutation = mutation

    result = execute_target_experiment(
        _inputs(), foundation, ledger, materializer, backtest
    )

    assert type(result).__name__ == "PublishedNoSelection"
    assert backtest.prepare_calls == backtest.run_calls == 0


def test_target_backtest_terminal_blocks_analysis_without_derivation(
    tmp_path: Path,
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    backtest.terminal_status = "BLOCKED"

    result = execute_target_experiment(
        _inputs(), foundation, ledger, materializer, backtest
    )

    assert type(result).__name__ == "PublishedNoSelection"
    assert backtest.run_calls == 1
    assert backtest.derive_calls == 0


def test_target_preparation_failure_is_a_terminal_trial_failure(tmp_path: Path) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    backtest.prepare_failure = True

    result = execute_target_experiment(
        _inputs(), foundation, ledger, materializer, backtest
    )

    assert type(result).__name__ == "PublishedNoSelection"
    assert materializer.calls == backtest.store_calls == backtest.prepare_calls == 1
    assert backtest.run_calls == 0
    outcomes = [
        json.loads(entry.payload)["payload"]
        for entry in foundation.entries(EXECUTION_LOG)
        if json.loads(entry.payload)["artifact_type"] == "task_outcome"
    ]
    assert [item["state"] for item in outcomes] == [
        "COMPLETED",
        "FAILED",
        "BLOCKED",
    ]


def test_reservation_failure_is_a_barrier_before_materialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)

    reserve = ledger.reserve

    def fail_reservation(record: object, producer_ref: object) -> None:
        if getattr(producer_ref, "artifact_type", None) == "trial_declaration":
            raise RuntimeError("reservation unavailable")
        reserve(record, producer_ref)

    monkeypatch.setattr(ledger, "reserve", fail_reservation)
    result = execute_target_experiment(
        _inputs(), foundation, ledger, materializer, backtest
    )

    assert type(result).__name__ == "PublishedNoSelection"
    assert materializer.calls == backtest.store_calls == backtest.prepare_calls == 0


def test_target_mode_rejects_model_combination_and_materializer_artifact_mismatch(
    tmp_path: Path,
) -> None:
    recipe = _recipe()
    spec = _spec(recipe)
    object.__setattr__(spec, "model_build_plan", "foreign")
    with pytest.raises(ResearchCoreError):
        FrozenTargetExperimentInputs(
            spec,
            recipe,
            _policy(spec),
            {"type": "actor_ref", "actor_id": "research"},
            RESERVED_AT,
        )

    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    materializer.strategy_artifact = _strategy("f")
    with pytest.raises(ResearchCoreError) as raised:
        execute_target_experiment(
            _inputs(), foundation, ledger, materializer, backtest
        )
    assert raised.value.code == "TARGET_RECIPE_INVALID"
    assert foundation.entries(ARTIFACT_LOG) == ()


def test_schema_dispatch_rejects_wrong_entry_before_publication(tmp_path: Path) -> None:
    foundation, _, _, _ = _runtime(tmp_path)
    target = _spec()
    trial = build_trial_declarations(target)[0]
    with pytest.raises(ResearchCoreError) as raised:
        FrozenExperimentInputs(
            target,
            _policy(target),
            {"type": "actor_ref", "actor_id": "research"},
            (
                TrialExecution(
                    trial.ref,
                    {"trial": trial.ref},
                    {"type": "backtest_request_ref", "id": trial.ref},
                ),
            ),
            RESERVED_AT,
        )
    assert raised.value.code == "EXPERIMENT_SPEC_INVALID"
    wrong_target = _spec()
    object.__setattr__(wrong_target, "target_recipe_ref", None)
    with pytest.raises(ResearchCoreError) as target_raised:
        FrozenTargetExperimentInputs(
            wrong_target,
            _recipe(),
            _policy(wrong_target),
            {"type": "actor_ref", "actor_id": "research"},
            RESERVED_AT,
        )
    assert target_raised.value.code == "TARGET_RECIPE_INVALID"
    with pytest.raises(ValueError, match="trial_declaration@2"):
        research_runtime._publish(
            foundation,
            ARTIFACT_LOG,
            "trial_declaration",
            {"wrong": "entry"},
            schema_version=2,
        )
    assert foundation.entries(ARTIFACT_LOG) == ()


def test_model_inputs_reject_target_spec_before_publication(tmp_path: Path) -> None:
    foundation, _, _, _ = _runtime(tmp_path)
    feature = FeatureRecipe("features", _hash("1"), _hash("2"), ("bars.open",))
    trainer = TrainerRecipe("trainer", _hash("3"), "model", {})
    ordinary = _ordinary_spec()
    plan = ModelBuildPlan(feature.ref, trainer.ref, ordinary.data_slices[0], 1)
    values = {field.name: getattr(ordinary, field.name) for field in fields(ordinary)}
    values["model_build_plan"] = plan
    model_spec = ExperimentSpec(**values)
    object.__setattr__(model_spec, "target_recipe_ref", _recipe().ref)
    with pytest.raises(ResearchCoreError) as raised:
        FrozenModelExperimentInputs(
            model_spec,
            feature,
            trainer,
            plan,
            _policy(model_spec),
            {"type": "actor_ref", "actor_id": "research"},
            RESERVED_AT,
        )
    assert raised.value.code == "MODEL_BUILD_PLAN_INVALID"
    assert foundation.entries(ARTIFACT_LOG) == ()


def test_candidate_v3_carries_full_target_recipe_task_request_ref_digest_chain(
    tmp_path: Path,
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    result = execute_target_experiment(
        _inputs(), foundation, ledger, materializer, backtest
    )
    assert type(result) is PublishedStrategyCandidate
    candidate = _payload(foundation, result.strategy_candidate_ref)
    evidence_wire = candidate["selected_target_materialization_evidence_ref"]
    evidence_ref = ArtifactRef(
        evidence_wire["artifact_type"],
        evidence_wire["schema_version"],
        evidence_wire["content_hash"],
    )
    evidence = _payload(foundation, evidence_ref)
    target_task_ref = evidence["target_build_task_ref"]
    target_task = _payload(
        foundation,
        ArtifactRef(
            target_task_ref["artifact_type"],
            target_task_ref["schema_version"],
            target_task_ref["content_hash"],
        ),
    )
    assert target_task["trial_declaration_ref"] == candidate[
        "selected_trial_declaration_ref"
    ]
    assert target_task["target_recipe_ref"] == evidence["target_recipe_ref"]
    assert evidence["materialization_request_hash"] == canonical_sha256(
        materializer.requests[0]
    )
    loaded = backtest.load_target(evidence["target_stream_ref"])
    assert loaded["digest"] == evidence["target_stream_digest"]
    assert len(loaded["target_stream"]["events"]) == evidence["event_count"]


def _interrupt_after_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> object:
    publish = research_runtime._publish
    interrupted = False

    def interrupt(*args: object, **kwargs: object):
        nonlocal interrupted
        result = publish(*args, **kwargs)
        if args[2] == "target_materialization_evidence" and not interrupted:
            interrupted = True
            raise KeyboardInterrupt("after evidence commit")
        return result

    monkeypatch.setattr(research_runtime, "_publish", interrupt)
    return publish


@pytest.mark.parametrize("mutation", ("producer", "stream", "digest", "count", "failure"))
def test_recovery_exact_load_rejects_tampered_or_stale_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    publish = _interrupt_after_evidence(monkeypatch)
    with pytest.raises(KeyboardInterrupt):
        execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)
    monkeypatch.setattr(research_runtime, "_publish", publish)
    backtest.load_mutation = mutation
    with pytest.raises(ResearchCoreError) as raised:
        execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)
    assert raised.value.code == "TARGET_MATERIALIZATION_INVALID"
    assert materializer.calls == backtest.store_calls == 1


def _seed_evidence_without_valid_reservation(
    foundation: LocalFoundation,
    ledger: SampleConsumptionLedger,
    backtest: _Backtest,
    mode: str,
) -> None:
    inputs = _inputs()
    base = research_runtime._publish_target_base(inputs, foundation)
    trial = base.trials[0]
    task = next(task for task in base.universe if task.kind == "TARGET_BUILD")
    target_stream = _target_stream()
    target_ref = backtest.publish_target(
        base.refs[trial.ref].to_canonical_dict(), target_stream
    )
    request_hash = canonical_sha256(research_runtime._target_request(base, trial))
    evidence = TargetMaterializationEvidence(
        task.task_artifact_ref,
        trial.ref,
        inputs.target_recipe.ref,
        request_hash,
        _hash("d"),
        target_ref,
        canonical_sha256(target_stream),
        1,
    )
    if mode == "forged":
        forged = ArtifactEnvelope.create(
            "sample_consumption_append",
            1,
            {
                "record": {
                    "dataset_revision": trial.data_slice.dataset_revision,
                    "interval_start": trial.data_slice.interval_start,
                    "interval_end": trial.data_slice.interval_end,
                    "purpose": "discovery",
                    "consumer_id": _hash("f"),
                    "consumed_at": RESERVED_AT,
                },
                "producer_ref": base.refs[trial.ref],
            },
        )
        foundation.append(SAMPLE_LOG, _hash("e"), canonical_bytes(forged))
    if mode == "wrong_event_id":
        foundation.append(
            SAMPLE_LOG,
            _hash("e"),
            research_runtime._target_reservation_bytes(base, trial),
        )
    research_runtime._publish(
        foundation,
        ARTIFACT_LOG,
        "target_materialization_evidence",
        research_runtime._translate(evidence.payload, base.refs),
    )
    if mode == "late":
        research_runtime._reserve(
            ledger, base.refs[trial.ref], trial, "discovery", RESERVED_AT
        )


@pytest.mark.parametrize(
    "mode", ("missing", "forged", "wrong_event_id", "late")
)
def test_recovery_requires_exact_preceding_trial_reservation(
    tmp_path: Path, mode: str
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    _seed_evidence_without_valid_reservation(foundation, ledger, backtest, mode)
    with pytest.raises(ResearchCoreError) as raised:
        execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)
    assert raised.value.code == "TARGET_MATERIALIZATION_INVALID"
    assert materializer.calls == 0


def test_recovery_recomputes_request_hash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    publish = _interrupt_after_evidence(monkeypatch)
    with pytest.raises(KeyboardInterrupt):
        execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)
    monkeypatch.setattr(research_runtime, "_publish", publish)
    evidence_entry = next(
        entry
        for entry in foundation.entries(ARTIFACT_LOG)
        if json.loads(entry.payload)["artifact_type"] == "target_materialization_evidence"
    )
    envelope = json.loads(evidence_entry.payload)
    envelope["payload"]["materialization_request_hash"] = _hash("f")
    research_runtime._publish(
        foundation,
        ARTIFACT_LOG,
        "target_materialization_evidence",
        envelope["payload"],
    )
    with pytest.raises(ResearchCoreError) as raised:
        execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)
    assert raised.value.code == "TARGET_MATERIALIZATION_INVALID"


def test_boundary_failures_have_exact_task_outcome_witness_codes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cases = (
        ("materializer", "TARGET_MATERIALIZATION_INVALID"),
        ("store", "TARGET_STORE_INVALID"),
        ("evidence", "TARGET_EVIDENCE_PUBLICATION_FAILED"),
        ("preparation", "TARGET_PREPARATION_FAILED"),
    )
    for index, (boundary, code) in enumerate(cases):
        foundation, ledger, materializer, backtest = _runtime(tmp_path / str(index))
        if boundary == "materializer":
            materializer.mutate = "failure"
        elif boundary == "store":
            backtest.store_mutation = "failure"
        elif boundary == "preparation":
            backtest.prepare_failure = True
        else:
            publish = research_runtime._publish

            def fail_evidence(*args: object, **kwargs: object):
                if args[2] == "target_materialization_evidence":
                    raise RuntimeError("evidence unavailable")
                return publish(*args, **kwargs)

            monkeypatch.setattr(research_runtime, "_publish", fail_evidence)
        execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)
        if boundary == "evidence":
            monkeypatch.setattr(research_runtime, "_publish", publish)
        outcomes = _outcomes(foundation)
        failure = next(
            item
            for item in outcomes
            if item["witness"].get("local_failure") is not None
        )
        assert failure["witness"]["local_failure"]["failure_code"] == code


@pytest.mark.parametrize("status", ("BLOCKED", "FAILED", "CANCELLED"))
def test_backtest_terminal_status_maps_exactly(
    tmp_path: Path, status: str
) -> None:
    foundation, ledger, materializer, backtest = _runtime(tmp_path)
    backtest.terminal_status = status
    execute_target_experiment(_inputs(), foundation, ledger, materializer, backtest)
    trial = next(
        item
        for item in _outcomes(foundation)
        if item["task_ref"]["kind"] == "TRIAL"
    )
    assert trial["state"] == status
    assert trial["witness"]["backtest_terminal"]["status"] == status
