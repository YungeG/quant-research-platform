from __future__ import annotations

import hashlib
import json
from dataclasses import fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import crypto_quant_research as public
import pytest
from crypto_quant_research.integration import (
    AnalysisTask,
    DataSlice,
    DependencyBlock,
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
    ParameterCombination,
    ResearchCoreError,
    TaskOutcome,
    TaskRef,
    TrainerRecipe,
    TrialDeclaration,
    UpstreamTaskOutcome,
    build_execution_manifest,
    build_task_universe,
    validate_model_build,
)


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


def _slice() -> DataSlice:
    return DataSlice(
        _tagged_ref("backtest_market_bundle_ref", "backtest_market_bundle", "1"),
        "dataset-1",
        "2026-01-01T00:00:00.000000Z",
        "2026-02-01T00:00:00.000000Z",
    )


def _recipes() -> tuple[FeatureRecipe, TrainerRecipe, ModelBuildPlan]:
    feature = FeatureRecipe("returns-v1", _hash("a"), _hash("b"), ("close",))
    trainer = TrainerRecipe(
        "linear-v1", _hash("c"), "alpha.primary", {"ridge": "0.1"}
    )
    return feature, trainer, ModelBuildPlan(feature.ref, trainer.ref, _slice(), 7)


def _spec(model_build_plan: object | None = None) -> ExperimentSpec:
    return ExperimentSpec(
        hypothesis_ref=_artifact_ref("hypothesis", "1"),
        strategy_definition_ref=_artifact_ref("strategy_definition", "1"),
        data_slices=(_slice(),),
        parameter_combinations=(
            ParameterCombination((("lookback", "10"),)),
            ParameterCombination((("lookback", "20"),)),
        ),
        seeds=(1, 2),
        scenario_refs=(_artifact_ref("scenario", "1"),),
        backtest_template_ref=_artifact_ref("backtest_template", "1"),
        model_build_plan=model_build_plan,
        metric_profile_refs=(
            {
                "type": "artifact_ref",
                "artifact_type": "backtest_metric_profile",
                "schema_version": 1,
                "content_hash": "sha256:3f65ba3e85739e6a71298b13738afbf78dd5c5f98d17eb4e42c13b2d607bae2a",
            },
        ),
        budget={"max_trials": 4},
    )


def _epoch_nanoseconds(value: str) -> int:
    instant = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    return int(instant.timestamp()) * 1_000_000_000 + instant.microsecond * 1_000


def _model_artifact(
    feature: FeatureRecipe,
    trainer: TrainerRecipe,
    manifest: FeatureDatasetManifest,
    **overrides: object,
) -> dict[str, object]:
    body: dict[str, object] = {
        "type": "model_artifact_ref",
        "schema_version": 1,
        "model_key": trainer.model_key,
        "model_hash": _hash("d"),
        "training_data_hash": manifest.training_data_hash,
        "training_start": {
            "type": "utc_instant",
            "epoch_nanoseconds": _epoch_nanoseconds(manifest.interval_start),
        },
        "training_end": {
            "type": "utc_instant",
            "epoch_nanoseconds": _epoch_nanoseconds(manifest.interval_end),
        },
        "training_code_hash": trainer.training_code_hash,
        "feature_schema_hash": feature.feature_schema_hash,
        "available_at": {
            "type": "simulation_instant",
            "instant": {
                "type": "utc_instant",
                "epoch_nanoseconds": _epoch_nanoseconds(manifest.interval_end),
            },
            "phase": {
                "type": "timeline_phase",
                "rank": 70,
                "code": "model_availability",
            },
            "source_sequence": {"type": "source_sequence", "value": 1},
        },
        "revision_id": "genesis",
        "supersedes_revision_id": None,
    }
    body.update(overrides)
    artifact = dict(body)
    artifact["artifact_ref_hash"] = "sha256:" + hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return artifact


def _manifest(plan: ModelBuildPlan, feature: FeatureRecipe) -> FeatureDatasetManifest:
    return FeatureDatasetManifest(
        plan.ref,
        plan.training_slice.dataset_revision,
        plan.training_slice.interval_start,
        plan.training_slice.interval_end,
        feature.feature_schema_hash,
        _hash("e"),
        100,
    )


def _assert_code(code: str, action: Any) -> None:
    with pytest.raises(ResearchCoreError) as caught:
        action()
    assert caught.value.code == code


def test_null_plan_keeps_v1_experiment_and_task_identity() -> None:
    spec = _spec()
    wires = "\n".join(task.canonical_wire for task in build_task_universe(spec))

    assert spec.ref == (
        "rp-core:experiment_spec@1:"
        "sha256:77d5e939e805e2cea4bb4687e28f91273f108eb3f788dc2104a36bb19ace8bc3"
    )
    assert hashlib.sha256(wires.encode()).hexdigest() == (
        "57db1b458ea6009cf708767a6fb732cd5bb479619777124cb559d935eff20d2a"
    )


def test_non_null_plan_adds_two_predeclared_tasks_and_model_binding() -> None:
    feature, trainer, plan = _recipes()
    spec = _spec(plan)
    tasks = build_task_universe(spec)

    assert len(tasks) == 10
    assert [task.kind for task in tasks[:2]] == ["FEATURE_BUILD", "MODEL_TRAINING"]
    assert sum(task.kind == "TRIAL" for task in tasks) == 4
    assert sum(task.kind == "ANALYSIS" for task in tasks) == 4
    feature_task = tasks[0].artifact
    training_task = tasks[1].artifact
    assert type(feature_task) is FeatureBuildTask
    assert type(training_task) is ModelTrainingTask
    assert training_task.feature_build_task_ref == feature_task.ref
    assert all(
        task.artifact.model_input_bindings == (("primary_model", plan.ref),)
        for task in tasks
        if task.kind == "TRIAL" and type(task.artifact) is TrialDeclaration
    )

    changed = FeatureRecipe(
        feature.feature_key,
        _hash("f"),
        feature.feature_schema_hash,
        feature.input_names,
    )
    assert changed.ref != feature.ref
    assert ModelBuildPlan(changed.ref, trainer.ref, plan.training_slice, plan.seed).ref != plan.ref
    _assert_code(
        "MODEL_BUILD_PLAN_INVALID",
        lambda: FeatureRecipe("returns-v1", _hash("a"), _hash("b"), ("close", "close")),
    )
    foreign_slice = DataSlice(
        plan.training_slice.market_bundle_ref,
        "foreign",
        plan.training_slice.interval_start,
        plan.training_slice.interval_end,
    )
    _assert_code(
        "MODEL_BUILD_PLAN_INVALID",
        lambda: _spec(ModelBuildPlan(feature.ref, trainer.ref, foreign_slice, 7)),
    )


def test_validate_model_build_binds_recipes_interval_and_owner_model_wire() -> None:
    feature, trainer, plan = _recipes()
    manifest = _manifest(plan, feature)
    artifact = _model_artifact(feature, trainer, manifest)

    evidence = validate_model_build(plan, feature, trainer, manifest, artifact)

    assert type(evidence) is ModelBuildEvidence
    assert evidence.model_build_plan_ref == plan.ref
    assert evidence.feature_dataset_manifest_ref == manifest.ref
    assert evidence.model_artifact["artifact_ref_hash"] == artifact["artifact_ref_hash"]
    assert {field.name for field in fields(ModelBuildEvidence)} == {
        "model_build_plan_ref",
        "feature_dataset_manifest_ref",
        "model_artifact",
    }

    wrong_feature = FeatureRecipe("other", _hash("a"), _hash("b"), ("close",))
    _assert_code(
        "MODEL_BINDING_INVALID",
        lambda: validate_model_build(plan, wrong_feature, trainer, manifest, artifact),
    )
    wrong_manifest = FeatureDatasetManifest(
        plan.ref,
        "other-revision",
        manifest.interval_start,
        manifest.interval_end,
        manifest.feature_schema_hash,
        manifest.training_data_hash,
        manifest.row_count,
    )
    _assert_code(
        "MODEL_BINDING_INVALID",
        lambda: validate_model_build(
            plan, feature, trainer, wrong_manifest, artifact
        ),
    )

    for overrides in (
        {"schema_version": 2},
        {"model_key": "wrong"},
        {"training_data_hash": _hash("f")},
        {"training_code_hash": _hash("f")},
        {"feature_schema_hash": _hash("f")},
        {"supersedes_revision_id": "prior"},
        {
            "training_end": {
                "type": "utc_instant",
                "epoch_nanoseconds": _epoch_nanoseconds(manifest.interval_end) - 1_000,
            }
        },
    ):
        mutated = _model_artifact(feature, trainer, manifest, **overrides)
        _assert_code(
            "MODEL_BINDING_INVALID",
            lambda mutated=mutated: validate_model_build(
                plan, feature, trainer, manifest, mutated
            ),
        )

    malformed = _model_artifact(feature, trainer, manifest)
    malformed["artifact_ref_hash"] = _hash("0")
    _assert_code(
        "MODEL_BINDING_INVALID",
        lambda: validate_model_build(plan, feature, trainer, manifest, malformed),
    )


def test_ten_task_manifest_exact_cover_and_build_failure_propagation() -> None:
    feature, trainer, plan = _recipes()
    spec = _spec(plan)
    tasks = build_task_universe(spec)
    feature_task, training_task = tasks[:2]
    manifest = _manifest(plan, feature)
    evidence = validate_model_build(
        plan, feature, trainer, manifest, _model_artifact(feature, trainer, manifest)
    )

    outcomes: dict[TaskRef, TaskOutcome] = {
        feature_task: TaskOutcome(
            feature_task, "COMPLETED", FeatureDatasetPublication(manifest.ref)
        ),
        training_task: TaskOutcome(
            training_task, "COMPLETED", ModelBuildPublication(evidence.ref)
        ),
    }
    trials: dict[str, TaskOutcome] = {}
    for task in tasks[2:]:
        if task.kind == "TRIAL":
            outcome = TaskOutcome(task, "BLOCKED", DependencyBlock("NO_BACKTEST"))
            outcomes[task] = outcome
            trials[task.task_artifact_ref] = outcome  # type: ignore[index]
    for task in tasks[2:]:
        if task.kind == "ANALYSIS":
            analysis = task.artifact
            assert type(analysis) is AnalysisTask
            upstream = trials[analysis.trial_declaration_ref]  # type: ignore[index]
            outcomes[task] = TaskOutcome(
                task, "BLOCKED", UpstreamTaskOutcome(upstream.ref)
            )

    execution = build_execution_manifest(
        spec,
        outcomes,
        {
            "log_name": "research.execution.v1",
            "log_sequence": 100,
            "receipt_hash": _hash("f"),
        },
    )
    assert len(execution.task_outcome_refs) == 10

    feature_failed = TaskOutcome(feature_task, "FAILED", LocalFailure("FEATURE_FAILED"))
    training_blocked = TaskOutcome(
        training_task, "BLOCKED", UpstreamTaskOutcome(feature_failed.ref)
    )
    failed_outcomes: dict[TaskRef, TaskOutcome] = {
        feature_task: feature_failed,
        training_task: training_blocked,
    }
    for task in tasks[2:]:
        if task.kind == "TRIAL":
            failed_outcomes[task] = TaskOutcome(
                task, "BLOCKED", UpstreamTaskOutcome(training_blocked.ref)
            )
    for task in tasks[2:]:
        if task.kind == "ANALYSIS":
            analysis = task.artifact
            assert type(analysis) is AnalysisTask
            upstream_task = next(
                trial
                for trial in tasks
                if trial.kind == "TRIAL"
                and trial.task_artifact_ref == analysis.trial_declaration_ref
            )
            failed_outcomes[task] = TaskOutcome(
                task,
                "BLOCKED",
                UpstreamTaskOutcome(failed_outcomes[upstream_task].ref),
            )
    assert len(
        build_execution_manifest(
            spec,
            failed_outcomes,
            {
                "log_name": "research.execution.v1",
                "log_sequence": 101,
                "receipt_hash": _hash("e"),
            },
        ).task_outcome_refs
    ) == 10

    failed_outcomes[next(task for task in tasks if task.kind == "TRIAL")] = TaskOutcome(
        next(task for task in tasks if task.kind == "TRIAL"),
        "BLOCKED",
        DependencyBlock("WRONG_UPSTREAM"),
    )
    _assert_code(
        "TASK_OUTCOME_INVALID",
        lambda: build_execution_manifest(
            spec,
            failed_outcomes,
            {
                "log_name": "research.execution.v1",
                "log_sequence": 102,
                "receipt_hash": _hash("d"),
            },
        ),
    )


def test_public_surface_adds_values_without_model_runtime_abi() -> None:
    for name in (
        "FeatureRecipe",
        "TrainerRecipe",
        "ModelBuildPlan",
        "FeatureBuildTask",
        "ModelTrainingTask",
        "FeatureDatasetManifest",
        "ModelBuildEvidence",
        "validate_model_build",
    ):
        assert getattr(public, name) is not None

    source = (
        Path(__file__).resolve().parents[1]
        / "src/crypto_quant_research/integration.py"
    ).read_text(encoding="utf-8")
    for forbidden in (
        "Protocol",
        "ModelArtifactRef",
        "ModelRevisionTimeline",
        "pickle",
        "joblib",
        "torch",
        "tensorflow",
        "sklearn",
        "load_model",
        "model_registry",
    ):
        assert forbidden not in source
