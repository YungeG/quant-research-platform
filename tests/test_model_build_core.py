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
        "DataSlice",
        "FeatureRecipe",
        "FrozenModelExperimentInputs",
        "TrainerRecipe",
        "ModelBuildPlan",
        "FeatureBuildTask",
        "ModelTrainingTask",
        "FeatureDatasetManifest",
        "ModelBuildEvidence",
        "execute_model_experiment",
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

    runtime_source = (
        Path(__file__).resolve().parents[1]
        / "src/crypto_quant_research/runtime.py"
    ).read_text(encoding="utf-8")
    for forbidden in (
        "Protocol",
        "Callable",
        "model_registry",
        "load_model",
        "pickle",
        "joblib",
        "torch",
        "tensorflow",
        "sklearn",
    ):
        assert forbidden not in runtime_source


def _evaluation_spec(**overrides):
    feature, trainer, plan = _recipes()
    evaluation = DataSlice(plan.training_slice.market_bundle_ref, 'evaluation',
                           '2026-02-01T00:00:00.000000Z',
                           '2026-03-01T00:00:00.000000Z')
    legacy = _spec(plan)
    values = {field.name: getattr(legacy, field.name) for field in fields(legacy)}
    values.update(model_build_plan=plan,
                  data_slices=tuple(sorted((plan.training_slice, evaluation), key=lambda x: x.canonical_wire)),
                  evaluation_slices=(evaluation,))
    values.update(overrides)
    return ExperimentSpec(**values)


def test_model_evaluation_axis_is_exact_union_and_ten_tasks():
    spec = _evaluation_spec()
    assert spec.schema_version == 3
    assert len(spec.data_slices) == 2
    assert spec.trial_data_slices == spec.evaluation_slices
    tasks = build_task_universe(spec)
    assert len(tasks) == 10
    assert all(t.artifact.data_slice == spec.evaluation_slices[0] for t in tasks if t.kind == 'TRIAL')


@pytest.mark.parametrize('axis,code', [((), 'EXPERIMENT_SPEC_INVALID'), ([], 'EXPERIMENT_SPEC_INVALID'),
    (42, 'EXPERIMENT_SPEC_INVALID'), (('wrong',), 'EXPERIMENT_SPEC_INVALID')])
def test_evaluation_axis_rejects_malformed_values(axis, code):
    _assert_code(code, lambda: _evaluation_spec(evaluation_slices=axis))


def test_evaluation_none_preserves_legacy_model_and_target_wires():
    for plan in (None, _recipes()[2]):
        legacy = _spec(plan)
        values = {field.name: getattr(legacy, field.name) for field in fields(legacy)}
        values.update(model_build_plan=plan, evaluation_slices=None)
        assert ExperimentSpec(**values).ref == legacy.ref
        assert ExperimentSpec(**values).payload == legacy.payload
    assert _spec().ref.endswith('77d5e939e805e2cea4bb4687e28f91273f108eb3f788dc2104a36bb19ace8bc3')


def test_schema3_local_ref_is_experiment_only():
    from crypto_quant_research.integration import _canonical_ref
    assert _canonical_ref(_evaluation_spec().ref, 'experiment', expected_artifact_type='experiment_spec')
    with pytest.raises(ValueError):
        _canonical_ref('rp-core:model_build_plan@3:' + _hash('a'), 'plan', expected_artifact_type='model_build_plan')


def test_schema3_publish_gate_is_experiment_only():
    from crypto_quant_research.runtime import _artifact_schema_versions
    assert _artifact_schema_versions('experiment_spec') == {1, 2, 3}
    for kind in ('model_build_plan', 'model_build_evidence', 'trial_declaration'):
        assert _artifact_schema_versions(kind) == {1}


def test_evaluation_union_chronology_and_plan_failures():
    spec = _evaluation_spec()
    evaluation = spec.evaluation_slices[0]
    training = _recipes()[2].training_slice
    _assert_code('MODEL_BUILD_PLAN_INVALID', lambda: _evaluation_spec(model_build_plan=None))
    _assert_code('MODEL_BUILD_PLAN_INVALID', lambda: _evaluation_spec(model_build_plan=_recipes()[2].ref))
    _assert_code('MODEL_BUILD_PLAN_INVALID', lambda: _evaluation_spec(data_slices=(evaluation,)))
    _assert_code('EXPERIMENT_SPEC_INVALID', lambda: _evaluation_spec(evaluation_slices=(training,)))
    _assert_code('TASK_AXIS_DUPLICATE', lambda: _evaluation_spec(evaluation_slices=(evaluation, evaluation)))
    extra = DataSlice(evaluation.market_bundle_ref, 'extra', evaluation.interval_start, evaluation.interval_end)
    _assert_code('EXPERIMENT_SPEC_INVALID', lambda: _evaluation_spec(data_slices=tuple(sorted((*spec.data_slices, extra), key=lambda x: x.canonical_wire))))
    _assert_code('EXPERIMENT_SPEC_INVALID', lambda: _evaluation_spec(evaluation_slices=(extra,)))


def test_both_frozen_inputs_accept_schema3_not_target():
    from crypto_quant_research import FrozenExperimentInputs, FrozenModelExperimentInputs, TrialExecution
    from crypto_quant_research.integration import build_trial_declarations
    from . import test_research_shell as shell
    from . import test_target_stream_research as target
    feature, trainer, plan = _recipes()
    spec = _evaluation_spec()
    policy = shell._policy(spec)
    FrozenModelExperimentInputs(spec, feature, trainer, plan, policy, 'research', shell.RESERVED_AT)
    executions = tuple(TrialExecution(t.ref, {}, {'request': index}) for index, t in enumerate(build_trial_declarations(spec)))
    FrozenExperimentInputs(spec, policy, 'research', executions, shell.RESERVED_AT)
    _assert_code('TASK_REF_FOREIGN', lambda: FrozenExperimentInputs(spec, policy, 'research', (), shell.RESERVED_AT))
    target_spec = target._spec()
    _assert_code('EXPERIMENT_SPEC_INVALID', lambda: FrozenExperimentInputs(target_spec, target._policy(target_spec), 'research', (), shell.RESERVED_AT))
    _assert_code('MODEL_BUILD_PLAN_INVALID', lambda: FrozenModelExperimentInputs(target_spec, feature, trainer, plan, target._policy(target_spec), 'research', shell.RESERVED_AT))
    assert _spec(plan).ref.endswith('41d35ed1c671b2b8b3db7e749703279622e402004acaae7ded955fed089e2bf1')
    assert target_spec.ref.endswith('e22066cea993e4acbf6b20c7959a4d49a779a6ae3374fb66b0999065aaadce0f')


def test_evaluation_order_and_target_exclusion():
    evaluation = _evaluation_spec().evaluation_slices[0]
    later = DataSlice(evaluation.market_bundle_ref, 'later',
                      '2026-03-01T00:00:00.000000Z', '2026-04-01T00:00:00.000000Z')
    sorted_axis = tuple(sorted((evaluation, later), key=lambda item: item.canonical_wire))
    _assert_code('EXPERIMENT_SPEC_INVALID', lambda: _evaluation_spec(evaluation_slices=sorted_axis[::-1]))
    _assert_code('EXPERIMENT_SPEC_INVALID', lambda: _evaluation_spec(target_recipe_ref='target'))
