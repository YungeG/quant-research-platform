"""Read-only reconstruction of Research-owned closure and outcome provenance."""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from crypto_quant_domain import ArtifactEnvelope, ArtifactRef, canonical_bytes, canonical_sha256
from crypto_quant_foundation import FoundationFailure, LocalFoundation, LogEntry
from crypto_quant_validation import (
    SampleConsumptionLedger, SampleConsumptionRecord, VerifiedCandidateProvenance,
)

from .integration import (
    AnalysisDerivation, BacktestTerminal, DataSlice, DependencyBlock, ExperimentSpec,
    FeatureDatasetManifest, FeatureDatasetPublication, FeatureRecipe,
    LocalFailure, ModelBuildPublication, TargetBuildPublication,
    TaskAttemptClosed, TaskOutcome, TaskRef, TrialCompletedPublication,
    UpstreamTaskOutcome, VerifiedAnalysis, _canonical_target_stream,
    map_backtest_observation,
    ModelBuildEvidence, ModelBuildPlan, NoSelection, ParameterCombination,
    ResearchCoreError,
    SelectionDeclaration, SelectionPolicy,
    TargetMaterializationEvidence, TargetRecipe, TrainerRecipe,
    build_candidate_family, build_execution_manifest, build_task_universe,
    build_trial_declarations, select_candidate, validate_execution_prefix,
    validate_model_build,
)
from .runtime import (
    RESEARCH_EXECUTION_LOG, _RESEARCH_ARTIFACTS_LOG, _ExecutionReplayContext,
    _RuntimeFailure, _load_verified_analyses, _load_verified_outcome_analyses,
    _recover_execution_prefix, _target_request_for_trial,
    _plain, _published_entries,
    _ref_payload, _replay_execution_entries, _reverse, _translate, _wire,
)


_EXPERIMENT_BASE_FIELDS = {
    "hypothesis_ref",
    "strategy_definition_ref",
    "data_slices",
    "parameter_combinations",
    "seeds",
    "scenario_refs",
    "backtest_template_ref",
    "model_build_plan",
    "metric_profile_refs",
    "budget",
}
_EXPERIMENT_FIELDS_BY_VERSION = {
    1: _EXPERIMENT_BASE_FIELDS,
    2: _EXPERIMENT_BASE_FIELDS | {"target_recipe_ref"},
    3: _EXPERIMENT_BASE_FIELDS | {"evaluation_slices"},
}


def _artifact_ref(value: object) -> ArtifactRef:
    if type(value) is not dict or set(value) != {
        "type", "artifact_type", "schema_version", "content_hash"
    } or value["type"] != "artifact_ref":
        raise ValueError("invalid artifact reference")
    return ArtifactRef(value["artifact_type"], value["schema_version"], value["content_hash"])


def _read_published_node(
    foundation: LocalFoundation, ref: ArtifactRef, log_name: str, versions: tuple[int, ...]
) -> tuple[LogEntry, ArtifactRef, dict[str, Any]]:
    if type(ref) is not ArtifactRef or ref.schema_version not in versions:
        raise ValueError("unsupported published reference")
    matches = [node for node in _published_entries(foundation, log_name) if node[1] == ref]
    if len(matches) != 1:
        raise ValueError("missing or ambiguous owner publication")
    node = matches[0]
    if foundation.read(ref=ref).source_bytes != node[0].payload:
        raise ValueError("publication and source bytes disagree")
    return node


@dataclass
class _CandidateProvenanceContext:
    replay: _ExecutionReplayContext
    policy: SelectionPolicy
    candidate_ref: ArtifactRef
    candidate: dict[str, Any]
    family_ref: ArtifactRef
    family: dict[str, Any]
    manifest_ref: ArtifactRef
    manifest: dict[str, Any]
    selection_ref: ArtifactRef
    selection_entry: LogEntry
    manifest_entry: LogEntry
    trial_specs: dict[str, ArtifactRef]
    model_evidence_ref: ArtifactRef | None
    target_evidence_refs: dict[str, ArtifactRef]


@dataclass
class _ExperimentProvenanceContext:
    replay: _ExecutionReplayContext
    policy: SelectionPolicy
    selection_ref: ArtifactRef
    selection_entry: LogEntry
    trial_specs: dict[str, ArtifactRef]
    model_evidence_ref: ArtifactRef | None
    target_evidence_refs: dict[str, ArtifactRef]
    model_plan: ModelBuildPlan | None


def _load_declared_context(
    experiment_ref: ArtifactRef, selection_ref: ArtifactRef, foundation: LocalFoundation
) -> _ExperimentProvenanceContext:
    refs: dict[str, ArtifactRef] = {}

    def read(ref, kind, versions=(1,), log=_RESEARCH_ARTIFACTS_LOG):
        if type(ref) is not ArtifactRef:
            ref = _artifact_ref(ref)
        if ref.artifact_type != kind:
            raise ValueError("wrong artifact kind")
        return _read_published_node(foundation, ref, log, versions)

    def bind(value, kind, version=1):
        payload = _translate(value.payload, refs)
        ref = ArtifactRef.from_envelope(ArtifactEnvelope.create(kind, version, payload))
        node = read(ref, kind, (version,))
        if node[2] != payload:
            raise ValueError("reconstructed declaration differs")
        refs[value.ref] = ref
        return node

    _, experiment_ref, experiment = read(experiment_ref, "experiment_spec", (1, 2, 3))
    if (
        type(experiment) is not dict
        or set(experiment) != _EXPERIMENT_FIELDS_BY_VERSION[experiment_ref.schema_version]
    ):
        raise ValueError("invalid Experiment fields")
    for axis in (
        "data_slices", "parameter_combinations", "seeds", "scenario_refs", "metric_profile_refs"
    ):
        if type(experiment[axis]) is not list:
            raise ValueError("Experiment axes must be arrays")
    slice_axes = (
        ("data_slices", "evaluation_slices")
        if experiment_ref.schema_version == 3 else ("data_slices",)
    )
    for axis in slice_axes:
        values = experiment[axis]
        if type(values) is not list or not values or any(
            type(item) is not dict
            or set(item) != {
                "market_bundle_ref", "dataset_revision", "interval_start", "interval_end"
            }
            for item in values
        ):
            raise ValueError("slice axes must be nonempty arrays of DataSlice objects")
    selection_entry, selection_ref, selection_payload = read(
        selection_ref, "selection_declaration"
    )
    _, _, policy_payload = read(selection_payload["selection_policy_ref"], "selection_policy")

    plan = None
    if experiment["model_build_plan"] is not None:
        _, _, payload = read(experiment["model_build_plan"], "model_build_plan")
        _, _, feature = read(payload["feature_recipe_ref"], "feature_recipe")
        feature = FeatureRecipe(**{**feature, "input_names": tuple(feature["input_names"])})
        bind(feature, "feature_recipe")
        _, _, trainer = read(payload["trainer_recipe_ref"], "trainer_recipe")
        trainer = TrainerRecipe(**trainer)
        bind(trainer, "trainer_recipe")
        plan = ModelBuildPlan(feature.ref, trainer.ref, DataSlice(**payload["training_slice"]), payload["seed"])
        bind(plan, "model_build_plan")
    target_ref = experiment.get("target_recipe_ref")
    if target_ref is not None:
        _, _, recipe = read(target_ref, "target_recipe")
        recipe = TargetRecipe(**{**recipe, "input_names": tuple(recipe["input_names"])})
        bind(recipe, "target_recipe")
        target_ref = recipe.ref
    parameter_axis = experiment["parameter_combinations"]
    if type(parameter_axis) is not list or any(type(item) is not dict for item in parameter_axis):
        raise ValueError("parameter_combinations must be an array of objects")
    spec_args = dict(experiment)
    spec_args.update(
        data_slices=tuple(DataSlice(**item) for item in experiment["data_slices"]),
        parameter_combinations=tuple(ParameterCombination(tuple(sorted(item.items()))) for item in parameter_axis),
        seeds=tuple(experiment["seeds"]), scenario_refs=tuple(experiment["scenario_refs"]),
        metric_profile_refs=tuple(experiment["metric_profile_refs"]), model_build_plan=plan,
    )
    if target_ref is not None:
        spec_args["target_recipe_ref"] = target_ref
    if experiment_ref.schema_version == 3:
        spec_args["evaluation_slices"] = tuple(
            DataSlice(**item) for item in experiment["evaluation_slices"]
        )
    spec = ExperimentSpec(**spec_args)
    if spec.schema_version != experiment_ref.schema_version:
        raise ValueError("Experiment schema mismatch")
    if bind(spec, "experiment_spec", spec.schema_version)[1] != experiment_ref:
        raise ValueError("Experiment reconstruction mismatch")
    trials = build_trial_declarations(spec)
    universe = build_task_universe(spec)
    for trial in trials:
        bind(trial, "trial_declaration")
    task_kinds = {"ANALYSIS": "analysis_task", "FEATURE_BUILD": "feature_build_task",
                  "MODEL_TRAINING": "model_training_task", "TARGET_BUILD": "target_build_task"}
    for task in universe:
        if task.kind in task_kinds:
            bind(task.artifact, task_kinds[task.kind])
    policy_args = dict(policy_payload)
    for key in ("eligible_trial_statuses", "accepted_backtest_grades", "hard_filters", "ordering"):
        policy_args[key] = tuple(policy_args[key])
    policy = SelectionPolicy(**policy_args)
    bind(policy, "selection_policy")
    selection = SelectionDeclaration(spec.ref, policy.ref, selection_payload["universe_kind"], selection_payload["declared_by_ref"])
    if bind(selection, "selection_declaration")[1] != selection_ref:
        raise ValueError("selection reconstruction mismatch")

    trial_specs: dict[str, ArtifactRef] = {}
    target_evidence_refs: dict[str, ArtifactRef] = {}
    feature_manifest = None
    trial_refs = {item.ref for item in trials}
    target_tasks = {
        task.artifact.trial_declaration_ref: task.artifact
        for task in universe if task.kind == "TARGET_BUILD"
    }
    model_evidence_ref = None
    # These Research-owned evidence refs occur inside execution witnesses. Decode in
    # publication/dependency order, leaving all opaque Backtest/Domain refs untouched.
    for _, ref, payload in _published_entries(foundation, _RESEARCH_ARTIFACTS_LOG):
        reverse = {_wire(_ref_payload(actual)): local for local, actual in refs.items()}
        converted = _reverse(payload, reverse)
        if ref.artifact_type == "backtest_trial_spec":
            if type(converted) is not dict:
                raise ValueError("trial spec must reconstruct to an object")
            trial = converted.get("trial_declaration_ref")
            if type(trial) is str and trial in trial_refs:
                read(ref, "backtest_trial_spec")
                if set(payload) != {"trial_declaration_ref", "resolved_model_refs", "backtest_request_ref"}:
                    raise ValueError("invalid trial spec")
                if trial in trial_specs:
                    raise ValueError("ambiguous trial spec")
                trial_specs[trial] = ref
        elif ref.artifact_type in {"feature_dataset_manifest", "model_build_evidence"} and plan is not None:
            if type(converted) is not dict:
                raise ValueError("model evidence must reconstruct to an object")
            if converted.get("model_build_plan_ref") != plan.ref:
                continue
            cls = FeatureDatasetManifest if ref.artifact_type == "feature_dataset_manifest" else ModelBuildEvidence
            value = cls(**converted)
            bind(value, ref.artifact_type)
            if ref.artifact_type == "model_build_evidence":
                if model_evidence_ref is not None:
                    raise ValueError("ambiguous model evidence")
                if feature_manifest is None or validate_model_build(
                    plan, feature, trainer, feature_manifest, value.model_artifact
                ) != value:
                    raise ValueError("model evidence does not match build inputs")
                model_evidence_ref = ref
            else:
                if feature_manifest is not None:
                    raise ValueError("ambiguous feature manifest")
                feature_manifest = value
        elif ref.artifact_type == "target_materialization_evidence" and target_ref is not None:
            if type(converted) is not dict:
                raise ValueError("target evidence must reconstruct to an object")
            if type(converted.get("trial_declaration_ref")) is not str or converted["trial_declaration_ref"] not in trial_refs:
                continue
            value = TargetMaterializationEvidence(**converted)
            target_task = target_tasks[value.trial_declaration_ref]
            if value.target_build_task_ref != target_task.ref or value.target_recipe_ref != target_ref:
                raise ValueError("target evidence does not match its task and recipe")
            bind(value, ref.artifact_type)
            if value.trial_declaration_ref in target_evidence_refs:
                raise ValueError("ambiguous target evidence")
            target_evidence_refs[value.trial_declaration_ref] = ref
    return _ExperimentProvenanceContext(
        _ExecutionReplayContext(spec, universe, refs, selection), policy,
        selection_ref, selection_entry, trial_specs,
        model_evidence_ref, target_evidence_refs, plan,
    )


def _load_provenance_context(
    candidate_ref: ArtifactRef, foundation: LocalFoundation
) -> _CandidateProvenanceContext:
    _, candidate_ref, candidate = _read_published_node(
        foundation, candidate_ref, _RESEARCH_ARTIFACTS_LOG, (1, 2, 3)
    )
    if candidate_ref.artifact_type != "strategy_candidate":
        raise ValueError("wrong artifact kind")
    _, family_ref, family = _read_published_node(
        foundation, _artifact_ref(candidate["candidate_family_ref"]), _RESEARCH_ARTIFACTS_LOG, (1,)
    )
    if family_ref.artifact_type != "candidate_family":
        raise ValueError("wrong artifact kind")
    manifest_entry, manifest_ref, manifest = _read_published_node(
        foundation, _artifact_ref(family["execution_manifest_ref"]), RESEARCH_EXECUTION_LOG, (1,)
    )
    if manifest_ref.artifact_type != "experiment_execution_manifest":
        raise ValueError("wrong artifact kind")
    context = _load_declared_context(
        _artifact_ref(family["experiment_ref"]),
        _artifact_ref(candidate["selection_declaration_ref"]), foundation,
    )
    return _CandidateProvenanceContext(
        context.replay, context.policy, candidate_ref, candidate, family_ref, family,
        manifest_ref, manifest, context.selection_ref, context.selection_entry,
        manifest_entry, context.trial_specs, context.model_evidence_ref,
        context.target_evidence_refs,
    )


def _load_experiment_context(
    experiment_ref: ArtifactRef, foundation: LocalFoundation
) -> _ExperimentProvenanceContext:
    selections = [ref for _, ref, payload in _published_entries(foundation, _RESEARCH_ARTIFACTS_LOG)
                  if ref.artifact_type == "selection_declaration"
                  and payload.get("experiment_ref") == _ref_payload(experiment_ref)]
    if len(selections) != 1:
        raise ResearchCoreError("SELECTION_PRECOMMIT_MISSING")
    return _load_declared_context(experiment_ref, selections[0], foundation)


def _reconstruct_closure(candidate_ref, foundation):
    context = _load_provenance_context(candidate_ref, foundation)
    replay = context.replay
    matching = [node for node in _published_entries(foundation, RESEARCH_EXECUTION_LOG)
                if node[1].artifact_type == "experiment_execution_manifest"
                and node[2].get("experiment_ref") == _ref_payload(replay.experiment_ref)]
    if len(matching) != 1 or matching[0][1] != context.manifest_ref:
        raise ValueError("ambiguous Experiment manifest")
    entries, first = _replay_execution_entries(replay, foundation, context.manifest_entry.log_sequence)
    if first is None or context.selection_entry.ledger_sequence >= first:
        raise ValueError("selection was not precommitted")
    cutoff = {"log_name": RESEARCH_EXECUTION_LOG, "log_sequence": len(entries),
              "receipt_hash": context.manifest_entry.receipt_hash}
    projection = validate_execution_prefix(replay.experiment_spec, entries, cutoff)
    manifest = build_execution_manifest(replay.experiment_spec, projection, cutoff)
    if _translate(manifest.payload, replay.refs) != context.manifest:
        raise ValueError("manifest reconstruction mismatch")
    replay.refs[manifest.ref] = context.manifest_ref
    family = build_candidate_family(replay.experiment_spec.ref, manifest)
    family_payload = _translate({"experiment_ref": family.experiment_ref,
                                 "execution_manifest_ref": family.execution_manifest_ref}, replay.refs)
    expected_family = ArtifactRef.from_envelope(ArtifactEnvelope.create("candidate_family", 1, family_payload))
    if family_payload != context.family or expected_family != context.family_ref:
        raise ValueError("family reconstruction mismatch")
    return context, manifest, family


def _verify_selection(context, manifest, family, analyses):
    replay = context.replay
    selected = select_candidate(family, manifest, context.policy, analyses)
    if type(selected) is NoSelection:
        raise ValueError("candidate has no selection")
    trial = selected.trial_declaration_ref
    expected = {
        "candidate_family_ref": _ref_payload(context.family_ref),
        "selection_declaration_ref": _ref_payload(context.selection_ref),
        "selected_trial_declaration_ref": _ref_payload(replay.refs[trial]),
        "selected_trial_spec_ref": _ref_payload(context.trial_specs[trial]),
        "selected_publication_ref": _plain(selected.trial_publication_ref),
        "selected_analysis_ref": _plain(selected.analysis_ref),
        "selection_rank": selected.selection_rank, "validated": False,
    }
    version = 1
    if replay.experiment_spec.model_build_plan is not None:
        if context.model_evidence_ref is None:
            raise ValueError("missing model evidence")
        training = [outcome for task, outcome in manifest.outcome_map.items()
                    if task.kind == "MODEL_TRAINING" and outcome.state == "COMPLETED"]
        if len(training) != 1 or _translate(training[0].payload["witness"], replay.refs) != {
            "model_build_evidence": {"model_build_evidence_ref": _ref_payload(context.model_evidence_ref)}
        }:
            raise ValueError("model evidence is not the closed training witness")
        expected["model_build_evidence_ref"] = _ref_payload(context.model_evidence_ref)
        version = 2
    if replay.experiment_spec.target_recipe_ref is not None:
        target_ref = context.target_evidence_refs[trial]
        targets = [outcome for task, outcome in manifest.outcome_map.items()
                   if task.kind == "TARGET_BUILD" and task.artifact.trial_declaration_ref == trial
                   and outcome.state == "COMPLETED"]
        if len(targets) != 1 or _translate(targets[0].payload["witness"], replay.refs) != {
            "target_materialization_evidence": {"target_materialization_evidence_ref": _ref_payload(target_ref)}
        }:
            raise ValueError("target evidence is not the closed build witness")
        expected["selected_target_materialization_evidence_ref"] = _ref_payload(target_ref)
        version = 3
    if canonical_bytes(expected) != canonical_bytes(context.candidate) or context.candidate_ref.schema_version != version:
        raise ValueError("candidate selection mismatch")
    return VerifiedCandidateProvenance(
        _ref_payload(context.candidate_ref), _ref_payload(context.family_ref),
        _ref_payload(replay.experiment_ref), _ref_payload(context.manifest_ref),
        _ref_payload(context.selection_ref), {
            "log_name": context.manifest_entry.entry_ref.log_name,
            "log_sequence": context.manifest_entry.entry_ref.log_sequence,
            "receipt_hash": context.manifest_entry.entry_ref.receipt_hash,
        },
    )


def verify_candidate_provenance(candidate_ref: ArtifactRef, foundation: LocalFoundation,
                                backtest: object) -> VerifiedCandidateProvenance:
    """Reconstruct the full Experiment closure and selection without writing or running."""
    if type(candidate_ref) is not ArtifactRef:
        raise TypeError("candidate_ref must be an ArtifactRef")
    if not isinstance(foundation, LocalFoundation):
        raise TypeError("foundation must be a LocalFoundation")
    try:
        context, manifest, family = _reconstruct_closure(candidate_ref, foundation)
    except FoundationFailure:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("CANDIDATE_PROVENANCE_INVALID") from error
    # Provider/PORT failures retain their original exception and code. This read
    # intentionally sits outside semantic-graph error normalization.
    try:
        analyses = _load_verified_analyses(manifest, backtest)
    except ResearchCoreError as error:
        raise ValueError("CANDIDATE_PROVENANCE_INVALID") from error
    try:
        return _verify_selection(context, manifest, family, analyses)
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("CANDIDATE_PROVENANCE_INVALID") from error


def _reconstruct_experiment_closure(
    context: _ExperimentProvenanceContext, foundation: LocalFoundation
):
    replay = context.replay
    manifests = [node for node in _published_entries(foundation, RESEARCH_EXECUTION_LOG)
                 if node[1].artifact_type == "experiment_execution_manifest"
                 and node[2].get("experiment_ref") == _ref_payload(replay.experiment_ref)]
    families = [node for node in _published_entries(foundation, _RESEARCH_ARTIFACTS_LOG)
                if node[1].artifact_type == "candidate_family"
                and node[2].get("experiment_ref") == _ref_payload(replay.experiment_ref)]
    if len(manifests) > 1 or len(families) > 1 or (families and not manifests):
        raise ResearchCoreError("MANIFEST_CUTOFF_INVALID")
    manifest_node = manifests[0] if manifests else None
    family_node = families[0] if families else None
    entries, first = _replay_execution_entries(
        replay, foundation, manifest_node[0].log_sequence if manifest_node else None
    )
    if first is not None and context.selection_entry.ledger_sequence >= first:
        raise ResearchCoreError("SELECTION_PRECOMMIT_MISSING")
    manifest = family = None
    if manifest_node is not None:
        cutoff = {"log_name": RESEARCH_EXECUTION_LOG, "log_sequence": len(entries),
                  "receipt_hash": manifest_node[0].receipt_hash}
        projection = validate_execution_prefix(replay.experiment_spec, entries, cutoff)
        manifest = build_execution_manifest(replay.experiment_spec, projection, cutoff)
        if _translate(manifest.payload, replay.refs) != manifest_node[2]:
            raise ResearchCoreError("MANIFEST_CUTOFF_INVALID")
        replay.refs[manifest.ref] = manifest_node[1]
        family = build_candidate_family(replay.experiment_spec.ref, manifest)
        if family_node is not None:
            expected = _translate({"experiment_ref": family.experiment_ref,
                                   "execution_manifest_ref": family.execution_manifest_ref}, replay.refs)
            if (family_node[2] != expected or family_node[1] != ArtifactRef.from_envelope(
                    ArtifactEnvelope.create("candidate_family", 1, expected))):
                raise ResearchCoreError("MANIFEST_CUTOFF_INVALID")
    for entry in entries[1:]:
        value = entry.payload
        actual = replay.refs[value.ref]
        _, _, payload = _read_published_node(foundation, actual, RESEARCH_EXECUTION_LOG, (1,))
        if _translate(value.payload, replay.refs) != payload:
            raise ResearchCoreError("TASK_OUTCOME_INVALID")
    recovery = _recover_execution_prefix(replay, entries[:-1] if manifest_node else entries)
    for started in recovery.starts.values():
        if _wire(started.selection_declaration_refs) != _wire((replay.selection.ref,)):
            raise ResearchCoreError("SELECTION_PRECOMMIT_MISSING")
    return manifest, family, manifest_node, family_node, recovery


def _validate_observed_outcome_links(
    context: _ExperimentProvenanceContext, outcomes: Mapping[TaskRef, TaskOutcome]
) -> None:
    """Validate only observed child edges; absent children impose no closure requirement."""
    universe = context.replay.universe
    trials = {task.artifact.ref: task for task in universe if task.kind == "TRIAL"}
    features = {task.artifact.ref: task for task in universe if task.kind == "FEATURE_BUILD"}
    trainings = [task for task in universe if task.kind == "MODEL_TRAINING"]
    targets = {task.artifact.trial_declaration_ref: task for task in universe
               if task.kind == "TARGET_BUILD"}
    for task, outcome in outcomes.items():
        parent_task = None
        if task.kind == "ANALYSIS":
            parent_task = trials.get(task.artifact.trial_declaration_ref)
        elif task.kind == "MODEL_TRAINING":
            parent_task = features.get(task.artifact.feature_build_task_ref)
        elif task.kind == "TRIAL":
            parent_task = targets.get(task.artifact.ref)
            if parent_task is None and trainings:
                parent_task = trainings[0]
        if parent_task is None:
            if task.kind in {"ANALYSIS", "MODEL_TRAINING"} or type(outcome.witness) is UpstreamTaskOutcome:
                raise ResearchCoreError("TASK_OUTCOME_INVALID")
            continue
        parent = outcomes.get(parent_task)
        if parent is None:
            raise ResearchCoreError("TASK_OUTCOME_INVALID")
        if parent.state != "COMPLETED":
            if not (outcome.state == "BLOCKED" and type(outcome.witness) is UpstreamTaskOutcome
                    and _wire(outcome.witness.task_outcome_ref) == _wire(parent.ref)):
                raise ResearchCoreError("TASK_OUTCOME_INVALID")
        elif type(outcome.witness) is UpstreamTaskOutcome:
            raise ResearchCoreError("TASK_OUTCOME_INVALID")
        elif task.kind == "ANALYSIS":
            if outcome.state == "COMPLETED":
                if not (type(parent.witness) is TrialCompletedPublication
                        and type(outcome.witness) is AnalysisDerivation
                        and _wire(outcome.witness.source_publication_ref) == _wire(parent.witness.publication_ref)):
                    raise ResearchCoreError("TASK_OUTCOME_INVALID")
            elif not (outcome.state == "FAILED" and type(outcome.witness) is LocalFailure):
                raise ResearchCoreError("TASK_OUTCOME_INVALID")
        elif task.kind == "MODEL_TRAINING" and outcome.state == "BLOCKED":
            witness = outcome.witness
            if not (type(witness) is DependencyBlock
                    and witness.reason_code == "SAMPLE_RESERVATION_FAILED"
                    and witness.dependency_ref is None):
                raise ResearchCoreError("TASK_OUTCOME_INVALID")


def _verify_observed_outcome_sources(
    context: _ExperimentProvenanceContext, outcomes: Mapping[TaskRef, TaskOutcome],
    foundation: LocalFoundation, backtest: object,
) -> tuple[VerifiedAnalysis, ...]:
    replay = context.replay
    trials = {trial.ref: trial for trial in build_trial_declarations(replay.experiment_spec)}
    analyses = _load_verified_outcome_analyses(outcomes, backtest)
    for task, outcome in outcomes.items():
        witness = outcome.witness
        if type(witness) is BacktestTerminal:
            # Keep the provider exception outside semantic validation/normalization.
            record = backtest.load_terminal(_plain(witness.durable_evidence_ref))
            mapped = map_backtest_observation(task, record)
            if _wire(mapped.payload) != _wire(outcome.payload):
                raise ResearchCoreError("TASK_OUTCOME_INVALID")
        elif type(witness) is FeatureDatasetPublication:
            ref = replay.refs.get(witness.feature_dataset_manifest_ref)
            if ref is None or ref.artifact_type != "feature_dataset_manifest":
                raise ResearchCoreError("MODEL_BINDING_INVALID")
            _read_published_node(foundation, ref, _RESEARCH_ARTIFACTS_LOG, (1,))
        elif type(witness) is ModelBuildPublication:
            if replay.refs.get(witness.model_build_evidence_ref) != context.model_evidence_ref or context.model_evidence_ref is None:
                raise ResearchCoreError("MODEL_BINDING_INVALID")
        elif type(witness) is TargetBuildPublication:
            trial = trials[task.artifact.trial_declaration_ref]
            ref = replay.refs.get(witness.target_materialization_evidence_ref)
            if ref is None or ref != context.target_evidence_refs.get(trial.ref):
                raise ResearchCoreError("TARGET_MATERIALIZATION_INVALID")
    for trial_ref, ref in context.target_evidence_refs.items():
        _verify_target_source(context, trials[trial_ref], ref, foundation, backtest)
    return analyses


def _verify_target_source(
    context: _ExperimentProvenanceContext, trial, ref: ArtifactRef,
    foundation: LocalFoundation, backtest: object,
) -> None:
    replay = context.replay
    entry, _, payload = _read_published_node(foundation, ref, _RESEARCH_ARTIFACTS_LOG, (1,))
    if payload["materialization_request_hash"] != canonical_sha256(_target_request_for_trial(replay, trial)):
        raise ResearchCoreError("TARGET_MATERIALIZATION_INVALID")
    producer = replay.refs[trial.ref]
    expected = {
        "dataset_revision": trial.data_slice.dataset_revision,
        "interval_start": trial.data_slice.interval_start,
        "interval_end": trial.data_slice.interval_end,
        "purpose": "discovery",
        "consumer_id": canonical_sha256(("sample-consumer-v1", producer)),
    }
    reservations = []
    for reservation in foundation.entries("validation.sample-consumption.v1"):
        append = json.loads(reservation.payload)["payload"]
        record = append.get("record")
        if append.get("producer_ref") == _ref_payload(producer) and type(record) is dict and all(
            record.get(key) == value for key, value in expected.items()
        ):
            reservations.append((reservation, record))
    if len(reservations) != 1:
        raise ResearchCoreError("TARGET_MATERIALIZATION_INVALID")
    reservation, record = reservations[0]
    if record["consumed_at"] > reservation.accepted_at:
        raise ResearchCoreError("TARGET_MATERIALIZATION_INVALID")
    SampleConsumptionLedger(foundation).require_reservation(
        SampleConsumptionRecord(**record), producer, before_entry_ref=entry.entry_ref
    )
    loaded = _plain(backtest.load_target(payload["target_stream_ref"]))
    try:
        if type(loaded) is not dict or set(loaded) != {"ref", "producer_context_ref", "target_stream", "digest"}:
            raise ValueError("invalid target record")
        stream = _canonical_target_stream(loaded["target_stream"])
        if (_wire(loaded["ref"]) != _wire(payload["target_stream_ref"])
                or _wire(loaded["producer_context_ref"]) != _wire(_ref_payload(producer))
                or loaded["digest"] != canonical_sha256(stream)
                or loaded["digest"] != payload["target_stream_digest"]
                or len(stream["events"]) != payload["event_count"]):
            raise ValueError("target source mismatch")
    except (KeyError, TypeError, ValueError) as error:
        raise _RuntimeFailure("TARGET_STORE_INVALID") from error

def _source_field(ref, pointer, kind="artifact_payload"):
    return {"source_ref": ref, "document_kind": kind, "pointer": pointer}


def _fact(value=None, ref=None, pointer=None):
    if ref is None:
        return {"value": None, "source_refs": [], "source_fields": [], "basis": None}
    return {"value": value, "source_refs": [ref],
            "source_fields": [_source_field(ref, pointer, "backtest_port_record")],
            "basis": "owner_evidence"}


def _entry_wire(entry_ref):
    return {"log_name": entry_ref.log_name, "log_sequence": entry_ref.log_sequence,
            "receipt_hash": entry_ref.receipt_hash}


def read_experiment_outcome_view(
    experiment_ref: ArtifactRef, foundation: LocalFoundation, backtest: object
) -> bytes:
    """Read declared and observed outcomes without execution, selection publication or repair."""
    if type(experiment_ref) is not ArtifactRef:
        raise TypeError("experiment_ref must be an ArtifactRef")
    if not isinstance(foundation, LocalFoundation):
        raise TypeError("foundation must be a LocalFoundation")

    def heads():
        result = {}
        for log in (_RESEARCH_ARTIFACTS_LOG, RESEARCH_EXECUTION_LOG, "validation.sample-consumption.v1"):
            entries = foundation.entries(log)
            result[log] = _entry_wire(entries[-1].entry_ref) if entries else None
        return result

    before = heads()
    context = _load_experiment_context(experiment_ref, foundation)
    replay = context.replay
    manifest, family, manifest_node, family_node, recovery = _reconstruct_experiment_closure(context, foundation)
    _validate_observed_outcome_links(context, recovery.outcomes)
    analyses = _verify_observed_outcome_sources(context, recovery.outcomes, foundation, backtest)
    analysis_by_ref = {_wire(value.analysis_ref): value for value in analyses}
    candidates = []
    if family_node is not None:
        candidates = [node for node in _published_entries(foundation, _RESEARCH_ARTIFACTS_LOG)
                      if node[1].artifact_type == "strategy_candidate"
                      and node[2].get("candidate_family_ref") == _ref_payload(family_node[1])]
    if len(candidates) > 1:
        raise ValueError("CANDIDATE_PROVENANCE_INVALID")
    for _, candidate_ref, _ in candidates:
        candidate_context = _load_provenance_context(candidate_ref, foundation)
        _verify_selection(candidate_context, manifest, family, analyses)

    def ref(value):
        return _ref_payload(replay.refs[value]) if value is not None else None

    def translated(value):
        return _translate(value, replay.refs)

    task_rows = []
    scope_rows = []
    trial_rows = []
    closes = {_wire(entry.payload.started_attempt_ref): entry.payload
              for entry in recovery.entries if type(entry.payload) is TaskAttemptClosed}
    for task in sorted(replay.universe, key=lambda task: (task.kind, _wire(ref(task.task_artifact_ref)))):
        outcome = recovery.outcomes.get(task)
        trial_ref = (ref(task.artifact.ref) if task.kind == "TRIAL" else
                     ref(task.artifact.trial_declaration_ref) if task.kind in {"ANALYSIS", "TARGET_BUILD"} else None)
        attempts = []
        for started in sorted((value for value in recovery.starts.values() if value.task_ref == task),
                              key=lambda value: value.ordinal):
            closed = closes.get(_wire(started.ref))
            attempts.append({"ordinal": started.ordinal, "started_ref": ref(started.ref),
                             "closed_ref": ref(closed.ref) if closed else None,
                             "disposition": closed.disposition if closed else None,
                             "failure_code": closed.failure_code if closed else None,
                             "task_outcome_ref": ref(closed.task_outcome_ref) if closed else None})
        outcome_ref = ref(outcome.ref) if outcome else None
        task_rows.append({"task_ref": {"kind": task.kind, "task_artifact_ref": ref(task.task_artifact_ref)},
                          "trial_ref": trial_ref, "task_outcome_ref": outcome_ref,
                          "state": outcome.state if outcome else None,
                          "witness": translated(outcome.payload["witness"]) if outcome else None,
                          "attempt_rows": attempts,
                          "source_fields": [_source_field(outcome_ref, "/state"), _source_field(outcome_ref, "/witness")] if outcome else []})
    for trial in sorted(build_trial_declarations(replay.experiment_spec), key=lambda trial: _wire(ref(trial.ref))):
        rows = []
        tasks = [task for task in replay.universe if task.kind == "ANALYSIS"
                 and task.artifact.trial_declaration_ref == trial.ref]
        for task in sorted(tasks, key=lambda task: _wire(ref(task.task_artifact_ref))):
            outcome = recovery.outcomes.get(task)
            analysis = analysis_by_ref[_wire(outcome.witness.analysis_ref)] if outcome and outcome.state == "COMPLETED" else None
            analysis_ref = _plain(analysis.analysis_ref) if analysis else None
            profile = _plain(task.artifact.metric_profile_ref)
            measurements = []
            if analysis:
                for key, unit in (("simple_period_return", "fraction"), ("trade_count", "count")):
                    measurements.append({"metric_key": key, "unit": unit, "metric_profile_ref": profile,
                                         "fact": _fact(getattr(analysis, key), analysis_ref, "/" + key)})
            rows.append({"analysis_task_ref": ref(task.task_artifact_ref), "metric_profile_ref": profile,
                         "analysis_ref": analysis_ref,
                         "publication_ref": _plain(analysis.trial_publication_ref) if analysis else None,
                         "result_grade": _fact(analysis.result_grade, analysis_ref, "/result_grade") if analysis else None,
                         "measurements": measurements})
            scope_rows.append({"trial_ref": ref(trial.ref), "analysis_task_ref": ref(task.task_artifact_ref),
                               "case_ref": None, "strategy_ref": _plain(replay.experiment_spec.strategy_definition_ref),
                               "parameters": _plain(trial.parameter_values.payload),
                               "data_slice": _plain(trial.data_slice.payload), "scenario_ref": _plain(trial.scenario_ref),
                               "metric_profile_ref": profile,
                               "model_artifact_ref": None, "target_recipe_ref": ref(replay.experiment_spec.target_recipe_ref)})
        trial_rows.append({"trial_ref": ref(trial.ref), "seed": trial.seed, "analysis_rows": rows})
    if context.model_evidence_ref is not None:
        _, _, evidence = _read_published_node(foundation, context.model_evidence_ref, _RESEARCH_ARTIFACTS_LOG, (1,))
        for row in scope_rows:
            row["model_artifact_ref"] = evidence["model_artifact"]

    used = set(replay.refs.values()) | {node[1] for node in candidates}
    if family_node is not None:
        used.add(family_node[1])
    publications = []
    for log in (_RESEARCH_ARTIFACTS_LOG, RESEARCH_EXECUTION_LOG):
        for entry, source_ref, _ in _published_entries(foundation, log):
            if source_ref not in used:
                continue
            _read_published_node(foundation, source_ref, log, (source_ref.schema_version,))
            publications.append({"subject_ref": _ref_payload(source_ref), "entry_ref": _entry_wire(entry.entry_ref),
                                 "accepted_at": entry.accepted_at})
    publications.sort(key=lambda value: (value["entry_ref"]["log_name"], value["entry_ref"]["log_sequence"]))
    spec = replay.experiment_spec
    view = {"view_format_version": 1, "view_kind": "experiment_outcomes",
            "experiment_ref": _ref_payload(experiment_ref), "spec_schema_version": experiment_ref.schema_version,
            "scope": {"rows": scope_rows,
                      "training_slices": [_plain(context.model_plan.training_slice.payload)] if context.model_plan else [],
                      "evaluation_slices": [_plain(value.payload) for value in spec.evaluation_slices] if spec.evaluation_slices else []},
            "closure_complete": manifest_node is not None and family_node is not None,
            "candidate_family_ref": _ref_payload(family_node[1]) if family_node else None,
            "execution_manifest_ref": _ref_payload(manifest_node[1]) if manifest_node else None,
            "manifest_cutoff_ref": _entry_wire(manifest_node[0].entry_ref) if manifest_node else None,
            "source_log_heads": before, "publication_facts": publications,
            "trial_rows": trial_rows, "task_rows": task_rows,
            "selection": {"candidate_ref": _ref_payload(candidates[0][1]) if candidates else None,
                          "original_return_reason": _fact()},
            "limitations": ["Metrics other than simple_period_return and trade_count are not calculated by this view.",
                            "Original NoSelection return reason is not persisted; absence of a Candidate is not a scientific result.",
                            "Scenario identity does not independently establish market, cost or slippage assumptions."]}
    if heads() != before:
        raise ValueError("REPORT_SOURCE_CHANGED")
    return canonical_bytes(view)
