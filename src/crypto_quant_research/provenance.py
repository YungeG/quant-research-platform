"""Read-only reconstruction of Research-owned candidate provenance."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from crypto_quant_domain import ArtifactEnvelope, ArtifactRef, canonical_bytes
from crypto_quant_foundation import FoundationFailure, LocalFoundation, LogEntry
from crypto_quant_validation import VerifiedCandidateProvenance

from .integration import (
    DataSlice, ExperimentSpec, FeatureDatasetManifest, FeatureRecipe,
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
    _load_verified_analyses, _plain, _published_entries,
    _ref_payload, _replay_execution_entries, _reverse, _translate, _wire,
)


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


def _load_provenance_context(
    candidate_ref: ArtifactRef, foundation: LocalFoundation
) -> _CandidateProvenanceContext:
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

    _, candidate_ref, candidate = read(candidate_ref, "strategy_candidate", (1, 2, 3))
    _, family_ref, family = read(candidate["candidate_family_ref"], "candidate_family")
    _, experiment_ref, experiment = read(family["experiment_ref"], "experiment_spec", (1, 2))
    manifest_entry, manifest_ref, manifest = read(
        family["execution_manifest_ref"], "experiment_execution_manifest", log=RESEARCH_EXECUTION_LOG
    )
    selection_entry, selection_ref, selection_payload = read(
        candidate["selection_declaration_ref"], "selection_declaration"
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
    spec_args = dict(experiment)
    spec_args.update(
        data_slices=tuple(DataSlice(**item) for item in experiment["data_slices"]),
        parameter_combinations=tuple(ParameterCombination(tuple(sorted(item.items()))) for item in experiment["parameter_combinations"]),
        seeds=tuple(experiment["seeds"]), scenario_refs=tuple(experiment["scenario_refs"]),
        metric_profile_refs=tuple(experiment["metric_profile_refs"]), model_build_plan=plan,
    )
    if target_ref is not None:
        spec_args["target_recipe_ref"] = target_ref
    spec = ExperimentSpec(**spec_args)
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
            trial = converted.get("trial_declaration_ref")
            if type(trial) is str and trial in trial_refs:
                read(ref, "backtest_trial_spec")
                if set(payload) != {"trial_declaration_ref", "resolved_model_refs", "backtest_request_ref"}:
                    raise ValueError("invalid trial spec")
                if trial in trial_specs:
                    raise ValueError("ambiguous trial spec")
                trial_specs[trial] = ref
        elif ref.artifact_type in {"feature_dataset_manifest", "model_build_evidence"} and plan is not None:
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
    return _CandidateProvenanceContext(
        _ExecutionReplayContext(spec, universe, refs, selection), policy,
        candidate_ref, candidate, family_ref, family, manifest_ref, manifest,
        selection_ref, selection_entry, manifest_entry, trial_specs,
        model_evidence_ref, target_evidence_refs,
    )


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
