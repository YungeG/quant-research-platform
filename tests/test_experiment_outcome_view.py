from __future__ import annotations

import json
from dataclasses import replace

import pytest
from crypto_quant_domain import ArtifactRef, canonical_bytes
from crypto_quant_research import execute_experiment, read_experiment_outcome_view
from crypto_quant_research import runtime
from crypto_quant_validation import ValidationCoreFailure
from crypto_quant_research.integration import (
    BacktestTerminal, TaskAttemptClosed, TaskAttemptStarted, TaskOutcome,
    UpstreamTaskOutcome, build_task_universe,
)

from .test_candidate_provenance import _module, shell


def _experiment(foundation, result):
    payload = shell._payload(foundation, result.execution_manifest_ref)["experiment_ref"]
    return ArtifactRef(payload["artifact_type"], payload["schema_version"], payload["content_hash"])


def _forbid(monkeypatch, foundation, port, ledger=None, producer=None):
    def forbidden(*args, **kwargs):
        pytest.fail("outcome view attempted execution, publication, reservation or repair")
    for name in ("put", "append", "checkpoint", "_clock"):
        monkeypatch.setattr(foundation, name, forbidden)
    for name in ("run", "derive", "publish_target", "prepare_trials"):
        if hasattr(port, name):
            monkeypatch.setattr(port, name, forbidden)
    from crypto_quant_validation import SampleConsumptionLedger
    monkeypatch.setattr(SampleConsumptionLedger, "reserve", forbidden)
    if producer is not None:
        for name in ("materialize_target", "build_features", "train_model"):
            if hasattr(producer, name):
                monkeypatch.setattr(producer, name, forbidden)
    monkeypatch.setattr(runtime, "_select_and_publish", forbidden)
    monkeypatch.setattr(runtime, "_close_persisted_outcomes", forbidden)


def test_closed_experiment_without_candidate_keeps_every_trial(tmp_path, monkeypatch):
    foundation, ledger, port = shell._runtime(tmp_path)
    result = execute_experiment(shell._inputs(grades=("decision_grade",)), foundation, ledger, port)
    experiment = _experiment(foundation, result)
    before = {log: tuple(foundation.entries(log)) for log in (shell.ARTIFACT_LOG, shell.EXECUTION_LOG, shell.SAMPLE_LOG)}
    _forbid(monkeypatch, foundation, port, ledger)
    raw = read_experiment_outcome_view(experiment, foundation, port)
    view = json.loads(raw)
    assert raw == read_experiment_outcome_view(experiment, foundation, port)
    assert view["closure_complete"] is True
    assert len(view["trial_rows"]) == 4
    assert len(view["task_rows"]) == 8
    assert {row["state"] for row in view["task_rows"]} == {"COMPLETED", "BLOCKED"}
    assert view["selection"] == {"candidate_ref": None, "original_return_reason": {
        "value": None, "source_refs": [], "source_fields": [], "basis": None}}
    for row in view["trial_rows"]:
        for analysis in row["analysis_rows"]:
            if analysis["analysis_ref"] is not None:
                assert [m["metric_key"] for m in analysis["measurements"]] == ["simple_period_return", "trade_count"]
                for measurement in analysis["measurements"]:
                    assert measurement["fact"]["source_fields"] == [{
                        "source_ref": analysis["analysis_ref"], "document_kind": "backtest_port_record",
                        "pointer": "/" + measurement["metric_key"]}]
            else:
                assert analysis["measurements"] == []
                assert analysis["result_grade"] is None
    assert before == {log: tuple(foundation.entries(log)) for log in before}


def test_terminal_cold_load_failure_is_not_hidden(tmp_path, monkeypatch):
    foundation, ledger, port = shell._runtime(tmp_path)
    result = execute_experiment(shell._inputs(), foundation, ledger, port)
    error = RuntimeError("retained terminal missing")
    def missing(ref):
        raise error
    monkeypatch.setattr(port, "load_terminal", missing)
    _forbid(monkeypatch, foundation, port, ledger)
    with pytest.raises(RuntimeError) as caught:
        read_experiment_outcome_view(_experiment(foundation, result), foundation, port)
    assert caught.value is error


def _prefix(tmp_path):
    foundation, ledger, port = shell._runtime(tmp_path)
    base = runtime._publish_base(shell._inputs(), foundation)
    return foundation, ledger, port, base


def _start(base, foundation, task, ordinal=1, parent=None):
    return runtime._append_start(base, foundation, [], task, ordinal, parent)


def _outcome(base, foundation, value):
    runtime._publish_execution(base, foundation, "task_outcome", value)


@pytest.mark.parametrize("prefix", ["empty", "started", "outcome", "retry", "second_start"])
def test_open_prefix_never_completes_or_repairs(prefix, tmp_path, monkeypatch):
    foundation, ledger, port, base = _prefix(tmp_path)
    task = next(task for task in base.universe if task.kind == "TRIAL")
    if prefix != "empty":
        started = _start(base, foundation, task)
        if prefix == "outcome":
            _outcome(base, foundation, TaskOutcome(task, "FAILED", runtime.LocalFailure("FIXTURE")))
        elif prefix in {"retry", "second_start"}:
            close = TaskAttemptClosed(started.ref, "RETRYABLE_FAILURE", None, "FIXTURE")
            runtime._append_close(base, foundation, [], close)
            if prefix == "second_start":
                _start(base, foundation, task, 2, close.ref)
    old = runtime._recover_execution(base, foundation)
    pure = runtime._recover_execution_prefix(runtime._ExecutionReplayContext.from_base(base), old.entries)
    assert pure == old
    _forbid(monkeypatch, foundation, port, ledger)
    view = json.loads(read_experiment_outcome_view(base.experiment_ref, foundation, port))
    assert view["closure_complete"] is False
    assert view["execution_manifest_ref"] is None
    assert view["candidate_family_ref"] is None
    rows = [row for row in view["task_rows"] if row["attempt_rows"]]
    if prefix == "empty":
        assert rows == []
    else:
        row = rows[0]
        assert row["state"] == ("FAILED" if prefix == "outcome" else None)
        if prefix == "retry":
            assert row["attempt_rows"][0]["disposition"] == "RETRYABLE_FAILURE"
        else:
            assert row["attempt_rows"][-1]["closed_ref"] is None
            assert row["attempt_rows"][-1]["task_outcome_ref"] is None


@pytest.mark.parametrize("wrong", [False, True, "missing"])
def test_open_analysis_requires_its_own_declared_terminal_parent(wrong, tmp_path, monkeypatch):
    foundation, ledger, port, base = _prefix(tmp_path)
    trials = [task for task in base.universe if task.kind == "TRIAL"]
    analysis = next(task for task in base.universe if task.kind == "ANALYSIS"
                    and task.artifact.trial_declaration_ref == trials[0].artifact.ref)
    terminals = [case["terminal"] for case in shell.load_contract_fixture()["cases"] if "terminal" in case]
    parents = []
    for task, terminal in zip(trials[:2], terminals[:2]):
        parent = TaskOutcome(task, terminal["status"], BacktestTerminal(**terminal))
        if not (wrong == "missing" and task == trials[0]):
            _start(base, foundation, task)
            _outcome(base, foundation, parent)
        parents.append(parent)
    _start(base, foundation, analysis)
    _outcome(base, foundation, TaskOutcome(analysis, "BLOCKED", UpstreamTaskOutcome(parents[1 if wrong is True else 0].ref)))
    loads = []
    original = port.load_terminal
    def load(ref):
        loads.append(ref)
        return original(ref)
    monkeypatch.setattr(port, "load_terminal", load)
    _forbid(monkeypatch, foundation, port, ledger)
    if wrong:
        with pytest.raises(runtime.ResearchCoreError, match="TASK_OUTCOME_INVALID"):
            read_experiment_outcome_view(base.experiment_ref, foundation, port)
        assert loads == []  # Edge validation precedes any cold-source read.
    else:
        view = json.loads(read_experiment_outcome_view(base.experiment_ref, foundation, port))
        assert len(loads) == 2
        assert view["closure_complete"] is False
        assert sum(row["state"] == "BLOCKED" for row in view["task_rows"]) >= 1


@pytest.mark.parametrize("status", ["BLOCKED", "FAILED", "CANCELLED"])
def test_terminal_states_and_upstream_sources_are_preserved(status, tmp_path, monkeypatch):
    foundation, ledger, port = shell._runtime(tmp_path)
    result = execute_experiment(shell._inputs(terminal_case="terminal_" + status.lower()), foundation, ledger, port)
    _forbid(monkeypatch, foundation, port, ledger)
    view = json.loads(read_experiment_outcome_view(_experiment(foundation, result), foundation, port))
    assert any(row["state"] == status and "backtest_terminal" in row["witness"] for row in view["task_rows"])
    assert any(row["witness"] and "upstream_task_outcome" in row["witness"] for row in view["task_rows"])


@pytest.mark.parametrize("mode", ["ordinary", "decision", "model", "target"])
def test_current_schema_branches_are_read_only(mode, tmp_path, monkeypatch):
    from crypto_quant_research import execute_model_experiment, execute_target_experiment, verify_candidate_provenance
    producer = None
    if mode in {"ordinary", "decision"}:
        foundation, ledger, port = shell._runtime(tmp_path)
        inputs = shell._inputs()
        if mode == "decision":
            port = shell.DecisionGradePort(foundation)
            inputs = shell._decision_grade_inputs()
        result = execute_experiment(inputs, foundation, ledger, port)
    elif mode == "model":
        module = _module("test_integrated_research")
        foundation, ledger, producer, port, inputs, _ = module._model_runtime(tmp_path)
        result = execute_model_experiment(inputs, foundation, ledger, producer, port)
    else:
        module = _module("test_target_stream_research")
        foundation, ledger, producer, port = module._runtime(tmp_path)
        inputs = module._inputs()
        result = execute_target_experiment(inputs, foundation, ledger, producer, port)
    experiment = _experiment(foundation, result)
    verified = verify_candidate_provenance(result.strategy_candidate_ref, foundation, port)
    _forbid(monkeypatch, foundation, port, ledger, producer)
    view = json.loads(read_experiment_outcome_view(experiment, foundation, port))
    assert view["selection"]["candidate_ref"] == result.strategy_candidate_ref.to_canonical_dict()
    assert verify_candidate_provenance(result.strategy_candidate_ref, foundation, port) == verified
    assert view["closure_complete"] is True
    assert view["spec_schema_version"] == (3 if mode == "model" else 2 if mode == "target" else 1)
    scope = view["scope"]
    if mode == "model":
        assert len(scope["training_slices"]) == 1
        assert len(scope["evaluation_slices"]) == 1
        assert scope["training_slices"] != scope["evaluation_slices"]
        assert all(row["data_slice"] == scope["evaluation_slices"][0] and row["model_artifact_ref"] is not None for row in scope["rows"])
    else:
        assert scope["training_slices"] == scope["evaluation_slices"] == []
    if mode == "target":
        assert all(row["target_recipe_ref"] is not None for row in scope["rows"])


def test_manifest_before_family_is_strict_but_not_complete(tmp_path, monkeypatch):
    foundation, ledger, port = shell._runtime(tmp_path)
    original = runtime._publish
    class Interrupted(Exception):
        pass
    def publish(foundation, log, kind, *args, **kwargs):
        if kind == "candidate_family":
            raise Interrupted
        return original(foundation, log, kind, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(runtime, "_publish", publish)
        with pytest.raises(Interrupted):
            execute_experiment(shell._inputs(), foundation, ledger, port)
    experiment = next(ref for _, ref, _ in runtime._published_entries(foundation, shell.ARTIFACT_LOG) if ref.artifact_type == "experiment_spec")
    _forbid(monkeypatch, foundation, port, ledger)
    view = json.loads(read_experiment_outcome_view(experiment, foundation, port))
    assert view["execution_manifest_ref"] is not None
    assert view["candidate_family_ref"] is None
    assert view["closure_complete"] is False


@pytest.mark.parametrize("mutation", ["precommit", "missing_start", "duplicate_outcome", "selection_set", "ordinal", "fake_manifest"])
def test_open_prefix_rejects_invalid_chains(mutation, tmp_path, monkeypatch):
    foundation, ledger, port, base = _prefix(tmp_path)
    task = next(task for task in base.universe if task.kind == "TRIAL")
    if mutation != "missing_start":
        started = TaskAttemptStarted(task, 2 if mutation == "ordinal" else 1, None,
                                     () if mutation == "selection_set" else (base.selection.ref,))
        runtime._publish_execution(base, foundation, "task_attempt_started", started)
    value = TaskOutcome(task, "FAILED", runtime.LocalFailure("FIXTURE"))
    _outcome(base, foundation, value)
    if mutation == "duplicate_outcome":
        _outcome(base, foundation, TaskOutcome(task, "FAILED", runtime.LocalFailure("OTHER")))
    elif mutation == "fake_manifest":
        runtime._publish(foundation, shell.EXECUTION_LOG, "experiment_execution_manifest", {
            "experiment_ref": base.experiment_ref.to_canonical_dict(), "task_outcome_refs": [base.refs[value.ref].to_canonical_dict()]})
    elif mutation == "precommit":
        original = foundation.entries
        def entries(log, *args):
            return tuple(replace(entry, ledger_sequence=100000) if json.loads(entry.payload)["artifact_type"] == "selection_declaration" else entry for entry in original(log, *args))
        monkeypatch.setattr(foundation, "entries", entries)
    _forbid(monkeypatch, foundation, port, ledger)
    with pytest.raises((runtime.ResearchCoreError, ValueError)):
        read_experiment_outcome_view(base.experiment_ref, foundation, port)


@pytest.mark.parametrize("mutation", ["producer", "stream", "digest", "count", "failure", "missing"])
def test_target_cold_load_detects_missing_or_substituted_evidence(mutation, tmp_path, monkeypatch):
    from crypto_quant_research import execute_target_experiment
    module = _module("test_target_stream_research")
    foundation, ledger, materializer, port = module._runtime(tmp_path)
    result = execute_target_experiment(module._inputs(), foundation, ledger, materializer, port)
    if mutation == "missing":
        port._targets.clear()
    else:
        port.load_mutation = mutation
    _forbid(monkeypatch, foundation, port, ledger, materializer)
    with pytest.raises((runtime.ResearchCoreError, RuntimeError, KeyError, runtime._RuntimeFailure)):
        read_experiment_outcome_view(_experiment(foundation, result), foundation, port)


@pytest.mark.parametrize("mutation", ["missing", "late", "future", "producer", "consumer", "duplicate"])
def test_target_requires_original_unambiguous_preceding_reservation(mutation, tmp_path, monkeypatch):
    from crypto_quant_research import execute_target_experiment
    module = _module("test_target_stream_research")
    foundation, ledger, materializer, port = module._runtime(tmp_path)
    result = execute_target_experiment(module._inputs(), foundation, ledger, materializer, port)
    original = foundation.entries
    def entries(log, *args):
        rows = list(original(log, *args))
        if log != shell.SAMPLE_LOG:
            return tuple(rows)
        for index, entry in enumerate(rows):
            envelope = json.loads(entry.payload)
            payload = envelope["payload"]
            if payload["record"]["purpose"] != "discovery":
                continue
            if mutation == "missing":
                del rows[index]
            elif mutation == "duplicate":
                rows.append(entry)
            elif mutation == "late":
                rows[index] = replace(entry, ledger_sequence=100000)
            elif mutation == "future":
                payload["record"]["consumed_at"] = "2099-01-01T00:00:00.000000Z"
            elif mutation == "producer":
                payload["producer_ref"]["content_hash"] = "sha256:" + "f" * 64
            elif mutation == "consumer":
                payload["record"]["consumer_id"] = "wrong"
            if mutation in {"future", "producer", "consumer"}:
                from crypto_quant_domain import ArtifactEnvelope
                rows[index] = replace(entry, payload=canonical_bytes(ArtifactEnvelope.create("sample_consumption_append", 1, payload)))
            break
        return tuple(rows)
    monkeypatch.setattr(foundation, "entries", entries)
    _forbid(monkeypatch, foundation, port, ledger, materializer)
    with pytest.raises((ValueError, ValidationCoreFailure)):
        read_experiment_outcome_view(_experiment(foundation, result), foundation, port)


@pytest.mark.parametrize("mutation", ["request", "event_count", "digest"])
def test_observed_target_evidence_before_outcome_is_also_verified(mutation, tmp_path, monkeypatch):
    from crypto_quant_domain import canonical_sha256
    module = _module("test_target_stream_research")
    foundation, ledger, materializer, port = module._runtime(tmp_path)
    inputs = module._inputs()
    base = runtime._publish_target_base(inputs, foundation)
    trial = base.trials[0]
    task = next(task for task in base.universe if task.kind == "TARGET_BUILD")
    runtime._reserve(ledger, base.refs[trial.ref], trial, "discovery", inputs.reservation_at)
    context = runtime._ExecutionReplayContext.from_base(base)
    assert canonical_bytes(runtime._target_request(base, trial)) == canonical_bytes(runtime._target_request_for_trial(context, trial))
    stream = module._target_stream()
    target = port.publish_target(base.refs[trial.ref].to_canonical_dict(), stream)
    evidence = module.TargetMaterializationEvidence(
        task.task_artifact_ref, trial.ref, inputs.target_recipe.ref,
        module._hash("f") if mutation == "request" else canonical_sha256(runtime._target_request(base, trial)),
        module._hash("d"), target,
        module._hash("f") if mutation == "digest" else canonical_sha256(stream),
        2 if mutation == "event_count" else 1,
    )
    runtime._publish(foundation, shell.ARTIFACT_LOG, "target_materialization_evidence", runtime._translate(evidence.payload, base.refs))
    _forbid(monkeypatch, foundation, port, ledger, materializer)
    with pytest.raises(ValueError, match="TARGET_(MATERIALIZATION|STORE)_INVALID"):
        read_experiment_outcome_view(base.experiment_ref, foundation, port)


@pytest.mark.parametrize("mode", ["model", "target"])
@pytest.mark.parametrize("wrong", [False, True])
def test_build_children_bind_their_declared_parent(mode, wrong, tmp_path, monkeypatch):
    if mode == "model":
        module = _module("test_integrated_research")
        foundation, ledger, producer, port, inputs, _ = module._model_runtime(tmp_path)
        base, _, _ = runtime._publish_model_base(inputs, foundation)
        parent_task = next(task for task in base.universe if task.kind == "FEATURE_BUILD")
        child = next(task for task in base.universe if task.kind == "MODEL_TRAINING")
        other_task = next(task for task in base.universe if task.kind == "TRIAL")
        # A foreign upstream ref is sufficient even if its payload is a valid local failure.
        other = TaskOutcome(other_task, "FAILED", runtime.LocalFailure("OTHER"))
    else:
        module = _module("test_target_stream_research")
        foundation, ledger, producer, port = module._runtime(tmp_path)
        inputs = module._inputs()
        inputs = replace(inputs, experiment_spec=replace(inputs.experiment_spec, seeds=(1, 2)))
        base = runtime._publish_target_base(inputs, foundation)
        targets = [task for task in base.universe if task.kind == "TARGET_BUILD"]
        parent_task = targets[0]
        other_task = targets[1]
        child = next(task for task in base.universe if task.kind == "TRIAL"
                     and task.artifact.ref == parent_task.artifact.trial_declaration_ref)
        other = TaskOutcome(other_task, "FAILED", runtime.LocalFailure("OTHER"))
        _start(base, foundation, other_task)
        _outcome(base, foundation, other)
    parent = TaskOutcome(parent_task, "FAILED", runtime.LocalFailure("PARENT"))
    _start(base, foundation, parent_task)
    _outcome(base, foundation, parent)
    _start(base, foundation, child)
    _outcome(base, foundation, TaskOutcome(child, "BLOCKED", UpstreamTaskOutcome(other.ref if wrong else parent.ref)))
    _forbid(monkeypatch, foundation, port, ledger, producer)
    if wrong:
        with pytest.raises(ValueError, match="TASK_OUTCOME_INVALID"):
            read_experiment_outcome_view(base.experiment_ref, foundation, port)
    else:
        view = json.loads(read_experiment_outcome_view(base.experiment_ref, foundation, port))
        assert view["closure_complete"] is False
        assert any(row["state"] == "BLOCKED" for row in view["task_rows"])


@pytest.mark.parametrize("feature_state", ("COMPLETED", "FAILED", "ABSENT"))
def test_partial_training_reservation_block_requires_completed_feature(feature_state, tmp_path, monkeypatch):
    module = _module("test_integrated_research")
    foundation, ledger, producer, port, inputs, _ = module._model_runtime(tmp_path)
    base, _, _ = runtime._publish_model_base(inputs, foundation)
    feature = next(task for task in base.universe if task.kind == "FEATURE_BUILD")
    training = next(task for task in base.universe if task.kind == "MODEL_TRAINING")
    if feature_state != "ABSENT":
        _start(base, foundation, feature)
    if feature_state == "COMPLETED":
        runtime._reserve_slice(ledger, base.refs[feature.task_artifact_ref],
                               inputs.model_build_plan.training_slice, "feature_build", inputs.reservation_at)
        manifest = producer.build_features(inputs.model_build_plan)
        ref, _ = runtime._publish(foundation, shell.ARTIFACT_LOG, "feature_dataset_manifest",
                                  runtime._translate(manifest.payload, base.refs))
        base.refs[manifest.ref] = ref
        parent = TaskOutcome(feature, "COMPLETED", runtime.FeatureDatasetPublication(manifest.ref))
    elif feature_state == "FAILED":
        parent = TaskOutcome(feature, "FAILED", runtime.LocalFailure("FEATURE_FAILED"))
    if feature_state != "ABSENT":
        _outcome(base, foundation, parent)
    _start(base, foundation, training)
    _outcome(base, foundation, TaskOutcome(training, "BLOCKED", runtime.DependencyBlock("SAMPLE_RESERVATION_FAILED")))
    before = {str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    _forbid(monkeypatch, foundation, port, ledger, producer)
    if feature_state == "COMPLETED":
        raw = read_experiment_outcome_view(base.experiment_ref, foundation, port)
        view = json.loads(raw)
        assert view["closure_complete"] is False
        assert sum(row["state"] == "COMPLETED" for row in view["task_rows"]) == 1
        assert sum(row["state"] == "BLOCKED" for row in view["task_rows"]) == 1
        assert raw == read_experiment_outcome_view(base.experiment_ref, foundation, port)
    else:
        with pytest.raises(ValueError, match="TASK_OUTCOME_INVALID"):
            read_experiment_outcome_view(base.experiment_ref, foundation, port)
    assert {str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == before


@pytest.mark.parametrize("mutation", [None, "missing", "replaced"])
def test_native_retained_terminal_cas_is_cold_verified(mutation, tmp_path, monkeypatch):
    import hashlib
    module = _module("test_integrated_research")
    foundation, ledger, port, inputs = module._runtime(tmp_path)
    result = execute_experiment(inputs, foundation, ledger, port)
    experiment = _experiment(foundation, result)
    terminal = next(payload["witness"]["backtest_terminal"]["durable_evidence_ref"]
                    for _, ref, payload in runtime._published_entries(foundation, shell.EXECUTION_LOG)
                    if ref.artifact_type == "task_outcome" and "backtest_terminal" in payload["witness"])
    digest = terminal["content_hash"].split(":")[1]
    path = tmp_path / "foundation" / "artifacts" / "sha256" / digest[:2] / digest
    if mutation == "missing":
        path.unlink()
    elif mutation == "replaced":
        path.write_bytes(b"corrupt retained terminal")
    before = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in tmp_path.rglob("*") if path.is_file()}
    _forbid(monkeypatch, foundation, port, ledger)
    if mutation:
        with pytest.raises(Exception) as caught:
            read_experiment_outcome_view(experiment, foundation, port)
        assert "PORT_" in str(caught.value) or "ARTIFACT_" in str(caught.value)
    else:
        view = json.loads(read_experiment_outcome_view(experiment, foundation, port))
        assert view["closure_complete"] is True
    after = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in tmp_path.rglob("*") if path.is_file()}
    assert before == after


def test_multiple_profiles_keep_independent_values_and_pointers(tmp_path, monkeypatch):
    from copy import deepcopy
    inputs = shell._inputs()
    first = inputs.experiment_spec.metric_profile_refs[0]
    second = shell._artifact_ref("backtest_metric_profile", "f")
    profiles = tuple(sorted((first, second), key=canonical_bytes))
    spec = replace(inputs.experiment_spec, metric_profile_refs=profiles)
    executions = tuple(replace(execution, trial_declaration_ref=trial.ref)
                       for execution, trial in zip(inputs.trial_executions, runtime.build_trial_declarations(spec)))
    inputs = replace(inputs, experiment_spec=spec, trial_executions=executions)
    foundation, ledger, port = shell._runtime(tmp_path)
    extra = {}
    derive = port.derive
    load = port.load_analysis
    def derive_profile(publication, profile):
        original = derive(publication, runtime._plain(first))
        if canonical_bytes(profile) == canonical_bytes(first):
            return original
        record = load(original)
        ref = deepcopy(original)
        ref["artifact_ref"]["content_hash"] = "sha256:" + ("f" + original["artifact_ref"]["content_hash"].split(":")[1][1:])
        extra[canonical_bytes(ref)] = {**record, "analysis_ref": ref, "metric_profile_ref": second,
                                      "simple_period_return": "0.75", "trade_count": 7}
        return ref
    def load_profile(ref):
        return extra[canonical_bytes(ref)] if canonical_bytes(ref) in extra else load(ref)
    monkeypatch.setattr(port, "derive", derive_profile)
    monkeypatch.setattr(port, "load_analysis", load_profile)
    result = execute_experiment(inputs, foundation, ledger, port)
    _forbid(monkeypatch, foundation, port, ledger)
    view = json.loads(read_experiment_outcome_view(_experiment(foundation, result), foundation, port))
    assert len(view["scope"]["rows"]) == 8
    for trial in view["trial_rows"]:
        assert len(trial["analysis_rows"]) == 2
        completed = [row for row in trial["analysis_rows"] if row["analysis_ref"]]
        if not completed:
            continue
        assert len({canonical_bytes(row["analysis_ref"]) for row in completed}) == 2
        for row in completed:
            for measurement in row["measurements"]:
                assert measurement["metric_profile_ref"] == row["metric_profile_ref"]
                assert measurement["fact"]["source_refs"] == [row["analysis_ref"]]
                assert measurement["fact"]["source_fields"][0]["pointer"] == "/" + measurement["metric_key"]
            if row["metric_profile_ref"] == second:
                assert [item["fact"]["value"] for item in row["measurements"]] == ["0.75", 7]
            else:
                assert row["measurements"][0]["fact"]["value"] != "0.75"


def test_changed_source_heads_fail_without_refreshing_results(tmp_path, monkeypatch):
    foundation, ledger, port = shell._runtime(tmp_path)
    result = execute_experiment(shell._inputs(), foundation, ledger, port)
    load = port.load_terminal
    original = foundation.entries
    changed = False
    def terminal(ref):
        nonlocal changed
        record = load(ref)
        changed = True
        return record
    def entries(log, *args):
        rows = original(log, *args)
        if changed and log == shell.SAMPLE_LOG:
            return rows[:-1]
        return rows
    monkeypatch.setattr(port, "load_terminal", terminal)
    monkeypatch.setattr(foundation, "entries", entries)
    _forbid(monkeypatch, foundation, port, ledger)
    with pytest.raises(ValueError, match="REPORT_SOURCE_CHANGED"):
        read_experiment_outcome_view(_experiment(foundation, result), foundation, port)


@pytest.mark.parametrize("mutation", ["reopened", "missing_close", "candidate", "analysis"])
def test_closed_graph_is_not_downgraded_to_an_open_view(mutation, tmp_path, monkeypatch):
    foundation, ledger, port = shell._runtime(tmp_path)
    result = execute_experiment(shell._inputs(), foundation, ledger, port)
    experiment = _experiment(foundation, result)
    if mutation == "reopened":
        task = next(payload["task_ref"] for _, ref, payload in runtime._published_entries(foundation, shell.EXECUTION_LOG) if ref.artifact_type == "task_outcome")
        runtime._publish(foundation, shell.EXECUTION_LOG, "task_attempt_started", {
            "task_ref": task, "ordinal": 2, "parent_closed_attempt_ref": None,
            "selection_declaration_refs": [], "dispatch_ref": None})
    elif mutation == "missing_close":
        original = foundation.entries
        def entries(log, *args):
            rows = list(original(log, *args))
            if log == shell.EXECUTION_LOG:
                rows.remove(next(entry for entry in rows if json.loads(entry.payload)["artifact_type"] == "task_attempt_closed"))
            return tuple(rows)
        monkeypatch.setattr(foundation, "entries", entries)
    elif mutation == "candidate":
        payload = shell._payload(foundation, result.strategy_candidate_ref)
        payload["selection_rank"] = 2
        runtime._publish(foundation, shell.ARTIFACT_LOG, "strategy_candidate", payload)
    else:
        load = port.load_analysis
        def analysis(ref):
            return {**load(ref), "source_execution_result_hash": "sha256:" + "f" * 64}
        monkeypatch.setattr(port, "load_analysis", analysis)
    _forbid(monkeypatch, foundation, port, ledger)
    with pytest.raises(ValueError):
        read_experiment_outcome_view(experiment, foundation, port)


def test_owner_view_v1_exact_keys_sorting_and_original_publications(tmp_path, monkeypatch):
    foundation, ledger, port = shell._runtime(tmp_path)
    result = execute_experiment(shell._inputs(), foundation, ledger, port)
    _forbid(monkeypatch, foundation, port, ledger)
    raw = read_experiment_outcome_view(_experiment(foundation, result), foundation, port)
    view = json.loads(raw)
    assert raw == canonical_bytes(view)
    assert set(view) == {"view_format_version", "view_kind", "experiment_ref", "spec_schema_version", "scope",
                         "closure_complete", "candidate_family_ref", "execution_manifest_ref", "manifest_cutoff_ref",
                         "source_log_heads", "publication_facts", "trial_rows", "task_rows", "selection", "limitations"}
    assert set(view["scope"]) == {"rows", "training_slices", "evaluation_slices"}
    assert set(view["selection"]) == {"candidate_ref", "original_return_reason"}
    assert view["trial_rows"] == sorted(view["trial_rows"], key=lambda row: canonical_bytes(row["trial_ref"]))
    assert view["task_rows"] == sorted(view["task_rows"], key=lambda row: (row["task_ref"]["kind"], canonical_bytes(row["task_ref"]["task_artifact_ref"])))
    for scope in view["scope"]["rows"]:
        assert set(scope) == {"trial_ref", "analysis_task_ref", "case_ref", "strategy_ref", "parameters", "data_slice",
                              "scenario_ref", "metric_profile_ref", "model_artifact_ref", "target_recipe_ref"}
        task_payload = next(payload for _, ref, payload in runtime._published_entries(foundation, shell.ARTIFACT_LOG)
                            if ref.to_canonical_dict() == scope["analysis_task_ref"])
        trial_payload = next(payload for _, ref, payload in runtime._published_entries(foundation, shell.ARTIFACT_LOG)
                             if ref.to_canonical_dict() == scope["trial_ref"])
        assert task_payload["trial_declaration_ref"] == scope["trial_ref"]
        assert task_payload["metric_profile_ref"] == scope["metric_profile_ref"]
        assert trial_payload["parameter_values"] == scope["parameters"]
        assert trial_payload["data_slice"] == scope["data_slice"]
        assert trial_payload["scenario_ref"] == scope["scenario_ref"]
    for trial in view["trial_rows"]:
        assert set(trial) == {"trial_ref", "seed", "analysis_rows"}
        assert trial["analysis_rows"] == sorted(trial["analysis_rows"], key=lambda row: canonical_bytes(row["analysis_task_ref"]))
        for row in trial["analysis_rows"]:
            assert set(row) == {"analysis_task_ref", "metric_profile_ref", "analysis_ref", "publication_ref", "result_grade", "measurements"}
            for measurement in row["measurements"]:
                assert set(measurement) == {"metric_key", "unit", "metric_profile_ref", "fact"}
                assert set(measurement["fact"]) == {"value", "source_refs", "source_fields", "basis"}
    for task in view["task_rows"]:
        assert set(task) == {"task_ref", "trial_ref", "task_outcome_ref", "state", "witness", "attempt_rows", "source_fields"}
        for attempt in task["attempt_rows"]:
            assert set(attempt) == {"ordinal", "started_ref", "closed_ref", "disposition", "failure_code", "task_outcome_ref"}
            assert attempt["disposition"] == "TERMINAL"
            assert attempt["task_outcome_ref"] == task["task_outcome_ref"]
        assert task["source_fields"] == [
            {"source_ref": task["task_outcome_ref"], "document_kind": "artifact_payload", "pointer": "/state"},
            {"source_ref": task["task_outcome_ref"], "document_kind": "artifact_payload", "pointer": "/witness"}]
    for publication in view["publication_facts"]:
        assert set(publication) == {"subject_ref", "entry_ref", "accepted_at"}
        entry = next(entry for entry in foundation.entries(publication["entry_ref"]["log_name"])
                     if entry.log_sequence == publication["entry_ref"]["log_sequence"])
        assert entry.accepted_at == publication["accepted_at"]
        assert entry.receipt_hash == publication["entry_ref"]["receipt_hash"]
    assert view["manifest_cutoff_ref"] == {
        "log_name": result.manifest_cutoff.log_name, "log_sequence": result.manifest_cutoff.log_sequence,
        "receipt_hash": result.manifest_cutoff.receipt_hash}


def test_legacy_model_scope_does_not_infer_explicit_evaluation(tmp_path, monkeypatch):
    module = _module("test_integrated_research")
    foundation, ledger, producer, port, inputs, _ = module._model_runtime(tmp_path)
    spec = replace(inputs.experiment_spec, model_build_plan=inputs.model_build_plan, evaluation_slices=None)
    inputs = replace(inputs, experiment_spec=spec)
    base, _, _ = runtime._publish_model_base(inputs, foundation)
    _forbid(monkeypatch, foundation, port, ledger, producer)
    view = json.loads(read_experiment_outcome_view(base.experiment_ref, foundation, port))
    assert view["spec_schema_version"] == 1
    assert view["scope"]["evaluation_slices"] == []
    assert len(view["scope"]["training_slices"]) == 1
    assert len(view["trial_rows"]) == 8
