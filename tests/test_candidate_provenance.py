from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
from crypto_quant_domain import ArtifactRef, canonical_bytes
from crypto_quant_research import execute_experiment
from crypto_quant_validation import VerifiedCandidateProvenance

def _module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


shell = _module("test_research_shell")


def test_complete_closure_is_read_only_and_replay_stable(tmp_path, monkeypatch):
    from crypto_quant_research import verify_candidate_provenance

    foundation, ledger, port = shell._runtime(tmp_path)
    result = execute_experiment(shell._inputs(), foundation, ledger, port)
    before = tuple(foundation.entries(shell.EXECUTION_LOG))

    def forbidden(*args, **kwargs):
        pytest.fail("provenance verification must be read-only")

    for name in ("put", "append", "checkpoint", "_clock"):
        monkeypatch.setattr(foundation, name, forbidden)
    for name in ("run", "derive"):
        monkeypatch.setattr(port, name, forbidden)
    verified = verify_candidate_provenance(result.strategy_candidate_ref, foundation, port)
    assert type(verified) is VerifiedCandidateProvenance
    assert canonical_bytes(verified.candidate_ref) == canonical_bytes(result.strategy_candidate_ref)
    assert canonical_bytes(verified.manifest_cutoff_ref) == canonical_bytes({"log_name": result.manifest_cutoff.log_name, "log_sequence": result.manifest_cutoff.log_sequence, "receipt_hash": result.manifest_cutoff.receipt_hash})
    assert verify_candidate_provenance(result.strategy_candidate_ref, foundation, port) == verified
    assert tuple(foundation.entries(shell.EXECUTION_LOG)) == before


@pytest.mark.parametrize("mode", ["decision", "model", "target"])
def test_schema_smoke(mode, tmp_path, monkeypatch):
    from crypto_quant_research import (
        execute_model_experiment, execute_target_experiment, verify_candidate_provenance,
    )

    if mode == "decision":
        foundation, ledger, port = shell._runtime(tmp_path)
        port = shell.DecisionGradePort(foundation)
        result = execute_experiment(shell._decision_grade_inputs(), foundation, ledger, port)
    elif mode == "model":
        module = _module("test_integrated_research")
        foundation, ledger, builder, port, inputs, _ = module._model_runtime(tmp_path)
        result = execute_model_experiment(inputs, foundation, ledger, builder, port)
    else:
        module = _module("test_target_stream_research")
        foundation, ledger, materializer, port = module._runtime(tmp_path)
        result = execute_target_experiment(module._inputs(), foundation, ledger, materializer, port)

    def forbidden(*args, **kwargs):
        pytest.fail("read-only verifier attempted an economic run or write")

    for name in ("put", "append", "checkpoint", "_clock"):
        monkeypatch.setattr(foundation, name, forbidden)
    for name in ("run", "derive"):
        monkeypatch.setattr(port, name, forbidden)
    verified = verify_candidate_provenance(result.strategy_candidate_ref, foundation, port)
    assert canonical_bytes(verified.candidate_ref) == canonical_bytes(result.strategy_candidate_ref)


def test_selection_loads_nonselected_completed_witnesses(tmp_path, monkeypatch):
    from crypto_quant_research import verify_candidate_provenance

    foundation, ledger, port = shell._runtime(tmp_path)
    result = execute_experiment(shell._inputs(), foundation, ledger, port)
    loads = []
    original = port.load_analysis

    def load(ref):
        loads.append(ref)
        return original(ref)

    monkeypatch.setattr(port, "load_analysis", load)
    verify_candidate_provenance(result.strategy_candidate_ref, foundation, port)
    assert len(loads) == 3


def test_wrong_selected_rank_rejected(tmp_path):
    from crypto_quant_research import verify_candidate_provenance
    from crypto_quant_research.runtime import _publish

    foundation, ledger, port = shell._runtime(tmp_path)
    result = execute_experiment(shell._inputs(), foundation, ledger, port)
    candidate = shell._payload(foundation, result.strategy_candidate_ref)
    candidate["selection_rank"] = 2
    ref, _ = _publish(foundation, shell.ARTIFACT_LOG, "strategy_candidate", candidate)
    with pytest.raises(ValueError, match="CANDIDATE_PROVENANCE_INVALID"):
        verify_candidate_provenance(ref, foundation, port)


@pytest.mark.parametrize("kind", ["trial_declaration", "analysis_task", "feature_build_task", "model_training_task", "target_build_task"])
def test_foreign_claim_after_cutoff_rejected(kind, tmp_path):
    from crypto_quant_research import verify_candidate_provenance
    from crypto_quant_research.runtime import _publish

    foundation, ledger, port = shell._runtime(tmp_path)
    result = execute_experiment(shell._inputs(), foundation, ledger, port)
    manifest = shell._payload(foundation, result.execution_manifest_ref)
    foreign, _ = _publish(foundation, shell.ARTIFACT_LOG, kind, {"experiment_ref": manifest["experiment_ref"]})
    _publish(foundation, shell.EXECUTION_LOG, "task_outcome", {
        "task_ref": {"kind": "TRIAL", "task_artifact_ref": foreign.to_canonical_dict()},
        "state": "FAILED", "witness": {"local_failure": {"failure_code": "FOREIGN"}},
    })
    with pytest.raises(ValueError, match="CANDIDATE_PROVENANCE_INVALID"):
        verify_candidate_provenance(result.strategy_candidate_ref, foundation, port)


@pytest.mark.parametrize("mutation", ["failed_trial", "closure", "attempt", "precommit"])
def test_incomplete_closure_rejected(mutation, tmp_path, monkeypatch):
    from crypto_quant_research import verify_candidate_provenance
    import json
    from dataclasses import replace

    foundation, ledger, port = shell._runtime(tmp_path)
    result = execute_experiment(shell._inputs(), foundation, ledger, port)
    original = foundation.entries
    def entries(log):
        removed = False
        result = []
        for entry in original(log):
            envelope = json.loads(entry.payload)
            kind, payload = envelope["artifact_type"], envelope["payload"]
            if mutation == "precommit" and kind == "selection_declaration":
                entry = replace(entry, ledger_sequence=100000)
            elif not removed and (
                (mutation == "closure" and kind == "task_attempt_closed")
                or (mutation == "attempt" and kind == "task_attempt_started")
                or (mutation == "failed_trial" and kind == "task_outcome" and payload["task_ref"]["kind"] == "TRIAL" and payload["state"] != "COMPLETED")
            ):
                removed = True
                continue
            result.append(entry)
        return tuple(result)

    monkeypatch.setattr(foundation, "entries", entries)
    with pytest.raises(ValueError, match="CANDIDATE_PROVENANCE_INVALID"):
        verify_candidate_provenance(result.strategy_candidate_ref, foundation, port)


def test_unrelated_experiment_allowed(tmp_path):
    from dataclasses import replace
    from crypto_quant_research import verify_candidate_provenance

    foundation, ledger, port = shell._runtime(tmp_path)
    inputs = shell._inputs()
    first = execute_experiment(inputs, foundation, ledger, port)
    # A distinct declaration universe shares the owner log without reopening first.
    spec = replace(inputs.experiment_spec, budget={"max_trials": 99})
    executions = tuple(replace(item, trial_declaration_ref=trial.ref) for item, trial in zip(inputs.trial_executions, shell.build_trial_declarations(spec)))
    execute_experiment(replace(inputs, experiment_spec=spec, trial_executions=executions), foundation, ledger, port)
    verify_candidate_provenance(first.strategy_candidate_ref, foundation, port)


def test_infrastructure_and_port_failures_preserved(tmp_path, monkeypatch):
    from crypto_quant_research import verify_candidate_provenance
    from tests.support.backtest_consumer_port import PortFailure

    foundation, ledger, port = shell._runtime(tmp_path)
    result = execute_experiment(shell._inputs(), foundation, ledger, port)
    error = PortFailure("PORT_REF_NOT_FOUND")

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(port, "load_completed", fail)
    with pytest.raises(PortFailure) as raised:
        verify_candidate_provenance(result.strategy_candidate_ref, foundation, port)
    assert raised.value is error


def test_foundation_infrastructure_failure_preserved(tmp_path, monkeypatch):
    from crypto_quant_foundation import FoundationFailure
    from crypto_quant_research import verify_candidate_provenance

    foundation, ledger, port = shell._runtime(tmp_path)
    result = execute_experiment(shell._inputs(), foundation, ledger, port)
    error = FoundationFailure("ARTIFACT_NOT_FOUND")

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(foundation, "read", fail)
    with pytest.raises(FoundationFailure) as raised:
        verify_candidate_provenance(result.strategy_candidate_ref, foundation, port)
    assert raised.value is error


def test_malformed_experiment_is_not_provenance(tmp_path):
    from crypto_quant_research import verify_candidate_provenance
    from crypto_quant_research.runtime import _publish

    foundation, ledger, port = shell._runtime(tmp_path)
    result = execute_experiment(shell._inputs(), foundation, ledger, port)
    experiment, _ = _publish(foundation, shell.ARTIFACT_LOG, "experiment_spec", {"fixture": "one"})
    family, _ = _publish(foundation, shell.ARTIFACT_LOG, "candidate_family", {
        "experiment_ref": experiment.to_canonical_dict(),
        "execution_manifest_ref": result.execution_manifest_ref.to_canonical_dict(),
    })
    candidate = shell._payload(foundation, result.strategy_candidate_ref)
    candidate["candidate_family_ref"] = family.to_canonical_dict()
    ref, _ = _publish(foundation, shell.ARTIFACT_LOG, "strategy_candidate", candidate)
    with pytest.raises(ValueError, match="CANDIDATE_PROVENANCE_INVALID"):
        verify_candidate_provenance(ref, foundation, port)


@pytest.mark.parametrize("item", [0, "scalar", [], None], ids=["integer", "string", "list", "null"])
def test_malformed_parameter_axis_item_is_rejected_read_only(item, tmp_path, monkeypatch):
    from crypto_quant_research import verify_candidate_provenance
    from crypto_quant_research.runtime import _publish

    foundation, ledger, port = shell._runtime(tmp_path)
    result = execute_experiment(shell._inputs(), foundation, ledger, port)
    payload = shell._payload(foundation, result.execution_manifest_ref)
    experiment_ref = payload["experiment_ref"]
    experiment = shell._payload(foundation, ArtifactRef(
        experiment_ref["artifact_type"], experiment_ref["schema_version"],
        experiment_ref["content_hash"],
    ))
    experiment["parameter_combinations"] = [item]
    experiment_ref, _ = _publish(foundation, shell.ARTIFACT_LOG, "experiment_spec", experiment)
    family_ref, _ = _publish(foundation, shell.ARTIFACT_LOG, "candidate_family", {
        "experiment_ref": experiment_ref.to_canonical_dict(),
        "execution_manifest_ref": result.execution_manifest_ref.to_canonical_dict(),
    })
    candidate = shell._payload(foundation, result.strategy_candidate_ref)
    candidate["candidate_family_ref"] = family_ref.to_canonical_dict()
    candidate_ref, _ = _publish(foundation, shell.ARTIFACT_LOG, "strategy_candidate", candidate)

    def forbidden(*args, **kwargs):
        pytest.fail("invalid provenance verification must be read-only")

    for name in ("put", "append", "checkpoint", "_clock"):
        monkeypatch.setattr(foundation, name, forbidden)
    for name in ("run", "derive"):
        monkeypatch.setattr(port, name, forbidden)
    with pytest.raises(ValueError, match="^CANDIDATE_PROVENANCE_INVALID$"):
        verify_candidate_provenance(candidate_ref, foundation, port)


@pytest.mark.parametrize("kind", [
    "backtest_trial_spec", "feature_dataset_manifest", "model_build_evidence",
    "target_materialization_evidence",
])
def test_reversed_evidence_must_remain_an_object(kind, tmp_path, monkeypatch):
    from crypto_quant_research import (
        execute_model_experiment, execute_target_experiment, verify_candidate_provenance,
    )
    from crypto_quant_research.runtime import _publish

    if kind in {"feature_dataset_manifest", "model_build_evidence"}:
        module = _module("test_integrated_research")
        foundation, ledger, builder, port, inputs, _ = module._model_runtime(tmp_path)
        result = execute_model_experiment(inputs, foundation, ledger, builder, port)
    elif kind == "target_materialization_evidence":
        module = _module("test_target_stream_research")
        foundation, ledger, materializer, port = module._runtime(tmp_path)
        result = execute_target_experiment(module._inputs(), foundation, ledger, materializer, port)
    else:
        foundation, ledger, port = shell._runtime(tmp_path)
        result = execute_experiment(shell._inputs(), foundation, ledger, port)
    manifest = shell._payload(foundation, result.execution_manifest_ref)
    # An object-shaped publication can reverse to a local-ref scalar, not a dict.
    _publish(foundation, shell.ARTIFACT_LOG, kind, manifest["experiment_ref"])

    def forbidden(*args, **kwargs):
        pytest.fail("invalid provenance verification must be read-only")

    for name in ("put", "append", "checkpoint", "_clock"):
        monkeypatch.setattr(foundation, name, forbidden)
    for name in ("run", "derive"):
        monkeypatch.setattr(port, name, forbidden)
    with pytest.raises(ValueError, match="^CANDIDATE_PROVENANCE_INVALID$"):
        verify_candidate_provenance(result.strategy_candidate_ref, foundation, port)


def test_public_root_keeps_runtime_lazy():
    import os
    import subprocess

    code = """
import sys
import crypto_quant_research
assert 'crypto_quant_research.runtime' not in sys.modules
assert 'crypto_quant_research.provenance' not in sys.modules
from crypto_quant_research import verify_candidate_provenance
assert callable(verify_candidate_provenance)
"""
    subprocess.run([sys.executable, "-B", "-c", code], check=True, env=os.environ.copy())


@pytest.mark.parametrize("mode,kind", [("model", "FEATURE_BUILD"), ("model", "MODEL_TRAINING"), ("target", "TARGET_BUILD")])
def test_known_build_task_reopening_rejected(mode, kind, tmp_path):
    from crypto_quant_research import execute_model_experiment, execute_target_experiment, verify_candidate_provenance
    from crypto_quant_research.runtime import _publish
    import json

    if mode == "model":
        module = _module("test_integrated_research")
        foundation, ledger, builder, port, inputs, _ = module._model_runtime(tmp_path)
        result = execute_model_experiment(inputs, foundation, ledger, builder, port)
    else:
        module = _module("test_target_stream_research")
        foundation, ledger, materializer, port = module._runtime(tmp_path)
        result = execute_target_experiment(module._inputs(), foundation, ledger, materializer, port)
    task = next(json.loads(entry.payload)["payload"]["task_ref"] for entry in foundation.entries(shell.EXECUTION_LOG)
                if json.loads(entry.payload)["artifact_type"] == "task_outcome"
                and json.loads(entry.payload)["payload"]["task_ref"]["kind"] == kind)
    _publish(foundation, shell.EXECUTION_LOG, "task_attempt_started", {
        "task_ref": task, "ordinal": 2, "parent_closed_attempt_ref": None,
        "selection_declaration_refs": [], "dispatch_ref": None,
    })
    with pytest.raises(ValueError, match="CANDIDATE_PROVENANCE_INVALID"):
        verify_candidate_provenance(result.strategy_candidate_ref, foundation, port)
