"""Public metadata publishers coexist with ordinary/model/target Research; no real data."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import importlib.util
import json
from pathlib import Path
import sys

import pytest
from crypto_quant_domain import ArtifactEnvelope, ArtifactRef, canonical_bytes, canonical_sha256
from crypto_quant_research import (
    StatisticalReadTask, RetrospectiveReadTask, execute_experiment,
    execute_model_experiment, execute_target_experiment, publish_statistical_study,
    publish_statistical_read_task, publish_retrospective_protocol,
    publish_retrospective_study, publish_retrospective_read_task,
    read_experiment_outcome_view, verify_candidate_provenance,
)
from crypto_quant_research import runtime

LOG = "research.artifacts.v1"
# Frozen from unchanged public publishers before the consumer repair.
OWNER_EVENT_GOLDENS = {
    "statistical:TRAIN_FEATURE": "sha256:91c7258b0d878b5f7bee9c05b52129aeed47b20f6a8499e969623e33e13a8521",
    "statistical:TRAIN_FIT": "sha256:a8782c03a1dc34a423a68a703ef109c67465482a73b4628c75f557cdebb342a7",
    "statistical:DEVELOPMENT_FEATURE": "sha256:2b9031624ad7c2a8774e8a8b149700979095305c7e9686e23d7ff6e5a2d4b4b4",
    "statistical:HELD_OUT_FEATURE": "sha256:82a3e32482ccede9b63a34511a5a8e15e0d607255636ebc1b36d9245acedbb6c",
    "statistical:DEVELOPMENT_OUTCOME": "sha256:b84c1fbb590269bd3418998ba5d0a64169403eb1bbd6e7d67b3a24d5986642f1",
    "statistical:HELD_OUT_OUTCOME": "sha256:2a335797fa4395e4654552bea01af2839522fc6abad6629bf93d35cfe5d2eb83",
    "retrospective:INPUT_QUALIFICATION": "sha256:2dcf47be6790184f9a2990158ad81fd199bce3335dee94aa3ddb659321dbeda6",
    "retrospective:SNAPSHOT_FREEZE": "sha256:ab63598b9517deccba1f051d3e9e9295d8662a1072d76bed4075f8a218348b1e",
    "retrospective:TRAIN_FEATURE": "sha256:4b0fb92a324d8e2fab3fe5abf3ec5f7087cb3435b4a68962146aceea205c85d0",
    "retrospective:TRAIN_FIT": "sha256:93358254e1a02c451f6f5bf16d7a60a8d17a3d3b19b096954cf02bbbb5e55126",
    "retrospective:DEVELOPMENT_FEATURE": "sha256:a9b7f640067289e46bc085800929e171d26b5992077ecba0cc818692ad927481",
    "retrospective:RETROSPECTIVE_EVAL_FEATURE": "sha256:dcd9cb08948e1ac1afc575ae04223c2fffbe45c5d8f2952de2c93cf9c045c001",
    "retrospective:DEVELOPMENT_OUTCOME": "sha256:1425a8f23b40fd90abc6582e7a910d6d0ec5230cc1472a4a7d208f70529f6596",
    "retrospective:RETROSPECTIVE_EVAL_OUTCOME": "sha256:e7e1af382b148e34ae3c1b2da7d37c54e58f09095dec482f367b0a553bb33660"
}


def _fixture(name):
    spec = importlib.util.spec_from_file_location("repair_owner_" + name, Path(__file__).with_name(name + ".py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def publish_study_tasks(foundation):
    """Reuse original owners' synthetic declarations, but publish on this Foundation."""
    statistical = _fixture("test_statistical_study")
    retrospective = _fixture("test_retrospective_study")
    study = statistical._study()
    published = publish_statistical_study(study, foundation)
    refs = {}
    for phase, (index, _, _, hashes) in zip(statistical.PHASES, statistical.PHASE_CONTRACTS, strict=True):
        task = StatisticalReadTask(published.ref, phase, study.partitions[index][1], **hashes)
        refs["statistical:" + phase] = publish_statistical_read_task(task, foundation).ref
    protocol = retrospective.protocol()
    protocol_publication = publish_retrospective_protocol(protocol, foundation)
    for phase, hashes in (("INPUT_QUALIFICATION", {}), ("SNAPSHOT_FREEZE", {"input_check_hash": retrospective.H[9]})):
        task = RetrospectiveReadTask(protocol_publication.ref, phase, protocol.source_slices[0], **hashes)
        refs["retrospective:" + phase] = publish_retrospective_read_task(task, foundation).ref
    study = retrospective.study_for(protocol, protocol_publication.ref)
    published = publish_retrospective_study(study, foundation)
    for phase, index, hashes in (
        ("TRAIN_FEATURE", 0, {}), ("TRAIN_FIT", 0, {"feature_rows_hash": retrospective.H[10]}),
        ("DEVELOPMENT_FEATURE", 1, {"model_cards_hash": retrospective.H[11]}),
        ("RETROSPECTIVE_EVAL_FEATURE", 2, {"model_cards_hash": retrospective.H[11]}),
        ("DEVELOPMENT_OUTCOME", 1, {"model_cards_hash": retrospective.H[11], "predictions_hash": retrospective.H[12]}),
        ("RETROSPECTIVE_EVAL_OUTCOME", 2, {"model_cards_hash": retrospective.H[11], "predictions_hash": retrospective.H[12]}),
    ):
        task = RetrospectiveReadTask(published.ref, phase, study.partitions[index][1], **hashes)
        refs["retrospective:" + phase] = publish_retrospective_read_task(task, foundation).ref
    return refs


def _environment(root, mode):
    if mode == "ordinary":
        module = _fixture("test_research_shell")
        foundation, ledger, port = module._runtime(root)
        execute = lambda: execute_experiment(module._inputs(), foundation, ledger, port)
        producer = None
    elif mode == "model":
        module = _fixture("test_integrated_research")
        foundation, ledger, producer, port, inputs, _ = module._model_runtime(root)
        execute = lambda: execute_model_experiment(inputs, foundation, ledger, producer, port)
    else:
        module = _fixture("test_target_stream_research")
        foundation, ledger, producer, port = module._runtime(root)
        execute = lambda: execute_target_experiment(module._inputs(), foundation, ledger, producer, port)
    return foundation, ledger, port, producer, execute


def _experiment(result, foundation):
    wire = foundation.read(ref=result.execution_manifest_ref).envelope.payload["experiment_ref"]
    return ArtifactRef(wire["artifact_type"], wire["schema_version"], wire["content_hash"])


def _frozen(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def _readonly(monkeypatch, foundation, ledger, port, producer=None):
    def forbidden(*args, **kwargs):
        pytest.fail("owner-log cold read attempted execution, reservation or publication")
    for obj, names in ((foundation, ("put", "append", "checkpoint", "_clock")),
                       (ledger, ("reserve", "freeze_snapshot", "assess_holdout")),
                       (port, ("run", "derive", "prepare_trials", "publish_target")),
                       (producer, ("train_model", "build_features", "materialize_target"))):
        for name in names:
            if obj is not None and hasattr(obj, name):
                monkeypatch.setattr(obj, name, forbidden)


def test_public_owner_event_vectors_are_frozen(tmp_path):
    module = _fixture("test_statistical_study")
    foundation = module._foundation(tmp_path)
    refs = publish_study_tasks(foundation)
    entries = {ArtifactRef.from_envelope(foundation.read(ref=ref).envelope):
               next(entry for entry in foundation.entries(LOG) if entry.payload == foundation.read(ref=ref).source_bytes)
               for ref in refs.values()}
    assert {name: entries[ref].event_id for name, ref in refs.items()} == OWNER_EVENT_GOLDENS


@pytest.mark.parametrize("mode", ("ordinary", "model", "target"))
def test_public_execution_and_cold_views_accept_all_legal_study_tasks(mode, tmp_path, monkeypatch):
    foundation, ledger, port, producer, execute = _environment(tmp_path, mode)
    refs = publish_study_tasks(foundation)
    result = execute()
    experiment = _experiment(result, foundation)
    before = _frozen(tmp_path)
    _readonly(monkeypatch, foundation, ledger, port, producer)
    raw = read_experiment_outcome_view(experiment, foundation, port)
    assert raw == read_experiment_outcome_view(experiment, foundation, port)
    assert json.loads(raw)["closure_complete"] is True
    if result.strategy_candidate_ref is not None:
        verify_candidate_provenance(result.strategy_candidate_ref, foundation, port)
    assert set(refs.values()) <= {ref for _, ref, _ in runtime._published_entries(foundation, LOG)}
    assert _frozen(tmp_path) == before


def mutated_task_publication(foundation, refs, family, mutation):
    """One shared adversarial vector generator for both owner-log consumers."""
    selected = next(entry for entry in foundation.entries(LOG) if entry.payload == foundation.read(ref=refs[family]).source_bytes)
    decoded = json.loads(selected.payload)
    payload = deepcopy(decoded["payload"])
    kind, version = decoded["artifact_type"], decoded["schema_version"]
    if mutation == "type":
        kind = kind.replace("training", "feature")
    elif mutation == "fields":
        payload["unknown"] = True
    elif mutation == "missing_field":
        payload.pop("phase")
    elif mutation == "reference":
        key = "study_ref" if family.startswith("statistical:") else "context_ref"
        payload[key]["artifact_type"] = "unexpected_study"
    elif mutation == "required_hash":
        payload["feature_rows_hash"] = None
    elif mutation == "extra_hash":
        payload["predictions_hash"] = "sha256:" + "f" * 64
    elif mutation == "future_schema":
        version = 2
    envelope = ArtifactEnvelope.create(kind, version, payload)
    raw = canonical_bytes(envelope)
    event = (canonical_sha256(("artifact-publication-v1", LOG, ArtifactRef.from_envelope(envelope)))
             if mutation == "event" else selected.event_id)
    if mutation == "bool_schema":
        wire = json.loads(raw)
        wire["schema_version"] = True
        raw = canonical_bytes(wire)
    elif mutation == "duplicate_json":
        raw = raw.replace(b'{"artifact_type":', b'{"schema_version":1,"artifact_type":', 1)
    elif mutation == "noncanonical":
        raw += b"\n"
    return selected, replace(selected, payload=raw, event_id=event)


@pytest.mark.parametrize("family", ("statistical:TRAIN_FIT", "retrospective:TRAIN_FIT"))
@pytest.mark.parametrize("mutation", ("event", "type", "fields", "missing_field", "reference", "required_hash", "extra_hash", "future_schema", "bool_schema", "duplicate_json", "noncanonical"))
def test_malformed_study_task_is_not_ignored_or_granted_generic_event(family, mutation, tmp_path, monkeypatch):
    foundation, ledger, port, producer, execute = _environment(tmp_path, "ordinary")
    result = execute()
    experiment = _experiment(result, foundation)
    refs = publish_study_tasks(foundation)
    selected, malformed = mutated_task_publication(foundation, refs, family, mutation)
    original = foundation.entries
    monkeypatch.setattr(foundation, "entries", lambda log, through=None:
                        tuple(malformed if entry == selected else entry for entry in original(log, through))
                        if log == LOG else original(log, through))
    before = _frozen(tmp_path)
    _readonly(monkeypatch, foundation, ledger, port, producer)
    with pytest.raises(ValueError):
        runtime._published_entries(foundation, LOG)
    with pytest.raises(ValueError):
        read_experiment_outcome_view(experiment, foundation, port)
    assert _frozen(tmp_path) == before
