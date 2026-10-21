"""Synthetic metadata only: declarations are not source qualification or permission."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest
from crypto_quant_domain import ArtifactEnvelope, ArtifactRef, canonical_bytes
from crypto_quant_foundation import FoundationFailure, LocalFoundation
from crypto_quant_research import (
    RetrospectiveReadTask,
    RetrospectiveSourceSlice,
    RetrospectiveStudyDeclaration,
    RetrospectiveStudyProtocol,
    publish_retrospective_protocol,
    publish_retrospective_read_task,
    publish_retrospective_study,
)

CLOCK = "2026-02-01T00:00:00.000000Z"
START = "2000-01-01T00:00:00.000000Z"
END = "2000-07-01T00:00:00.000000Z"
H = tuple("sha256:" + value * 64 for value in "123456789abcdef")


def protocol():
    return RetrospectiveStudyProtocol(
        "SYNTHETIC-METADATA-ONLY",
        H[0],
        H[1],
        H[2],
        H[3],
        H[4],
        (("capture", START, END),),
    )


def foundation(root: Path):
    return LocalFoundation(root, clock=lambda: CLOCK)


def study_for(p, ref):
    parts = tuple(
        (
            role,
            RetrospectiveSourceSlice(
                "capture",
                p.capture_revision,
                p.source_scope_hash,
                f"2000-{month:02d}-01T00:00:00.000000Z",
                f"2000-{month + 1:02d}-01T00:00:00.000000Z",
                H[5],
            ),
            f"2000-{month:02d}-10T00:00:00.000000Z",
            f"2000-{month:02d}-20T00:00:00.000000Z",
        )
        for role, month in (("TRAIN", 1), ("DEVELOPMENT", 3), ("RETROSPECTIVE_EVAL", 5))
    )
    return RetrospectiveStudyDeclaration(ref, H[5], H[6], H[7], H[8], parts)


def setup_study(root):
    f = foundation(root)
    p = protocol()
    pp = publish_retrospective_protocol(p, f)
    for phase, hashes in (
        ("INPUT_QUALIFICATION", {}),
        ("SNAPSHOT_FREEZE", {"input_check_hash": H[9]}),
    ):
        publish_retrospective_read_task(
            RetrospectiveReadTask(pp.ref, phase, p.source_slices[0], **hashes), f
        )
    s = study_for(p, pp.ref)
    sp = publish_retrospective_study(s, f)
    return f, p, pp, s, sp


def test_first_public_protocol_and_pre_freeze_read(tmp_path):
    f = foundation(tmp_path)
    p = protocol()
    pp = publish_retrospective_protocol(p, f)
    task = RetrospectiveReadTask(pp.ref, "INPUT_QUALIFICATION", p.source_slices[0])
    tp = publish_retrospective_read_task(task, f)
    assert p.capture_revision.startswith("local-capture:sha256:")
    assert p.source_slices[0].input_manifest_hash is None
    assert (
        tp.ref.artifact_type == "retrospective_input_read_task"
        and task.purpose == "discovery"
    )
    assert p.untouched_claim is p.causal_pit_claim is False
    assert p.models == ("CLIMATOLOGY", "PRICE_ONLY", "PRICE_VOLUME", "SHUFFLED_VOLUME")
    assert publish_retrospective_protocol(p, f) == pp
    assert publish_retrospective_read_task(task, f) == tp
    assert not f.entries("validation.sample-consumption.v1")


def test_final_study_and_fixed_phase_publication(tmp_path):
    f, p, pp, s, sp = setup_study(tmp_path)
    assert s.partitions[2][0] == "RETROSPECTIVE_EVAL"
    assert s.protocol_ref == pp.ref
    phases = (
        ("TRAIN_FEATURE", 0, {}),
        ("TRAIN_FIT", 0, {"feature_rows_hash": H[10]}),
        ("DEVELOPMENT_FEATURE", 1, {"model_cards_hash": H[11]}),
        ("RETROSPECTIVE_EVAL_FEATURE", 2, {"model_cards_hash": H[11]}),
        (
            "DEVELOPMENT_OUTCOME",
            1,
            {"model_cards_hash": H[11], "predictions_hash": H[12]},
        ),
        (
            "RETROSPECTIVE_EVAL_OUTCOME",
            2,
            {"model_cards_hash": H[11], "predictions_hash": H[12]},
        ),
    )
    for phase, index, hashes in phases:
        t = RetrospectiveReadTask(sp.ref, phase, s.partitions[index][1], **hashes)
        a = publish_retrospective_read_task(t, f)
        assert publish_retrospective_read_task(t, f) == a
    assert f.read(ref=sp.ref).source_bytes == canonical_bytes(
        ArtifactEnvelope.create("retrospective_study_declaration", 1, s.payload)
    )
    assert p.capture_revision == s.partitions[0][1].capture_revision


@pytest.mark.parametrize(
    "intervals",
    [
        (("same", START, END), ("same", START, "2000-02-01T00:00:00.000000Z")),
        (("z", START, END), ("a", START, END)),
        (("x", END, START),),
    ],
)
def test_invalid_source_intervals(intervals):
    with pytest.raises(ValueError):
        replace(protocol(), source_intervals=intervals)


@pytest.mark.parametrize(
    "phase,kwargs",
    [
        ("HELD_OUT_FEATURE", {}),
        ("INPUT_QUALIFICATION", {"model_cards_hash": H[1]}),
        ("SNAPSHOT_FREEZE", {}),
        ("TRAIN_FIT", {}),
        ("DEVELOPMENT_OUTCOME", {"model_cards_hash": H[1]}),
    ],
)
def test_invalid_phases_and_exact_hash_fields(phase, kwargs):
    p = protocol()
    ref = ArtifactRef("retrospective_study_protocol", 1, H[14])
    with pytest.raises(ValueError):
        RetrospectiveReadTask(ref, phase, p.source_slices[0], **kwargs)


def test_bool_schema_reference_is_not_a_retrospective_context():
    p = protocol()
    try:
        ref = ArtifactRef("retrospective_study_protocol", True, H[14])
    except (TypeError, ValueError):
        return  # Domain itself may enforce the stricter boundary.
    with pytest.raises(ValueError):
        RetrospectiveReadTask(ref, "INPUT_QUALIFICATION", p.source_slices[0])


def test_wrong_capture_namespace_source_hash_and_time():
    p = protocol()
    source = p.source_slices[0]
    for changes in (
        {"capture_revision": "pretend-provider-v1"},
        {"source_scope_hash": "a" * 64},
        {"interval_start": "2000-01-01T00:00:00Z"},
    ):
        with pytest.raises(ValueError):
            replace(source, **changes)
    with pytest.raises(FrozenInstanceError):
        p.study_key = "mutated"  # pyright: ignore[reportAttributeAccessIssue] -- negative frozen-value fixture


def test_orphan_protocol_cas_and_missing_parent_rejected(tmp_path):
    f = foundation(tmp_path)
    p = protocol()
    ref = f.put(
        envelope=ArtifactEnvelope.create("retrospective_study_protocol", 1, p.payload)
    )
    with pytest.raises(ValueError, match="publication"):
        publish_retrospective_read_task(
            RetrospectiveReadTask(ref, "INPUT_QUALIFICATION", p.source_slices[0]), f
        )
    pp = publish_retrospective_protocol(p, f)
    with pytest.raises(ValueError, match="prerequisite"):
        publish_retrospective_read_task(
            RetrospectiveReadTask(
                pp.ref, "SNAPSHOT_FREEZE", p.source_slices[0], input_check_hash=H[9]
            ),
            f,
        )


@pytest.mark.parametrize("late_inputs", (False, True))
def test_raw_early_study_cannot_bypass_input_publication_graph(tmp_path, late_inputs):
    from crypto_quant_domain import canonical_sha256

    f = foundation(tmp_path)
    p = protocol()
    pp = publish_retrospective_protocol(p, f)
    s = study_for(p, pp.ref)
    e = ArtifactEnvelope.create("retrospective_study_declaration", 1, s.payload)
    ref = f.put(envelope=e)
    f.append(
        "research.artifacts.v1",
        canonical_sha256(("artifact-publication-v1", "research.artifacts.v1", ref)),
        canonical_bytes(e),
    )
    if late_inputs:
        publish_retrospective_read_task(
            RetrospectiveReadTask(pp.ref, "INPUT_QUALIFICATION", p.source_slices[0]), f
        )
        publish_retrospective_read_task(
            RetrospectiveReadTask(
                pp.ref, "SNAPSHOT_FREEZE", p.source_slices[0], input_check_hash=H[9]
            ),
            f,
        )
        with pytest.raises(ValueError, match="prerequisite|order"):
            publish_retrospective_study(s, f)
    else:
        with pytest.raises(ValueError, match="prerequisite|order"):
            publish_retrospective_read_task(
                RetrospectiveReadTask(ref, "TRAIN_FEATURE", s.partitions[0][1]), f
            )


def test_scope_conflicts_before_publication(tmp_path):
    f, _p, _pp, s, sp = setup_study(tmp_path)
    bad = replace(s.partitions[0][1], capture_revision="local-capture:" + H[14])
    with pytest.raises(ValueError, match="scope"):
        publish_retrospective_read_task(
            RetrospectiveReadTask(sp.ref, "TRAIN_FEATURE", bad), f
        )
    with pytest.raises(ValueError):
        publish_retrospective_study(
            replace(s, partitions=(s.partitions[1], s.partitions[0], s.partitions[2])),
            f,
        )


def test_phase_reentry_conflict_and_model_anchor(tmp_path):
    f, _p, _pp, s, sp = setup_study(tmp_path)
    ft = RetrospectiveReadTask(sp.ref, "TRAIN_FEATURE", s.partitions[0][1])
    publish_retrospective_read_task(ft, f)
    fit = RetrospectiveReadTask(
        sp.ref, "TRAIN_FIT", s.partitions[0][1], feature_rows_hash=H[10]
    )
    publish_retrospective_read_task(fit, f)
    with pytest.raises(FoundationFailure):
        publish_retrospective_read_task(replace(fit, feature_rows_hash=H[11]), f)
    dev = RetrospectiveReadTask(
        sp.ref, "DEVELOPMENT_FEATURE", s.partitions[1][1], model_cards_hash=H[11]
    )
    publish_retrospective_read_task(dev, f)
    with pytest.raises(ValueError, match="model_cards_hash"):
        publish_retrospective_read_task(
            RetrospectiveReadTask(
                sp.ref,
                "RETROSPECTIVE_EVAL_FEATURE",
                s.partitions[2][1],
                model_cards_hash=H[12],
            ),
            f,
        )
