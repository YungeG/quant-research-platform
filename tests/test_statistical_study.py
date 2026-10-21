"""Synthetic declaration/publication contracts; no market rows or Reader."""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import FrozenInstanceError, fields, replace
from datetime import UTC, datetime, timedelta
from itertools import count
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from crypto_quant_domain import (
    ArtifactEnvelope,
    ArtifactRef,
    canonical_bytes,
    canonical_sha256,
)
from crypto_quant_foundation import FoundationFailure, LocalFoundation, LogCheckpoint
from crypto_quant_research import (
    DataSlice,
    PublishedStatisticalReadTask,
    PublishedStatisticalStudy,
    StatisticalReadTask,
    StatisticalStudyDeclaration,
    publish_statistical_read_task,
    publish_statistical_study,
)

LOG = "research.artifacts.v1"
MODELS = ("CLIMATOLOGY", "PRICE_ONLY", "PRICE_VOLUME", "SHUFFLED_VOLUME")
PHASES = (
    "TRAIN_FEATURE",
    "TRAIN_FIT",
    "DEVELOPMENT_FEATURE",
    "HELD_OUT_FEATURE",
    "DEVELOPMENT_OUTCOME",
    "HELD_OUT_OUTCOME",
)
PHASE_CONTRACTS = (
    (0, "statistical_feature_read_task", "feature_build", {}),
    (
        0,
        "statistical_training_read_task",
        "model_training",
        {"feature_rows_hash": "sha256:" + "6" * 64},
    ),
    (
        1,
        "statistical_feature_read_task",
        "feature_build",
        {"model_cards_hash": "sha256:" + "7" * 64},
    ),
    (
        2,
        "statistical_feature_read_task",
        "feature_build",
        {"model_cards_hash": "sha256:" + "7" * 64},
    ),
    (
        1,
        "statistical_outcome_read_task",
        "validation",
        {
            "model_cards_hash": "sha256:" + "7" * 64,
            "predictions_hash": "sha256:" + "8" * 64,
        },
    ),
    (
        2,
        "statistical_outcome_read_task",
        "validation",
        {
            "model_cards_hash": "sha256:" + "7" * 64,
            "predictions_hash": "sha256:" + "8" * 64,
        },
    ),
)
SOURCE = ArtifactRef("backtest_market_bundle", 1, "sha256:" + "a" * 64)


def _study() -> StatisticalStudyDeclaration:
    # Metadata only. These short intervals deliberately do NOT certify 65 sessions.
    partitions = tuple(
        (
            name,
            DataSlice(
                SOURCE.to_canonical_dict(),
                "synthetic-registration-only-v1",
                f"2000-{month:02d}-01T00:00:00.000000Z",
                f"2000-{month + 1:02d}-01T00:00:00.000000Z",
            ),
            f"2000-{month:02d}-10T00:00:00.000000Z",
            f"2000-{month:02d}-20T00:00:00.000000Z",
        )
        for name, month in (("TRAIN", 1), ("DEVELOPMENT", 3), ("HELD_OUT", 5))
    )
    return StatisticalStudyDeclaration(
        "synthetic-study-only",
        "sha256:" + "1" * 64,
        "sha256:" + "2" * 64,
        "sha256:" + "3" * 64,
        "sha256:" + "4" * 64,
        "sha256:" + "5" * 64,
        MODELS,
        partitions,
    )


def _foundation(tmp_path: Path) -> LocalFoundation:
    ticks = count()
    start = datetime(2026, 2, 1, tzinfo=UTC)
    return LocalFoundation(
        tmp_path,
        clock=lambda: (
            (start + timedelta(microseconds=next(ticks)))
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        ),
    )


def _task(
    study: StatisticalStudyDeclaration, ref: ArtifactRef, phase: str, **changes: Any
) -> StatisticalReadTask:
    index, _, _, hashes = PHASE_CONTRACTS[PHASES.index(phase)]
    values = {
        "study_ref": ref,
        "phase": phase,
        "data_slice": study.partitions[index][1],
        **hashes,
        **changes,
    }
    return StatisticalReadTask(**values)


def _published(
    tmp_path: Path,
) -> tuple[LocalFoundation, StatisticalStudyDeclaration, PublishedStatisticalStudy]:
    foundation = _foundation(tmp_path)
    study = _study()
    return foundation, study, publish_statistical_study(study, foundation)


def test_public_roundtrip_has_only_four_artifact_types_and_original_proofs(
    tmp_path: Path,
) -> None:
    foundation, study, published = _published(tmp_path)
    assert tuple(field.name for field in fields(study)) == (
        "study_key",
        "research_design_hash",
        "core_code_hash",
        "adapter_code_hash",
        "input_manifest_hash",
        "calendar_manifest_hash",
        "models",
        "partitions",
    )
    assert tuple(field.name for field in fields(StatisticalReadTask)) == (
        "study_ref",
        "phase",
        "data_slice",
        "feature_rows_hash",
        "model_cards_hash",
        "predictions_hash",
    )
    assert published.publication == (
        published.ref,
        published.entry_ref,
        published.checkpoint,
    )
    assert publish_statistical_study(study, foundation) == published
    study_entry = foundation.entries(LOG, published.entry_ref)[-1]
    assert study_entry.event_id == canonical_sha256(
        ("artifact-publication-v1", LOG, published.ref)
    )
    assert published.checkpoint == LogCheckpoint(
        LOG, study_entry.accepted_at, study_entry.log_sequence, study_entry.receipt_hash
    )
    assert foundation.read(ref=published.ref).source_bytes == study_entry.payload
    study_payload = json.loads(study_entry.payload)["payload"]
    assert set(study_payload) == {field.name for field in fields(study)}
    assert study_payload["models"] == list(MODELS)
    assert study_payload["partitions"][2]["data_slice"] == json.loads(
        canonical_bytes(study.partitions[2][1].payload)
    )
    assert (
        study_payload["partitions"][2]["decision_interval_start"]
        != study_payload["partitions"][2]["data_slice"]["interval_start"]
    )

    types = {published.ref.artifact_type}
    for phase, (_, artifact_type, purpose, _) in zip(
        PHASES, PHASE_CONTRACTS, strict=True
    ):
        task = _task(study, published.ref, phase)
        result = publish_statistical_read_task(task, foundation)
        assert type(result) is PublishedStatisticalReadTask
        assert result.publication == (result.ref, result.entry_ref, result.checkpoint)
        assert publish_statistical_read_task(task, foundation) == result
        assert task.artifact_type == artifact_type
        assert task.purpose == purpose
        entry = foundation.entries(LOG, result.entry_ref)[-1]
        assert entry.event_id == canonical_sha256(
            ("statistical-read-task-publication-v1", published.ref, phase)
        )
        assert entry.payload == canonical_bytes(
            ArtifactEnvelope.create(artifact_type, 1, task.payload)
        )
        assert result.checkpoint == LogCheckpoint(
            LOG, entry.accepted_at, entry.log_sequence, entry.receipt_hash
        )
        assert set(json.loads(entry.payload)["payload"]) == {
            field.name for field in fields(task)
        }
        types.add(result.ref.artifact_type)
    assert types == {
        "statistical_study_declaration",
        "statistical_feature_read_task",
        "statistical_training_read_task",
        "statistical_outcome_read_task",
    }
    assert len(foundation.entries(LOG, foundation.checkpoint(LOG))) == 7
    with pytest.raises(FrozenInstanceError):
        study.study_key = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError):
        study.payload["extra"] = True  # type: ignore[index]


@pytest.mark.parametrize(
    "changes",
    (
        {"study_key": ""},
        {"study_key": "  "},
        {"research_design_hash": "UNKNOWN"},
        {"core_code_hash": "sha256:" + "A" * 64},
        {"adapter_code_hash": 1.0},
        {"input_manifest_hash": True},
        {"calendar_manifest_hash": None},
        {"models": list(MODELS)},
        {"models": tuple(reversed(MODELS))},
        {"models": MODELS + ("EXTRA",)},
        {"models": MODELS[:3]},
    ),
)
def test_study_requires_explicit_canonical_identity(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(_study(), **changes)


@pytest.mark.parametrize(
    "case",
    (
        "empty",
        "missing",
        "list",
        "duplicate",
        "reordered",
        "overlap",
        "decision-outside",
        "decision-empty",
        "implicit-time",
    ),
)
def test_study_requires_exact_ordered_disjoint_full_partitions(case: str) -> None:
    study = _study()
    parts: Any = study.partitions
    if case == "empty":
        parts = ()
    elif case == "missing":
        parts = parts[:2]
    elif case == "list":
        parts = list(parts)
    elif case == "duplicate":
        parts = (parts[0], parts[0], parts[2])
    elif case == "reordered":
        parts = (parts[1], parts[0], parts[2])
    else:
        name, data, start, end = parts[1]
        if case == "overlap":
            data = replace(data, interval_start=parts[0][1].interval_start)
        elif case == "decision-outside":
            start = parts[0][2]
        elif case == "decision-empty":
            end = start
        elif case == "implicit-time":
            start = "2000-03-10"
        parts = (parts[0], (name, data, start, end), parts[2])
    with pytest.raises(ValueError):
        replace(study, partitions=parts)


@pytest.mark.parametrize(
    "source",
    (
        "UNKNOWN",
        {"type": "unknown_source"},
        ArtifactRef("model_artifact", 1, "sha256:" + "a" * 64).to_canonical_dict(),
        ArtifactRef(
            "backtest_market_bundle", 2, "sha256:" + "a" * 64
        ).to_canonical_dict(),
        {"type": "wrong_tag", "artifact_ref": SOURCE.to_canonical_dict()},
    ),
)
def test_study_rejects_placeholder_or_wrong_source(source: Any) -> None:
    study = _study()
    part = study.partitions[0]
    with pytest.raises(ValueError):
        data = replace(part[1], market_bundle_ref=source)
        replace(
            study, partitions=((part[0], data, part[2], part[3]), *study.partitions[1:])
        )


def test_canonical_tagged_source_is_preserved_without_qualification(
    tmp_path: Path,
) -> None:
    study = _study()
    name, data, start, end = study.partitions[0]
    tagged = replace(
        data,
        market_bundle_ref={
            "type": "backtest_market_bundle_ref",
            "artifact_ref": SOURCE.to_canonical_dict(),
        },
    )
    study = replace(
        study, partitions=((name, tagged, start, end), *study.partitions[1:])
    )
    ref = publish_statistical_study(study, _foundation(tmp_path)).ref
    assert ref.artifact_type == "statistical_study_declaration"
    assert "source_qualified" not in study.payload


@pytest.mark.parametrize("phase", PHASES)
def test_task_hash_fields_are_required_and_null_closed(phase: str) -> None:
    study = _study()
    ref = ArtifactRef("statistical_study_declaration", 1, "sha256:" + "b" * 64)
    task = _task(study, ref, phase)
    for name in ("feature_rows_hash", "model_cards_hash", "predictions_hash"):
        with pytest.raises(ValueError):
            replace(
                task,
                **{
                    name: None
                    if getattr(task, name) is not None
                    else "sha256:" + "f" * 64
                },
            )
        if getattr(task, name) is not None:
            with pytest.raises(ValueError):
                replace(task, **{name: "future/implicit"})
    with pytest.raises(ValueError):
        replace(task, phase="UNKNOWN")
    with pytest.raises(ValueError):
        replace(task, study_ref=SOURCE)


@pytest.mark.parametrize(
    "field", ("market_bundle_ref", "dataset_revision", "interval_start", "interval_end")
)
def test_task_publication_requires_exact_source_revision_and_warmup_tail(
    tmp_path: Path, field: str
) -> None:
    foundation, study, published = _published(tmp_path)
    data = study.partitions[0][1]
    value = {
        "market_bundle_ref": ArtifactRef(
            "backtest_market_bundle", 1, "sha256:" + "c" * 64
        ).to_canonical_dict(),
        "dataset_revision": "different-revision",
        "interval_start": study.partitions[0][2],
        "interval_end": study.partitions[0][3],
    }[field]
    task = _task(
        study,
        published.ref,
        "TRAIN_FEATURE",
        data_slice=replace(data, **{field: value}),
    )
    with pytest.raises(ValueError, match="exactly"):
        publish_statistical_read_task(task, foundation)
    assert len(foundation.entries(LOG, foundation.checkpoint(LOG))) == 1


@pytest.mark.parametrize("phase", PHASES[1:])
def test_missing_parent_publication_is_rejected(tmp_path: Path, phase: str) -> None:
    foundation, study, published = _published(tmp_path)
    with pytest.raises(ValueError, match="prerequisite"):
        publish_statistical_read_task(_task(study, published.ref, phase), foundation)
    assert len(foundation.entries(LOG, foundation.checkpoint(LOG))) == 1


def test_shared_model_and_complete_prediction_anchors_cannot_change(
    tmp_path: Path,
) -> None:
    foundation, study, published = _published(tmp_path)
    for phase in PHASES[:3]:
        publish_statistical_read_task(_task(study, published.ref, phase), foundation)
    with pytest.raises(ValueError, match="model_cards_hash"):
        publish_statistical_read_task(
            _task(
                study,
                published.ref,
                "HELD_OUT_FEATURE",
                model_cards_hash="sha256:" + "f" * 64,
            ),
            foundation,
        )
    with pytest.raises(ValueError, match="HELD_OUT_FEATURE"):
        publish_statistical_read_task(
            _task(study, published.ref, "DEVELOPMENT_OUTCOME"), foundation
        )
    for phase in PHASES[3:5]:
        publish_statistical_read_task(_task(study, published.ref, phase), foundation)
    with pytest.raises(ValueError, match="predictions_hash"):
        publish_statistical_read_task(
            _task(
                study,
                published.ref,
                "HELD_OUT_OUTCOME",
                predictions_hash="sha256:" + "f" * 64,
            ),
            foundation,
        )


def test_same_phase_changed_bytes_reaches_actual_foundation_conflict(
    tmp_path: Path,
) -> None:
    foundation, study, published = _published(tmp_path)
    for phase in PHASES[:2]:
        publish_statistical_read_task(_task(study, published.ref, phase), foundation)
    changed = _task(
        study, published.ref, "TRAIN_FIT", feature_rows_hash="sha256:" + "f" * 64
    )
    with pytest.raises(FoundationFailure) as raised:
        publish_statistical_read_task(changed, foundation)
    assert raised.value.code == "LOG_CONFLICT"
    assert len(foundation.entries(LOG, foundation.checkpoint(LOG))) == 3


def test_cas_only_study_and_failed_append_are_not_publications(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    foundation = _foundation(tmp_path)
    study = _study()
    ref = foundation.put(
        envelope=ArtifactEnvelope.create(
            "statistical_study_declaration", 1, study.payload
        )
    )
    with pytest.raises(ValueError, match="actual Research owner publication"):
        publish_statistical_read_task(_task(study, ref, "TRAIN_FEATURE"), foundation)
    failure = FoundationFailure("LOG_PUBLICATION_FAILED")
    monkeypatch.setattr(foundation, "append", Mock(side_effect=failure))
    with pytest.raises(FoundationFailure) as raised:
        publish_statistical_study(study, foundation)
    assert raised.value is failure
    assert foundation.entries(LOG, foundation.checkpoint(LOG)) == ()


def test_publish_revalidates_forged_immutable_values_before_io(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    foundation = _foundation(tmp_path)
    put = Mock(side_effect=AssertionError("must not publish invalid values"))
    monkeypatch.setattr(foundation, "put", put)
    study = _study()
    object.__setattr__(study, "models", ("UNKNOWN",))
    with pytest.raises(ValueError):
        publish_statistical_study(study, foundation)
    task = _task(
        _study(),
        ArtifactRef("statistical_study_declaration", 1, "sha256:" + "b" * 64),
        "TRAIN_FEATURE",
    )
    object.__setattr__(task, "phase", "UNKNOWN")
    with pytest.raises(ValueError):
        publish_statistical_read_task(task, foundation)
    put.assert_not_called()
    with pytest.raises(TypeError):
        publish_statistical_study(object(), foundation)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        publish_statistical_read_task(object(), foundation)  # type: ignore[arg-type]


def test_new_owner_lookups_are_bounded_and_receipt_checkpoint_is_not_issued(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    foundation, study, published = _published(tmp_path)
    with pytest.raises(FoundationFailure) as raised:
        foundation.entries(LOG, published.checkpoint)
    assert raised.value.code == "LOG_INTEGRITY"
    original = foundation.entries
    seen = []

    def bounded(log_name: str, through: Any = None):
        assert through is not None
        seen.append(through)
        return original(log_name, through)

    monkeypatch.setattr(foundation, "entries", bounded)
    for phase in PHASES:
        publish_statistical_read_task(_task(study, published.ref, phase), foundation)
    assert len(seen) == 6
    assert all(type(cutoff) is LogCheckpoint for cutoff in seen)


def test_public_statistical_import_keeps_financial_research_runtime_lazy() -> None:
    code = "import sys; from crypto_quant_research import StatisticalStudyDeclaration, StatisticalReadTask, publish_statistical_study, publish_statistical_read_task; assert 'crypto_quant_research.runtime' not in sys.modules"
    result = subprocess.run(
        [sys.executable, "-B", "-c", code],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("case_name", ("task-before-study", "fit-before-feature"))
def test_owner_rejects_preexisting_publications_with_invalid_order(
    tmp_path: Path, case_name: str
) -> None:
    foundation = _foundation(tmp_path)
    study = _study()
    study_ref = foundation.put(
        envelope=ArtifactEnvelope.create(
            "statistical_study_declaration", 1, study.payload
        )
    )
    if case_name != "task-before-study":
        publish_statistical_study(study, foundation)
    phases = (
        ("TRAIN_FEATURE",)
        if case_name == "task-before-study"
        else ("TRAIN_FIT", "TRAIN_FEATURE")
    )
    for phase in phases:
        task = _task(study, study_ref, phase)
        envelope = ArtifactEnvelope.create(task.artifact_type, 1, task.payload)
        foundation.put(envelope=envelope)
        foundation.append(
            LOG,
            canonical_sha256(
                ("statistical-read-task-publication-v1", study_ref, phase)
            ),
            canonical_bytes(envelope),
        )
    if case_name == "task-before-study":
        publish_statistical_study(study, foundation)
    phase = (
        "TRAIN_FEATURE" if case_name == "task-before-study" else "DEVELOPMENT_FEATURE"
    )
    before = foundation.entries(LOG, foundation.checkpoint(LOG))
    with pytest.raises(ValueError, match="order"):
        publish_statistical_read_task(_task(study, study_ref, phase), foundation)
    assert foundation.entries(LOG, foundation.checkpoint(LOG)) == before
