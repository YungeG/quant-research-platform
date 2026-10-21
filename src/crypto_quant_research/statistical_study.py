"""Declarations and owner publication for one frozen statistical study.

These values bind identities, not source qualification, physical file freezes,
Reader permission, or statistical results.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any

from crypto_quant_domain import (
    ArtifactEnvelope,
    ArtifactRef,
    canonical_bytes,
    canonical_sha256,
)
from crypto_quant_foundation import (
    LocalFoundation,
    LogCheckpoint,
    LogEntry,
    LogEntryRef,
)

from .integration import DataSlice

_LOG = "research.artifacts.v1"
_STUDY_TYPE = "statistical_study_declaration"
_MODELS = ("CLIMATOLOGY", "PRICE_ONLY", "PRICE_VOLUME", "SHUFFLED_VOLUME")
_PARTITIONS = ("TRAIN", "DEVELOPMENT", "HELD_OUT")
_HASH = re.compile(r"sha256:[0-9a-f]{64}\Z")
_UTC = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z\Z")
# phase -> partition, producer, purpose, required hash fields, parent phases
_PHASES = {
    "TRAIN_FEATURE": (
        "TRAIN",
        "statistical_feature_read_task",
        "feature_build",
        (),
        (),
    ),
    "TRAIN_FIT": (
        "TRAIN",
        "statistical_training_read_task",
        "model_training",
        ("feature_rows_hash",),
        ("TRAIN_FEATURE",),
    ),
    "DEVELOPMENT_FEATURE": (
        "DEVELOPMENT",
        "statistical_feature_read_task",
        "feature_build",
        ("model_cards_hash",),
        ("TRAIN_FIT",),
    ),
    "HELD_OUT_FEATURE": (
        "HELD_OUT",
        "statistical_feature_read_task",
        "feature_build",
        ("model_cards_hash",),
        ("TRAIN_FIT",),
    ),
    "DEVELOPMENT_OUTCOME": (
        "DEVELOPMENT",
        "statistical_outcome_read_task",
        "validation",
        ("model_cards_hash", "predictions_hash"),
        ("DEVELOPMENT_FEATURE", "HELD_OUT_FEATURE"),
    ),
    "HELD_OUT_OUTCOME": (
        "HELD_OUT",
        "statistical_outcome_read_task",
        "validation",
        ("model_cards_hash", "predictions_hash"),
        ("DEVELOPMENT_FEATURE", "HELD_OUT_FEATURE"),
    ),
}
_TASK_TYPES = frozenset(item[1] for item in _PHASES.values())
_HASH_FIELDS = ("feature_rows_hash", "model_cards_hash", "predictions_hash")
_STUDY_HASH_FIELDS = (
    "research_design_hash",
    "core_code_hash",
    "adapter_code_hash",
    "input_manifest_hash",
    "calendar_manifest_hash",
)


def _text(value: object, name: str) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{name} must be an explicit nonempty string")
    return value


def _hash(value: object, name: str) -> str:
    if type(value) is not str or _HASH.fullmatch(value) is None:
        raise ValueError(f"{name} must be sha256:<64 lowercase hex>")
    return value


def _instant(value: object) -> str:
    if type(value) is not str or _UTC.fullmatch(value) is None:
        raise ValueError("time must be an explicit canonical UTC instant")
    datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")
    return value


def _mapping(value: object, keys: set[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise ValueError("payload must have exact canonical fields")
    return value


def _ref(value: object, artifact_type: str) -> ArtifactRef:
    if type(value) is not ArtifactRef:
        raise ValueError("reference must be a Domain ArtifactRef")
    normalized = ArtifactRef(
        value.artifact_type, value.schema_version, value.content_hash
    )
    if (
        normalized != value
        or value.artifact_type != artifact_type
        or value.schema_version != 1
    ):
        raise ValueError(f"reference must address {artifact_type}@1")
    return value


def _ref_from_wire(value: object, artifact_type: str) -> ArtifactRef:
    wire = _mapping(value, {"type", "artifact_type", "schema_version", "content_hash"})
    ref = ArtifactRef(
        wire["artifact_type"], wire["schema_version"], wire["content_hash"]
    )
    _ref(ref, artifact_type)
    if wire != ref.to_canonical_dict():
        raise ValueError("reference must be canonical")
    return ref


def _slice(value: object) -> DataSlice:
    if type(value) is not DataSlice:
        raise ValueError("data_slice must be a public DataSlice")
    normalized = DataSlice(
        value.market_bundle_ref,
        value.dataset_revision,
        value.interval_start,
        value.interval_end,
    )
    if normalized != value:
        raise ValueError("data_slice must be canonical")
    source = value.market_bundle_ref
    if (
        isinstance(source, Mapping)
        and source.get("type") == "backtest_market_bundle_ref"
    ):
        source = _mapping(source, {"type", "artifact_ref"})["artifact_ref"]
    _ref_from_wire(source, "backtest_market_bundle")
    _text(value.dataset_revision, "dataset_revision")
    _instant(value.interval_start)
    _instant(value.interval_end)
    return normalized


def _slice_from_wire(value: object) -> DataSlice:
    wire = _mapping(
        value,
        {
            "market_bundle_ref",
            "dataset_revision",
            "interval_start",
            "interval_end",
        },
    )
    result = _slice(DataSlice(**wire))
    if canonical_bytes(result.payload) != canonical_bytes(wire):
        raise ValueError("data_slice must use canonical values")
    return result


@dataclass(frozen=True, slots=True)
class StatisticalStudyDeclaration:
    study_key: str
    research_design_hash: str
    core_code_hash: str
    adapter_code_hash: str
    input_manifest_hash: str
    calendar_manifest_hash: str
    models: tuple[str, ...]
    partitions: tuple[tuple[str, DataSlice, str, str], ...]

    def __post_init__(self) -> None:
        _text(self.study_key, "study_key")
        for name in _STUDY_HASH_FIELDS:
            _hash(getattr(self, name), name)
        if type(self.models) is not tuple or self.models != _MODELS:
            raise ValueError("models must be the four fixed ordered controls")
        if type(self.partitions) is not tuple or len(self.partitions) != 3:
            raise ValueError("partitions must be exactly TRAIN/DEVELOPMENT/HELD_OUT")
        previous_end: str | None = None
        for expected, item in zip(_PARTITIONS, self.partitions, strict=True):
            if type(item) is not tuple or len(item) != 4 or item[0] != expected:
                raise ValueError("partitions must be immutable and canonically ordered")
            _, data_slice, start, end = item
            _slice(data_slice)
            _instant(start)
            _instant(end)
            if not data_slice.interval_start <= start < end <= data_slice.interval_end:
                raise ValueError(
                    "decision interval must be nonempty and contained in read interval"
                )
            if previous_end is not None and previous_end > data_slice.interval_start:
                raise ValueError(
                    "partition read intervals must be ordered and disjoint"
                )
            previous_end = data_slice.interval_end

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "study_key": self.study_key,
                **{name: getattr(self, name) for name in _STUDY_HASH_FIELDS},
                "models": self.models,
                "partitions": tuple(
                    MappingProxyType(
                        {
                            "partition": name,
                            "data_slice": data_slice.payload,
                            "decision_interval_start": start,
                            "decision_interval_end": end,
                        }
                    )
                    for name, data_slice, start, end in self.partitions
                ),
            }
        )


@dataclass(frozen=True, slots=True)
class StatisticalReadTask:
    study_ref: ArtifactRef
    phase: str
    data_slice: DataSlice
    feature_rows_hash: str | None = None
    model_cards_hash: str | None = None
    predictions_hash: str | None = None

    def __post_init__(self) -> None:
        _ref(self.study_ref, _STUDY_TYPE)
        if type(self.phase) is not str or self.phase not in _PHASES:
            raise ValueError("phase is not a fixed statistical read phase")
        _slice(self.data_slice)
        required = _PHASES[self.phase][3]
        for name in _HASH_FIELDS:
            value = getattr(self, name)
            if name in required:
                _hash(value, name)
            elif value is not None:
                raise ValueError(f"{name} must be null for {self.phase}")

    @property
    def artifact_type(self) -> str:
        return _PHASES[self.phase][1]

    @property
    def purpose(self) -> str:
        return _PHASES[self.phase][2]

    @property
    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "study_ref": self.study_ref,
                "phase": self.phase,
                "data_slice": self.data_slice.payload,
                **{name: getattr(self, name) for name in _HASH_FIELDS},
            }
        )


@dataclass(frozen=True, slots=True)
class PublishedStatisticalStudy:
    ref: ArtifactRef
    entry_ref: LogEntryRef
    checkpoint: LogCheckpoint

    @property
    def publication(self) -> tuple[ArtifactRef, LogEntryRef, LogCheckpoint]:
        return self.ref, self.entry_ref, self.checkpoint


@dataclass(frozen=True, slots=True)
class PublishedStatisticalReadTask:
    ref: ArtifactRef
    entry_ref: LogEntryRef
    checkpoint: LogCheckpoint

    @property
    def publication(self) -> tuple[ArtifactRef, LogEntryRef, LogCheckpoint]:
        return self.ref, self.entry_ref, self.checkpoint


def _study(payload: object) -> StatisticalStudyDeclaration:
    wire = _mapping(payload, {"study_key", "models", "partitions", *_STUDY_HASH_FIELDS})
    if type(wire["partitions"]) is not tuple:
        raise ValueError("partitions must be a canonical sequence")
    partitions = []
    for value in wire["partitions"]:
        item = _mapping(
            value,
            {
                "partition",
                "data_slice",
                "decision_interval_start",
                "decision_interval_end",
            },
        )
        partitions.append(
            (
                item["partition"],
                _slice_from_wire(item["data_slice"]),
                item["decision_interval_start"],
                item["decision_interval_end"],
            )
        )
    return StatisticalStudyDeclaration(
        wire["study_key"],
        wire["research_design_hash"],
        wire["core_code_hash"],
        wire["adapter_code_hash"],
        wire["input_manifest_hash"],
        wire["calendar_manifest_hash"],
        wire["models"],
        tuple(partitions),
    )


def _task(payload: object) -> StatisticalReadTask:
    wire = _mapping(payload, {"study_ref", "phase", "data_slice", *_HASH_FIELDS})
    return StatisticalReadTask(
        _ref_from_wire(wire["study_ref"], _STUDY_TYPE),
        wire["phase"],
        _slice_from_wire(wire["data_slice"]),
        *(wire[name] for name in _HASH_FIELDS),
    )


def _envelope(source: bytes) -> ArtifactEnvelope:
    try:
        decoded = json.loads(source)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("owner entry must contain JSON Envelope bytes") from error
    wire = _mapping(
        decoded,
        {
            "artifact_type",
            "schema_version",
            "payload",
            "content_hash",
        },
    )
    result = ArtifactEnvelope(**wire)
    if canonical_bytes(result) != source:
        raise ValueError("owner entry must contain canonical Envelope bytes")
    return result


def _scope(task: StatisticalReadTask, study: StatisticalStudyDeclaration) -> None:
    partition = _PHASES[task.phase][0]
    expected = next(item[1] for item in study.partitions if item[0] == partition)
    if task.data_slice != expected:
        raise ValueError("task must exactly cover its study partition and source")


def _tasks(
    foundation: LocalFoundation,
    entries: tuple[LogEntry, ...],
    study_ref: ArtifactRef,
    study: StatisticalStudyDeclaration,
    study_entry: LogEntry,
) -> dict[str, tuple[StatisticalReadTask, LogEntry]]:
    result = {}
    for entry in entries:
        envelope = _envelope(entry.payload)
        if envelope.artifact_type not in _TASK_TYPES:
            continue
        if (
            not isinstance(envelope.payload, Mapping)
            or envelope.payload.get("study_ref") != study_ref.to_canonical_dict()
        ):
            continue
        task = _task(envelope.payload)
        if envelope.schema_version != 1 or envelope.artifact_type != task.artifact_type:
            raise ValueError("task producer and phase must agree")
        if entry.event_id != canonical_sha256(
            ("statistical-read-task-publication-v1", study_ref, task.phase)
        ):
            raise ValueError("task must have its canonical owner publication")
        if (
            foundation.read(ref=ArtifactRef.from_envelope(envelope)).source_bytes
            != entry.payload
        ):
            raise ValueError("published task bytes must be addressable")
        _scope(task, study)
        if task.phase in result:
            raise ValueError("study phase has more than one published task")
        result[task.phase] = task, entry
    for task, entry in result.values():
        if entry.ledger_sequence <= study_entry.ledger_sequence:
            raise ValueError("published phases must follow Study publication order")
        for parent in _PHASES[task.phase][4]:
            if (
                parent not in result
                or result[parent][1].ledger_sequence >= entry.ledger_sequence
            ):
                raise ValueError("published phases must respect prerequisite order")
        for name in ("model_cards_hash", "predictions_hash"):
            value = getattr(task, name)
            if value is not None and any(
                getattr(prior, name) not in (None, value)
                for prior, _ in result.values()
            ):
                raise ValueError(f"study must keep one frozen {name} anchor")
    return result


def _publish(
    envelope: ArtifactEnvelope,
    event_id: str,
    foundation: LocalFoundation,
) -> tuple[ArtifactRef, LogEntryRef, LogCheckpoint]:
    if type(foundation) is not LocalFoundation:
        raise TypeError("foundation must be a LocalFoundation")
    ref = foundation.put(envelope=envelope)
    receipt = foundation.append(_LOG, event_id, canonical_bytes(envelope))
    # This is original receipt proof, NOT a Foundation-issued lookup checkpoint.
    checkpoint = LogCheckpoint(
        _LOG, receipt.accepted_at, receipt.log_sequence, receipt.receipt_hash
    )
    return ref, receipt.entry_ref, checkpoint


def publish_statistical_study(
    study: StatisticalStudyDeclaration,
    foundation: LocalFoundation,
) -> PublishedStatisticalStudy:
    if type(study) is not StatisticalStudyDeclaration:
        raise TypeError("study must be a StatisticalStudyDeclaration")
    normalized = _study(ArtifactEnvelope.create(_STUDY_TYPE, 1, study.payload).payload)
    if normalized != study:
        raise ValueError("study must be canonical")
    envelope = ArtifactEnvelope.create(_STUDY_TYPE, 1, normalized.payload)
    ref = ArtifactRef.from_envelope(envelope)
    return PublishedStatisticalStudy(
        *_publish(
            envelope,
            canonical_sha256(("artifact-publication-v1", _LOG, ref)),
            foundation,
        )
    )


def publish_statistical_read_task(
    task: StatisticalReadTask,
    foundation: LocalFoundation,
) -> PublishedStatisticalReadTask:
    if type(task) is not StatisticalReadTask:
        raise TypeError("task must be a StatisticalReadTask")
    if type(foundation) is not LocalFoundation:
        raise TypeError("foundation must be a LocalFoundation")
    normalized = StatisticalReadTask(
        task.study_ref,
        task.phase,
        task.data_slice,
        task.feature_rows_hash,
        task.model_cards_hash,
        task.predictions_hash,
    )
    if normalized != task:
        raise ValueError("task must be canonical")
    source = foundation.read(ref=task.study_ref)
    study = _study(source.envelope.payload)
    if (
        source.envelope.artifact_type != _STUDY_TYPE
        or source.envelope.schema_version != 1
    ):
        raise ValueError("study must be a schema-1 statistical declaration")
    checkpoint = foundation.checkpoint(_LOG)
    entries = foundation.entries(_LOG, checkpoint)
    study_event = canonical_sha256(("artifact-publication-v1", _LOG, task.study_ref))
    study_entry = next(
        (
            entry
            for entry in entries
            if entry.event_id == study_event and entry.payload == source.source_bytes
        ),
        None,
    )
    if study_entry is None:
        raise ValueError("study must have an actual Research owner publication")
    _scope(task, study)
    tasks = _tasks(foundation, entries, task.study_ref, study, study_entry)
    if task.phase not in tasks:
        for parent in _PHASES[task.phase][4]:
            if parent not in tasks:
                raise ValueError(f"missing published prerequisite: {parent}")
        for name in ("model_cards_hash", "predictions_hash"):
            value = getattr(task, name)
            if value is not None and any(
                getattr(prior, name) not in (None, value) for prior, _ in tasks.values()
            ):
                raise ValueError(f"study must keep one frozen {name} anchor")
    # Existing phase: append itself enforces unchanged bytes, including hashes/time.
    envelope = ArtifactEnvelope.create(task.artifact_type, 1, task.payload)
    event_id = canonical_sha256(
        ("statistical-read-task-publication-v1", task.study_ref, task.phase)
    )
    return PublishedStatisticalReadTask(*_publish(envelope, event_id, foundation))
