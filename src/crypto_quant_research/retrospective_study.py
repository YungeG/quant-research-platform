"""Local-capture retrospective declarations and owner publication.

A planned capture revision is a governance coordinate, not provider revision,
completed capture, source qualification, untouched evidence or Reader permission.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
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

from .statistical_study import (
    _envelope,
    _hash,
    _instant,
    _mapping,
    _publish,
    _ref,
    _ref_from_wire,
    _text,
)

_LOG = "research.artifacts.v1"
_PROTOCOL = "retrospective_study_protocol"
_STUDY = "retrospective_study_declaration"
_ROLES = ("TRAIN", "DEVELOPMENT", "RETROSPECTIVE_EVAL")
_MODELS = ("CLIMATOLOGY", "PRICE_ONLY", "PRICE_VOLUME", "SHUFFLED_VOLUME")
_MODE = "CURRENT_SNAPSHOT_RETROSPECTIVE"
# partition, producer, purpose, required hashes, parent phases
_PHASES = {
    "INPUT_QUALIFICATION": (None, "retrospective_input_read_task", "discovery", (), ()),
    "SNAPSHOT_FREEZE": (
        None,
        "retrospective_input_read_task",
        "discovery",
        ("input_check_hash",),
        ("INPUT_QUALIFICATION",),
    ),
    "TRAIN_FEATURE": (
        "TRAIN",
        "retrospective_feature_read_task",
        "feature_build",
        (),
        (),
    ),
    "TRAIN_FIT": (
        "TRAIN",
        "retrospective_training_read_task",
        "model_training",
        ("feature_rows_hash",),
        ("TRAIN_FEATURE",),
    ),
    "DEVELOPMENT_FEATURE": (
        "DEVELOPMENT",
        "retrospective_feature_read_task",
        "feature_build",
        ("model_cards_hash",),
        ("TRAIN_FIT",),
    ),
    "RETROSPECTIVE_EVAL_FEATURE": (
        "RETROSPECTIVE_EVAL",
        "retrospective_feature_read_task",
        "feature_build",
        ("model_cards_hash",),
        ("TRAIN_FIT",),
    ),
    "DEVELOPMENT_OUTCOME": (
        "DEVELOPMENT",
        "retrospective_outcome_read_task",
        "validation",
        ("model_cards_hash", "predictions_hash"),
        ("DEVELOPMENT_FEATURE", "RETROSPECTIVE_EVAL_FEATURE"),
    ),
    "RETROSPECTIVE_EVAL_OUTCOME": (
        "RETROSPECTIVE_EVAL",
        "retrospective_outcome_read_task",
        "validation",
        ("model_cards_hash", "predictions_hash"),
        ("DEVELOPMENT_FEATURE", "RETROSPECTIVE_EVAL_FEATURE"),
    ),
}
_TASK_TYPES = frozenset(row[1] for row in _PHASES.values())
_HASH_FIELDS = (
    "feature_rows_hash",
    "model_cards_hash",
    "predictions_hash",
    "input_check_hash",
)
_PROTOCOL_HASHES = (
    "research_design_hash",
    "core_code_hash",
    "adapter_code_hash",
    "source_scope_hash",
    "prior_observation_hash",
)
_STUDY_HASHES = (
    "input_manifest_hash",
    "calendar_manifest_hash",
    "cohort_manifest_hash",
    "source_aliases_hash",
)
_CLAIMS = {
    "data_mode": _MODE,
    "models": _MODELS,
    "untouched_claim": False,
    "causal_pit_claim": False,
}


@dataclass(frozen=True, slots=True)
class RetrospectiveSourceSlice:
    scope_key: str
    capture_revision: str
    source_scope_hash: str
    interval_start: str
    interval_end: str
    input_manifest_hash: str | None = None

    def __post_init__(self):
        _text(self.scope_key, "scope_key")
        if (
            type(self.capture_revision) is not str
            or re.fullmatch(r"local-capture:sha256:[0-9a-f]{64}", self.capture_revision)
            is None
        ):
            raise ValueError(
                "capture_revision must explicitly name a planned local capture"
            )
        _hash(self.source_scope_hash, "source_scope_hash")
        _instant(self.interval_start)
        _instant(self.interval_end)
        if self.interval_start >= self.interval_end:
            raise ValueError("source interval must be nonempty")
        if self.input_manifest_hash is not None:
            _hash(self.input_manifest_hash, "input_manifest_hash")

    @property
    def capture_state(self) -> str:
        return (
            "CANDIDATE_SCOPE" if self.input_manifest_hash is None else "FROZEN_CAPTURE"
        )

    @property
    def payload(self) -> Mapping[str, Any]:
        return MappingProxyType(
            {
                "scope_key": self.scope_key,
                "capture_revision": self.capture_revision,
                "source_scope_hash": self.source_scope_hash,
                "interval_start": self.interval_start,
                "interval_end": self.interval_end,
                "input_manifest_hash": self.input_manifest_hash,
            }
        )


def _source(value: object) -> RetrospectiveSourceSlice:
    wire = _mapping(
        value,
        {
            "scope_key",
            "capture_revision",
            "source_scope_hash",
            "interval_start",
            "interval_end",
            "input_manifest_hash",
        },
    )
    return RetrospectiveSourceSlice(**wire)


@dataclass(frozen=True, slots=True)
class RetrospectiveStudyProtocol:
    study_key: str
    research_design_hash: str
    core_code_hash: str
    adapter_code_hash: str
    source_scope_hash: str
    prior_observation_hash: str
    source_intervals: tuple[tuple[str, str, str], ...]

    def __post_init__(self):
        _text(self.study_key, "study_key")
        for name in _PROTOCOL_HASHES:
            _hash(getattr(self, name), name)
        if type(self.source_intervals) is not tuple or not self.source_intervals:
            raise ValueError("source intervals must be an explicit immutable sequence")
        keys = []
        for row in self.source_intervals:
            if type(row) is not tuple or len(row) != 3:
                raise ValueError("source interval must be an exact triple")
            key, start, end = row
            RetrospectiveSourceSlice(
                key, self.capture_revision, self.source_scope_hash, start, end
            )
            keys.append(key)
        if keys != sorted(set(keys)):
            raise ValueError("scope_key must be sorted and unique")

    @property
    def capture_revision(self) -> str:
        return "local-capture:" + canonical_sha256(
            (
                "retrospective-local-capture-v1",
                self.study_key,
                self.research_design_hash,
                self.source_scope_hash,
                self.prior_observation_hash,
            )
        )

    @property
    def source_slices(self) -> tuple[RetrospectiveSourceSlice, ...]:
        return tuple(
            RetrospectiveSourceSlice(
                key, self.capture_revision, self.source_scope_hash, start, end
            )
            for key, start, end in self.source_intervals
        )

    @property
    def models(self):
        return _MODELS

    @property
    def data_mode(self):
        return _MODE

    @property
    def untouched_claim(self):
        return False

    @property
    def causal_pit_claim(self):
        return False

    @property
    def payload(self) -> Mapping[str, Any]:
        return MappingProxyType(
            {
                "study_key": self.study_key,
                **{name: getattr(self, name) for name in _PROTOCOL_HASHES},
                "source_intervals": self.source_intervals,
                "capture_revision": self.capture_revision,
                **_CLAIMS,
            }
        )


@dataclass(frozen=True, slots=True)
class RetrospectiveStudyDeclaration:
    protocol_ref: ArtifactRef
    input_manifest_hash: str
    calendar_manifest_hash: str
    cohort_manifest_hash: str
    source_aliases_hash: str
    partitions: tuple[tuple[str, RetrospectiveSourceSlice, str, str], ...]

    def __post_init__(self):
        _ref(self.protocol_ref, _PROTOCOL)
        for name in _STUDY_HASHES:
            _hash(getattr(self, name), name)
        if type(self.partitions) is not tuple or len(self.partitions) != 3:
            raise ValueError("exactly three retrospective partitions required")
        previous = None
        for role, row in zip(_ROLES, self.partitions, strict=True):
            if type(row) is not tuple or len(row) != 4 or row[0] != role:
                raise ValueError(
                    "partition order must be TRAIN/DEVELOPMENT/RETROSPECTIVE_EVAL"
                )
            _, source, start, end = row
            if type(source) is not RetrospectiveSourceSlice:
                raise ValueError("source_slice must be a RetrospectiveSourceSlice")
            _instant(start)
            _instant(end)
            if source.input_manifest_hash != self.input_manifest_hash:
                raise ValueError("frozen input manifest mismatch")
            if not source.interval_start <= start < end <= source.interval_end:
                raise ValueError("decision interval outside read interval")
            if previous is not None and previous > source.interval_start:
                raise ValueError("read intervals overlap")
            previous = source.interval_end

    @property
    def payload(self) -> Mapping[str, Any]:
        return MappingProxyType(
            {
                "protocol_ref": self.protocol_ref,
                **{name: getattr(self, name) for name in _STUDY_HASHES},
                "partitions": tuple(
                    {
                        "partition": role,
                        "source_slice": source.payload,
                        "decision_interval_start": start,
                        "decision_interval_end": end,
                    }
                    for role, source, start, end in self.partitions
                ),
                **_CLAIMS,
            }
        )


@dataclass(frozen=True, slots=True)
class RetrospectiveReadTask:
    context_ref: ArtifactRef
    phase: str
    source_slice: RetrospectiveSourceSlice
    feature_rows_hash: str | None = None
    model_cards_hash: str | None = None
    predictions_hash: str | None = None
    input_check_hash: str | None = None

    def __post_init__(self):
        if type(self.phase) is not str or self.phase not in _PHASES:
            raise ValueError("unknown retrospective phase")
        role, _, _, required, _ = _PHASES[self.phase]
        _ref(self.context_ref, _PROTOCOL if role is None else _STUDY)
        if type(self.source_slice) is not RetrospectiveSourceSlice:
            raise ValueError("source_slice must be a RetrospectiveSourceSlice")
        if (self.source_slice.input_manifest_hash is None) != (role is None):
            raise ValueError("phase and capture state disagree")
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
    def payload(self) -> Mapping[str, Any]:
        return MappingProxyType(
            {
                "context_ref": self.context_ref,
                "phase": self.phase,
                "source_slice": self.source_slice.payload,
                **{name: getattr(self, name) for name in _HASH_FIELDS},
            }
        )


@dataclass(frozen=True, slots=True)
class PublishedRetrospectiveArtifact:
    ref: ArtifactRef
    entry_ref: LogEntryRef
    checkpoint: LogCheckpoint

    @property
    def publication(self) -> tuple[ArtifactRef, LogEntryRef, LogCheckpoint]:
        return self.ref, self.entry_ref, self.checkpoint


def _claims(wire: Mapping[str, Any]) -> None:
    if any(
        canonical_bytes(wire[key]) != canonical_bytes(value)
        for key, value in _CLAIMS.items()
    ):
        raise ValueError("retrospective interpretation cannot be widened")


def _protocol(value: object) -> RetrospectiveStudyProtocol:
    w = _mapping(
        value,
        {
            "study_key",
            *_PROTOCOL_HASHES,
            "source_intervals",
            "capture_revision",
            *_CLAIMS,
        },
    )
    _claims(w)
    p = RetrospectiveStudyProtocol(
        study_key=w["study_key"],
        source_intervals=w["source_intervals"],
        **{k: w[k] for k in _PROTOCOL_HASHES},
    )
    if w["capture_revision"] != p.capture_revision:
        raise ValueError("planned local capture identity mismatch")
    return p


def _study(value: object) -> RetrospectiveStudyDeclaration:
    w = _mapping(value, {"protocol_ref", *_STUDY_HASHES, "partitions", *_CLAIMS})
    _claims(w)
    if type(w["partitions"]) is not tuple:
        raise ValueError("partitions must be immutable")
    parts = []
    for row in w["partitions"]:
        v = _mapping(
            row,
            {
                "partition",
                "source_slice",
                "decision_interval_start",
                "decision_interval_end",
            },
        )
        parts.append(
            (
                v["partition"],
                _source(v["source_slice"]),
                v["decision_interval_start"],
                v["decision_interval_end"],
            )
        )
    return RetrospectiveStudyDeclaration(
        protocol_ref=_ref_from_wire(w["protocol_ref"], _PROTOCOL),
        partitions=tuple(parts),
        **{k: w[k] for k in _STUDY_HASHES},
    )


def _task(value: object) -> RetrospectiveReadTask:
    w = _mapping(value, {"context_ref", "phase", "source_slice", *_HASH_FIELDS})
    phase = w["phase"]
    if type(phase) is not str or phase not in _PHASES:
        raise ValueError("unknown retrospective phase")
    return RetrospectiveReadTask(
        _ref_from_wire(
            w["context_ref"], _PROTOCOL if _PHASES[phase][0] is None else _STUDY
        ),
        phase,
        _source(w["source_slice"]),
        *(w[k] for k in _HASH_FIELDS),
    )


def _owner(f: LocalFoundation, ref: ArtifactRef, kind: str):
    _ref(ref, kind)
    source = f.read(ref=ref)
    if (
        source.envelope.artifact_type != kind
        or type(source.envelope.schema_version) is not int
        or source.envelope.schema_version != 1
    ):
        raise ValueError("wrong owner artifact schema")
    event = canonical_sha256(("artifact-publication-v1", _LOG, ref))
    entries = f.entries(_LOG, f.checkpoint(_LOG))
    entry = next(
        (
            e
            for e in entries
            if e.event_id == event and e.payload == source.source_bytes
        ),
        None,
    )
    if entry is None:
        raise ValueError("missing actual Research owner publication")
    return source.envelope, entry, entries


def _study_scope(study: RetrospectiveStudyDeclaration, p: RetrospectiveStudyProtocol):
    candidates = {s.scope_key: s for s in p.source_slices}
    for _, s, _, _ in study.partitions:
        old = candidates.get(s.scope_key)
        if (
            old is None
            or s.capture_revision != p.capture_revision
            or s.source_scope_hash != p.source_scope_hash
            or not old.interval_start
            <= s.interval_start
            < s.interval_end
            <= old.interval_end
        ):
            raise ValueError("study source scope conflicts with protocol")


def _task_scope(
    task: RetrospectiveReadTask,
    context: RetrospectiveStudyProtocol | RetrospectiveStudyDeclaration,
):
    role = _PHASES[task.phase][0]
    if role is None:
        if (
            type(context) is not RetrospectiveStudyProtocol
            or task.source_slice not in context.source_slices
        ):
            raise ValueError("task source scope differs from protocol")
    elif type(
        context
    ) is not RetrospectiveStudyDeclaration or task.source_slice != next(
        row[1] for row in context.partitions if row[0] == role
    ):
        raise ValueError("task source scope differs from study")


def _parents(
    task: RetrospectiveReadTask,
    context: RetrospectiveStudyProtocol | RetrospectiveStudyDeclaration,
):
    result = []
    for phase in _PHASES[task.phase][4]:
        key = task.source_slice.scope_key
        if type(context) is RetrospectiveStudyDeclaration:
            key = next(
                row[1].scope_key
                for row in context.partitions
                if row[0] == _PHASES[phase][0]
            )
        result.append((phase, key))
    return tuple(result)


def _tasks(
    f: LocalFoundation,
    context_ref: ArtifactRef,
    context,
    context_entry: LogEntry,
    entries,
):
    result = {}
    for entry in entries:
        e = _envelope(entry.payload)
        if (
            e.artifact_type not in _TASK_TYPES
            or not isinstance(e.payload, Mapping)
            or e.payload.get("context_ref") != context_ref.to_canonical_dict()
        ):
            continue
        task = _task(e.payload)
        if (
            type(e.schema_version) is not int
            or e.schema_version != 1
            or e.artifact_type != task.artifact_type
        ):
            raise ValueError("task producer/version mismatch")
        event = canonical_sha256(
            (
                "retrospective-read-task-publication-v1",
                context_ref,
                task.phase,
                task.source_slice.scope_key,
            )
        )
        if (
            entry.event_id != event
            or f.read(ref=ArtifactRef.from_envelope(e)).source_bytes != entry.payload
        ):
            raise ValueError("invalid task publication")
        _task_scope(task, context)
        key = (task.phase, task.source_slice.scope_key)
        if key in result or entry.ledger_sequence <= context_entry.ledger_sequence:
            raise ValueError("conflicting task publication order")
        result[key] = (task, entry)
    for task, entry in result.values():
        if any(
            key not in result or result[key][1].ledger_sequence >= entry.ledger_sequence
            for key in _parents(task, context)
        ):
            raise ValueError("missing or late published prerequisite")
    return result


def _foundation(f):
    if type(f) is not LocalFoundation:
        raise TypeError("foundation must be LocalFoundation")


def publish_retrospective_protocol(
    protocol: RetrospectiveStudyProtocol, foundation: LocalFoundation
) -> PublishedRetrospectiveArtifact:
    if type(protocol) is not RetrospectiveStudyProtocol:
        raise TypeError("protocol required")
    _foundation(foundation)
    e = ArtifactEnvelope.create(_PROTOCOL, 1, protocol.payload)
    if _protocol(e.payload) != protocol:
        raise ValueError("protocol is not canonical")
    ref = ArtifactRef.from_envelope(e)
    return PublishedRetrospectiveArtifact(
        *_publish(
            e, canonical_sha256(("artifact-publication-v1", _LOG, ref)), foundation
        )
    )


def _study_prerequisites(foundation, study, study_entry=None):
    p, pe, entries = _owner(foundation, study.protocol_ref, _PROTOCOL)
    protocol = _protocol(p.payload)
    _study_scope(study, protocol)
    tasks = _tasks(foundation, study.protocol_ref, protocol, pe, entries)
    required = [
        ("SNAPSHOT_FREEZE", source.scope_key) for _, source, _, _ in study.partitions
    ]
    if any(key not in tasks for key in required):
        raise ValueError("missing published snapshot prerequisite")
    if study_entry is not None and (
        pe.ledger_sequence >= study_entry.ledger_sequence
        or any(
            tasks[key][1].ledger_sequence >= study_entry.ledger_sequence
            for key in required
        )
    ):
        raise ValueError("Study prerequisite publication order is invalid")
    return entries


def publish_retrospective_study(
    study: RetrospectiveStudyDeclaration, foundation: LocalFoundation
) -> PublishedRetrospectiveArtifact:
    if type(study) is not RetrospectiveStudyDeclaration:
        raise TypeError("study required")
    _foundation(foundation)
    e = ArtifactEnvelope.create(_STUDY, 1, study.payload)
    if _study(e.payload) != study:
        raise ValueError("study is not canonical")
    entries = _study_prerequisites(foundation, study)
    ref = ArtifactRef.from_envelope(e)
    event = canonical_sha256(("artifact-publication-v1", _LOG, ref))
    original = next((entry for entry in entries if entry.event_id == event), None)
    if original is not None:
        if original.payload != canonical_bytes(e):
            raise ValueError("Study original publication bytes differ")
        _study_prerequisites(foundation, study, original)
    return PublishedRetrospectiveArtifact(*_publish(e, event, foundation))


def publish_retrospective_read_task(
    task: RetrospectiveReadTask, foundation: LocalFoundation
) -> PublishedRetrospectiveArtifact:
    if type(task) is not RetrospectiveReadTask:
        raise TypeError("task required")
    _foundation(foundation)
    e = ArtifactEnvelope.create(task.artifact_type, 1, task.payload)
    if _task(e.payload) != task:
        raise ValueError("task is not canonical")
    role = _PHASES[task.phase][0]
    context, ce, entries = _owner(
        foundation, task.context_ref, _PROTOCOL if role is None else _STUDY
    )
    decoded = _protocol(context.payload) if role is None else _study(context.payload)
    if type(decoded) is RetrospectiveStudyDeclaration:
        _study_prerequisites(foundation, decoded, ce)
    _task_scope(task, decoded)
    tasks = _tasks(foundation, task.context_ref, decoded, ce, entries)
    if any(key not in tasks for key in _parents(task, decoded)):
        raise ValueError("missing published prerequisite")
    for name in ("model_cards_hash", "predictions_hash"):
        value = getattr(task, name)
        if value is not None and any(
            getattr(t, name) not in (None, value) for t, _ in tasks.values()
        ):
            raise ValueError(f"one frozen {name} required")
    event = canonical_sha256(
        (
            "retrospective-read-task-publication-v1",
            task.context_ref,
            task.phase,
            task.source_slice.scope_key,
        )
    )
    return PublishedRetrospectiveArtifact(*_publish(e, event, foundation))
