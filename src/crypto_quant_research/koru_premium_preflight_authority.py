"""KORU-only authority spine for the fixed premium preflight pipeline."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import NoReturn, Protocol, cast

from crypto_quant_bundle_builder import (
    KORU_AGGREGATE_TRADE_BOUNDARY_INDEX_AUTHORITY_ARTIFACT_TYPE_V3,
)
from crypto_quant_domain import ArtifactRef, canonical_bytes, canonical_sha256
from crypto_quant_foundation import LogEntryRef

from .koru_boundary_indexes import (
    BOUNDARY_INDEXES_LOG,
    BoundaryIndexFoundation,
    BoundaryIndexPublicationFact,
    open_published_koru_aggregate_trade_boundary_index_authority_v3,
)
from .raw_blob_snapshots import (
    RAW_BLOB_SNAPSHOTS_LOG,
    RawBlobSnapshotPublicationFact,
    open_verified_raw_blob_snapshot,
)

_SOURCE_PROJECTIONS_LOG = "research.source_projections.v1"
_ARTIFACTS_LOG = "research.artifacts.v1"
_HASH = re.compile(r"sha256:[0-9a-f]{64}\Z")
_ADMISSION_TOKEN = object()

KORU_PREMIUM_DISCOVERY_SCOPE_V1 = MappingProxyType(
    {
        "timeline_window_start": MappingProxyType(
            {"epoch_nanoseconds": 1_784_109_600_000_000_000}
        ),
        "timeline_window_end_exclusive": MappingProxyType(
            {"epoch_nanoseconds": 1_787_569_200_000_000_000}
        ),
    }
)


class KoruPremiumPreflightAuthorityFailureCodeV1(str, Enum):
    MISSING_STAGE = "missing_stage"
    INVALID_STAGE = "invalid_stage"
    UNPUBLISHED_STAGE = "unpublished_stage"
    STAGE_ORDER = "stage_order"
    STAGE_SUBSTITUTION = "stage_substitution"


KORU_PREMIUM_PREFLIGHT_FAILURE_PRECEDENCE_V1 = (
    KoruPremiumPreflightAuthorityFailureCodeV1.MISSING_STAGE,
    KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE,
    KoruPremiumPreflightAuthorityFailureCodeV1.STAGE_ORDER,
    KoruPremiumPreflightAuthorityFailureCodeV1.STAGE_SUBSTITUTION,
    KoruPremiumPreflightAuthorityFailureCodeV1.UNPUBLISHED_STAGE,
)


class KoruPremiumPreflightAuthorityErrorV1(ValueError):
    """Fail-closed authority-spine error with a stable V1 code."""

    def __init__(
        self, code: KoruPremiumPreflightAuthorityFailureCodeV1, message: str | None = None
    ) -> None:
        self.code = code
        super().__init__(message or code.value)


def _fail(
    code: KoruPremiumPreflightAuthorityFailureCodeV1, message: str | None = None
) -> NoReturn:
    raise KoruPremiumPreflightAuthorityErrorV1(code, message)


class KoruPremiumPreflightStageKindV1(str, Enum):
    RAW_SNAPSHOT = "raw_snapshot"
    AGGREGATE_BOUNDARY = "aggregate_boundary"
    SOURCE_PROJECTION = "source_projection"
    ECONOMICS = "economics"
    TARGET_OVERLAY = "target_overlay"
    READER_SET = "reader_set"


_STAGE_ORDER = (
    KoruPremiumPreflightStageKindV1.RAW_SNAPSHOT,
    KoruPremiumPreflightStageKindV1.AGGREGATE_BOUNDARY,
    KoruPremiumPreflightStageKindV1.SOURCE_PROJECTION,
    KoruPremiumPreflightStageKindV1.ECONOMICS,
    KoruPremiumPreflightStageKindV1.TARGET_OVERLAY,
    KoruPremiumPreflightStageKindV1.READER_SET,
)
_STAGE_ARTIFACTS = {
    KoruPremiumPreflightStageKindV1.RAW_SNAPSHOT: (
        "raw_blob_snapshot_manifest",
        1,
    ),
    KoruPremiumPreflightStageKindV1.AGGREGATE_BOUNDARY: (
        KORU_AGGREGATE_TRADE_BOUNDARY_INDEX_AUTHORITY_ARTIFACT_TYPE_V3,
        3,
    ),
    KoruPremiumPreflightStageKindV1.SOURCE_PROJECTION: (
        "binance_usdm_koru_tradifi_source_projection_authority_v1",
        1,
    ),
    KoruPremiumPreflightStageKindV1.ECONOMICS: (
        "koru_tradifi_economics_authority_v3",
        3,
    ),
    KoruPremiumPreflightStageKindV1.TARGET_OVERLAY: (
        "koru_tradifi_target_overlay_authority_v3",
        3,
    ),
    KoruPremiumPreflightStageKindV1.READER_SET: (
        "koru_premium_reader_set_authority_v1",
        1,
    ),
}
_STAGE_LOGS = {
    KoruPremiumPreflightStageKindV1.RAW_SNAPSHOT: RAW_BLOB_SNAPSHOTS_LOG,
    KoruPremiumPreflightStageKindV1.AGGREGATE_BOUNDARY: BOUNDARY_INDEXES_LOG,
    KoruPremiumPreflightStageKindV1.SOURCE_PROJECTION: _SOURCE_PROJECTIONS_LOG,
    KoruPremiumPreflightStageKindV1.ECONOMICS: _ARTIFACTS_LOG,
    KoruPremiumPreflightStageKindV1.TARGET_OVERLAY: _ARTIFACTS_LOG,
    KoruPremiumPreflightStageKindV1.READER_SET: _ARTIFACTS_LOG,
}


def _wire(value: object) -> object:
    try:
        return json.loads(canonical_bytes(value))
    except (TypeError, ValueError) as error:
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, str(error))


def _freeze(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(value[key]) for key in sorted(value)})
    if type(value) is list:
        return tuple(_freeze(item) for item in value)
    return value


def _plain(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if type(value) is tuple:
        return [_plain(item) for item in value]
    return value


def _same(left: object, right: object) -> bool:
    return canonical_bytes(left) == canonical_bytes(right)


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(type(key) is not str for key in value):
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, name)
    return cast(Mapping[str, object], value)


def _digest(value: object, name: str) -> str:
    if type(value) is not str or _HASH.fullmatch(value) is None:
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, f"{name} must be sha256")
    return value


def _entry_wire(entry: LogEntryRef) -> dict[str, object]:
    return {
        "log_name": entry.log_name,
        "log_sequence": entry.log_sequence,
        "receipt_hash": entry.receipt_hash,
    }


def _entry(value: object, log_name: str) -> LogEntryRef:
    if (
        type(value) is not LogEntryRef
        or value.log_name != log_name
        or type(value.log_sequence) is not int
        or value.log_sequence < 1
        or _HASH.fullmatch(value.receipt_hash) is None
    ):
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "invalid publication entry")
    return cast(LogEntryRef, value)


def _artifact(value: object, kind: KoruPremiumPreflightStageKindV1) -> ArtifactRef:
    expected_type, expected_version = _STAGE_ARTIFACTS[kind]
    if (
        type(value) is not ArtifactRef
        or value.artifact_type != expected_type
        or value.schema_version != expected_version
        or _HASH.fullmatch(value.content_hash) is None
    ):
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "invalid stage artifact")
    return cast(ArtifactRef, value)


def _artifact_from_wire(value: object) -> ArtifactRef:
    if (
        not isinstance(value, Mapping)
        or set(value) != {"type", "artifact_type", "schema_version", "content_hash"}
        or value.get("type") != "artifact_ref"
    ):
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "invalid artifact wire")
    wire = _mapping(value, "invalid artifact wire")
    try:
        return ArtifactRef(
            cast(str, wire["artifact_type"]),
            cast(int, wire["schema_version"]),
            cast(str, wire["content_hash"]),
        )
    except (TypeError, ValueError) as error:
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, str(error))


def _entry_from_wire(value: object) -> LogEntryRef:
    if not isinstance(value, Mapping) or set(value) != {
        "log_name",
        "log_sequence",
        "receipt_hash",
    }:
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "invalid entry wire")
    wire = _mapping(value, "invalid entry wire")
    return LogEntryRef(
        cast(str, wire["log_name"]),
        cast(int, wire["log_sequence"]),
        cast(str, wire["receipt_hash"]),
    )


def _generic_publication_fact(
    kind: KoruPremiumPreflightStageKindV1,
    artifact_ref: ArtifactRef,
    semantic_digest: str,
    scope_identity: object,
    source_identity: object,
) -> dict[str, object]:
    return {
        "type": "koru_premium_preflight_stage_publication_v1",
        "schema_version": 1,
        "stage_kind": kind.value,
        "artifact_ref": artifact_ref.to_canonical_dict(),
        "semantic_digest": semantic_digest,
        "scope_identity": _plain(scope_identity),
        "source_identity": _plain(source_identity),
    }


def _generic_event_id(owner_log: str, fact: object) -> str:
    return canonical_sha256(("koru-premium-preflight-stage-publication-v1", owner_log, fact))


def _raw_event_id(fact: object) -> str:
    return canonical_sha256(("raw-blob-snapshot-publication-v1", RAW_BLOB_SNAPSHOTS_LOG, fact))


def _boundary_event_id(fact: object) -> str:
    return canonical_sha256(("research-boundary-indexes-v1", BOUNDARY_INDEXES_LOG, fact))


@dataclass(frozen=True, slots=True)
class KoruPremiumPreflightStagePublicationFactV1:
    """One exact KORU owner-log publication and its semantic identity."""

    kind: KoruPremiumPreflightStageKindV1
    artifact_ref: ArtifactRef
    publication_entry_ref: LogEntryRef
    publication_fact: object
    semantic_digest: str
    scope_identity: object | None = None
    source_identity: object | None = None
    _admission_token: object | None = field(
        init=False, default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if type(self.kind) is not KoruPremiumPreflightStageKindV1:
            _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "invalid stage kind")
        artifact_ref = _artifact(self.artifact_ref, self.kind)
        entry_ref = _entry(self.publication_entry_ref, self.owner_log)
        semantic_digest = _digest(self.semantic_digest, "semantic_digest")
        publication_fact = _freeze(_wire(self.publication_fact))
        scope_identity = None if self.scope_identity is None else _freeze(_wire(self.scope_identity))
        source_identity = None if self.source_identity is None else _freeze(_wire(self.source_identity))
        if self.kind in {
            KoruPremiumPreflightStageKindV1.RAW_SNAPSHOT,
            KoruPremiumPreflightStageKindV1.AGGREGATE_BOUNDARY,
        }:
            if scope_identity is not None:
                _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "unexpected scope")
        elif (
            not _same(scope_identity, KORU_PREMIUM_DISCOVERY_SCOPE_V1)
            or not isinstance(source_identity, Mapping)
        ):
            _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "invalid KORU scope or source")
        expected_fact = self._expected_publication_fact(
            artifact_ref, semantic_digest, scope_identity, source_identity
        )
        if not _same(publication_fact, expected_fact):
            _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "publication fact mismatch")
        object.__setattr__(self, "artifact_ref", artifact_ref)
        object.__setattr__(self, "publication_entry_ref", entry_ref)
        object.__setattr__(self, "publication_fact", publication_fact)
        object.__setattr__(self, "semantic_digest", semantic_digest)
        object.__setattr__(self, "scope_identity", scope_identity)
        object.__setattr__(self, "source_identity", source_identity)

    @property
    def _admitted(self) -> bool:
        return self._admission_token is _ADMISSION_TOKEN

    @property
    def owner_log(self) -> str:
        return _STAGE_LOGS[self.kind]

    def _expected_publication_fact(
        self,
        artifact_ref: ArtifactRef,
        semantic_digest: str,
        scope_identity: object | None,
        source_identity: object | None,
    ) -> object:
        if self.kind is KoruPremiumPreflightStageKindV1.RAW_SNAPSHOT:
            if source_identity is not None:
                _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "raw source identity")
            return RawBlobSnapshotPublicationFact(artifact_ref, semantic_digest).to_canonical_dict()
        if self.kind is KoruPremiumPreflightStageKindV1.AGGREGATE_BOUNDARY:
            source = _mapping(source_identity, "boundary source identity")
            expected_keys = {"type", "raw_snapshot", "request_hash"}
            if set(source) != expected_keys or source.get("type") != (
                "koru_premium_boundary_inputs_v1"
            ):
                _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "boundary source identity")
            raw_identity = _mapping(source["raw_snapshot"], "raw identity")
            raw_ref = _artifact_from_wire(raw_identity.get("artifact_ref"))
            raw_entry = _entry_from_wire(raw_identity.get("publication_entry_ref"))
            _entry(raw_entry, RAW_BLOB_SNAPSHOTS_LOG)
            return BoundaryIndexPublicationFact(
                raw_ref,
                raw_entry,
                artifact_ref,
                _digest(source["request_hash"], "request_hash"),
                semantic_digest,
            ).to_canonical_dict()
        return _generic_publication_fact(
            self.kind, artifact_ref, semantic_digest, scope_identity, source_identity
        )

    @property
    def publication_event_id(self) -> str:
        fact = _plain(self.publication_fact)
        if self.kind is KoruPremiumPreflightStageKindV1.RAW_SNAPSHOT:
            return _raw_event_id(fact)
        if self.kind is KoruPremiumPreflightStageKindV1.AGGREGATE_BOUNDARY:
            return _boundary_event_id(fact)
        return _generic_event_id(self.owner_log, fact)

    @property
    def stage_identity(self) -> dict[str, object]:
        return {
            "type": "koru_premium_preflight_stage_identity_v1",
            "stage_kind": self.kind.value,
            "artifact_ref": self.artifact_ref.to_canonical_dict(),
            "semantic_digest": self.semantic_digest,
            "owner_log": self.owner_log,
            "publication_entry_ref": _entry_wire(self.publication_entry_ref),
        }

    def _canonical_dict(self) -> dict[str, object]:
        return {
            "type": "koru_premium_preflight_stage_fact_v1",
            "schema_version": 1,
            "stage_kind": self.kind.value,
            "owner_log": self.owner_log,
            "artifact_ref": self.artifact_ref.to_canonical_dict(),
            "publication_entry_ref": _entry_wire(self.publication_entry_ref),
            "publication_fact": _plain(self.publication_fact),
            "semantic_digest": self.semantic_digest,
            "scope_identity": _plain(self.scope_identity),
            "source_identity": _plain(self.source_identity),
        }

    def to_canonical_dict(self) -> dict[str, object]:
        if not self._admitted:
            _fail(KoruPremiumPreflightAuthorityFailureCodeV1.UNPUBLISHED_STAGE)
        return self._canonical_dict()

    @classmethod
    def _from_canonical_dict(cls, value: object) -> KoruPremiumPreflightStagePublicationFactV1:
        if not isinstance(value, Mapping) or set(value) != {
            "type",
            "schema_version",
            "stage_kind",
            "owner_log",
            "artifact_ref",
            "publication_entry_ref",
            "publication_fact",
            "semantic_digest",
            "scope_identity",
            "source_identity",
        } or value.get("type") != "koru_premium_preflight_stage_fact_v1" or value.get("schema_version") != 1:
            _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "stage fact schema")
        try:
            kind = KoruPremiumPreflightStageKindV1(value["stage_kind"])
        except (TypeError, ValueError) as error:
            _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, str(error))
        result = cls(
            kind=kind,
            artifact_ref=_artifact_from_wire(value["artifact_ref"]),
            publication_entry_ref=_entry_from_wire(value["publication_entry_ref"]),
            publication_fact=value["publication_fact"],
            semantic_digest=cast(str, value["semantic_digest"]),
            scope_identity=value["scope_identity"],
            source_identity=value["source_identity"],
        )
        if not _same(value, result._canonical_dict()):
            _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "noncanonical stage")
        return result


def _admit(stage: KoruPremiumPreflightStagePublicationFactV1) -> KoruPremiumPreflightStagePublicationFactV1:
    object.__setattr__(stage, "_admission_token", _ADMISSION_TOKEN)
    return stage


def admit_raw_blob_snapshot_publication_fact_v1(
    foundation: KoruPremiumPreflightAuthorityFoundationV1,
    *,
    manifest_ref: ArtifactRef,
    publication_entry_ref: LogEntryRef,
) -> KoruPremiumPreflightStagePublicationFactV1:
    """Admit a raw snapshot only after its artifact and exact owner-log fact open."""
    try:
        view = open_verified_raw_blob_snapshot(foundation, manifest_ref, publication_entry_ref)
    except Exception as error:  # noqa: BLE001 - fail closed at admission boundary
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.UNPUBLISHED_STAGE, str(error))
    fact = RawBlobSnapshotPublicationFact(view.manifest.artifact_ref, view.manifest.snapshot_id)
    return _admit(
        KoruPremiumPreflightStagePublicationFactV1(
            KoruPremiumPreflightStageKindV1.RAW_SNAPSHOT,
            fact.manifest_ref,
            publication_entry_ref,
            fact.to_canonical_dict(),
            fact.snapshot_id,
        )
    )


def admit_koru_aggregate_trade_boundary_index_publication_fact_v1(
    foundation: KoruPremiumPreflightAuthorityFoundationV1,
    *,
    raw_snapshot: KoruPremiumPreflightStagePublicationFactV1,
    authority_ref: ArtifactRef,
    publication_entry_ref: LogEntryRef,
) -> KoruPremiumPreflightStagePublicationFactV1:
    """Admit a boundary only after its artifact and both exact owner-log facts open."""
    if (
        type(raw_snapshot) is not KoruPremiumPreflightStagePublicationFactV1
        or raw_snapshot.kind is not KoruPremiumPreflightStageKindV1.RAW_SNAPSHOT
        or not raw_snapshot._admitted
    ):
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "raw snapshot admission")
    try:
        opened = open_published_koru_aggregate_trade_boundary_index_authority_v3(
            foundation,
            manifest_ref=raw_snapshot.artifact_ref,
            raw_snapshot_publication_entry_ref=raw_snapshot.publication_entry_ref,
            authority_ref=authority_ref,
            publication_entry_ref=publication_entry_ref,
        )
    except Exception as error:  # noqa: BLE001 - fail closed at admission boundary
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.UNPUBLISHED_STAGE, str(error))
    fact = BoundaryIndexPublicationFact(
        raw_snapshot.artifact_ref,
        raw_snapshot.publication_entry_ref,
        authority_ref,
        opened.request.request_hash,
        opened.result_digest,
    )
    return _admit(
        KoruPremiumPreflightStagePublicationFactV1(
            KoruPremiumPreflightStageKindV1.AGGREGATE_BOUNDARY,
            fact.authority_ref,
            publication_entry_ref,
            fact.to_canonical_dict(),
            fact.result_digest,
            source_identity={
                "type": "koru_premium_boundary_inputs_v1",
                "raw_snapshot": raw_snapshot.stage_identity,
                "request_hash": fact.request_hash,
            },
        )
    )


def create_koru_premium_preflight_stage_publication_fact_v1(
    foundation: KoruPremiumPreflightAuthorityFoundationV1,
    *,
    kind: KoruPremiumPreflightStageKindV1,
    artifact_ref: ArtifactRef,
    semantic_digest: str,
    scope_identity: object,
    source_identity: object,
    publication_entry_ref: LogEntryRef,
) -> KoruPremiumPreflightStagePublicationFactV1:
    """Admit one generic KORU stage only after artifact and owner-log verification."""
    if kind in {
        KoruPremiumPreflightStageKindV1.RAW_SNAPSHOT,
        KoruPremiumPreflightStageKindV1.AGGREGATE_BOUNDARY,
    }:
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "use current-fact admission")
    stage = KoruPremiumPreflightStagePublicationFactV1(
        kind,
        artifact_ref,
        publication_entry_ref,
        _generic_publication_fact(
            kind, artifact_ref, semantic_digest, scope_identity, source_identity
        ),
        semantic_digest,
        scope_identity,
        source_identity,
    )
    _verify_stage(foundation, stage, None)
    return _admit(stage)


class KoruPremiumPreflightAuthorityFoundationV1(BoundaryIndexFoundation, Protocol):
    """Narrow Foundation seam needed to reopen every admitted spine fact."""


def _validate_stage_sequence(
    stages: tuple[KoruPremiumPreflightStagePublicationFactV1, ...],
) -> None:
    if type(stages) is not tuple or len(stages) != len(_STAGE_ORDER):
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.MISSING_STAGE)
    if any(type(stage) is not KoruPremiumPreflightStagePublicationFactV1 for stage in stages):
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE)
    kinds = tuple(stage.kind for stage in stages)
    if len(set(kinds)) != len(kinds):
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "duplicate stage")
    if kinds != _STAGE_ORDER:
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.STAGE_ORDER)
    raw, boundary, source, economics, overlay, readers = stages
    boundary_source = _mapping(boundary.source_identity, "boundary source identity")
    expected_boundary = {
        "type": "koru_premium_boundary_inputs_v1",
        "raw_snapshot": raw.stage_identity,
        "request_hash": boundary_source.get("request_hash"),
    }
    expected_source = {
        "type": "koru_premium_source_projection_inputs_v1",
        "raw_snapshot": raw.stage_identity,
        "aggregate_boundary": boundary.stage_identity,
    }
    expected_economics = {
        "type": "koru_premium_economics_inputs_v1",
        "source_projection": source.stage_identity,
    }
    expected_overlay = {
        "type": "koru_premium_target_overlay_inputs_v1",
        "source_projection": source.stage_identity,
        "economics": economics.stage_identity,
    }
    expected_readers = {
        "type": "koru_premium_reader_set_inputs_v1",
        "source_projection": source.stage_identity,
        "economics": economics.stage_identity,
        "target_overlay": overlay.stage_identity,
    }
    if not all(
        _same(actual, expected)
        for actual, expected in (
            (boundary.source_identity, expected_boundary),
            (source.source_identity, expected_source),
            (economics.source_identity, expected_economics),
            (overlay.source_identity, expected_overlay),
            (readers.source_identity, expected_readers),
        )
    ):
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.STAGE_SUBSTITUTION)


@dataclass(frozen=True, slots=True)
class KoruPremiumPreflightAuthorityV1:
    """The six exact published facts required before KORU premium preflight."""

    stages: tuple[KoruPremiumPreflightStagePublicationFactV1, ...]

    def __post_init__(self) -> None:
        _validate_stage_sequence(self.stages)
        if any(not stage._admitted for stage in self.stages):
            _fail(KoruPremiumPreflightAuthorityFailureCodeV1.UNPUBLISHED_STAGE)

    @property
    def raw_snapshot(self) -> KoruPremiumPreflightStagePublicationFactV1:
        return self.stages[0]

    @property
    def aggregate_boundary(self) -> KoruPremiumPreflightStagePublicationFactV1:
        return self.stages[1]

    @property
    def source_projection(self) -> KoruPremiumPreflightStagePublicationFactV1:
        return self.stages[2]

    @property
    def economics(self) -> KoruPremiumPreflightStagePublicationFactV1:
        return self.stages[3]

    @property
    def target_overlay(self) -> KoruPremiumPreflightStagePublicationFactV1:
        return self.stages[4]

    @property
    def reader_set(self) -> KoruPremiumPreflightStagePublicationFactV1:
        return self.stages[5]

    def to_canonical_dict(self) -> dict[str, object]:
        if any(not stage._admitted for stage in self.stages):
            _fail(KoruPremiumPreflightAuthorityFailureCodeV1.UNPUBLISHED_STAGE)
        return {
            "type": "koru_premium_preflight_authority_v1",
            "schema_version": 1,
            "stages": tuple(stage.to_canonical_dict() for stage in self.stages),
        }

    @property
    def authority_digest(self) -> str:
        return canonical_sha256(self.to_canonical_dict())

    @property
    def authority_id(self) -> str:
        return "koru_premium_preflight_authority_v1:" + self.authority_digest[7:]

    @classmethod
    def from_canonical_dict(
        cls,
        foundation: KoruPremiumPreflightAuthorityFoundationV1,
        value: object,
    ) -> KoruPremiumPreflightAuthorityV1:
        if not isinstance(value, Mapping) or set(value) != {
            "type",
            "schema_version",
            "stages",
        } or value.get("type") != "koru_premium_preflight_authority_v1" or value.get("schema_version") != 1 or type(value.get("stages")) not in {list, tuple}:
            _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "authority schema")
        stages_value = cast(tuple[object, ...] | list[object], value["stages"])
        pending_stages = tuple(
            KoruPremiumPreflightStagePublicationFactV1._from_canonical_dict(item)
            for item in stages_value
        )
        if not _same(
            value,
            {
                "type": "koru_premium_preflight_authority_v1",
                "schema_version": 1,
                "stages": tuple(stage._canonical_dict() for stage in pending_stages),
            },
        ):
            _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "noncanonical authority")
        _validate_stage_sequence(pending_stages)
        _verify_stages(foundation, pending_stages)
        return cls(tuple(_admit(stage) for stage in pending_stages))


def _verify_stage(
    foundation: KoruPremiumPreflightAuthorityFoundationV1,
    stage: KoruPremiumPreflightStagePublicationFactV1,
    raw_snapshot: KoruPremiumPreflightStagePublicationFactV1 | None,
) -> None:
    try:
        if stage.kind is KoruPremiumPreflightStageKindV1.RAW_SNAPSHOT:
            open_verified_raw_blob_snapshot(
                foundation, stage.artifact_ref, stage.publication_entry_ref
            )
        elif stage.kind is KoruPremiumPreflightStageKindV1.AGGREGATE_BOUNDARY:
            if raw_snapshot is None:
                _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE)
            open_published_koru_aggregate_trade_boundary_index_authority_v3(
                foundation,
                manifest_ref=raw_snapshot.artifact_ref,
                raw_snapshot_publication_entry_ref=raw_snapshot.publication_entry_ref,
                authority_ref=stage.artifact_ref,
                publication_entry_ref=stage.publication_entry_ref,
            )
        else:
            foundation.read(ref=stage.artifact_ref)
        entries = foundation.entries(stage.owner_log, through=stage.publication_entry_ref)
    except KoruPremiumPreflightAuthorityErrorV1:
        raise
    except Exception as error:  # noqa: BLE001 - fail closed at verification boundary
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.UNPUBLISHED_STAGE, str(error))
    if (
        type(entries) is not tuple
        or not entries
        or entries[-1].entry_ref != stage.publication_entry_ref
        or entries[-1].event_id != stage.publication_event_id
        or entries[-1].payload != canonical_bytes(stage.publication_fact)
    ):
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.UNPUBLISHED_STAGE)


def _verify_stages(
    foundation: KoruPremiumPreflightAuthorityFoundationV1,
    stages: tuple[KoruPremiumPreflightStagePublicationFactV1, ...],
) -> None:
    raw_snapshot = stages[0]
    for stage in stages:
        _verify_stage(foundation, stage, raw_snapshot)


def verify_koru_premium_preflight_authority_v1(
    foundation: KoruPremiumPreflightAuthorityFoundationV1,
    authority: KoruPremiumPreflightAuthorityV1,
) -> None:
    """Verify every supplied stage against its artifact and designated owner log."""
    if type(authority) is not KoruPremiumPreflightAuthorityV1:
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE, "authority type")
    _verify_stages(foundation, authority.stages)


def construct_koru_premium_preflight_authority_v1(
    foundation: KoruPremiumPreflightAuthorityFoundationV1,
    *,
    stages: tuple[KoruPremiumPreflightStagePublicationFactV1, ...],
) -> KoruPremiumPreflightAuthorityV1:
    """Construct only from facts admitted through verified publication opens."""
    if any(
        type(stage) is not KoruPremiumPreflightStagePublicationFactV1 or not stage._admitted
        for stage in stages
    ):
        _fail(KoruPremiumPreflightAuthorityFailureCodeV1.UNPUBLISHED_STAGE)
    authority = KoruPremiumPreflightAuthorityV1(stages)
    verify_koru_premium_preflight_authority_v1(foundation, authority)
    return authority


def open_koru_premium_preflight_authority_v1(
    foundation: KoruPremiumPreflightAuthorityFoundationV1,
    value: object,
) -> KoruPremiumPreflightAuthorityV1:
    """Reopen canonical manifest bytes/value and admit all facts through verified opens."""
    return KoruPremiumPreflightAuthorityV1.from_canonical_dict(foundation, value)


__all__ = [
    "KORU_PREMIUM_DISCOVERY_SCOPE_V1",
    "KORU_PREMIUM_PREFLIGHT_FAILURE_PRECEDENCE_V1",
    "KoruPremiumPreflightAuthorityErrorV1",
    "KoruPremiumPreflightAuthorityFailureCodeV1",
    "KoruPremiumPreflightAuthorityFoundationV1",
    "KoruPremiumPreflightAuthorityV1",
    "KoruPremiumPreflightStageKindV1",
    "admit_koru_aggregate_trade_boundary_index_publication_fact_v1",
    "admit_raw_blob_snapshot_publication_fact_v1",
    "construct_koru_premium_preflight_authority_v1",
    "create_koru_premium_preflight_stage_publication_fact_v1",
    "open_koru_premium_preflight_authority_v1",
    "verify_koru_premium_preflight_authority_v1",
]
