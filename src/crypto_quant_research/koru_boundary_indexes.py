from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from crypto_quant_bundle_builder import (
    KORU_AGGREGATE_TRADE_BOUNDARY_INDEX_AUTHORITY_ARTIFACT_TYPE_V3,
    BinanceUsdmKoruAggregateTradeBoundaryIndexResultV3,
    create_binance_usdm_koru_aggregate_trade_boundary_index_authority_v3,
    open_binance_usdm_koru_aggregate_trade_boundary_index_authority_v3,
)
from crypto_quant_domain import (
    ArtifactEnvelope,
    ArtifactRef,
    canonical_bytes,
    canonical_sha256,
)
from crypto_quant_foundation import AppendReceipt, LogEntry, LogEntryRef

from .raw_blob_snapshots import (
    RawBlobSnapshotFoundation,
    open_verified_raw_blob_snapshot,
)

BOUNDARY_INDEXES_LOG = "research.boundary_indexes.v1"
_BOUNDARY_INDEX_FACT_TYPE = "research.boundary_indexes.v1"


class BoundaryIndexFoundation(RawBlobSnapshotFoundation, Protocol):
    def put(self, *, envelope: ArtifactEnvelope) -> ArtifactRef: ...

    def append(self, log_name: str, event_id: str, payload: bytes) -> AppendReceipt: ...

    def entries(
        self, log_name: str, through: LogEntryRef | None = None
    ) -> tuple[LogEntry, ...]: ...


def _entry_dict(entry_ref: LogEntryRef) -> dict[str, object]:
    if type(entry_ref) is not LogEntryRef:
        raise TypeError("publication entry ref must be a LogEntryRef")
    return {
        "log_name": entry_ref.log_name,
        "log_sequence": entry_ref.log_sequence,
        "receipt_hash": entry_ref.receipt_hash,
    }


def _raw_snapshot_authority_identity(
    manifest_ref: ArtifactRef, publication_entry_ref: LogEntryRef, snapshot_id: str
) -> dict[str, object]:
    return {
        "type": "research.raw_blob_snapshot_authority_identity.v1",
        "manifest_ref": manifest_ref.to_canonical_dict(),
        "publication_entry_ref": _entry_dict(publication_entry_ref),
        "snapshot_id": snapshot_id,
    }


@dataclass(frozen=True, slots=True)
class BoundaryIndexPublicationFact:
    manifest_ref: ArtifactRef
    raw_snapshot_publication_entry_ref: LogEntryRef
    authority_ref: ArtifactRef
    request_hash: str
    result_digest: str

    def __post_init__(self) -> None:
        if (
            type(self.manifest_ref) is not ArtifactRef
            or self.manifest_ref.artifact_type != "raw_blob_snapshot_manifest"
            or self.manifest_ref.schema_version != 1
            or type(self.raw_snapshot_publication_entry_ref) is not LogEntryRef
            or type(self.authority_ref) is not ArtifactRef
            or self.authority_ref.artifact_type
            != KORU_AGGREGATE_TRADE_BOUNDARY_INDEX_AUTHORITY_ARTIFACT_TYPE_V3
            or self.authority_ref.schema_version != 3
        ):
            raise ValueError("boundary publication references are invalid")
        for value in (self.request_hash, self.result_digest):
            if type(value) is not str or not value.startswith("sha256:") or len(value) != 71:
                raise ValueError("boundary publication identity is invalid")

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "type": _BOUNDARY_INDEX_FACT_TYPE,
            "schema_version": 1,
            "manifest_ref": self.manifest_ref.to_canonical_dict(),
            "raw_snapshot_publication_entry_ref": _entry_dict(
                self.raw_snapshot_publication_entry_ref
            ),
            "authority_ref": self.authority_ref.to_canonical_dict(),
            "request_hash": self.request_hash,
            "result_digest": self.result_digest,
        }


def _event_id(fact: BoundaryIndexPublicationFact) -> str:
    return canonical_sha256(("research-boundary-indexes-v1", BOUNDARY_INDEXES_LOG, fact.to_canonical_dict()))


def _exact_entry(
    foundation: BoundaryIndexFoundation,
    fact: BoundaryIndexPublicationFact,
    entry_ref: LogEntryRef,
) -> None:
    if type(entry_ref) is not LogEntryRef or entry_ref.log_name != BOUNDARY_INDEXES_LOG:
        raise ValueError("boundary authority publication entry is invalid")
    try:
        entries = foundation.entries(BOUNDARY_INDEXES_LOG, through=entry_ref)
    except Exception as error:
        raise ValueError("boundary authority publication entry is unavailable") from error
    if (
        type(entries) is not tuple
        or not entries
        or entries[-1].entry_ref != entry_ref
        or entries[-1].event_id != _event_id(fact)
        or entries[-1].payload != canonical_bytes(fact.to_canonical_dict())
    ):
        raise ValueError("boundary authority publication entry does not bind authority")


@dataclass(frozen=True, slots=True)
class BoundaryIndexPublication:
    authority_ref: ArtifactRef
    publication_entry_ref: LogEntryRef
    result_digest: str


def publish_koru_aggregate_trade_boundary_index_authority_v3(
    foundation: BoundaryIndexFoundation,
    *,
    result: BinanceUsdmKoruAggregateTradeBoundaryIndexResultV3,
    manifest_ref: ArtifactRef,
    raw_snapshot_publication_entry_ref: LogEntryRef,
) -> BoundaryIndexPublication:
    """Publish a V3 result only after opening its exact raw-snapshot authority."""
    view = open_verified_raw_blob_snapshot(
        foundation, manifest_ref, raw_snapshot_publication_entry_ref
    )
    raw_identity = _raw_snapshot_authority_identity(
        manifest_ref, raw_snapshot_publication_entry_ref, view.manifest.snapshot_id
    )
    envelope, authority_ref = create_binance_usdm_koru_aggregate_trade_boundary_index_authority_v3(
        result, view, raw_identity
    )
    if authority_ref.content_hash == result.result_digest:
        raise ValueError("boundary authority ref must differ from result digest")
    if foundation.put(envelope=envelope) != authority_ref:
        raise ValueError("Foundation boundary authority ref does not match envelope")
    try:
        readback = foundation.read(ref=authority_ref)
    except Exception as error:
        raise ValueError("boundary authority artifact is unpublished") from error
    if readback.envelope != envelope or readback.source_bytes != canonical_bytes(envelope):
        raise ValueError("Foundation boundary authority readback does not match envelope")
    opened = open_binance_usdm_koru_aggregate_trade_boundary_index_authority_v3(
        readback.source_bytes, authority_ref, view, raw_identity
    )
    fact = BoundaryIndexPublicationFact(
        manifest_ref,
        raw_snapshot_publication_entry_ref,
        authority_ref,
        opened.request.request_hash,
        opened.result_digest,
    )
    receipt = foundation.append(
        BOUNDARY_INDEXES_LOG, _event_id(fact), canonical_bytes(fact.to_canonical_dict())
    )
    entry_ref = getattr(receipt, "entry_ref", None)
    if type(entry_ref) is not LogEntryRef:
        raise ValueError("Foundation boundary publication append did not return a LogEntryRef")
    _exact_entry(foundation, fact, entry_ref)
    return BoundaryIndexPublication(authority_ref, entry_ref, opened.result_digest)


def open_published_koru_aggregate_trade_boundary_index_authority_v3(
    foundation: BoundaryIndexFoundation,
    *,
    manifest_ref: ArtifactRef,
    raw_snapshot_publication_entry_ref: LogEntryRef,
    authority_ref: ArtifactRef,
    publication_entry_ref: LogEntryRef,
) -> BinanceUsdmKoruAggregateTradeBoundaryIndexResultV3:
    """Open only an authority tied to both raw and boundary owner-log facts."""
    view = open_verified_raw_blob_snapshot(
        foundation, manifest_ref, raw_snapshot_publication_entry_ref
    )
    raw_identity = _raw_snapshot_authority_identity(
        manifest_ref, raw_snapshot_publication_entry_ref, view.manifest.snapshot_id
    )
    try:
        readback = foundation.read(ref=authority_ref)
    except Exception as error:
        raise ValueError("boundary authority artifact is unavailable") from error
    opened = open_binance_usdm_koru_aggregate_trade_boundary_index_authority_v3(
        readback.source_bytes, authority_ref, view, raw_identity
    )
    _exact_entry(
        foundation,
        BoundaryIndexPublicationFact(
            manifest_ref,
            raw_snapshot_publication_entry_ref,
            authority_ref,
            opened.request.request_hash,
            opened.result_digest,
        ),
        publication_entry_ref,
    )
    return opened
