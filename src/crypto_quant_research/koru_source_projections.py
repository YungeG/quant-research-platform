"""Research publication composition for KORU SourceProjectionV3 authority."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from crypto_quant_bundle_builder import (
    BinanceUsdmKoruAggregateTradeBoundaryIndexResultV3,
    BinanceUsdmKoruFundingRateHistorySourceBoundedNormalizationResultV1,
    BinanceUsdmKoruPriceBarsSourceBoundedNormalizationResultV1,
    BinanceUsdmKoruTradifiSourceProjectionRequestV3,
    BinanceUsdmKoruTradifiSourceProjectionResultV3,
    KoruTradifiCalendarUnitAuthorityResultV1,
    build_binance_usdm_koru_tradifi_source_projection_v3,
    create_binance_usdm_koru_tradifi_source_projection_authority_v3,
    open_binance_usdm_koru_tradifi_source_projection_authority_v3,
)
from crypto_quant_domain import (
    ArtifactRef,
    Scale,
    UtcInstant,
    canonical_bytes,
    canonical_sha256,
)
from crypto_quant_foundation import LogEntryRef

from .koru_boundary_indexes import (
    BoundaryIndexFoundation,
    BoundaryIndexPublicationFact,
    open_published_koru_aggregate_trade_boundary_index_authority_v3,
)
from .raw_blob_snapshots import (
    RawBlobSnapshotPublicationFact,
    open_verified_raw_blob_snapshot,
)

SOURCE_PROJECTIONS_LOG_V3 = "research.source_projections.v3"
_SOURCE_PROJECTION_FACT_TYPE_V3 = "research.source_projections.v3"


class SourceProjectionFoundationV3(BoundaryIndexFoundation, Protocol):
    """The narrow Foundation surface used to publish and reopen SourceProjectionV3."""


def _entry_dict(entry_ref: LogEntryRef) -> dict[str, object]:
    if type(entry_ref) is not LogEntryRef:
        raise TypeError("publication entry ref must be a LogEntryRef")
    return {
        "log_name": entry_ref.log_name,
        "log_sequence": entry_ref.log_sequence,
        "receipt_hash": entry_ref.receipt_hash,
    }


@dataclass(frozen=True, slots=True)
class KoruTradifiSourceProjectionScopeV3:
    timeline_window_start: UtcInstant
    timeline_window_end_exclusive: UtcInstant

    def __post_init__(self) -> None:
        if (
            type(self.timeline_window_start) is not UtcInstant
            or type(self.timeline_window_end_exclusive) is not UtcInstant
            or self.timeline_window_start >= self.timeline_window_end_exclusive
        ):
            raise ValueError("KORU source-projection scope is invalid")

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "timeline_window_start": self.timeline_window_start.to_canonical_dict(),
            "timeline_window_end_exclusive": self.timeline_window_end_exclusive.to_canonical_dict(),
        }


@dataclass(frozen=True, slots=True)
class SourceProjectionPublicationFactV3:
    raw_snapshot_fact: RawBlobSnapshotPublicationFact
    raw_snapshot_publication_entry_ref: LogEntryRef
    boundary_index_fact: BoundaryIndexPublicationFact
    boundary_index_publication_entry_ref: LogEntryRef
    authority_ref: ArtifactRef
    source_request_hash: str
    source_fragment_digest: str
    scope: KoruTradifiSourceProjectionScopeV3

    def __post_init__(self) -> None:
        if (
            type(self.raw_snapshot_fact) is not RawBlobSnapshotPublicationFact
            or type(self.raw_snapshot_publication_entry_ref) is not LogEntryRef
            or type(self.boundary_index_fact) is not BoundaryIndexPublicationFact
            or type(self.boundary_index_publication_entry_ref) is not LogEntryRef
            or type(self.authority_ref) is not ArtifactRef
            or self.authority_ref.artifact_type
            != "binance_usdm_koru_tradifi_source_projection_authority_v3"
            or self.authority_ref.schema_version != 3
            or type(self.scope) is not KoruTradifiSourceProjectionScopeV3
            or self.boundary_index_fact.manifest_ref != self.raw_snapshot_fact.manifest_ref
            or self.boundary_index_fact.raw_snapshot_publication_entry_ref
            != self.raw_snapshot_publication_entry_ref
        ):
            raise ValueError("source-projection publication fact references are invalid")
        for value in (self.source_request_hash, self.source_fragment_digest):
            if type(value) is not str or len(value) != 71 or not value.startswith("sha256:"):
                raise ValueError("source-projection publication identity is invalid")

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "type": _SOURCE_PROJECTION_FACT_TYPE_V3,
            "schema_version": 3,
            "raw_snapshot_fact": self.raw_snapshot_fact.to_canonical_dict(),
            "raw_snapshot_publication_entry_ref": _entry_dict(self.raw_snapshot_publication_entry_ref),
            "boundary_index_fact": self.boundary_index_fact.to_canonical_dict(),
            "boundary_index_publication_entry_ref": _entry_dict(self.boundary_index_publication_entry_ref),
            "authority_ref": self.authority_ref.to_canonical_dict(),
            "source_request_hash": self.source_request_hash,
            "source_fragment_digest": self.source_fragment_digest,
            "scope": self.scope.to_canonical_dict(),
        }


def _event_id(fact: SourceProjectionPublicationFactV3) -> str:
    return canonical_sha256(("research-source-projections-v3", SOURCE_PROJECTIONS_LOG_V3, fact.to_canonical_dict()))


def _exact_entry(
    foundation: SourceProjectionFoundationV3,
    fact: SourceProjectionPublicationFactV3,
    entry_ref: LogEntryRef,
) -> None:
    if type(entry_ref) is not LogEntryRef or entry_ref.log_name != SOURCE_PROJECTIONS_LOG_V3:
        raise ValueError("source-projection publication entry is invalid")
    try:
        entries = foundation.entries(SOURCE_PROJECTIONS_LOG_V3, through=entry_ref)
    except Exception as error:
        raise ValueError("source-projection publication entry is unavailable") from error
    if (
        type(entries) is not tuple
        or not entries
        or entries[-1].entry_ref != entry_ref
        or entries[-1].event_id != _event_id(fact)
        or entries[-1].payload != canonical_bytes(fact.to_canonical_dict())
    ):
        raise ValueError("source-projection publication entry does not bind authority")


def _open_inputs(
    foundation: SourceProjectionFoundationV3,
    *,
    raw_snapshot_fact: RawBlobSnapshotPublicationFact,
    raw_snapshot_publication_entry_ref: LogEntryRef,
    boundary_index_fact: BoundaryIndexPublicationFact,
    boundary_index_publication_entry_ref: LogEntryRef,
) -> BinanceUsdmKoruAggregateTradeBoundaryIndexResultV3:
    if (
        type(raw_snapshot_fact) is not RawBlobSnapshotPublicationFact
        or type(boundary_index_fact) is not BoundaryIndexPublicationFact
        or boundary_index_fact.manifest_ref != raw_snapshot_fact.manifest_ref
        or boundary_index_fact.raw_snapshot_publication_entry_ref
        != raw_snapshot_publication_entry_ref
    ):
        raise ValueError("raw and boundary publication facts do not bind")
    view = open_verified_raw_blob_snapshot(
        foundation, raw_snapshot_fact.manifest_ref, raw_snapshot_publication_entry_ref
    )
    if view.manifest.snapshot_id != raw_snapshot_fact.snapshot_id:
        raise ValueError("raw publication fact snapshot identity is invalid")
    opened = open_published_koru_aggregate_trade_boundary_index_authority_v3(
        foundation,
        manifest_ref=raw_snapshot_fact.manifest_ref,
        raw_snapshot_publication_entry_ref=raw_snapshot_publication_entry_ref,
        authority_ref=boundary_index_fact.authority_ref,
        publication_entry_ref=boundary_index_publication_entry_ref,
    )
    if (
        opened.request.request_hash != boundary_index_fact.request_hash
        or opened.result_digest != boundary_index_fact.result_digest
    ):
        raise ValueError("boundary publication fact identity is invalid")
    return opened


def _scope_request(
    scope: KoruTradifiSourceProjectionScopeV3,
    boundary: BinanceUsdmKoruAggregateTradeBoundaryIndexResultV3,
    instrument_catalog_hash: str,
    projection_scale: Scale,
    mark_price_results: tuple[BinanceUsdmKoruPriceBarsSourceBoundedNormalizationResultV1, ...],
    index_price_results: tuple[BinanceUsdmKoruPriceBarsSourceBoundedNormalizationResultV1, ...],
    funding_result: BinanceUsdmKoruFundingRateHistorySourceBoundedNormalizationResultV1,
    authority_result: KoruTradifiCalendarUnitAuthorityResultV1,
) -> BinanceUsdmKoruTradifiSourceProjectionRequestV3:
    if type(scope) is not KoruTradifiSourceProjectionScopeV3:
        raise TypeError("scope must be exact KORU SourceProjectionV3 scope")
    return BinanceUsdmKoruTradifiSourceProjectionRequestV3(
        scope.timeline_window_start,
        scope.timeline_window_end_exclusive,
        instrument_catalog_hash,
        projection_scale,
        boundary,
        mark_price_results,
        index_price_results,
        funding_result,
        authority_result,
    )


@dataclass(frozen=True, slots=True)
class SourceProjectionPublicationV3:
    authority_ref: ArtifactRef
    publication_entry_ref: LogEntryRef
    source_request_hash: str
    source_fragment_digest: str


def publish_koru_tradifi_source_projection_authority_v3(
    foundation: SourceProjectionFoundationV3,
    *,
    raw_snapshot_fact: RawBlobSnapshotPublicationFact,
    raw_snapshot_publication_entry_ref: LogEntryRef,
    boundary_index_fact: BoundaryIndexPublicationFact,
    boundary_index_publication_entry_ref: LogEntryRef,
    scope: KoruTradifiSourceProjectionScopeV3,
    instrument_catalog_hash: str,
    projection_scale: Scale,
    mark_price_results: tuple[BinanceUsdmKoruPriceBarsSourceBoundedNormalizationResultV1, ...],
    index_price_results: tuple[BinanceUsdmKoruPriceBarsSourceBoundedNormalizationResultV1, ...],
    funding_result: BinanceUsdmKoruFundingRateHistorySourceBoundedNormalizationResultV1,
    authority_result: KoruTradifiCalendarUnitAuthorityResultV1,
) -> SourceProjectionPublicationV3:
    """Open raw/boundary publication facts, then publish their V3 source projection."""
    boundary = _open_inputs(
        foundation,
        raw_snapshot_fact=raw_snapshot_fact,
        raw_snapshot_publication_entry_ref=raw_snapshot_publication_entry_ref,
        boundary_index_fact=boundary_index_fact,
        boundary_index_publication_entry_ref=boundary_index_publication_entry_ref,
    )
    outcome = build_binance_usdm_koru_tradifi_source_projection_v3(
        _scope_request(
            scope,
            boundary,
            instrument_catalog_hash,
            projection_scale,
            mark_price_results,
            index_price_results,
            funding_result,
            authority_result,
        )
    )
    if outcome.result is None:
        raise ValueError("source-projection V3 construction failed")
    envelope, authority_ref = create_binance_usdm_koru_tradifi_source_projection_authority_v3(outcome.result)
    if foundation.put(envelope=envelope) != authority_ref:
        raise ValueError("Foundation source-projection authority ref does not match envelope")
    try:
        readback = foundation.read(ref=authority_ref)
    except Exception as error:
        raise ValueError("source-projection authority artifact is unpublished") from error
    if readback.envelope != envelope or readback.source_bytes != canonical_bytes(envelope):
        raise ValueError("Foundation source-projection authority readback does not match envelope")
    opened = open_binance_usdm_koru_tradifi_source_projection_authority_v3(readback.source_bytes)
    fact = SourceProjectionPublicationFactV3(
        raw_snapshot_fact,
        raw_snapshot_publication_entry_ref,
        boundary_index_fact,
        boundary_index_publication_entry_ref,
        authority_ref,
        opened.request.request_hash,
        opened.fragment_digest,
        scope,
    )
    receipt = foundation.append(
        SOURCE_PROJECTIONS_LOG_V3, _event_id(fact), canonical_bytes(fact.to_canonical_dict())
    )
    entry_ref = getattr(receipt, "entry_ref", None)
    if type(entry_ref) is not LogEntryRef:
        raise ValueError("Foundation source-projection publication append did not return a LogEntryRef")
    _exact_entry(foundation, fact, entry_ref)
    return SourceProjectionPublicationV3(
        authority_ref, entry_ref, opened.request.request_hash, opened.fragment_digest
    )


def open_published_koru_tradifi_source_projection_authority_v3(
    foundation: SourceProjectionFoundationV3,
    *,
    fact: SourceProjectionPublicationFactV3,
    publication_entry_ref: LogEntryRef,
    scope: KoruTradifiSourceProjectionScopeV3,
) -> BinanceUsdmKoruTradifiSourceProjectionResultV3:
    """Open only a source authority bound to exact raw, boundary, owner-log, and scope facts."""
    if type(fact) is not SourceProjectionPublicationFactV3 or type(scope) is not KoruTradifiSourceProjectionScopeV3:
        raise TypeError("source-projection publication fact and scope must be exact")
    if fact.scope != scope:
        raise ValueError("source-projection fixed scope does not match publication fact")
    boundary = _open_inputs(
        foundation,
        raw_snapshot_fact=fact.raw_snapshot_fact,
        raw_snapshot_publication_entry_ref=fact.raw_snapshot_publication_entry_ref,
        boundary_index_fact=fact.boundary_index_fact,
        boundary_index_publication_entry_ref=fact.boundary_index_publication_entry_ref,
    )
    try:
        readback = foundation.read(ref=fact.authority_ref)
    except Exception as error:
        raise ValueError("source-projection authority artifact is unavailable") from error
    opened = open_binance_usdm_koru_tradifi_source_projection_authority_v3(readback.source_bytes)
    if (
        opened.request.timeline_window_start != scope.timeline_window_start
        or opened.request.timeline_window_end_exclusive != scope.timeline_window_end_exclusive
        or opened.request.aggregate_trade_boundary_index_result.result_digest
        != boundary.result_digest
        or opened.request.request_hash != fact.source_request_hash
        or opened.fragment_digest != fact.source_fragment_digest
    ):
        raise ValueError("source-projection authority does not bind publication facts")
    _exact_entry(foundation, fact, publication_entry_ref)
    return opened
