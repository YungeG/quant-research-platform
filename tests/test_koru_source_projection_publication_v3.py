from __future__ import annotations

from dataclasses import replace

import pytest
from crypto_quant_bundle_builder import (
    BinanceUsdmKoruAggregateTradeBoundaryIndexRequestV3,
    BinanceUsdmKoruExecutionBoundaryV1,
    RawBlobSnapshotSourceMember,
    build_binance_usdm_koru_aggregate_trade_boundary_index_v3,
)
from crypto_quant_bundle_builder import (
    binance_usdm_koru_aggtrade_boundary_index_v1 as boundary_index,
)
from crypto_quant_domain import Scale, UtcInstant
from crypto_quant_foundation import LocalFoundation
from crypto_quant_research import (
    SOURCE_PROJECTIONS_LOG_V3,
    BoundaryIndexPublicationFact,
    KoruTradifiSourceProjectionScopeV3,
    RawBlobSnapshotPublicationFact,
    SourceProjectionPublicationFactV3,
    koru_source_projections,
    open_published_koru_tradifi_source_projection_authority_v3,
    publish_koru_aggregate_trade_boundary_index_authority_v3,
    publish_koru_tradifi_source_projection_authority_v3,
    publish_raw_blob_snapshot,
)
from tests.bundle_builder.providers.binance_usdm import (
    test_koru_tradifi_source_projection_v1 as source_fixture,
)


def _inputs(tmp_path):
    v1 = source_fixture._request(
        ((source_fixture.aggregate_fixture.DAY_START_MS + 22 * 3_600_000 + 30_000, "12.340"),)
    )
    v1_result = source_fixture.build_binance_usdm_koru_tradifi_source_projection_v1(v1).result
    assert v1_result is not None
    boundaries = tuple(
        BinanceUsdmKoruExecutionBoundaryV1(value.hourly_boundary, value.next_cash_market_open_or_window_end)
        for value in sorted(
            (*v1_result.projection_lineage, *v1_result.missing_boundaries),
            key=lambda value: value.hourly_boundary.epoch_nanoseconds,
        )
    )
    boundary = build_binance_usdm_koru_aggregate_trade_boundary_index_v3(
        BinanceUsdmKoruAggregateTradeBoundaryIndexRequestV3(
            tuple(value.capture for value in v1.aggregate_trade_results),
            v1.timeline_window_start,
            v1.timeline_window_end_exclusive,
            boundaries,
        )
    ).result
    assert boundary is not None
    foundation = LocalFoundation(tmp_path / "foundation")
    raw = publish_raw_blob_snapshot(
        foundation,
        members=tuple(
            RawBlobSnapshotSourceMember(
                "raw/" + member.member_key,
                capture.snapshot.member_bytes(member.member_key),
                member.mode,
            )
            for capture in boundary.request.captures
            for member in capture.snapshot.members
        ),
        provenance={"fixture": "source-projection-v3"},
    )
    boundary_publication = publish_koru_aggregate_trade_boundary_index_authority_v3(
        foundation,
        result=boundary,
        manifest_ref=raw.manifest_ref,
        raw_snapshot_publication_entry_ref=raw.publication_entry_ref,
    )
    raw_fact = RawBlobSnapshotPublicationFact(raw.manifest_ref, raw.manifest.snapshot_id)
    boundary_fact = BoundaryIndexPublicationFact(
        raw.manifest_ref,
        raw.publication_entry_ref,
        boundary_publication.authority_ref,
        boundary.request.request_hash,
        boundary.result_digest,
    )
    return foundation, v1, raw_fact, raw.publication_entry_ref, boundary_fact, boundary_publication.publication_entry_ref


def _publish(foundation, v1, raw_fact, raw_entry, boundary_fact, boundary_entry, scope):
    return publish_koru_tradifi_source_projection_authority_v3(
        foundation,
        raw_snapshot_fact=raw_fact,
        raw_snapshot_publication_entry_ref=raw_entry,
        boundary_index_fact=boundary_fact,
        boundary_index_publication_entry_ref=boundary_entry,
        scope=scope,
        instrument_catalog_hash=v1.instrument_catalog_hash,
        projection_scale=Scale(8),
        mark_price_results=v1.mark_price_results,
        index_price_results=v1.index_price_results,
        funding_result=v1.funding_result,
        authority_result=v1.authority_result,
    )


def test_published_v3_uses_boundary_opener_without_aggregate_replay(tmp_path, monkeypatch) -> None:
    foundation, v1, raw_fact, raw_entry, boundary_fact, boundary_entry = _inputs(tmp_path)
    scope = KoruTradifiSourceProjectionScopeV3(v1.timeline_window_start, v1.timeline_window_end_exclusive)
    monkeypatch.setattr(boundary_index, "_build_v3", lambda _request: pytest.fail("no aggregate boundary replay"))
    monkeypatch.setattr(boundary_index, "_parse_row", lambda *_args: pytest.fail("no aggregate parser"))
    boundary_opener = koru_source_projections.open_published_koru_aggregate_trade_boundary_index_authority_v3
    opener_calls = 0

    def counted_boundary_opener(*args, **kwargs):
        nonlocal opener_calls
        opener_calls += 1
        return boundary_opener(*args, **kwargs)

    monkeypatch.setattr(
        koru_source_projections,
        "open_published_koru_aggregate_trade_boundary_index_authority_v3",
        counted_boundary_opener,
    )
    publication = _publish(
        foundation, v1, raw_fact, raw_entry, boundary_fact, boundary_entry, scope
    )
    fact = SourceProjectionPublicationFactV3(
        raw_fact,
        raw_entry,
        boundary_fact,
        boundary_entry,
        publication.authority_ref,
        publication.source_request_hash,
        publication.source_fragment_digest,
        scope,
    )

    opened = open_published_koru_tradifi_source_projection_authority_v3(
        foundation, fact=fact, publication_entry_ref=publication.publication_entry_ref, scope=scope
    )

    assert opened.fragment_digest == publication.source_fragment_digest
    assert opener_calls == 2
    assert len(foundation.entries(SOURCE_PROJECTIONS_LOG_V3)) == 1
    for forge in (
        lambda: replace(fact, authority_ref=boundary_fact.authority_ref),
        lambda: replace(fact, boundary_index_publication_entry_ref=raw_entry),
        lambda: replace(
            fact,
            scope=KoruTradifiSourceProjectionScopeV3(
                v1.timeline_window_start,
                UtcInstant(v1.timeline_window_end_exclusive.epoch_nanoseconds - 1),
            ),
        ),
    ):
        with pytest.raises((TypeError, ValueError)):
            forged = forge()
            open_published_koru_tradifi_source_projection_authority_v3(
                foundation, fact=forged, publication_entry_ref=publication.publication_entry_ref, scope=scope
            )
    (foundation._root / "registries" / f"{SOURCE_PROJECTIONS_LOG_V3}.jsonl").unlink()
    with pytest.raises(ValueError, match="publication entry"):
        open_published_koru_tradifi_source_projection_authority_v3(
            foundation,
            fact=fact,
            publication_entry_ref=publication.publication_entry_ref,
            scope=scope,
        )


@pytest.mark.parametrize("owner_log", ("research.raw_snapshots.v1", "research.boundary_indexes.v1"))
def test_v3_publish_fails_closed_for_raw_or_boundary_owner_log(tmp_path, owner_log) -> None:
    foundation, v1, raw_fact, raw_entry, boundary_fact, boundary_entry = _inputs(tmp_path)
    scope = KoruTradifiSourceProjectionScopeV3(v1.timeline_window_start, v1.timeline_window_end_exclusive)
    (foundation._root / "registries" / f"{owner_log}.jsonl").unlink()

    with pytest.raises(ValueError, match="publication entry"):
        _publish(foundation, v1, raw_fact, raw_entry, boundary_fact, boundary_entry, scope)
