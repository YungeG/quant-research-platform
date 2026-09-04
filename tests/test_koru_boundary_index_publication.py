from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BACKTEST = ROOT / "backtest"
if str(BACKTEST) not in sys.path:
    sys.path.insert(0, str(BACKTEST))

from crypto_quant_bundle_builder import (
    BinanceUsdmKoruAggregateTradeBoundaryIndexRequestV3,
    BinanceUsdmKoruExecutionBoundaryV1,
    RawBlobSnapshotSourceMember,
    build_binance_usdm_koru_aggregate_trade_boundary_index_v3,
    capture_binance_usdm_koru_aggregate_trades_from_retained_rest_v1,
    create_binance_usdm_koru_aggregate_trade_boundary_index_authority_v3,
    open_binance_usdm_koru_aggregate_trade_boundary_index_authority_v3,
)
from crypto_quant_bundle_builder import (
    binance_usdm_koru_aggtrade_boundary_index_v1 as boundary_index,
)
from crypto_quant_bundle_builder.binance_usdm_koru_aggtrades_source_bounded_v1 import (
    _RETAINED_AVAILABILITY_AUTHORITY_MEMBER_KEY,
)
from crypto_quant_domain import ArtifactEnvelope, UtcInstant
from crypto_quant_foundation import LocalFoundation
from tests.bundle_builder.providers.binance_usdm.test_koru_aggtrade_boundary_index_v1 import (
    DAY_NS,
    day_start_ns,
    official_capture,
    row,
)
from tests.bundle_builder.providers.binance_usdm.test_koru_aggtrades_retained_rest_v1 import (
    COVERAGE_END_MS,
    COVERAGE_START_MS,
)
from tests.bundle_builder.providers.binance_usdm.test_koru_aggtrades_retained_rest_v1 import (
    evidence_for as retained_evidence_for,
)
from tests.bundle_builder.providers.binance_usdm.test_koru_aggtrades_retained_rest_v1 import (
    request_for as retained_request_for,
)

from crypto_quant_research import (
    BOUNDARY_INDEXES_LOG,
    open_published_koru_aggregate_trade_boundary_index_authority_v3,
    publish_koru_aggregate_trade_boundary_index_authority_v3,
    publish_raw_blob_snapshot,
)


def _result_and_snapshot(tmp_path: Path):
    start = day_start_ns("2026-07-16")
    capture = official_capture(
        "2026-07-16",
        (
            row(700, 900, 900, start // 1_000_000 + 1_000),
            row(701, 901, 901, start // 1_000_000 + 2_000),
        ),
        include_header=True,
    )
    request = BinanceUsdmKoruAggregateTradeBoundaryIndexRequestV3(
        (capture,),
        UtcInstant(start),
        UtcInstant(start + DAY_NS),
        (
            BinanceUsdmKoruExecutionBoundaryV1(UtcInstant(start), UtcInstant(start + DAY_NS)),
            BinanceUsdmKoruExecutionBoundaryV1(UtcInstant(start + 1_500_000_000), UtcInstant(start + DAY_NS)),
            BinanceUsdmKoruExecutionBoundaryV1(UtcInstant(start + 3_000_000_000), UtcInstant(start + DAY_NS)),
        ),
    )
    outcome = build_binance_usdm_koru_aggregate_trade_boundary_index_v3(request)
    assert outcome.result is not None
    members = tuple(
        RawBlobSnapshotSourceMember(
            "raw/" + member.member_key,
            capture.snapshot.member_bytes(member.member_key),
            member.mode,
        )
        for member in capture.snapshot.members
    )
    foundation = LocalFoundation(tmp_path / "foundation")
    raw = publish_raw_blob_snapshot(
        foundation,
        members=members,
        provenance={"fixture": "koru-boundary-index-v3"},
    )
    return foundation, raw, outcome.result


def _published(tmp_path: Path):
    foundation, raw, result = _result_and_snapshot(tmp_path)
    publication = publish_koru_aggregate_trade_boundary_index_authority_v3(
        foundation,
        result=result,
        manifest_ref=raw.manifest_ref,
        raw_snapshot_publication_entry_ref=raw.publication_entry_ref,
    )
    return foundation, raw, result, publication


def test_published_boundary_index_reopens_without_aggregate_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    foundation, raw, result, publication = _published(tmp_path)
    monkeypatch.setattr(
        boundary_index,
        "_build_v3",
        lambda _request: pytest.fail("published open must not rebuild aggregate rows"),
    )
    monkeypatch.setattr(
        boundary_index.ZipFile,
        "open",
        lambda *_args, **_kwargs: pytest.fail("published open must not parse aggregate members"),
    )

    opened = open_published_koru_aggregate_trade_boundary_index_authority_v3(
        foundation,
        manifest_ref=raw.manifest_ref,
        raw_snapshot_publication_entry_ref=raw.publication_entry_ref,
        authority_ref=publication.authority_ref,
        publication_entry_ref=publication.publication_entry_ref,
    )

    assert opened.result_digest == result.result_digest
    assert publication.authority_ref.content_hash != result.result_digest
    assert len(foundation.entries(BOUNDARY_INDEXES_LOG)) == 1


@pytest.mark.parametrize("field", ["capture_bindings", "boundary_index_identity"])
def test_builder_open_rejects_capture_member_boundary_and_gap_substitution(
    tmp_path: Path, field: str,
) -> None:
    foundation, raw, result = _result_and_snapshot(tmp_path)
    view = __import__("crypto_quant_research").open_verified_raw_blob_snapshot(
        foundation, raw.manifest_ref, raw.publication_entry_ref
    )
    identity = {
        "type": "research.raw_blob_snapshot_authority_identity.v1",
        "manifest_ref": raw.manifest_ref.to_canonical_dict(),
        "publication_entry_ref": {
            "log_name": raw.publication_entry_ref.log_name,
            "log_sequence": raw.publication_entry_ref.log_sequence,
            "receipt_hash": raw.publication_entry_ref.receipt_hash,
        },
        "snapshot_id": view.manifest.snapshot_id,
    }
    envelope, _ref = create_binance_usdm_koru_aggregate_trade_boundary_index_authority_v3(
        result, view, identity
    )
    payload = json.loads(__import__("crypto_quant_domain").canonical_bytes(envelope.payload))
    if field == "capture_bindings":
        payload[field][0]["source_members"][0]["raw_snapshot_member_key"] = "raw/substituted"
    else:
        payload[field]["ordered_boundaries"][0]["cutoff"] = {"epoch_nanoseconds": 0}
    forged = ArtifactEnvelope.create(envelope.artifact_type, envelope.schema_version, payload)

    with pytest.raises(ValueError):
        open_binance_usdm_koru_aggregate_trade_boundary_index_authority_v3(
            __import__("crypto_quant_domain").canonical_bytes(forged),
            __import__("crypto_quant_domain").ArtifactRef.from_envelope(forged),
            view,
            identity,
        )


def test_two_capture_retained_pages_are_bound_without_aggregate_replay(tmp_path: Path) -> None:
    official_start = day_start_ns("2026-08-23")
    retained_start = official_start + DAY_NS
    official = official_capture(
        "2026-08-23",
        (row(600, 800, 800, official_start // 1_000_000 + DAY_NS // 1_000_000 - 1_000),),
    )
    evidence = retained_evidence_for()
    retained_outcome = capture_binance_usdm_koru_aggregate_trades_from_retained_rest_v1(
        retained_request_for(evidence),
        evidence.manifest,
        evidence.pages,
        evidence.derived,
        evidence.archive,
        evidence.checksum,
    )
    assert retained_outcome.result is not None
    retained = retained_outcome.result
    request = BinanceUsdmKoruAggregateTradeBoundaryIndexRequestV3(
        (official, retained),
        UtcInstant(official_start),
        UtcInstant(official_start + 2 * DAY_NS),
        (
            BinanceUsdmKoruExecutionBoundaryV1(
                UtcInstant(official_start + DAY_NS - 1_000_000_000), UtcInstant(retained_start)
            ),
            BinanceUsdmKoruExecutionBoundaryV1(
                UtcInstant(COVERAGE_START_MS * 1_000_000), UtcInstant(COVERAGE_END_MS * 1_000_000)
            ),
        ),
    )
    outcome = build_binance_usdm_koru_aggregate_trade_boundary_index_v3(request)
    assert outcome.result is not None
    foundation = LocalFoundation(tmp_path / "foundation")
    raw = publish_raw_blob_snapshot(
        foundation,
        members=tuple(
            RawBlobSnapshotSourceMember(
                f"raw/{ordinal}/{member.member_key}",
                capture.snapshot.member_bytes(member.member_key),
                member.mode,
            )
            for ordinal, capture in enumerate((official, retained), start=1)
            for member in capture.snapshot.members
            if not (
                capture is retained
                and member.member_key == _RETAINED_AVAILABILITY_AUTHORITY_MEMBER_KEY
            )
        ),
        provenance={"fixture": "koru-boundary-index-v3-two-capture"},
    )
    publication = publish_koru_aggregate_trade_boundary_index_authority_v3(
        foundation,
        result=outcome.result,
        manifest_ref=raw.manifest_ref,
        raw_snapshot_publication_entry_ref=raw.publication_entry_ref,
    )

    opened = open_published_koru_aggregate_trade_boundary_index_authority_v3(
        foundation,
        manifest_ref=raw.manifest_ref,
        raw_snapshot_publication_entry_ref=raw.publication_entry_ref,
        authority_ref=publication.authority_ref,
        publication_entry_ref=publication.publication_entry_ref,
    )

    assert opened.result_digest == outcome.result.result_digest
    assert opened.capture_final_evidence[1].utc_date == "2026-08-24"


def test_public_open_fails_closed_for_raw_artifact_and_owner_log_unpublication(tmp_path: Path) -> None:
    foundation, raw, _result, publication = _published(tmp_path)
    raw_path = foundation.raw_blob_path(ref=raw.manifest.members[0].raw_blob_ref)
    raw_path.write_bytes(b"substituted")
    with pytest.raises(ValueError):
        open_published_koru_aggregate_trade_boundary_index_authority_v3(
            foundation,
            manifest_ref=raw.manifest_ref,
            raw_snapshot_publication_entry_ref=raw.publication_entry_ref,
            authority_ref=publication.authority_ref,
            publication_entry_ref=publication.publication_entry_ref,
        )

    foundation, raw, _result, publication = _published(tmp_path / "owner-log")
    (foundation._root / "registries" / f"{BOUNDARY_INDEXES_LOG}.jsonl").unlink()
    with pytest.raises(ValueError, match="publication entry"):
        open_published_koru_aggregate_trade_boundary_index_authority_v3(
            foundation,
            manifest_ref=raw.manifest_ref,
            raw_snapshot_publication_entry_ref=raw.publication_entry_ref,
            authority_ref=publication.authority_ref,
            publication_entry_ref=publication.publication_entry_ref,
        )
