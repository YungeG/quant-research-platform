from __future__ import annotations

import sys
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path

import crypto_quant_research
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
    create_raw_blob_snapshot_manifest,
)
from crypto_quant_domain import (
    ArtifactEnvelope,
    ArtifactRef,
    UtcInstant,
    canonical_bytes,
)
from crypto_quant_foundation import LocalFoundation, LogEntryRef
from crypto_quant_research import (
    BOUNDARY_INDEXES_LOG,
    KORU_PREMIUM_DISCOVERY_SCOPE_V1,
    RAW_BLOB_SNAPSHOTS_LOG,
    KoruPremiumPreflightAuthorityErrorV1,
    KoruPremiumPreflightAuthorityFailureCodeV1,
    KoruPremiumPreflightStageKindV1,
    admit_koru_aggregate_trade_boundary_index_publication_fact_v1,
    admit_raw_blob_snapshot_publication_fact_v1,
    construct_koru_premium_preflight_authority_v1,
    create_koru_premium_preflight_stage_publication_fact_v1,
    open_koru_premium_preflight_authority_v1,
    publish_koru_aggregate_trade_boundary_index_authority_v3,
    publish_raw_blob_snapshot,
    verify_koru_premium_preflight_authority_v1,
)
from crypto_quant_research import koru_premium_preflight_authority as authority_module
from crypto_quant_research.koru_boundary_indexes import BoundaryIndexPublicationFact
from crypto_quant_research.raw_blob_snapshots import RawBlobSnapshotPublicationFact
from tests.bundle_builder.providers.binance_usdm.test_koru_aggtrade_boundary_index_v1 import (
    DAY_NS,
    day_start_ns,
    official_capture,
    row,
)


def _clock() -> str:
    return "2026-09-05T00:00:00.000000Z"


def _hash(digit: str) -> str:
    return "sha256:" + digit * 64


def _admit_generic(
    foundation: LocalFoundation,
    *,
    kind: KoruPremiumPreflightStageKindV1,
    semantic_digest: str,
    source_identity: object,
):
    artifact_type, schema_version = authority_module._STAGE_ARTIFACTS[kind]
    artifact_ref = foundation.put(
        envelope=ArtifactEnvelope.create(
            artifact_type, schema_version, {"fixture_stage": kind.value}
        )
    )
    fact = authority_module._generic_publication_fact(
        kind,
        artifact_ref,
        semantic_digest,
        KORU_PREMIUM_DISCOVERY_SCOPE_V1,
        source_identity,
    )
    owner_log = authority_module._STAGE_LOGS[kind]
    receipt = foundation.append(
        owner_log,
        authority_module._generic_event_id(owner_log, fact),
        canonical_bytes(fact),
    )
    return create_koru_premium_preflight_stage_publication_fact_v1(
        foundation,
        kind=kind,
        artifact_ref=artifact_ref,
        semantic_digest=semantic_digest,
        scope_identity=KORU_PREMIUM_DISCOVERY_SCOPE_V1,
        source_identity=source_identity,
        publication_entry_ref=receipt.entry_ref,
    )


def _raw_and_boundary(foundation: LocalFoundation):
    start = day_start_ns("2026-07-16")
    capture = official_capture(
        "2026-07-16",
        (
            row(700, 900, 900, start // 1_000_000 + 1_000),
            row(701, 901, 901, start // 1_000_000 + 2_000),
        ),
        include_header=True,
    )
    outcome = build_binance_usdm_koru_aggregate_trade_boundary_index_v3(
        BinanceUsdmKoruAggregateTradeBoundaryIndexRequestV3(
            (capture,),
            UtcInstant(start),
            UtcInstant(start + DAY_NS),
            (
                BinanceUsdmKoruExecutionBoundaryV1(
                    UtcInstant(start),
                    UtcInstant(start + DAY_NS),
                ),
            ),
        )
    )
    assert outcome.result is not None
    raw_publication = publish_raw_blob_snapshot(
        foundation,
        members=tuple(
            RawBlobSnapshotSourceMember(
                "raw/" + member.member_key,
                capture.snapshot.member_bytes(member.member_key),
                member.mode,
            )
            for member in capture.snapshot.members
        ),
        provenance={"fixture": "koru-premium-preflight-authority"},
    )
    raw = admit_raw_blob_snapshot_publication_fact_v1(
        foundation,
        manifest_ref=raw_publication.manifest_ref,
        publication_entry_ref=raw_publication.publication_entry_ref,
    )
    boundary_publication = publish_koru_aggregate_trade_boundary_index_authority_v3(
        foundation,
        result=outcome.result,
        manifest_ref=raw_publication.manifest_ref,
        raw_snapshot_publication_entry_ref=raw_publication.publication_entry_ref,
    )
    boundary = admit_koru_aggregate_trade_boundary_index_publication_fact_v1(
        foundation,
        raw_snapshot=raw,
        authority_ref=boundary_publication.authority_ref,
        publication_entry_ref=boundary_publication.publication_entry_ref,
    )
    return raw, boundary


def _stages(foundation: LocalFoundation):
    raw, boundary = _raw_and_boundary(foundation)
    source = _admit_generic(
        foundation,
        kind=KoruPremiumPreflightStageKindV1.SOURCE_PROJECTION,
        semantic_digest=_hash("7"),
        source_identity={
            "type": "koru_premium_source_projection_inputs_v1",
            "raw_snapshot": raw.stage_identity,
            "aggregate_boundary": boundary.stage_identity,
        },
    )
    economics = _admit_generic(
        foundation,
        kind=KoruPremiumPreflightStageKindV1.ECONOMICS,
        semantic_digest=_hash("9"),
        source_identity={
            "type": "koru_premium_economics_inputs_v1",
            "source_projection": source.stage_identity,
        },
    )
    overlay = _admit_generic(
        foundation,
        kind=KoruPremiumPreflightStageKindV1.TARGET_OVERLAY,
        semantic_digest=_hash("b"),
        source_identity={
            "type": "koru_premium_target_overlay_inputs_v1",
            "source_projection": source.stage_identity,
            "economics": economics.stage_identity,
        },
    )
    readers = _admit_generic(
        foundation,
        kind=KoruPremiumPreflightStageKindV1.READER_SET,
        semantic_digest=_hash("d"),
        source_identity={
            "type": "koru_premium_reader_set_inputs_v1",
            "source_projection": source.stage_identity,
            "economics": economics.stage_identity,
            "target_overlay": overlay.stage_identity,
        },
    )
    return raw, boundary, source, economics, overlay, readers


def _authority(foundation: LocalFoundation):
    return construct_koru_premium_preflight_authority_v1(
        foundation, stages=_stages(foundation)
    )


def test_stage_facts_are_canonical_digest_stable_and_owner_log_bound(tmp_path: Path) -> None:
    foundation = LocalFoundation(tmp_path, clock=_clock)
    raw, boundary, *_ = _stages(foundation)

    assert raw.owner_log == RAW_BLOB_SNAPSHOTS_LOG
    assert raw.publication_fact == RawBlobSnapshotPublicationFact(
        raw.artifact_ref, raw.semantic_digest
    ).to_canonical_dict()
    assert boundary.owner_log == BOUNDARY_INDEXES_LOG
    assert isinstance(boundary.source_identity, Mapping)
    assert boundary.publication_fact == BoundaryIndexPublicationFact(
        raw.artifact_ref,
        raw.publication_entry_ref,
        boundary.artifact_ref,
        boundary.source_identity["request_hash"],
        boundary.semantic_digest,
    ).to_canonical_dict()
    assert isinstance(raw.publication_fact, Mapping)
    assert raw.semantic_digest == raw.publication_fact["snapshot_id"]

    authority = construct_koru_premium_preflight_authority_v1(
        foundation, stages=_stages(foundation)
    )
    verify_koru_premium_preflight_authority_v1(foundation, authority)
    assert authority.authority_digest.startswith("sha256:")


def test_cas_only_raw_snapshot_rejects_placeholder_owner_log_entry(tmp_path: Path) -> None:
    foundation = LocalFoundation(tmp_path, clock=_clock)
    member = RawBlobSnapshotSourceMember("fixture/raw", b"cas-only", "0644")
    manifest = create_raw_blob_snapshot_manifest(
        members=(member,), provenance={"fixture": "cas-only"}
    )
    foundation.put_raw_blob(blob=member.raw_bytes)
    manifest_ref = foundation.put(envelope=manifest.envelope)

    with pytest.raises(KoruPremiumPreflightAuthorityErrorV1) as error:
        admit_raw_blob_snapshot_publication_fact_v1(
            foundation,
            manifest_ref=manifest_ref,
            publication_entry_ref=LogEntryRef(RAW_BLOB_SNAPSHOTS_LOG, 1, _hash("f")),
        )

    assert error.value.code is KoruPremiumPreflightAuthorityFailureCodeV1.UNPUBLISHED_STAGE


def test_current_raw_snapshot_and_boundary_facts_are_admitted_exactly(tmp_path: Path) -> None:
    foundation = LocalFoundation(tmp_path, clock=_clock)
    raw, boundary, *_ = _stages(foundation)

    assert isinstance(raw.publication_fact, Mapping)
    assert isinstance(boundary.publication_fact, Mapping)
    assert raw.publication_fact["type"] == "raw_blob_snapshot_publication"
    assert boundary.publication_fact["type"] == "research.boundary_indexes.v1"
    authority = _authority(foundation)
    assert authority.raw_snapshot == raw
    assert authority.aggregate_boundary == boundary


def test_bare_stage_cannot_be_admitted_or_serialized(tmp_path: Path) -> None:
    foundation = LocalFoundation(tmp_path, clock=_clock)
    stages = _stages(foundation)
    raw = stages[0]
    placeholder = authority_module.KoruPremiumPreflightStagePublicationFactV1(
        raw.kind,
        raw.artifact_ref,
        raw.publication_entry_ref,
        raw.publication_fact,
        raw.semantic_digest,
        raw.scope_identity,
        raw.source_identity,
    )

    with pytest.raises(TypeError):
        authority_module.KoruPremiumPreflightStagePublicationFactV1(
            raw.kind,
            raw.artifact_ref,
            raw.publication_entry_ref,
            raw.publication_fact,
            raw.semantic_digest,
            raw.scope_identity,
            raw.source_identity,
            _admitted=True,  # pyright: ignore[reportCallIssue] - assert rejected caller input
        )
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV1) as error:
        placeholder.to_canonical_dict()
    assert error.value.code is KoruPremiumPreflightAuthorityFailureCodeV1.UNPUBLISHED_STAGE
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV1) as error:
        authority_module.KoruPremiumPreflightAuthorityV1((placeholder, *stages[1:]))
    assert error.value.code is KoruPremiumPreflightAuthorityFailureCodeV1.UNPUBLISHED_STAGE
    assert not hasattr(crypto_quant_research, "KoruPremiumPreflightStagePublicationFactV1")
    assert "KoruPremiumPreflightStagePublicationFactV1" not in authority_module.__all__


def test_manifest_replay_reverifies_all_owner_log_facts(tmp_path: Path) -> None:
    foundation = LocalFoundation(tmp_path, clock=_clock)
    authority = _authority(foundation)

    replay = open_koru_premium_preflight_authority_v1(
        foundation, authority.to_canonical_dict()
    )

    assert replay == authority
    assert replay.authority_digest == authority.authority_digest
    (foundation._root / "registries" / "research.artifacts.v1.jsonl").unlink()
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV1) as error:
        open_koru_premium_preflight_authority_v1(
            foundation, authority.to_canonical_dict()
        )
    assert error.value.code is KoruPremiumPreflightAuthorityFailureCodeV1.UNPUBLISHED_STAGE


@pytest.mark.parametrize(
    ("change", "code"),
    [
        (
            lambda stages: (stages[0], stages[1], stages[3], stages[2], stages[4], stages[5]),
            KoruPremiumPreflightAuthorityFailureCodeV1.STAGE_ORDER,
        ),
        (
            lambda stages: stages[:-1],
            KoruPremiumPreflightAuthorityFailureCodeV1.MISSING_STAGE,
        ),
        (
            lambda stages: (stages[0], stages[0], stages[2], stages[3], stages[4], stages[5]),
            KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE,
        ),
    ],
)
def test_manifest_rejects_swapped_missing_and_duplicate_stages(
    tmp_path: Path, change, code: KoruPremiumPreflightAuthorityFailureCodeV1
) -> None:
    foundation = LocalFoundation(tmp_path, clock=_clock)
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV1) as error:
        construct_koru_premium_preflight_authority_v1(
            foundation, stages=change(_stages(foundation))
        )
    assert error.value.code is code


@pytest.mark.parametrize("field", ["artifact_ref", "semantic_digest"])
def test_stage_rejects_ref_and_digest_substitution(tmp_path: Path, field: str) -> None:
    foundation = LocalFoundation(tmp_path, clock=_clock)
    source = _stages(foundation)[2]
    replacement = (
        ArtifactRef(source.artifact_ref.artifact_type, 1, _hash("e"))
        if field == "artifact_ref"
        else _hash("e")
    )
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV1) as error:
        replace(source, **{field: replacement})
    assert error.value.code is KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE


def test_manifest_rejects_unpublished_owner_log_substitution(tmp_path: Path) -> None:
    foundation = LocalFoundation(tmp_path, clock=_clock)
    stages = _stages(foundation)
    changed = replace(
        stages[-1],
        publication_entry_ref=LogEntryRef(stages[-1].owner_log, 999, _hash("e")),
    )
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV1) as error:
        construct_koru_premium_preflight_authority_v1(
            foundation, stages=(*stages[:-1], changed)
        )
    assert error.value.code is KoruPremiumPreflightAuthorityFailureCodeV1.UNPUBLISHED_STAGE


def test_manifest_rejects_scope_and_source_substitution(tmp_path: Path) -> None:
    foundation = LocalFoundation(tmp_path, clock=_clock)
    stages = _stages(foundation)
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV1) as error:
        create_koru_premium_preflight_stage_publication_fact_v1(
            foundation,
            kind=KoruPremiumPreflightStageKindV1.SOURCE_PROJECTION,
            artifact_ref=stages[2].artifact_ref,
            semantic_digest=stages[2].semantic_digest,
            scope_identity={
                "timeline_window_start": {"epoch_nanoseconds": 0},
                "timeline_window_end_exclusive": {"epoch_nanoseconds": 1},
            },
            source_identity=stages[2].source_identity,
            publication_entry_ref=stages[2].publication_entry_ref,
        )
    assert error.value.code is KoruPremiumPreflightAuthorityFailureCodeV1.INVALID_STAGE

    substituted = _admit_generic(
        foundation,
        kind=KoruPremiumPreflightStageKindV1.SOURCE_PROJECTION,
        semantic_digest=stages[2].semantic_digest,
        source_identity={
            "type": "koru_premium_source_projection_inputs_v1",
            "raw_snapshot": stages[0].stage_identity,
            "aggregate_boundary": stages[0].stage_identity,
        },
    )
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV1) as error:
        construct_koru_premium_preflight_authority_v1(
            foundation, stages=(stages[0], stages[1], substituted, *stages[3:])
        )
    assert error.value.code is KoruPremiumPreflightAuthorityFailureCodeV1.STAGE_SUBSTITUTION
