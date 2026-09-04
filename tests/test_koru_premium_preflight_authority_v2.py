from __future__ import annotations

import importlib.util
import inspect
import json
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BACKTEST = ROOT / "backtest"
for source in (
    BACKTEST / "packages/market-data-contracts/src",
    BACKTEST / "packages/market-bundle-builder/src",
    BACKTEST,
):
    if str(source) not in sys.path:
        sys.path.insert(0, str(source))

from crypto_quant_bundle_builder import (
    KoruMarkIndexPremiumParametersV1,
    KoruPremiumReaderSetBuildRequestV2,
    KoruPremiumRecipeAuthorityV1,
    KoruTradifiEconomicsBundleRequestV4,
    KoruTradifiEconomicsTermsV4,
    KoruTradifiSourceProjectionContentIdentityV3,
    LocalMarketBundleRepository,
    LocalMarketBundleRepositoryConfig,
    build_koru_premium_reader_set_v2,
    canonical_koru_premium_payload_v1,
    publish_koru_tradifi_economics_bundle_v4,
)
from crypto_quant_domain import (
    ArtifactEnvelope,
    ArtifactRef,
    canonical_bytes,
    canonical_sha256,
)
from crypto_quant_foundation import LocalFoundation
from crypto_quant_market_data import (
    KoruPremiumReaderSetV2,
    LocalMarketBundleReader,
    MarketBundleManifest,
    MarketStreamManifest,
)
from crypto_quant_research import (
    KORU_PREMIUM_ECONOMICS_V4_LOG,
    KORU_PREMIUM_OVERLAY_SET_V4_LOG,
    KORU_PREMIUM_PREFLIGHT_AUTHORITY_V2_LOG,
    KORU_PREMIUM_READER_SET_V2_LOG,
    KoruPremiumPreflightAuthorityErrorV2,
    open_published_koru_premium_preflight_authority_v2,
    open_published_koru_tradifi_source_projection_authority_v3,
    publish_koru_premium_preflight_authority_v2,
)
from crypto_quant_research import (
    koru_premium_preflight_authority_v2 as authority_module,
)

CONSUMER_SCRIPT = ROOT / "research/koruusdt/run_public_koru_retained_preflight.py"
CONSUMER_SPEC = importlib.util.spec_from_file_location("koru_v2_authority_consumer", CONSUMER_SCRIPT)
assert CONSUMER_SPEC is not None and CONSUMER_SPEC.loader is not None
consumer_runner = importlib.util.module_from_spec(CONSUMER_SPEC)
sys.modules[CONSUMER_SPEC.name] = consumer_runner
CONSUMER_SPEC.loader.exec_module(consumer_runner)

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_koru_source_projection_publication_v3 as source_publication


def _recipe(source, number: int, entry: str) -> KoruPremiumRecipeAuthorityV1:
    premium_id = f"KORU-PRM-{number:02d}"
    recipe = __import__("crypto_quant_bundle_builder", fromlist=["x"]).KoruDirectionalTargetRecipeV1(
        family="mark_index_premium",
        recipe_id=premium_id,
        strategy_id=f"strategy-{premium_id}",
        sleeve_id=f"sleeve-{premium_id}",
        strategy_ref=ArtifactRef("strategy_definition", 1, "sha256:" + "1" * 64),
        parameter_ref=ArtifactRef("strategy_parameter_set", 1, "sha256:" + "1" * 64),
        target_stream_key=premium_id,
        instrument_id=source.source_events[0].instrument_id,
        target_exposure="0.25",
        bar_interval="1h",
        parameters=KoruMarkIndexPremiumParametersV1(entry, "5", 12),
    )
    strategy = ArtifactEnvelope.create("strategy_definition", 1, canonical_koru_premium_payload_v1(recipe, artifact_type="strategy_definition"))
    parameters = ArtifactEnvelope.create("strategy_parameter_set", 1, canonical_koru_premium_payload_v1(recipe, artifact_type="strategy_parameter_set"))
    return KoruPremiumRecipeAuthorityV1(
        replace(recipe, strategy_ref=ArtifactRef.from_envelope(strategy), parameter_ref=ArtifactRef.from_envelope(parameters)),
        strategy,
        parameters,
    )


def _artifact_path(foundation: LocalFoundation, ref: ArtifactRef) -> Path:
    digest = ref.content_hash[7:]
    return foundation._root / "artifacts" / "sha256" / digest[:2] / digest


def _published(tmp_path: Path):
    foundation, v1, raw, raw_entry, boundary, boundary_entry = source_publication._inputs(tmp_path)
    scope = source_publication.KoruTradifiSourceProjectionScopeV3(v1.timeline_window_start, v1.timeline_window_end_exclusive)
    source_pub = source_publication._publish(foundation, v1, raw, raw_entry, boundary, boundary_entry, scope)
    source_fact = source_publication.SourceProjectionPublicationFactV3(
        raw, raw_entry, boundary, boundary_entry, source_pub.authority_ref,
        source_pub.source_request_hash, source_pub.source_fragment_digest, scope,
    )
    source = open_published_koru_tradifi_source_projection_authority_v3(
        foundation, fact=source_fact, publication_entry_ref=source_pub.publication_entry_ref, scope=scope
    )
    identity = KoruTradifiSourceProjectionContentIdentityV3(
        source_pub.authority_ref, source_pub.authority_ref.content_hash,
        source.fragment_digest, source.request.request_hash,
    )
    economics_outcome = publish_koru_tradifi_economics_bundle_v4(
        KoruTradifiEconomicsBundleRequestV4(
            source, identity,
            KoruTradifiEconomicsTermsV4.from_source_projection(source, execution_account_id="account-1"),
            foundation, tmp_path / "economics",
        )
    )
    assert economics_outcome.result is not None
    reader_outcome = build_koru_premium_reader_set_v2(
        KoruPremiumReaderSetBuildRequestV2(
            economics_outcome.result,
            tuple(_recipe(source, number, entry) for number, entry in enumerate(("20", "30", "40", "60"), 1)),
            tmp_path / "overlays",
        )
    )
    assert reader_outcome.result is not None
    publication = publish_koru_premium_preflight_authority_v2(
        foundation,
        raw_snapshot_fact=raw, raw_snapshot_publication_entry_ref=raw_entry,
        boundary_index_fact=boundary, boundary_index_publication_entry_ref=boundary_entry,
        source_projection_fact=source_fact, source_projection_publication_entry_ref=source_pub.publication_entry_ref,
        economics=economics_outcome.result, reader_set=reader_outcome.result,
    )
    return foundation, publication, tmp_path


def _consumer_locator(publication) -> dict[str, object]:
    return {
        "type": consumer_runner.PREMIUM_PREFLIGHT_AUTHORITY_V2_LOCATOR_SCHEMA,
        "schema_version": 1,
        "authority_ref": publication.authority_ref.to_canonical_dict(),
        "publication_entry_ref": {
            "log_name": publication.publication_entry_ref.log_name,
            "log_sequence": publication.publication_entry_ref.log_sequence,
            "receipt_hash": publication.publication_entry_ref.receipt_hash,
        },
    }


def _consume_published_v2(tmp_path: Path, foundation: LocalFoundation, publication, root: Path):
    receipt_root = tmp_path / "consumer-receipts"
    receipt_root.mkdir()
    locator = _consumer_locator(publication)
    return consumer_runner.consume_published_koru_premium_preflight_authority_v2(
        locator=locator,
        foundation_root=foundation._root,
        repository_root=root / "overlays",
        receipt_root=receipt_root,
    ), locator, receipt_root


def test_offline_v2_authority_consumer_writes_exact_readback_verified_receipt(tmp_path: Path) -> None:
    foundation, publication, root = _published(tmp_path)

    receipt, locator, receipt_root = _consume_published_v2(tmp_path, foundation, publication, root)

    final = receipt["final_authority"]
    assert receipt["outcome"] == "success"
    assert receipt["launch_gate"] == "GO_FOR_SEPARATE_EXPERIMENT_LAUNCH_REVIEW"
    assert receipt["network_performed"] is False
    assert receipt["holdout_touched"] is False
    assert receipt["upstream_producer_work_performed"] is False
    assert receipt["verification_replay_performed"] is True
    assert len(final) == 1
    assert final[0]["authority_ref"] == locator["authority_ref"]
    assert final[0]["publication_entry_ref"] == locator["publication_entry_ref"]
    assert canonical_bytes(final[0]["full_spine"]) == canonical_bytes(publication.authority.to_canonical_dict())
    assert final[0]["full_spine_sha256"] == canonical_sha256(final[0]["full_spine"])
    assert final[0]["premium_reader_ids"] == ["KORU-PRM-01", "KORU-PRM-02", "KORU-PRM-03", "KORU-PRM-04"]
    assert final[0]["reader_set_digest"] == publication.authority.reader_set.reader_set.reader_set_digest
    assert str(foundation._root) not in json.dumps(receipt)
    assert str(root / "overlays") not in json.dumps(receipt)
    assert str(receipt_root) not in json.dumps(receipt)
    expected = receipt_root / "receipts" / f"{canonical_sha256(locator).removeprefix('sha256:')}.json"
    assert json.loads(expected.read_text()) == receipt


@pytest.mark.parametrize("tamper", ("ref", "owner-log", "repository"))
def test_v2_authority_consumer_fails_before_legacy_producer_or_child_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tamper: str,
) -> None:
    foundation, publication, root = _published(tmp_path)
    locator = _consumer_locator(publication)
    repository_root = root / "overlays"
    if tamper == "ref":
        locator["authority_ref"] = {**locator["authority_ref"], "content_hash": "sha256:" + "0" * 64}
    elif tamper == "owner-log":
        locator["publication_entry_ref"] = {**locator["publication_entry_ref"], "receipt_hash": "sha256:" + "0" * 64}
    else:
        repository_root = tmp_path / "replacement-repository"
        repository_root.mkdir()
    receipt_root = tmp_path / f"consumer-receipts-{tamper}"
    receipt_root.mkdir()
    monkeypatch.setattr(consumer_runner, "full_preflight", lambda *_args, **_kwargs: pytest.fail("legacy full mode must not run"))
    monkeypatch.setattr(consumer_runner, "build_source", lambda *_args, **_kwargs: pytest.fail("source build must not run"))
    monkeypatch.setattr(consumer_runner, "publish_koru_source_projection_authority", lambda *_args, **_kwargs: pytest.fail("source publication must not run"))
    monkeypatch.setattr(consumer_runner.subprocess, "Popen", lambda *_args, **_kwargs: pytest.fail("child must not start"))

    receipt = consumer_runner.consume_published_koru_premium_preflight_authority_v2(
        locator=locator,
        foundation_root=foundation._root,
        repository_root=repository_root,
        receipt_root=receipt_root,
    )

    assert receipt["outcome"] == "non_success"
    assert receipt["failure_stage"] == "authority_spine_verification"
    assert receipt["launch_gate"] == "NO_GO"
    assert receipt["final_authority"] == []


def test_v2_authority_consumer_validates_locator_before_operational_roots(tmp_path: Path) -> None:
    receipt_root = tmp_path / "consumer-receipts"
    receipt_root.mkdir()

    with pytest.raises(ValueError, match="locator"):
        consumer_runner.consume_published_koru_premium_preflight_authority_v2(
            locator={}, foundation_root=Path("relative"), repository_root=Path("relative"), receipt_root=receipt_root,
        )
    locator = {
        "type": consumer_runner.PREMIUM_PREFLIGHT_AUTHORITY_V2_LOCATOR_SCHEMA,
        "schema_version": 1,
        "authority_ref": {
            "type": "artifact_ref", "artifact_type": "koru_premium_preflight_authority_v2",
            "schema_version": 2, "content_hash": "sha256:" + "0" * 64,
        },
        "publication_entry_ref": {
            "log_name": KORU_PREMIUM_PREFLIGHT_AUTHORITY_V2_LOG,
            "log_sequence": 1, "receipt_hash": "sha256:" + "0" * 64,
        },
    }
    with pytest.raises(ValueError, match="Foundation root"):
        consumer_runner.consume_published_koru_premium_preflight_authority_v2(
            locator=locator, foundation_root=Path("relative"), repository_root=Path("relative"), receipt_root=receipt_root,
        )
    assert not (receipt_root / "receipts").exists()


def test_v2_authority_consumer_returns_existing_exact_locator_receipt_without_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    foundation, publication, root = _published(tmp_path)
    first, locator, receipt_root = _consume_published_v2(tmp_path, foundation, publication, root)
    monkeypatch.setattr(
        consumer_runner, "open_published_koru_premium_preflight_authority_v2",
        lambda **_kwargs: pytest.fail("existing receipt must be returned before reopen"),
    )

    second = consumer_runner.consume_published_koru_premium_preflight_authority_v2(
        locator=locator,
        foundation_root=foundation._root,
        repository_root=root / "overlays",
        receipt_root=receipt_root,
    )

    assert second == first


def test_v2_authority_consumer_cli_branch_and_helper_exclude_legacy_execution() -> None:
    source = CONSUMER_SCRIPT.read_text(encoding="utf-8")
    helper = inspect.getsource(consumer_runner.consume_published_koru_premium_preflight_authority_v2)
    branch = source[
        source.index("elif args.consume_published_preflight_authority_v2:"):
        source.index("elif args.full:")
    ]

    assert "--consume-published-preflight-authority-v2" in source
    assert "consume_published_koru_premium_preflight_authority_v2" in branch
    for forbidden in ("full_preflight", "build_source", "publish_", "Popen", "Runtime", "Backtest", "Experiment", "Holdout"):
        assert forbidden not in helper
        assert forbidden not in branch


def test_v2_authority_consumer_cli_uses_the_new_mode(tmp_path: Path) -> None:
    foundation, publication, root = _published(tmp_path)
    receipt_root = tmp_path / "consumer-receipts"
    receipt_root.mkdir()

    assert consumer_runner.main([
        "--consume-published-preflight-authority-v2",
        "--preflight-authority-v2-locator", json.dumps(_consumer_locator(publication)),
        "--preflight-authority-v2-foundation-root", str(foundation._root),
        "--preflight-authority-v2-repository-root", str(root / "overlays"),
        "--preflight-authority-v2-receipt-root", str(receipt_root),
    ]) == 0


def _opened_source(foundation: LocalFoundation, authority):
    return open_published_koru_tradifi_source_projection_authority_v3(
        foundation,
        fact=authority.source_projection_fact,
        publication_entry_ref=authority.source_projection_publication_entry_ref,
        scope=authority.source_projection_fact.scope,
    )


def test_publishes_one_overlay_set_and_reopens_four_repository_readers(tmp_path: Path) -> None:
    foundation, publication, root = _published(tmp_path)

    replay = open_published_koru_premium_preflight_authority_v2(
        foundation, authority_ref=publication.authority_ref,
        publication_entry_ref=publication.publication_entry_ref, repository_root=root / "overlays",
    )

    assert canonical_bytes(replay.to_canonical_dict()) == canonical_bytes(
        publication.authority.to_canonical_dict()
    )
    assert tuple(row.premium_id for row in replay.reader_set.reader_set.bindings) == (
        "KORU-PRM-01", "KORU-PRM-02", "KORU-PRM-03", "KORU-PRM-04"
    )
    assert len(foundation.entries(KORU_PREMIUM_OVERLAY_SET_V4_LOG)) == 1
    assert tuple(row.overlay_bundle_ref for row in replay.reader_set.reader_set.bindings) == tuple(
        row.overlay_bundle_ref for row in publication.authority.reader_set.reader_set.bindings
    )


@pytest.mark.parametrize(
    "log_name",
    (KORU_PREMIUM_ECONOMICS_V4_LOG, KORU_PREMIUM_OVERLAY_SET_V4_LOG, KORU_PREMIUM_READER_SET_V2_LOG, KORU_PREMIUM_PREFLIGHT_AUTHORITY_V2_LOG),
)
def test_missing_v2_owner_log_fails_closed(tmp_path: Path, log_name: str) -> None:
    foundation, publication, root = _published(tmp_path)
    (foundation._root / "registries" / f"{log_name}.jsonl").unlink()

    with pytest.raises(KoruPremiumPreflightAuthorityErrorV2):
        open_published_koru_premium_preflight_authority_v2(
            foundation, authority_ref=publication.authority_ref,
            publication_entry_ref=publication.publication_entry_ref, repository_root=root / "overlays",
        )


@pytest.mark.parametrize("stage", ("raw", "boundary", "source", "economics", "overlay", "reader"))
def test_missing_stage_artifact_fails_closed(tmp_path: Path, stage: str) -> None:
    foundation, publication, root = _published(tmp_path)
    authority = publication.authority
    if stage == "raw":
        _artifact_path(foundation, authority.raw_snapshot_fact.manifest_ref).unlink()
    elif stage == "boundary":
        _artifact_path(foundation, authority.boundary_index_fact.authority_ref).unlink()
    elif stage == "source":
        _artifact_path(foundation, authority.source_projection_fact.authority_ref).unlink()
    else:
        fact = {"economics": authority.economics, "overlay": authority.overlay_set, "reader": authority.reader_set}[stage]
        _artifact_path(foundation, fact.artifact_ref).unlink()
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV2):
        open_published_koru_premium_preflight_authority_v2(
            foundation, authority_ref=publication.authority_ref,
            publication_entry_ref=publication.publication_entry_ref, repository_root=root / "overlays",
        )


def test_reader_and_repository_replacement_fail_closed_and_module_stays_v2_only(tmp_path: Path) -> None:
    foundation, publication, root = _published(tmp_path)
    replay = open_published_koru_premium_preflight_authority_v2(
        foundation, authority_ref=publication.authority_ref,
        publication_entry_ref=publication.publication_entry_ref, repository_root=root / "overlays",
    )
    with pytest.raises(ValueError, match="premium_reader_binding"):
        replace(replay.reader_set.reader_set.bindings[0], reader=replay.reader_set.reader_set.bindings[1].reader)
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV2):
        open_published_koru_premium_preflight_authority_v2(
            foundation, authority_ref=publication.authority_ref,
            publication_entry_ref=publication.publication_entry_ref, repository_root=tmp_path / "replacement",
        )
    source = inspect.getsource(authority_module)
    assert ".runtime" not in source
    assert "koru_premium_preflight_authority import" not in source


def test_public_v2_root_does_not_eagerly_import_v1_authority() -> None:
    paths = (
        ROOT / "research-platform" / "src",
        BACKTEST / "packages/trading-domain/src",
        BACKTEST / "packages/market-data-contracts/src",
        BACKTEST / "packages/market-bundle-builder/src",
        ROOT / "foundation" / "src",
    )
    environment = os.environ | {
        "PYTHONPATH": os.pathsep.join(
            (*map(str, paths), os.environ.get("PYTHONPATH", ""))
        )
    }
    code = (
        "import sys\n"
        "import crypto_quant_research as research\n"
        "assert 'crypto_quant_research.koru_premium_preflight_authority' not in sys.modules\n"
        "assert research.KoruPremiumPreflightAuthorityV2.__name__ == 'KoruPremiumPreflightAuthorityV2'\n"
        "assert 'crypto_quant_research.koru_premium_preflight_authority' not in sys.modules\n"
        "from crypto_quant_research import KoruPremiumPreflightAuthorityV1\n"
        "assert KoruPremiumPreflightAuthorityV1.__name__ == 'KoruPremiumPreflightAuthorityV1'\n"
        "assert 'crypto_quant_research.koru_premium_preflight_authority' in sys.modules\n"
    )
    completed = subprocess.run(
        (sys.executable, "-c", code),
        cwd=ROOT / "research-platform",
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_v2_recompiles_all_fixed_targets_from_opened_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = authority_module.compile_binance_usdm_koru_directional_targets_v2
    requests = []

    def recording_compiler(request):
        requests.append(request)
        return original(request)

    monkeypatch.setattr(
        authority_module,
        "compile_binance_usdm_koru_directional_targets_v2",
        recording_compiler,
    )
    foundation, publication, root = _published(tmp_path)
    open_published_koru_premium_preflight_authority_v2(
        foundation,
        authority_ref=publication.authority_ref,
        publication_entry_ref=publication.publication_entry_ref,
        repository_root=root / "overlays",
    )
    assert len(requests) == 2
    for request in requests:
        assert tuple(recipe.recipe_id for recipe in request.recipes) == (
            "KORU-PRM-01",
            "KORU-PRM-02",
            "KORU-PRM-03",
            "KORU-PRM-04",
        )
        assert request.source_projection_authority_ref == publication.authority.source_projection_fact.authority_ref
        assert request.source_projection_authority_content_hash == publication.authority.source_projection_fact.authority_ref.content_hash


def test_unadmitted_v2_wrappers_cannot_create_or_serialize_authority(
    tmp_path: Path,
) -> None:
    foundation, publication, _ = _published(tmp_path)
    authority = publication.authority
    before = {
        log: len(foundation.entries(log))
        for log in (
            KORU_PREMIUM_ECONOMICS_V4_LOG,
            KORU_PREMIUM_OVERLAY_SET_V4_LOG,
            KORU_PREMIUM_READER_SET_V2_LOG,
            KORU_PREMIUM_PREFLIGHT_AUTHORITY_V2_LOG,
        )
    }
    forged_economics = replace(authority.economics)
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV2, match="unpublished"):
        forged_economics.to_canonical_dict()
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV2, match="unpublished"):
        authority_module.publish_koru_premium_overlay_set_authority_v4(
            foundation,
            economics=forged_economics,
            reader_set=authority.reader_set.reader_set,
            source_projection=None,
        )
    forged_overlay = replace(authority.overlay_set)
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV2, match="unpublished"):
        authority_module.publish_koru_premium_reader_set_authority_v2(
            foundation,
            overlay_set=forged_overlay,
            reader_set=authority.reader_set.reader_set,
        )
    forged_reader = replace(authority.reader_set)
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV2, match="unpublished"):
        authority_module.KoruPremiumPreflightAuthorityV2(
            authority.raw_snapshot_fact,
            authority.raw_snapshot_publication_entry_ref,
            authority.boundary_index_fact,
            authority.boundary_index_publication_entry_ref,
            authority.source_projection_fact,
            authority.source_projection_publication_entry_ref,
            authority.economics,
            authority.overlay_set,
            forged_reader,
        )
    direct = authority_module.KoruPremiumPreflightAuthorityV2(
        authority.raw_snapshot_fact,
        authority.raw_snapshot_publication_entry_ref,
        authority.boundary_index_fact,
        authority.boundary_index_publication_entry_ref,
        authority.source_projection_fact,
        authority.source_projection_publication_entry_ref,
        authority.economics,
        authority.overlay_set,
        authority.reader_set,
    )
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV2, match="unpublished"):
        direct.to_canonical_dict()
    assert {
        log: len(foundation.entries(log)) for log in before
    } == before


def _publish_semantically_tampered_overlay(
    tmp_path: Path, binding
):
    reader = binding.reader
    streams = {}
    for manifest in reader.manifest.streams:
        cursor = reader.open_cursor(manifest.stream_key, batch_size=64)
        events = []
        while not cursor.exhausted:
            batch, cursor = reader.read_batch(cursor)
            events.extend(batch)
        streams[manifest.stream_key] = tuple(events)
    event = streams["binance_usdm.tradifi.target_overlay_authority.koruusdt.v4"][0]
    payload = dict(event.payload)
    payload["scope_digest"] = "sha256:" + "0" * 64
    source_hash = canonical_sha256(
        {"type": event.event_type, "payload": payload}
    )
    streams[event.stream_key] = (
        replace(
            event,
            event_id=f"{event.event_type}:{source_hash}",
            revision_id=canonical_sha256(
                {"type": f"{event.event_type}_revision", "source_hash": source_hash}
            ),
            source_hash=source_hash,
            payload=payload,
        ),
    )
    manifests = tuple(
        MarketStreamManifest.from_events(stream_key, events)
        for stream_key, events in sorted(streams.items())
    )
    manifest = MarketBundleManifest.build(
        bundle_key=reader.manifest.bundle_key,
        schema_version=reader.manifest.schema_version,
        coverage_start=reader.manifest.coverage_start,
        coverage_end_exclusive=reader.manifest.coverage_end_exclusive,
        instrument_catalog_hash=reader.manifest.instrument_catalog_hash,
        capabilities=tuple(sorted({value.capability for value in manifests})),
        streams=manifests,
    )
    root = tmp_path / "syntactically-valid-tamper"
    outcome = LocalMarketBundleRepository(
        config=LocalMarketBundleRepositoryConfig(root=root)
    ).publish_market_bundle_v1(
        manifest=manifest,
        stream_payloads={
            stream_key: canonical_bytes(events) for stream_key, events in streams.items()
        },
        retention_policy_ref="koru-tradifi-target-overlay-v4",
    )
    assert outcome.result is not None
    replacement_reader = LocalMarketBundleReader.open(
        repository_root=root, bundle_ref=outcome.result.bundle_ref
    )
    return replace(
        binding,
        overlay_bundle_ref=outcome.result.bundle_ref,
        overlay_bundle_digest=outcome.result.bundle_ref.manifest_hash,
        reader=replacement_reader,
    )


def test_v4_overlay_semantic_and_prm_row_substitutions_fail_closed(
    tmp_path: Path,
) -> None:
    foundation, publication, root = _published(tmp_path)
    authority = publication.authority
    bindings = authority.reader_set.reader_set.bindings
    bad_binding = _publish_semantically_tampered_overlay(tmp_path, bindings[0])
    bad_set = KoruPremiumReaderSetV2((bad_binding, *bindings[1:]))
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV2):
        authority_module.publish_koru_premium_overlay_set_authority_v4(
            foundation,
            economics=authority.economics,
            reader_set=bad_set,
            source_projection=_opened_source(foundation, authority),
        )
    with pytest.raises(ValueError, match="canonical premium rows"):
        KoruPremiumReaderSetV2((bindings[1], bindings[0], *bindings[2:]))
    row = bindings[0]
    stream_index = next(
        index
        for index, stream in enumerate(row.reader.manifest.streams)
        if stream.stream_key == row.target_stream_key
    )
    stream_path = (
        root
        / "overlays"
        / row.premium_id
        / "bundles"
        / row.overlay_bundle_ref.bundle_key
        / row.overlay_bundle_ref.manifest_hash.removeprefix("sha256:")
        / "streams"
        / f"{stream_index:03d}.payload"
    )
    stream_path.chmod(0o600)
    stream_path.write_bytes(b"[]")
    stream_path.chmod(0o444)
    with pytest.raises(KoruPremiumPreflightAuthorityErrorV2):
        open_published_koru_premium_preflight_authority_v2(
            foundation,
            authority_ref=publication.authority_ref,
            publication_entry_ref=publication.publication_entry_ref,
            repository_root=root / "overlays",
        )
