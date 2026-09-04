"""Published KORU V2 premium-preflight authority without V1 or Runtime imports."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, cast

from crypto_quant_bundle_builder import (
    KoruDirectionalDiscoveryScopeV1,
    KoruDirectionalTargetCompileRequestV2,
    KoruDirectionalTargetRecipeV1,
    KoruDirectionalTargetStreamV2,
    KoruMarkIndexPremiumParametersV1,
    KoruPremiumRecipeAuthorityV1,
    KoruTradifiEconomicsBundleV4,
    KoruTradifiEconomicsTermsV4,
    build_binance_usdm_koru_source_profile_authority_v3,
    compile_binance_usdm_koru_directional_targets_v2,
)
from crypto_quant_domain import (
    ArtifactEnvelope,
    ArtifactRef,
    InstrumentId,
    SourceSequence,
    TimelinePhase,
    VenueId,
    canonical_bytes,
    canonical_sha256,
)
from crypto_quant_foundation import LogEntryRef
from crypto_quant_market_data import (
    KoruPremiumReaderBindingV2,
    KoruPremiumReaderSetV2,
    LocalMarketBundleReader,
    MarketBundleCapability,
    MarketBundleManifest,
    MarketBundleRef,
    MarketEvent,
    MarketStreamManifest,
)

from .koru_boundary_indexes import (
    BoundaryIndexPublicationFact,
    open_published_koru_aggregate_trade_boundary_index_authority_v3,
)
from .koru_source_projections import (
    KoruTradifiSourceProjectionScopeV3,
    SourceProjectionPublicationFactV3,
    open_published_koru_tradifi_source_projection_authority_v3,
)
from .raw_blob_snapshots import (
    RawBlobSnapshotPublicationFact,
    open_verified_raw_blob_snapshot,
)

KORU_PREMIUM_ECONOMICS_V4_LOG = "research.koru_premium.economics.v4"
KORU_PREMIUM_OVERLAY_SET_V4_LOG = "research.koru_premium.overlay_set.v4"
KORU_PREMIUM_READER_SET_V2_LOG = "research.koru_premium.reader_set.v2"
KORU_PREMIUM_PREFLIGHT_AUTHORITY_V2_LOG = "research.koru_premium.preflight_authority.v2"

_ECONOMICS_TYPE = "koru_premium_economics_authority_v4"
_OVERLAY_SET_TYPE = "koru_premium_overlay_set_authority_v4"
_READER_SET_TYPE = "koru_premium_reader_set_authority_v2"
_AUTHORITY_TYPE = "koru_premium_preflight_authority_v2"
_PREMIUM_IDS = tuple(f"KORU-PRM-{number:02d}" for number in range(1, 5))
_OVERLAY_STREAM = "binance_usdm.tradifi.target_overlay_authority.koruusdt.v4"
_OVERLAY_EVENT = "koru_tradifi_target_overlay_authority_v4"
_OVERLAY_CAPABILITY = MarketBundleCapability(
    "binance_usdm.tradifi.target-overlay-authority", 1
)
_ECONOMICS_STREAM = "binance_usdm.tradifi.economics_authority.koruusdt.v4"
_ECONOMICS_EVENT = "koru_tradifi_economics_authority_v4"
_OVERLAY_PREFIX = "koru-tradifi-target-overlay-development-v4-"
_V2_ADMISSION_TOKEN = object()


class KoruPremiumPreflightAuthorityFoundationV2(Protocol):
    """Only public Foundation CAS and owner-log operations used by V2."""

    def put(self, *, envelope: ArtifactEnvelope) -> ArtifactRef: ...

    def read(self, *, ref: ArtifactRef): ...

    def append(self, log_name: str, event_id: str, payload: bytes): ...

    def entries(
        self, log_name: str, through: LogEntryRef | None = None
    ) -> tuple[object, ...]: ...


class KoruPremiumPreflightAuthorityErrorV2(ValueError):
    """V2 authority verification failed closed."""


def _fail(message: str) -> None:
    raise KoruPremiumPreflightAuthorityErrorV2(message)


def _same(left: object, right: object) -> bool:
    return canonical_bytes(left) == canonical_bytes(right)


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(type(key) is not str for key in value):
        _fail(name)
    return cast(Mapping[str, object], value)


def _digest(value: object, name: str) -> str:
    if (
        type(value) is not str
        or len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        _fail(name)
    return cast(str, value)


def _artifact_ref(
    value: object,
    name: str,
    *,
    artifact_type: str | None = None,
    schema_version: int | None = None,
) -> ArtifactRef:
    if type(value) is not ArtifactRef:
        _fail(name)
    ref = cast(ArtifactRef, value)
    if (
        (artifact_type is not None and ref.artifact_type != artifact_type)
        or (schema_version is not None and ref.schema_version != schema_version)
    ):
        _fail(name)
    _digest(ref.content_hash, name)
    return ref


def _ref(value: object, name: str) -> ArtifactRef:
    wire = _mapping(value, name)
    if set(wire) != {
        "type",
        "artifact_type",
        "schema_version",
        "content_hash",
    } or wire["type"] != "artifact_ref":
        _fail(name)
    try:
        return _artifact_ref(
            ArtifactRef(
                cast(str, wire["artifact_type"]),
                cast(int, wire["schema_version"]),
                cast(str, wire["content_hash"]),
            ),
            name,
        )
    except (TypeError, ValueError) as error:
        _fail(str(error))


def _entry_value(value: object, log_name: str) -> LogEntryRef:
    if type(value) is not LogEntryRef:
        _fail("publication entry")
    entry = cast(LogEntryRef, value)
    if entry.log_name != log_name or entry.log_sequence < 1:
        _fail("publication entry")
    _digest(entry.receipt_hash, "publication entry")
    return entry


def _entry(value: object, log_name: str) -> LogEntryRef:
    wire = _mapping(value, "publication entry")
    if set(wire) != {"log_name", "log_sequence", "receipt_hash"}:
        _fail("publication entry")
    try:
        return _entry_value(
            LogEntryRef(
                cast(str, wire["log_name"]),
                cast(int, wire["log_sequence"]),
                cast(str, wire["receipt_hash"]),
            ),
            log_name,
        )
    except (TypeError, ValueError) as error:
        _fail(str(error))


def _entry_wire(entry: LogEntryRef) -> dict[str, object]:
    entry = _entry_value(entry, entry.log_name if type(entry) is LogEntryRef else "")
    return {
        "log_name": entry.log_name,
        "log_sequence": entry.log_sequence,
        "receipt_hash": entry.receipt_hash,
    }


def _bundle_ref_value(value: object, name: str) -> MarketBundleRef:
    if type(value) is not MarketBundleRef:
        _fail(name)
    ref = cast(MarketBundleRef, value)
    _digest(ref.manifest_hash, name)
    return ref


def _bundle_ref(value: object, name: str) -> MarketBundleRef:
    wire = _mapping(value, name)
    if set(wire) != {"type", "bundle_key", "manifest_hash"} or wire["type"] != "market_bundle_ref":
        _fail(name)
    try:
        return _bundle_ref_value(
            MarketBundleRef(
                cast(str, wire["bundle_key"]), cast(str, wire["manifest_hash"])
            ),
            name,
        )
    except (TypeError, ValueError) as error:
        _fail(str(error))


def _envelope(value: object, name: str) -> ArtifactEnvelope:
    wire = _mapping(value, name)
    if set(wire) != {"artifact_type", "schema_version", "payload", "content_hash"}:
        _fail(name)
    try:
        envelope = ArtifactEnvelope(
            cast(str, wire["artifact_type"]),
            cast(int, wire["schema_version"]),
            wire["payload"],
            cast(str, wire["content_hash"]),
        )
    except (TypeError, ValueError) as error:
        _fail(str(error))
    return envelope


def _read_artifact(
    foundation: KoruPremiumPreflightAuthorityFoundationV2,
    ref: ArtifactRef,
    artifact_type: str,
    schema_version: int,
) -> ArtifactEnvelope:
    ref = _artifact_ref(
        ref, "artifact ref", artifact_type=artifact_type, schema_version=schema_version
    )
    try:
        readback = foundation.read(ref=ref)
        envelope = readback.envelope
        source_bytes = readback.source_bytes
        source_hash = readback.source_hash
    except Exception as error:  # noqa: BLE001 - Foundation is a trust boundary
        _fail(f"artifact unavailable: {error}")
    if (
        type(envelope) is not ArtifactEnvelope
        or ArtifactRef.from_envelope(envelope) != ref
        or source_bytes != canonical_bytes(envelope)
        or source_hash != canonical_sha256(envelope)
    ):
        _fail("artifact readback")
    return envelope


def _read_exact_artifact(
    foundation: KoruPremiumPreflightAuthorityFoundationV2, ref: ArtifactRef
) -> ArtifactEnvelope:
    return _read_artifact(foundation, ref, ref.artifact_type, ref.schema_version)


def _publication_fact(
    log_name: str, ref: ArtifactRef, payload: object
) -> dict[str, object]:
    return {
        "type": "koru_premium_preflight_publication_v2",
        "schema_version": 2,
        "owner_log": log_name,
        "artifact_ref": ref.to_canonical_dict(),
        "payload_digest": canonical_sha256(payload),
    }


def _event_id(log_name: str, fact: object) -> str:
    return canonical_sha256(("koru-premium-preflight-publication-v2", log_name, fact))


def _append_exact(
    foundation: KoruPremiumPreflightAuthorityFoundationV2,
    log_name: str,
    ref: ArtifactRef,
    payload: object,
) -> LogEntryRef:
    fact = _publication_fact(log_name, ref, payload)
    try:
        receipt = foundation.append(log_name, _event_id(log_name, fact), canonical_bytes(fact))
        entry = receipt.entry_ref
    except Exception as error:  # noqa: BLE001 - Foundation is a trust boundary
        _fail(f"publication append: {error}")
    _exact_log_fact(foundation, log_name, ref, payload, entry)
    return entry


def _exact_log_fact(
    foundation: KoruPremiumPreflightAuthorityFoundationV2,
    log_name: str,
    ref: ArtifactRef,
    payload: object,
    entry: LogEntryRef,
) -> None:
    entry = _entry_value(entry, log_name)
    fact = _publication_fact(log_name, ref, payload)
    try:
        entries = foundation.entries(log_name, through=entry)
    except Exception as error:  # noqa: BLE001 - Foundation is a trust boundary
        _fail(f"publication log unavailable: {error}")
    if (
        type(entries) is not tuple
        or not entries
        or getattr(entries[-1], "entry_ref", None) != entry
        or getattr(entries[-1], "event_id", None) != _event_id(log_name, fact)
        or getattr(entries[-1], "payload", None) != canonical_bytes(fact)
    ):
        _fail("publication fact")


def _open_wrapper(
    foundation: KoruPremiumPreflightAuthorityFoundationV2,
    *,
    ref: ArtifactRef,
    entry: LogEntryRef,
    log_name: str,
    artifact_type: str,
    schema_version: int,
) -> Mapping[str, object]:
    envelope = _read_artifact(foundation, ref, artifact_type, schema_version)
    payload = _mapping(envelope.payload, "artifact payload")
    _exact_log_fact(foundation, log_name, ref, payload, entry)
    return payload


def _publish_wrapper(
    foundation: KoruPremiumPreflightAuthorityFoundationV2,
    *,
    log_name: str,
    artifact_type: str,
    schema_version: int,
    payload: object,
) -> tuple[ArtifactRef, LogEntryRef]:
    envelope = ArtifactEnvelope.create(artifact_type, schema_version, payload)
    try:
        ref = foundation.put(envelope=envelope)
    except Exception as error:  # noqa: BLE001 - Foundation is a trust boundary
        _fail(f"artifact publication: {error}")
    if ref != ArtifactRef.from_envelope(envelope):
        _fail("artifact publication ref")
    _read_artifact(foundation, ref, artifact_type, schema_version)
    return ref, _append_exact(foundation, log_name, ref, envelope.payload)


def _events(reader: LocalMarketBundleReader, stream_key: str) -> tuple[MarketEvent, ...]:
    try:
        cursor = reader.open_cursor(stream_key, batch_size=64)
        values: list[MarketEvent] = []
        while not cursor.exhausted:
            batch, cursor = reader.read_batch(cursor)
            if any(type(event) is not MarketEvent for event in batch):
                _fail("bundle event")
            values.extend(cast(tuple[MarketEvent, ...], batch))
        return tuple(values)
    except KoruPremiumPreflightAuthorityErrorV2:
        raise
    except Exception as error:  # noqa: BLE001 - repository is a trust boundary
        _fail(f"bundle stream: {error}")


def _repository_reader(
    repository_root: Path, ref: MarketBundleRef
) -> LocalMarketBundleReader:
    if not isinstance(repository_root, Path) or not repository_root.is_absolute():
        _fail("repository root")
    try:
        reader = LocalMarketBundleReader.open(
            repository_root=repository_root, bundle_ref=ref
        )
        if (
            reader.bundle_ref != ref
            or reader.bundle_ref.manifest_hash != ref.manifest_hash
            or MarketBundleRef.from_manifest(reader.manifest) != ref
            or LocalMarketBundleReader.validate_repository_open_reader_v1(reader)
            is not reader
        ):
            _fail("repository reader")
        return reader
    except KoruPremiumPreflightAuthorityErrorV2:
        raise
    except Exception as error:  # noqa: BLE001 - repository is a trust boundary
        _fail(f"repository reader: {error}")


def _raw_fact(value: object) -> RawBlobSnapshotPublicationFact:
    wire = _mapping(value, "raw fact")
    if set(wire) != {
        "type",
        "schema_version",
        "manifest_ref",
        "snapshot_id",
    } or wire["type"] != "raw_blob_snapshot_publication" or wire[
        "schema_version"
    ] != 1:
        _fail("raw fact")
    try:
        return RawBlobSnapshotPublicationFact(
            _ref(wire["manifest_ref"], "raw manifest"),
            cast(str, wire["snapshot_id"]),
        )
    except (TypeError, ValueError) as error:
        _fail(str(error))


def _boundary_fact(value: object) -> BoundaryIndexPublicationFact:
    wire = _mapping(value, "boundary fact")
    keys = {
        "type",
        "schema_version",
        "manifest_ref",
        "raw_snapshot_publication_entry_ref",
        "authority_ref",
        "request_hash",
        "result_digest",
    }
    if set(wire) != keys or wire["type"] != "research.boundary_indexes.v1" or wire[
        "schema_version"
    ] != 1:
        _fail("boundary fact")
    try:
        return BoundaryIndexPublicationFact(
            _ref(wire["manifest_ref"], "boundary manifest"),
            _entry(
                wire["raw_snapshot_publication_entry_ref"],
                "research.raw_snapshots.v1",
            ),
            _ref(wire["authority_ref"], "boundary authority"),
            cast(str, wire["request_hash"]),
            cast(str, wire["result_digest"]),
        )
    except (TypeError, ValueError) as error:
        _fail(str(error))


def _source_fact(value: object) -> SourceProjectionPublicationFactV3:
    wire = _mapping(value, "source fact")
    keys = {
        "type",
        "schema_version",
        "raw_snapshot_fact",
        "raw_snapshot_publication_entry_ref",
        "boundary_index_fact",
        "boundary_index_publication_entry_ref",
        "authority_ref",
        "source_request_hash",
        "source_fragment_digest",
        "scope",
    }
    if set(wire) != keys or wire["type"] != "research.source_projections.v3" or wire[
        "schema_version"
    ] != 3:
        _fail("source fact")
    scope_wire = _mapping(wire["scope"], "source scope")
    try:
        from crypto_quant_domain import UtcInstant

        scope = KoruTradifiSourceProjectionScopeV3(
            UtcInstant(
                cast(
                    int,
                    _mapping(scope_wire["timeline_window_start"], "scope start")[
                        "epoch_nanoseconds"
                    ],
                )
            ),
            UtcInstant(
                cast(
                    int,
                    _mapping(
                        scope_wire["timeline_window_end_exclusive"], "scope end"
                    )["epoch_nanoseconds"],
                )
            ),
        )
        return SourceProjectionPublicationFactV3(
            _raw_fact(wire["raw_snapshot_fact"]),
            _entry(
                wire["raw_snapshot_publication_entry_ref"],
                "research.raw_snapshots.v1",
            ),
            _boundary_fact(wire["boundary_index_fact"]),
            _entry(
                wire["boundary_index_publication_entry_ref"],
                "research.boundary_indexes.v1",
            ),
            _ref(wire["authority_ref"], "source authority"),
            cast(str, wire["source_request_hash"]),
            cast(str, wire["source_fragment_digest"]),
            scope,
        )
    except (KeyError, TypeError, ValueError) as error:
        _fail(str(error))


def _verify_inputs(
    foundation: KoruPremiumPreflightAuthorityFoundationV2,
    raw: RawBlobSnapshotPublicationFact,
    raw_entry: LogEntryRef,
    boundary: BoundaryIndexPublicationFact,
    boundary_entry: LogEntryRef,
    source: SourceProjectionPublicationFactV3,
    source_entry: LogEntryRef,
):
    """Reopen raw, boundary, and source through their public authority seams."""
    _read_artifact(foundation, raw.manifest_ref, "raw_blob_snapshot_manifest", 1)
    view = open_verified_raw_blob_snapshot(foundation, raw.manifest_ref, raw_entry)
    if view.manifest.snapshot_id != raw.snapshot_id:
        _fail("raw snapshot identity")
    _read_artifact(
        foundation,
        boundary.authority_ref,
        "binance_usdm_koru_aggregate_trade_boundary_index_authority_v3",
        3,
    )
    opened_boundary = open_published_koru_aggregate_trade_boundary_index_authority_v3(
        foundation,
        manifest_ref=raw.manifest_ref,
        raw_snapshot_publication_entry_ref=raw_entry,
        authority_ref=boundary.authority_ref,
        publication_entry_ref=boundary_entry,
    )
    if (
        boundary.manifest_ref != raw.manifest_ref
        or boundary.raw_snapshot_publication_entry_ref != raw_entry
        or opened_boundary.request.request_hash != boundary.request_hash
        or opened_boundary.result_digest != boundary.result_digest
    ):
        _fail("boundary identity")
    _read_artifact(
        foundation,
        source.authority_ref,
        "binance_usdm_koru_tradifi_source_projection_authority_v3",
        3,
    )
    opened_source = open_published_koru_tradifi_source_projection_authority_v3(
        foundation,
        fact=source,
        publication_entry_ref=source_entry,
        scope=source.scope,
    )
    if (
        source.raw_snapshot_fact != raw
        or source.raw_snapshot_publication_entry_ref != raw_entry
        or source.boundary_index_fact != boundary
        or source.boundary_index_publication_entry_ref != boundary_entry
        or opened_source.request.request_hash != source.source_request_hash
        or opened_source.fragment_digest != source.source_fragment_digest
    ):
        _fail("source identity")
    return opened_source


def _binding_from_wire(
    value: object, repository_root: Path
) -> KoruPremiumReaderBindingV2:
    wire = _mapping(value, "reader binding")
    expected = {
        "type",
        "premium_id",
        "premium_key",
        "strategy_definition_envelope",
        "strategy_parameter_set_envelope",
        "strategy_ref",
        "parameter_ref",
        "recipe_digest",
        "compiler_result_ref",
        "compiler_result_digest",
        "scope_ref",
        "scope_digest",
        "source_projection_authority_ref",
        "source_projection_authority_content_hash",
        "source_fragment_digest",
        "target_stream_key",
        "target_stream_digest",
        "overlay_bundle_ref",
        "overlay_bundle_digest",
        "economics_bundle_ref",
        "economics_bundle_digest",
        "economics_authority_digest",
    }
    if set(wire) != expected or wire["type"] != "koru_premium_reader_binding_v2":
        _fail("reader binding")
    premium_id = wire["premium_id"]
    if (
        type(premium_id) is not str
        or premium_id not in _PREMIUM_IDS
        or wire["premium_key"] != premium_id
        or wire["target_stream_key"] != premium_id
    ):
        _fail("canonical premium id")
    overlay_ref = _bundle_ref(wire["overlay_bundle_ref"], "overlay bundle")
    try:
        return KoruPremiumReaderBindingV2(
            premium_id,
            cast(str, wire["premium_key"]),
            _envelope(wire["strategy_definition_envelope"], "strategy envelope"),
            _envelope(wire["strategy_parameter_set_envelope"], "parameter envelope"),
            _ref(wire["strategy_ref"], "strategy ref"),
            _ref(wire["parameter_ref"], "parameter ref"),
            cast(str, wire["recipe_digest"]),
            _ref(wire["compiler_result_ref"], "compiler result ref"),
            cast(str, wire["compiler_result_digest"]),
            _ref(wire["scope_ref"], "scope ref"),
            cast(str, wire["scope_digest"]),
            _ref(wire["source_projection_authority_ref"], "source ref"),
            cast(str, wire["source_projection_authority_content_hash"]),
            cast(str, wire["source_fragment_digest"]),
            cast(str, wire["target_stream_key"]),
            cast(str, wire["target_stream_digest"]),
            overlay_ref,
            cast(str, wire["overlay_bundle_digest"]),
            _bundle_ref(wire["economics_bundle_ref"], "economics bundle"),
            cast(str, wire["economics_bundle_digest"]),
            cast(str, wire["economics_authority_digest"]),
            _repository_reader(repository_root / premium_id, overlay_ref),
        )
    except (TypeError, ValueError) as error:
        _fail(str(error))


def _bindings(reader_set: KoruPremiumReaderSetV2) -> tuple[object, ...]:
    if (
        type(reader_set) is not KoruPremiumReaderSetV2
        or tuple(row.premium_id for row in reader_set.bindings) != _PREMIUM_IDS
    ):
        _fail("reader set")
    return tuple(row.to_canonical_dict() for row in reader_set.bindings)


def _sealed_recipe(binding: KoruPremiumReaderBindingV2) -> KoruDirectionalTargetRecipeV1:
    """Rebuild one fixed premium recipe solely from its sealed envelopes."""
    parameter = _mapping(
        binding.strategy_parameter_set_envelope.payload, "parameter envelope"
    )
    strategy = _mapping(
        binding.strategy_definition_envelope.payload, "strategy envelope"
    )
    expected_keys = {
        "type",
        "schema_version",
        "premium_id",
        "premium_key",
        "family",
        "strategy_id",
        "sleeve_id",
        "instrument_id",
        "bar_interval",
        "target_exposure",
        "entry_premium_bps",
        "exit_premium_bps",
        "max_hold_hours",
        "flat_when_inside_band",
    }
    if (
        set(parameter) != expected_keys
        or set(strategy) != expected_keys
        or parameter.get("type") != "koru_premium_strategy_parameter_set_v1"
        or strategy.get("type") != "koru_premium_strategy_definition_v1"
        or parameter.get("schema_version") != 1
        or strategy.get("schema_version") != 1
    ):
        _fail("premium envelope")
    shared = expected_keys - {"type"}
    if any(not _same(parameter[key], strategy[key]) for key in shared):
        _fail("premium envelope")
    premium_id = parameter.get("premium_id")
    instrument = _mapping(parameter.get("instrument_id"), "premium instrument")
    expected_entry = {premium: entry for premium, entry in zip(_PREMIUM_IDS, ("20", "30", "40", "60"), strict=True)}
    if (
        premium_id != binding.premium_id
        or parameter.get("premium_key") != binding.premium_key
        or parameter.get("family") != "mark_index_premium"
        or parameter.get("bar_interval") != "1h"
        or parameter.get("target_exposure") != "0.25"
        or parameter.get("entry_premium_bps") != expected_entry.get(premium_id)
        or parameter.get("exit_premium_bps") != "5"
        or parameter.get("max_hold_hours") != 12
        or parameter.get("flat_when_inside_band") is not True
        or set(instrument) != {"venue", "stable_key"}
        or instrument.get("venue") != "binance_usdm"
        or instrument.get("stable_key") != "koru-usdt-tradifi-perpetual"
    ):
        _fail("fixed premium recipe")
    try:
        recipe = KoruDirectionalTargetRecipeV1(
            family="mark_index_premium",
            recipe_id=cast(str, premium_id),
            strategy_id=cast(str, parameter["strategy_id"]),
            sleeve_id=cast(str, parameter["sleeve_id"]),
            strategy_ref=binding.strategy_ref,
            parameter_ref=binding.parameter_ref,
            target_stream_key=cast(str, parameter["premium_key"]),
            instrument_id=InstrumentId(
                VenueId(cast(str, instrument["venue"])),
                cast(str, instrument["stable_key"]),
            ),
            target_exposure=cast(str, parameter["target_exposure"]),
            bar_interval=cast(str, parameter["bar_interval"]),
            parameters=KoruMarkIndexPremiumParametersV1(
                cast(str, parameter["entry_premium_bps"]),
                cast(str, parameter["exit_premium_bps"]),
                cast(int, parameter["max_hold_hours"]),
                cast(bool, parameter["flat_when_inside_band"]),
            ),
        )
        authority = KoruPremiumRecipeAuthorityV1(
            recipe,
            binding.strategy_definition_envelope,
            binding.strategy_parameter_set_envelope,
        )
    except (TypeError, ValueError) as error:
        _fail(f"fixed premium recipe: {error}")
    if (
        authority.strategy_ref != binding.strategy_ref
        or authority.parameter_ref != binding.parameter_ref
        or authority.recipe.recipe_digest != binding.recipe_digest
    ):
        _fail("premium recipe binding")
    return recipe


def _recompile_targets(
    source_projection: object, reader_set: KoruPremiumReaderSetV2
) -> dict[str, KoruDirectionalTargetStreamV2]:
    """Recompile all four fixed PRM targets from the verified V3 source."""
    if type(reader_set) is not KoruPremiumReaderSetV2:
        _fail("reader set")
    scope = KoruDirectionalDiscoveryScopeV1()
    scope_ref = ArtifactRef("koru_directional_discovery_scope", 1, scope.scope_digest)
    recipes = tuple(_sealed_recipe(binding) for binding in reader_set.bindings)
    first = reader_set.bindings[0]
    if any(
        binding.scope_ref != scope_ref
        or binding.scope_digest != scope.scope_digest
        or binding.source_projection_authority_ref
        != first.source_projection_authority_ref
        or binding.source_projection_authority_content_hash
        != first.source_projection_authority_content_hash
        or binding.source_fragment_digest != first.source_fragment_digest
        for binding in reader_set.bindings
    ):
        _fail("compiler source identity")
    try:
        outcome = compile_binance_usdm_koru_directional_targets_v2(
            KoruDirectionalTargetCompileRequestV2(
                source_projection,
                first.source_projection_authority_ref,
                first.source_projection_authority_content_hash,
                scope,
                recipes,
            )
        )
        result = outcome.result
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        _fail(f"target recompilation: {error}")
    if result is None:
        _fail("target recompilation")
    expected_ref = ArtifactRef(
        "koru_directional_target_compile_result", 2, result.result_digest
    )
    if (
        tuple(stream.target_stream_key for stream in result.streams) != _PREMIUM_IDS
        or any(
            binding.compiler_result_ref != expected_ref
            or binding.compiler_result_digest != result.result_digest
            for binding in reader_set.bindings
        )
    ):
        _fail("compiler result")
    streams = {stream.target_stream_key: stream for stream in result.streams}
    for binding in reader_set.bindings:
        stream = streams.get(binding.target_stream_key)
        if (
            stream is None
            or stream.recipe_ref != binding.parameter_ref
            or stream.source_fragment_digest != binding.source_fragment_digest
            or stream.target_stream_digest != binding.target_stream_digest
        ):
            _fail("target stream")
    return streams


def _verify_builder_economics(
    economics: KoruTradifiEconomicsBundleV4, source_projection: object
) -> None:
    """Bind Builder V4 economics input to the just-opened public source result."""
    if type(economics) is not KoruTradifiEconomicsBundleV4:
        _fail("economics type")
    identity = economics.request.source_projection_content_identity
    try:
        expected_terms = KoruTradifiEconomicsTermsV4.from_source_projection(
            source_projection, execution_account_id="account-1"
        )
        profile, profile_ref = build_binance_usdm_koru_source_profile_authority_v3(
            source_projection
        )
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        _fail(f"economics source: {error}")
    if (
        not _same(economics.request.source_projection, source_projection)
        or not _same(economics.request.terms, expected_terms)
        or economics.source_profile_authority != profile
        or not economics.authority_refs
        or economics.authority_refs[0] != profile_ref
        or not _same(
            identity.source_projection_authority_ref,
            profile.payload.get("source_projection_authority_ref"),
        )
        or identity.source_fragment_digest != profile.payload.get("source_fragment_digest")
        or identity.source_projection_request_hash
        != profile.payload.get("source_projection_request_hash")
    ):
        _fail("economics source binding")


def _verify_opened_economics_source(
    economics: KoruPremiumEconomicsPublicationV4, source_projection: object
) -> None:
    """Verify the sealed V4 terms/profile event against the verified source result."""
    try:
        expected_terms = KoruTradifiEconomicsTermsV4.from_source_projection(
            source_projection, execution_account_id="account-1"
        )
        profile, profile_ref = build_binance_usdm_koru_source_profile_authority_v3(
            source_projection
        )
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        _fail(f"economics source: {error}")
    event = _mapping(economics.economics_authority_event, "economics authority event")
    payload = _mapping(event.get("payload"), "economics authority payload")
    source_identity = {
        "type": "koru_tradifi_source_projection_content_identity_v3",
        "schema_version": 3,
        "source_projection_authority_ref": economics.source_projection_authority_ref.to_canonical_dict(),
        "source_projection_authority_content_hash": economics.source_projection_authority_ref.content_hash,
        "source_fragment_digest": economics.source_fragment_digest,
        "source_projection_request_hash": economics.source_projection_request_hash,
    }
    if (
        economics.source_profile_authority_envelope != profile
        or economics.authority_artifact_refs[0] != profile_ref
        or not _same(payload.get("source_projection_content_identity"), source_identity)
        or payload.get("source_fragment_digest") != economics.source_fragment_digest
        or not _same(payload.get("terms"), expected_terms.to_canonical_dict())
        or not _same(
            payload.get("artifact_refs"),
            tuple(value.to_canonical_dict() for value in economics.authority_artifact_refs),
        )
        or payload.get("authority_digest") != economics.economics_authority_digest
        or payload.get("authority_digest")
        != canonical_sha256(
            {key: value for key, value in payload.items() if key != "authority_digest"}
        )
    ):
        _fail("economics source binding")


def _admit_v2(value: object) -> object:
    object.__setattr__(value, "_admission_token", _V2_ADMISSION_TOKEN)
    return value


def _is_admitted_v2(value: object) -> bool:
    return getattr(value, "_admission_token", None) is _V2_ADMISSION_TOKEN


def _require_admitted(value: object, exact_type: type[object], name: str) -> None:
    if type(value) is not exact_type or not _is_admitted_v2(value):
        _fail(f"unpublished {name}")


@dataclass(frozen=True, slots=True)
class KoruPremiumEconomicsPublicationV4:
    """Foundation-published V4 economics facts, admitted only after exact open."""

    artifact_ref: ArtifactRef
    publication_entry_ref: LogEntryRef
    economics_bundle_ref: MarketBundleRef
    economics_bundle_manifest: object
    economics_authority_digest: str
    economics_authority_event: object
    source_projection_authority_ref: ArtifactRef
    source_fragment_digest: str
    source_projection_request_hash: str
    economics_result_digest: str
    source_profile_authority_envelope: ArtifactEnvelope
    authority_artifact_refs: tuple[ArtifactRef, ...]
    _admission_token: object | None = field(
        init=False, default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        _artifact_ref(
            self.artifact_ref,
            "economics publication ref",
            artifact_type=_ECONOMICS_TYPE,
            schema_version=4,
        )
        _entry_value(self.publication_entry_ref, KORU_PREMIUM_ECONOMICS_V4_LOG)
        _bundle_ref_value(self.economics_bundle_ref, "economics bundle")
        _mapping(self.economics_bundle_manifest, "economics bundle manifest")
        _mapping(self.economics_authority_event, "economics authority event")
        _artifact_ref(
            self.source_projection_authority_ref,
            "economics source",
            artifact_type="binance_usdm_koru_tradifi_source_projection_authority_v3",
            schema_version=3,
        )
        for value, name in (
            (self.economics_authority_digest, "economics authority digest"),
            (self.source_fragment_digest, "source fragment digest"),
            (self.source_projection_request_hash, "source request hash"),
            (self.economics_result_digest, "economics result digest"),
        ):
            _digest(value, name)
        if (
            type(self.source_profile_authority_envelope) is not ArtifactEnvelope
            or type(self.authority_artifact_refs) is not tuple
            or len(self.authority_artifact_refs) != 4
        ):
            _fail("economics artifacts")
        refs = tuple(
            _artifact_ref(value, "economics artifact")
            for value in self.authority_artifact_refs
        )
        if refs[0] != ArtifactRef.from_envelope(self.source_profile_authority_envelope):
            _fail("source profile artifact")

    @property
    def _admitted(self) -> bool:
        return _is_admitted_v2(self)

    def _canonical_dict(self) -> dict[str, object]:
        return {
            "artifact_ref": self.artifact_ref.to_canonical_dict(),
            "publication_entry_ref": _entry_wire(self.publication_entry_ref),
            "economics_bundle_ref": self.economics_bundle_ref.to_canonical_dict(),
            "economics_bundle_manifest": self.economics_bundle_manifest,
            "economics_authority_digest": self.economics_authority_digest,
            "economics_authority_event": self.economics_authority_event,
            "source_projection_authority_ref": self.source_projection_authority_ref.to_canonical_dict(),
            "source_fragment_digest": self.source_fragment_digest,
            "source_projection_request_hash": self.source_projection_request_hash,
            "economics_result_digest": self.economics_result_digest,
            "source_profile_authority_envelope": self.source_profile_authority_envelope.to_canonical_dict(),
            "authority_artifact_refs": tuple(
                value.to_canonical_dict() for value in self.authority_artifact_refs
            ),
        }

    def to_canonical_dict(self) -> dict[str, object]:
        if not self._admitted:
            _fail("unpublished economics authority")
        return self._canonical_dict()


@dataclass(frozen=True, slots=True)
class KoruPremiumOverlaySetPublicationV4:
    """One owner-log-published exact PRM-01..04 OverlayV4 set."""

    artifact_ref: ArtifactRef
    publication_entry_ref: LogEntryRef
    economics_artifact_ref: ArtifactRef
    bindings: tuple[object, ...]
    _admission_token: object | None = field(
        init=False, default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        _artifact_ref(
            self.artifact_ref,
            "overlay publication ref",
            artifact_type=_OVERLAY_SET_TYPE,
            schema_version=4,
        )
        _entry_value(self.publication_entry_ref, KORU_PREMIUM_OVERLAY_SET_V4_LOG)
        _artifact_ref(
            self.economics_artifact_ref,
            "overlay economics ref",
            artifact_type=_ECONOMICS_TYPE,
            schema_version=4,
        )
        if type(self.bindings) is not tuple or len(self.bindings) != len(_PREMIUM_IDS):
            _fail("overlay bindings")
        canonical_bytes(self.bindings)

    @property
    def _admitted(self) -> bool:
        return _is_admitted_v2(self)

    def _canonical_dict(self) -> dict[str, object]:
        return {
            "artifact_ref": self.artifact_ref.to_canonical_dict(),
            "publication_entry_ref": _entry_wire(self.publication_entry_ref),
            "economics_artifact_ref": self.economics_artifact_ref.to_canonical_dict(),
            "bindings": self.bindings,
        }

    def to_canonical_dict(self) -> dict[str, object]:
        if not self._admitted:
            _fail("unpublished overlay set")
        return self._canonical_dict()


@dataclass(frozen=True, slots=True)
class KoruPremiumReaderSetPublicationV2:
    """Owner-log-published reader-set identity with repository-open readers."""

    artifact_ref: ArtifactRef
    publication_entry_ref: LogEntryRef
    overlay_set_artifact_ref: ArtifactRef
    reader_set: KoruPremiumReaderSetV2
    _admission_token: object | None = field(
        init=False, default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        _artifact_ref(
            self.artifact_ref,
            "reader publication ref",
            artifact_type=_READER_SET_TYPE,
            schema_version=2,
        )
        _entry_value(self.publication_entry_ref, KORU_PREMIUM_READER_SET_V2_LOG)
        _artifact_ref(
            self.overlay_set_artifact_ref,
            "reader overlay ref",
            artifact_type=_OVERLAY_SET_TYPE,
            schema_version=4,
        )
        if type(self.reader_set) is not KoruPremiumReaderSetV2:
            _fail("reader set")

    @property
    def _admitted(self) -> bool:
        return _is_admitted_v2(self)

    def _canonical_dict(self) -> dict[str, object]:
        return {
            "artifact_ref": self.artifact_ref.to_canonical_dict(),
            "publication_entry_ref": _entry_wire(self.publication_entry_ref),
            "overlay_set_artifact_ref": self.overlay_set_artifact_ref.to_canonical_dict(),
            "reader_set_digest": self.reader_set.reader_set_digest,
        }

    def to_canonical_dict(self) -> dict[str, object]:
        if not self._admitted:
            _fail("unpublished reader set")
        return self._canonical_dict()


def _economics_payload(economics: KoruTradifiEconomicsBundleV4) -> dict[str, object]:
    identity = economics.request.source_projection_content_identity
    return {
        "type": _ECONOMICS_TYPE,
        "schema_version": 4,
        "economics_bundle_ref": economics.bundle_ref.to_canonical_dict(),
        "economics_bundle_manifest": economics.manifest.to_canonical_dict(),
        "economics_authority_digest": economics.authority_digest,
        "economics_authority_event": economics.economics_authority_event.to_canonical_dict(),
        "source_projection_authority_ref": identity.source_projection_authority_ref.to_canonical_dict(),
        "source_fragment_digest": identity.source_fragment_digest,
        "source_projection_request_hash": identity.source_projection_request_hash,
        "economics_result_digest": economics.result_digest,
        "source_profile_authority_envelope": economics.source_profile_authority.to_canonical_dict(),
        "authority_artifact_refs": tuple(
            value.to_canonical_dict() for value in economics.authority_refs
        ),
    }


def _economics_from_payload(
    foundation: KoruPremiumPreflightAuthorityFoundationV2,
    *,
    artifact_ref: ArtifactRef,
    publication_entry_ref: LogEntryRef,
    payload: Mapping[str, object],
    source: SourceProjectionPublicationFactV3,
) -> KoruPremiumEconomicsPublicationV4:
    expected = {
        "type",
        "schema_version",
        "economics_bundle_ref",
        "economics_bundle_manifest",
        "economics_authority_digest",
        "economics_authority_event",
        "source_projection_authority_ref",
        "source_fragment_digest",
        "source_projection_request_hash",
        "economics_result_digest",
        "source_profile_authority_envelope",
        "authority_artifact_refs",
    }
    if (
        set(payload) != expected
        or payload.get("type") != _ECONOMICS_TYPE
        or payload.get("schema_version") != 4
    ):
        _fail("economics payload")
    bundle_ref = _bundle_ref(payload["economics_bundle_ref"], "economics bundle")
    source_ref = _ref(payload["source_projection_authority_ref"], "economics source")
    authority_digest = _digest(
        payload["economics_authority_digest"], "economics authority digest"
    )
    fragment_digest = _digest(payload["source_fragment_digest"], "source fragment digest")
    request_hash = _digest(payload["source_projection_request_hash"], "source request hash")
    result_digest = _digest(payload["economics_result_digest"], "economics result digest")
    manifest = _mapping(payload["economics_bundle_manifest"], "economics bundle manifest")
    authority_event = _mapping(
        payload["economics_authority_event"], "economics authority event"
    )
    profile = _envelope(
        payload["source_profile_authority_envelope"], "source profile envelope"
    )
    raw_refs = payload["authority_artifact_refs"]
    if type(raw_refs) not in {tuple, list}:
        _fail("economics artifact refs")
    refs = tuple(_ref(value, "economics artifact") for value in raw_refs)
    if len(refs) != 4 or refs[0] != ArtifactRef.from_envelope(profile):
        _fail("source profile artifact")
    for ref in refs:
        readback = _read_exact_artifact(foundation, ref)
        if ref == refs[0] and readback != profile:
            _fail("source profile readback")
    if (
        source_ref != source.authority_ref
        or fragment_digest != source.source_fragment_digest
        or request_hash != source.source_request_hash
    ):
        _fail("economics source")
    return cast(
        KoruPremiumEconomicsPublicationV4,
        _admit_v2(
            KoruPremiumEconomicsPublicationV4(
                artifact_ref,
                publication_entry_ref,
                bundle_ref,
                manifest,
                authority_digest,
                authority_event,
                source_ref,
                fragment_digest,
                request_hash,
                result_digest,
                profile,
                refs,
            )
        ),
    )


def publish_koru_premium_economics_authority_v4(
    foundation: KoruPremiumPreflightAuthorityFoundationV2,
    *,
    economics: KoruTradifiEconomicsBundleV4,
    source: SourceProjectionPublicationFactV3,
    source_projection: object,
) -> KoruPremiumEconomicsPublicationV4:
    """Seal economics only after it rebinds to the opened V3 source result."""
    _verify_builder_economics(economics, source_projection)
    identity = economics.request.source_projection_content_identity
    if (
        identity.source_projection_authority_ref != source.authority_ref
        or identity.source_fragment_digest != source.source_fragment_digest
        or identity.source_projection_request_hash != source.source_request_hash
    ):
        _fail("economics source")
    for ref in economics.authority_refs:
        _read_exact_artifact(foundation, ref)
    payload = _economics_payload(economics)
    ref, entry = _publish_wrapper(
        foundation,
        log_name=KORU_PREMIUM_ECONOMICS_V4_LOG,
        artifact_type=_ECONOMICS_TYPE,
        schema_version=4,
        payload=payload,
    )
    opened = _economics_from_payload(
        foundation,
        artifact_ref=ref,
        publication_entry_ref=entry,
        payload=payload,
        source=source,
    )
    _verify_opened_economics_source(opened, source_projection)
    return opened


def open_published_koru_premium_economics_authority_v4(
    foundation: KoruPremiumPreflightAuthorityFoundationV2,
    *,
    artifact_ref: ArtifactRef,
    publication_entry_ref: LogEntryRef,
    source: SourceProjectionPublicationFactV3,
    source_projection: object,
) -> KoruPremiumEconomicsPublicationV4:
    """Open a CAS-and-owner-log-verified V4 economics publication."""
    payload = _open_wrapper(
        foundation,
        ref=artifact_ref,
        entry=publication_entry_ref,
        log_name=KORU_PREMIUM_ECONOMICS_V4_LOG,
        artifact_type=_ECONOMICS_TYPE,
        schema_version=4,
    )
    opened = _economics_from_payload(
        foundation,
        artifact_ref=artifact_ref,
        publication_entry_ref=publication_entry_ref,
        payload=payload,
        source=source,
    )
    _verify_opened_economics_source(opened, source_projection)
    return opened


def _source_profile_payload(
    economics: KoruPremiumEconomicsPublicationV4,
) -> Mapping[str, object]:
    profile = _mapping(
        economics.source_profile_authority_envelope.payload, "source profile payload"
    )
    expected = {
        "type",
        "schema_version",
        "timeline_window",
        "source_projection_request_hash",
        "source_fragment_digest",
        "source_projection_authority_ref",
        "aggregate_trade_boundary_index_request_hash",
        "aggregate_trade_boundary_index_result_digest",
        "aggregate_trade_streamed_reconstruction_digest",
        "aggregate_trade_intra_day_raw_id_gap_stream",
        "aggregate_trade_cross_date_raw_id_gap_stream",
        "aggregate_trade_coverage_gaps",
        "aggregate_trade_capture_final_evidence",
        "missing_boundaries",
        "source_stream_manifests",
        "source_event_bindings",
        "execution_projection_stream_manifest",
        "execution_projection_event_bindings",
        "xkrx_calendar_ref",
        "arcx_calendar_ref",
        "post_adjustment_unit_regime_ref",
        "development_only",
        "decision_grade_eligible",
        "deployment_authorized",
    }
    if (
        set(profile) != expected
        or profile.get("type") != "binance_usdm_koru_source_profile_authority_v3"
        or profile.get("schema_version") != 3
        or profile.get("source_projection_request_hash")
        != economics.source_projection_request_hash
        or profile.get("source_fragment_digest") != economics.source_fragment_digest
        or not _same(
            profile.get("source_projection_authority_ref"),
            economics.source_projection_authority_ref.to_canonical_dict(),
        )
    ):
        _fail("source profile authority")
    return profile


def _reader_streams(
    reader: LocalMarketBundleReader,
) -> dict[str, tuple[MarketEvent, ...]]:
    values: dict[str, tuple[MarketEvent, ...]] = {}
    for manifest in reader.manifest.streams:
        events = _events(reader, manifest.stream_key)
        try:
            rebuilt = MarketStreamManifest.from_events(manifest.stream_key, events)
        except (TypeError, ValueError) as error:
            _fail(f"stream manifest: {error}")
        if rebuilt != manifest:
            _fail("stream manifest")
        values[manifest.stream_key] = events
    return values


def _economics_manifest_from_overlay(
    binding: KoruPremiumReaderBindingV2,
    reader: LocalMarketBundleReader,
    streams: Mapping[str, tuple[MarketEvent, ...]],
) -> MarketBundleManifest:
    if reader.manifest.schema_version != 4:
        _fail("overlay schema")
    economics_streams = tuple(
        manifest
        for manifest in reader.manifest.streams
        if manifest.stream_key not in {binding.target_stream_key, _OVERLAY_STREAM}
    )
    if not economics_streams:
        _fail("economics stream cover")
    try:
        manifest = MarketBundleManifest.build(
            bundle_key=binding.economics_bundle_ref.bundle_key,
            schema_version=4,
            coverage_start=reader.manifest.coverage_start,
            coverage_end_exclusive=reader.manifest.coverage_end_exclusive,
            instrument_catalog_hash=reader.manifest.instrument_catalog_hash,
            capabilities=tuple(
                sorted({value.capability for value in economics_streams})
            ),
            streams=economics_streams,
        )
    except (TypeError, ValueError) as error:
        _fail(f"economics manifest: {error}")
    if MarketBundleRef.from_manifest(manifest) != binding.economics_bundle_ref:
        _fail("economics bundle ref")
    if _ECONOMICS_STREAM not in streams:
        _fail("economics authority stream")
    return manifest


def _verify_overlay_event(
    binding: KoruPremiumReaderBindingV2,
    economics: KoruPremiumEconomicsPublicationV4,
    compiled_target: KoruDirectionalTargetStreamV2,
) -> None:
    """Recompute every public V4 Overlay relation from its repository-open bytes."""
    if type(binding) is not KoruPremiumReaderBindingV2:
        _fail("reader binding")
    _require_admitted(
        economics, KoruPremiumEconomicsPublicationV4, "economics authority"
    )
    try:
        reader = LocalMarketBundleReader.validate_repository_open_reader_v1(
            binding.reader
        )
    except (AttributeError, TypeError, ValueError) as error:
        _fail(f"overlay reader: {error}")
    if reader is not binding.reader or reader.bundle_ref != binding.overlay_bundle_ref:
        _fail("overlay reader")
    streams = _reader_streams(reader)
    required_streams = {
        value["stream_key"]
        for value in cast(
            tuple[Mapping[str, object], ...],
            _mapping(economics.economics_bundle_manifest, "economics manifest").get(
                "streams", ()
            ),
        )
        if isinstance(value, Mapping) and type(value.get("stream_key")) is str
    }
    if not required_streams:
        _fail("economics manifest")
    if set(streams) != required_streams | {
        binding.target_stream_key,
        _OVERLAY_STREAM,
    }:
        _fail("overlay stream cover")
    economics_manifest = _economics_manifest_from_overlay(binding, reader, streams)
    if not _same(
        economics.economics_bundle_manifest, economics_manifest.to_canonical_dict()
    ):
        _fail("economics bundle manifest")
    economics_events = streams[_ECONOMICS_STREAM]
    if (
        len(economics_events) != 1
        or economics_events[0].event_type != _ECONOMICS_EVENT
        or not _same(
            economics_events[0].to_canonical_dict(), economics.economics_authority_event
        )
    ):
        _fail("economics authority event")
    target_events = streams[binding.target_stream_key]
    try:
        target_manifest = MarketStreamManifest.from_events(
            binding.target_stream_key, target_events
        )
    except (TypeError, ValueError) as error:
        _fail(f"target manifest: {error}")
    if (
        not _same(target_events, compiled_target.events)
        or target_manifest != compiled_target.manifest
        or binding.target_stream_digest != compiled_target.target_stream_digest
        or compiled_target.recipe_ref != binding.parameter_ref
        or compiled_target.source_fragment_digest != binding.source_fragment_digest
    ):
        _fail("target stream")
    authority_events = streams[_OVERLAY_STREAM]
    if len(authority_events) != 1:
        _fail("overlay authority event")
    event = authority_events[0]
    if (
        event.event_type != _OVERLAY_EVENT
        or event.stream_key != _OVERLAY_STREAM
        or event.capability != _OVERLAY_CAPABILITY
        or event.instrument_id is not None
        or event.event_time != economics_manifest.coverage_start
        or event.available_time != economics_manifest.coverage_start
        or event.phase != TimelinePhase(0, "market_data")
        or event.source_sequence != SourceSequence(0)
        or event.supersedes_revision_id is not None
        or event.source_key != _OVERLAY_STREAM
    ):
        _fail("overlay authority event")
    payload = _mapping(event.payload, "overlay authority payload")
    source_profile = _source_profile_payload(economics)
    recipe = _mapping(payload.get("recipe"), "overlay recipe")
    scope = _mapping(payload.get("scope"), "overlay scope")
    if (
        canonical_sha256(recipe) != binding.recipe_digest
        or canonical_sha256(scope) != binding.scope_digest
        or recipe.get("recipe_id") != binding.premium_id
        or recipe.get("target_stream_key") != binding.target_stream_key
        or not _same(recipe.get("strategy_ref"), binding.strategy_ref.to_canonical_dict())
        or not _same(recipe.get("parameter_ref"), binding.parameter_ref.to_canonical_dict())
        or payload.get("strategy_id") != recipe.get("strategy_id")
        or payload.get("sleeve_id") != recipe.get("sleeve_id")
    ):
        _fail("overlay recipe")
    expected = {
        "schema_version": 4,
        "economics_bundle_ref": binding.economics_bundle_ref.to_canonical_dict(),
        "economics_bundle_digest": binding.economics_bundle_digest,
        "economics_authority_digest": binding.economics_authority_digest,
        "economics_bundle_manifest": economics_manifest.to_canonical_dict(),
        "economics_stream_manifests": tuple(
            value.to_canonical_dict() for value in economics_manifest.streams
        ),
        "coverage_start": economics_manifest.coverage_start.to_canonical_dict(),
        "coverage_end_exclusive": economics_manifest.coverage_end_exclusive.to_canonical_dict(),
        "instrument_catalog_hash": economics_manifest.instrument_catalog_hash,
        "source_fragment_digest": binding.source_fragment_digest,
        "source_projection_request_hash": economics.source_projection_request_hash,
        "source_projection_authority_ref": binding.source_projection_authority_ref.to_canonical_dict(),
        "source_projection_authority_content_hash": binding.source_projection_authority_content_hash,
        "source_profile_authority_envelope": economics.source_profile_authority_envelope.to_canonical_dict(),
        "source_profile_authority_ref": economics.authority_artifact_refs[0].to_canonical_dict(),
        "aggregate_trade_boundary_index_request_hash": source_profile[
            "aggregate_trade_boundary_index_request_hash"
        ],
        "aggregate_trade_boundary_index_result_digest": source_profile[
            "aggregate_trade_boundary_index_result_digest"
        ],
        "aggregate_trade_streamed_reconstruction_digest": source_profile[
            "aggregate_trade_streamed_reconstruction_digest"
        ],
        "aggregate_trade_capture_final_evidence": source_profile[
            "aggregate_trade_capture_final_evidence"
        ],
        "compiler_result_ref": binding.compiler_result_ref.to_canonical_dict(),
        "compiler_result_digest": binding.compiler_result_digest,
        "scope_ref": binding.scope_ref.to_canonical_dict(),
        "scope_digest": binding.scope_digest,
        "scope": scope,
        "compiler_source_projection_authority_ref": binding.source_projection_authority_ref.to_canonical_dict(),
        "compiler_source_projection_authority_content_hash": binding.source_projection_authority_content_hash,
        "recipe": recipe,
        "recipe_ref": binding.parameter_ref.to_canonical_dict(),
        "recipe_digest": binding.recipe_digest,
        "strategy_ref": binding.strategy_ref.to_canonical_dict(),
        "strategy_id": recipe["strategy_id"],
        "sleeve_id": recipe["sleeve_id"],
        "target_stream_key": binding.target_stream_key,
        "target_stream_digest": binding.target_stream_digest,
        "target_stream_manifest": target_manifest.to_canonical_dict(),
        "target_events": tuple(value.to_canonical_dict() for value in target_events),
        "target_events_digest": canonical_sha256(target_events),
    }
    if not _same(payload, expected):
        _fail("overlay authority binding")
    source_hash = canonical_sha256({"type": _OVERLAY_EVENT, "payload": payload})
    if (
        event.source_hash != source_hash
        or event.event_id != f"{_OVERLAY_EVENT}:{source_hash}"
        or event.revision_id
        != canonical_sha256(
            {"type": f"{_OVERLAY_EVENT}_revision", "source_hash": source_hash}
        )
    ):
        _fail("overlay authority identity")
    try:
        expected_overlay_key = _OVERLAY_PREFIX + canonical_sha256(
            {
                "economics_bundle_ref": binding.economics_bundle_ref,
                "economics_authority_digest": binding.economics_authority_digest,
                "compiler_result_ref": binding.compiler_result_ref,
                "scope_ref": binding.scope_ref,
                "target_stream_digest": binding.target_stream_digest,
            }
        )[7:]
        expected_overlay_manifest = MarketBundleManifest.build(
            bundle_key=expected_overlay_key,
            schema_version=4,
            coverage_start=economics_manifest.coverage_start,
            coverage_end_exclusive=economics_manifest.coverage_end_exclusive,
            instrument_catalog_hash=economics_manifest.instrument_catalog_hash,
            capabilities=tuple(
                sorted({value.capability for value in reader.manifest.streams})
            ),
            streams=reader.manifest.streams,
        )
    except (TypeError, ValueError) as error:
        _fail(f"overlay manifest: {error}")
    if (
        reader.manifest != expected_overlay_manifest
        or MarketBundleRef.from_manifest(expected_overlay_manifest)
        != binding.overlay_bundle_ref
    ):
        _fail("overlay manifest")


def _verify_overlay_binding(
    binding: KoruPremiumReaderBindingV2,
    economics: KoruPremiumEconomicsPublicationV4,
    compiled_target: KoruDirectionalTargetStreamV2,
) -> None:
    if (
        binding.economics_bundle_ref != economics.economics_bundle_ref
        or binding.economics_authority_digest != economics.economics_authority_digest
        or binding.source_projection_authority_ref
        != economics.source_projection_authority_ref
        or binding.source_fragment_digest != economics.source_fragment_digest
    ):
        _fail("overlay economics binding")
    _verify_overlay_event(binding, economics, compiled_target)


def _verify_overlay_set(
    reader_set: KoruPremiumReaderSetV2,
    economics: KoruPremiumEconomicsPublicationV4,
    source_projection: object,
) -> None:
    targets = _recompile_targets(source_projection, reader_set)
    for binding in reader_set.bindings:
        target = targets.get(binding.target_stream_key)
        if target is None:
            _fail("target stream")
        _verify_overlay_binding(binding, economics, target)


def publish_koru_premium_overlay_set_authority_v4(
    foundation: KoruPremiumPreflightAuthorityFoundationV2,
    *,
    economics: KoruPremiumEconomicsPublicationV4,
    reader_set: KoruPremiumReaderSetV2,
    source_projection: object,
) -> KoruPremiumOverlaySetPublicationV4:
    """Publish exactly one source-recompiled OverlayV4 authority set."""
    _require_admitted(
        economics, KoruPremiumEconomicsPublicationV4, "economics authority"
    )
    bindings = _bindings(reader_set)
    _verify_overlay_set(reader_set, economics, source_projection)
    payload = {
        "type": _OVERLAY_SET_TYPE,
        "schema_version": 4,
        "economics_artifact_ref": economics.artifact_ref.to_canonical_dict(),
        "bindings": bindings,
    }
    ref, entry = _publish_wrapper(
        foundation,
        log_name=KORU_PREMIUM_OVERLAY_SET_V4_LOG,
        artifact_type=_OVERLAY_SET_TYPE,
        schema_version=4,
        payload=payload,
    )
    return cast(
        KoruPremiumOverlaySetPublicationV4,
        _admit_v2(KoruPremiumOverlaySetPublicationV4(ref, entry, economics.artifact_ref, bindings)),
    )


def open_published_koru_premium_overlay_set_authority_v4(
    foundation: KoruPremiumPreflightAuthorityFoundationV2,
    *,
    artifact_ref: ArtifactRef,
    publication_entry_ref: LogEntryRef,
    repository_root: Path,
    economics: KoruPremiumEconomicsPublicationV4,
    source_projection: object,
) -> KoruPremiumOverlaySetPublicationV4:
    """Reopen all four OverlayV4 readers and rederive their exact semantics."""
    _require_admitted(
        economics, KoruPremiumEconomicsPublicationV4, "economics authority"
    )
    payload = _open_wrapper(
        foundation,
        ref=artifact_ref,
        entry=publication_entry_ref,
        log_name=KORU_PREMIUM_OVERLAY_SET_V4_LOG,
        artifact_type=_OVERLAY_SET_TYPE,
        schema_version=4,
    )
    if (
        set(payload) != {"type", "schema_version", "economics_artifact_ref", "bindings"}
        or payload.get("type") != _OVERLAY_SET_TYPE
        or payload.get("schema_version") != 4
        or _ref(payload["economics_artifact_ref"], "overlay economics")
        != economics.artifact_ref
        or type(payload["bindings"]) not in {tuple, list}
    ):
        _fail("overlay set payload")
    bindings = tuple(
        _binding_from_wire(value, repository_root)
        for value in cast(tuple[object, ...] | list[object], payload["bindings"])
    )
    try:
        reader_set = KoruPremiumReaderSetV2(bindings)
    except (TypeError, ValueError) as error:
        _fail(str(error))
    _verify_overlay_set(reader_set, economics, source_projection)
    return cast(
        KoruPremiumOverlaySetPublicationV4,
        _admit_v2(
            KoruPremiumOverlaySetPublicationV4(
                artifact_ref,
                publication_entry_ref,
                economics.artifact_ref,
                tuple(payload["bindings"]),
            )
        ),
    )


def publish_koru_premium_reader_set_authority_v2(
    foundation: KoruPremiumPreflightAuthorityFoundationV2,
    *,
    overlay_set: KoruPremiumOverlaySetPublicationV4,
    reader_set: KoruPremiumReaderSetV2,
) -> KoruPremiumReaderSetPublicationV2:
    """Publish sealed V2 reader identities after an admitted OverlayV4 reopen."""
    _require_admitted(
        overlay_set, KoruPremiumOverlaySetPublicationV4, "overlay set"
    )
    bindings = _bindings(reader_set)
    if not _same(bindings, overlay_set.bindings):
        _fail("reader set overlay binding")
    payload = {
        "type": _READER_SET_TYPE,
        "schema_version": 2,
        "overlay_set_artifact_ref": overlay_set.artifact_ref.to_canonical_dict(),
        "reader_set_digest": reader_set.reader_set_digest,
        "bindings": bindings,
    }
    ref, entry = _publish_wrapper(
        foundation,
        log_name=KORU_PREMIUM_READER_SET_V2_LOG,
        artifact_type=_READER_SET_TYPE,
        schema_version=2,
        payload=payload,
    )
    return cast(
        KoruPremiumReaderSetPublicationV2,
        _admit_v2(
            KoruPremiumReaderSetPublicationV2(
                ref, entry, overlay_set.artifact_ref, reader_set
            )
        ),
    )


def open_published_koru_premium_reader_set_authority_v2(
    foundation: KoruPremiumPreflightAuthorityFoundationV2,
    *,
    artifact_ref: ArtifactRef,
    publication_entry_ref: LogEntryRef,
    repository_root: Path,
    overlay_set: KoruPremiumOverlaySetPublicationV4,
) -> KoruPremiumReaderSetPublicationV2:
    """Recreate V2 readers only by repository-open against admitted overlays."""
    _require_admitted(
        overlay_set, KoruPremiumOverlaySetPublicationV4, "overlay set"
    )
    payload = _open_wrapper(
        foundation,
        ref=artifact_ref,
        entry=publication_entry_ref,
        log_name=KORU_PREMIUM_READER_SET_V2_LOG,
        artifact_type=_READER_SET_TYPE,
        schema_version=2,
    )
    if (
        set(payload)
        != {
            "type",
            "schema_version",
            "overlay_set_artifact_ref",
            "reader_set_digest",
            "bindings",
        }
        or payload.get("type") != _READER_SET_TYPE
        or payload.get("schema_version") != 2
        or _ref(payload["overlay_set_artifact_ref"], "reader set overlay")
        != overlay_set.artifact_ref
        or not _same(payload["bindings"], overlay_set.bindings)
        or type(payload["bindings"]) not in {tuple, list}
    ):
        _fail("reader set payload")
    reader_set = KoruPremiumReaderSetV2(
        tuple(
            _binding_from_wire(value, repository_root)
            for value in cast(tuple[object, ...] | list[object], payload["bindings"])
        )
    )
    if reader_set.reader_set_digest != _digest(
        payload["reader_set_digest"], "reader set digest"
    ):
        _fail("reader set digest")
    return cast(
        KoruPremiumReaderSetPublicationV2,
        _admit_v2(
            KoruPremiumReaderSetPublicationV2(
                artifact_ref, publication_entry_ref, overlay_set.artifact_ref, reader_set
            )
        ),
    )


@dataclass(frozen=True, slots=True)
class KoruPremiumPreflightAuthorityV2:
    """The admitted V2 manifest: raw, boundary, source, economics, overlays, readers."""

    raw_snapshot_fact: RawBlobSnapshotPublicationFact
    raw_snapshot_publication_entry_ref: LogEntryRef
    boundary_index_fact: BoundaryIndexPublicationFact
    boundary_index_publication_entry_ref: LogEntryRef
    source_projection_fact: SourceProjectionPublicationFactV3
    source_projection_publication_entry_ref: LogEntryRef
    economics: KoruPremiumEconomicsPublicationV4
    overlay_set: KoruPremiumOverlaySetPublicationV4
    reader_set: KoruPremiumReaderSetPublicationV2
    _admission_token: object | None = field(
        init=False, default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if (
            type(self.raw_snapshot_fact) is not RawBlobSnapshotPublicationFact
            or type(self.boundary_index_fact) is not BoundaryIndexPublicationFact
            or type(self.source_projection_fact) is not SourceProjectionPublicationFactV3
        ):
            _fail("preflight predecessors")
        _entry_value(
            self.raw_snapshot_publication_entry_ref, "research.raw_snapshots.v1"
        )
        _entry_value(
            self.boundary_index_publication_entry_ref, "research.boundary_indexes.v1"
        )
        _entry_value(
            self.source_projection_publication_entry_ref,
            "research.source_projections.v3",
        )
        _require_admitted(
            self.economics, KoruPremiumEconomicsPublicationV4, "economics authority"
        )
        _require_admitted(
            self.overlay_set, KoruPremiumOverlaySetPublicationV4, "overlay set"
        )
        _require_admitted(
            self.reader_set, KoruPremiumReaderSetPublicationV2, "reader set"
        )
        if (
            self.source_projection_fact.authority_ref
            != self.economics.source_projection_authority_ref
            or self.source_projection_fact.source_fragment_digest
            != self.economics.source_fragment_digest
            or self.source_projection_fact.source_request_hash
            != self.economics.source_projection_request_hash
            or self.overlay_set.economics_artifact_ref != self.economics.artifact_ref
            or self.reader_set.overlay_set_artifact_ref != self.overlay_set.artifact_ref
        ):
            _fail("preflight spine binding")

    @property
    def _admitted(self) -> bool:
        return _is_admitted_v2(self)

    def _canonical_dict(self) -> dict[str, object]:
        return {
            "type": _AUTHORITY_TYPE,
            "schema_version": 2,
            "raw_snapshot_fact": self.raw_snapshot_fact.to_canonical_dict(),
            "raw_snapshot_publication_entry_ref": _entry_wire(
                self.raw_snapshot_publication_entry_ref
            ),
            "boundary_index_fact": self.boundary_index_fact.to_canonical_dict(),
            "boundary_index_publication_entry_ref": _entry_wire(
                self.boundary_index_publication_entry_ref
            ),
            "source_projection_fact": self.source_projection_fact.to_canonical_dict(),
            "source_projection_publication_entry_ref": _entry_wire(
                self.source_projection_publication_entry_ref
            ),
            "economics": self.economics._canonical_dict(),
            "overlay_set": self.overlay_set._canonical_dict(),
            "reader_set": self.reader_set._canonical_dict(),
        }

    def to_canonical_dict(self) -> dict[str, object]:
        if not self._admitted:
            _fail("unpublished preflight authority")
        return self._canonical_dict()


@dataclass(frozen=True, slots=True)
class KoruPremiumPreflightAuthorityPublicationV2:
    authority_ref: ArtifactRef
    publication_entry_ref: LogEntryRef
    authority: KoruPremiumPreflightAuthorityV2

    def __post_init__(self) -> None:
        _artifact_ref(
            self.authority_ref,
            "preflight authority ref",
            artifact_type=_AUTHORITY_TYPE,
            schema_version=2,
        )
        _entry_value(
            self.publication_entry_ref, KORU_PREMIUM_PREFLIGHT_AUTHORITY_V2_LOG
        )
        _require_admitted(
            self.authority, KoruPremiumPreflightAuthorityV2, "preflight authority"
        )


def publish_koru_premium_preflight_authority_v2(
    foundation: KoruPremiumPreflightAuthorityFoundationV2,
    *,
    raw_snapshot_fact: RawBlobSnapshotPublicationFact,
    raw_snapshot_publication_entry_ref: LogEntryRef,
    boundary_index_fact: BoundaryIndexPublicationFact,
    boundary_index_publication_entry_ref: LogEntryRef,
    source_projection_fact: SourceProjectionPublicationFactV3,
    source_projection_publication_entry_ref: LogEntryRef,
    economics: KoruTradifiEconomicsBundleV4,
    reader_set: KoruPremiumReaderSetV2,
) -> KoruPremiumPreflightAuthorityPublicationV2:
    """Publish the complete immutable KORU V2 spine after every predecessor opens."""
    source_projection = _verify_inputs(
        foundation,
        raw_snapshot_fact,
        raw_snapshot_publication_entry_ref,
        boundary_index_fact,
        boundary_index_publication_entry_ref,
        source_projection_fact,
        source_projection_publication_entry_ref,
    )
    economics_fact = publish_koru_premium_economics_authority_v4(
        foundation,
        economics=economics,
        source=source_projection_fact,
        source_projection=source_projection,
    )
    overlays = publish_koru_premium_overlay_set_authority_v4(
        foundation,
        economics=economics_fact,
        reader_set=reader_set,
        source_projection=source_projection,
    )
    readers = publish_koru_premium_reader_set_authority_v2(
        foundation, overlay_set=overlays, reader_set=reader_set
    )
    authority = KoruPremiumPreflightAuthorityV2(
        raw_snapshot_fact,
        raw_snapshot_publication_entry_ref,
        boundary_index_fact,
        boundary_index_publication_entry_ref,
        source_projection_fact,
        source_projection_publication_entry_ref,
        economics_fact,
        overlays,
        readers,
    )
    ref, entry = _publish_wrapper(
        foundation,
        log_name=KORU_PREMIUM_PREFLIGHT_AUTHORITY_V2_LOG,
        artifact_type=_AUTHORITY_TYPE,
        schema_version=2,
        payload=authority._canonical_dict(),
    )
    admitted = cast(KoruPremiumPreflightAuthorityV2, _admit_v2(authority))
    return KoruPremiumPreflightAuthorityPublicationV2(ref, entry, admitted)


def open_published_koru_premium_preflight_authority_v2(
    foundation: KoruPremiumPreflightAuthorityFoundationV2,
    *,
    authority_ref: ArtifactRef,
    publication_entry_ref: LogEntryRef,
    repository_root: Path,
) -> KoruPremiumPreflightAuthorityV2:
    """Fail closed unless every Foundation fact and all four V4 readers reopen exactly."""
    payload = _open_wrapper(
        foundation,
        ref=authority_ref,
        entry=publication_entry_ref,
        log_name=KORU_PREMIUM_PREFLIGHT_AUTHORITY_V2_LOG,
        artifact_type=_AUTHORITY_TYPE,
        schema_version=2,
    )
    required = {
        "type",
        "schema_version",
        "raw_snapshot_fact",
        "raw_snapshot_publication_entry_ref",
        "boundary_index_fact",
        "boundary_index_publication_entry_ref",
        "source_projection_fact",
        "source_projection_publication_entry_ref",
        "economics",
        "overlay_set",
        "reader_set",
    }
    if (
        set(payload) != required
        or payload.get("type") != _AUTHORITY_TYPE
        or payload.get("schema_version") != 2
    ):
        _fail("authority payload")
    raw = _raw_fact(payload["raw_snapshot_fact"])
    raw_entry = _entry(
        payload["raw_snapshot_publication_entry_ref"], "research.raw_snapshots.v1"
    )
    boundary = _boundary_fact(payload["boundary_index_fact"])
    boundary_entry = _entry(
        payload["boundary_index_publication_entry_ref"], "research.boundary_indexes.v1"
    )
    source = _source_fact(payload["source_projection_fact"])
    source_entry = _entry(
        payload["source_projection_publication_entry_ref"],
        "research.source_projections.v3",
    )
    source_projection = _verify_inputs(
        foundation, raw, raw_entry, boundary, boundary_entry, source, source_entry
    )
    economics = open_published_koru_premium_economics_authority_v4(
        foundation,
        artifact_ref=_ref(_mapping(payload["economics"], "economics fact")["artifact_ref"], "economics ref"),
        publication_entry_ref=_entry(
            _mapping(payload["economics"], "economics fact")["publication_entry_ref"],
            KORU_PREMIUM_ECONOMICS_V4_LOG,
        ),
        source=source,
        source_projection=source_projection,
    )
    if not _same(payload["economics"], economics.to_canonical_dict()):
        _fail("economics fact")
    overlays = open_published_koru_premium_overlay_set_authority_v4(
        foundation,
        artifact_ref=_ref(
            _mapping(payload["overlay_set"], "overlay set fact")["artifact_ref"],
            "overlay set ref",
        ),
        publication_entry_ref=_entry(
            _mapping(payload["overlay_set"], "overlay set fact")[
                "publication_entry_ref"
            ],
            KORU_PREMIUM_OVERLAY_SET_V4_LOG,
        ),
        repository_root=repository_root,
        economics=economics,
        source_projection=source_projection,
    )
    if not _same(payload["overlay_set"], overlays.to_canonical_dict()):
        _fail("overlay set fact")
    readers = open_published_koru_premium_reader_set_authority_v2(
        foundation,
        artifact_ref=_ref(
            _mapping(payload["reader_set"], "reader set fact")["artifact_ref"],
            "reader set ref",
        ),
        publication_entry_ref=_entry(
            _mapping(payload["reader_set"], "reader set fact")[
                "publication_entry_ref"
            ],
            KORU_PREMIUM_READER_SET_V2_LOG,
        ),
        repository_root=repository_root,
        overlay_set=overlays,
    )
    if not _same(payload["reader_set"], readers.to_canonical_dict()):
        _fail("reader set fact")
    return cast(
        KoruPremiumPreflightAuthorityV2,
        _admit_v2(
            KoruPremiumPreflightAuthorityV2(
                raw,
                raw_entry,
                boundary,
                boundary_entry,
                source,
                source_entry,
                economics,
                overlays,
                readers,
            )
        ),
    )


__all__ = [
    "KORU_PREMIUM_ECONOMICS_V4_LOG",
    "KORU_PREMIUM_OVERLAY_SET_V4_LOG",
    "KORU_PREMIUM_PREFLIGHT_AUTHORITY_V2_LOG",
    "KORU_PREMIUM_READER_SET_V2_LOG",
    "KoruPremiumEconomicsPublicationV4",
    "KoruPremiumOverlaySetPublicationV4",
    "KoruPremiumPreflightAuthorityErrorV2",
    "KoruPremiumPreflightAuthorityFoundationV2",
    "KoruPremiumPreflightAuthorityPublicationV2",
    "KoruPremiumPreflightAuthorityV2",
    "KoruPremiumReaderSetPublicationV2",
    "open_published_koru_premium_economics_authority_v4",
    "open_published_koru_premium_overlay_set_authority_v4",
    "open_published_koru_premium_preflight_authority_v2",
    "open_published_koru_premium_reader_set_authority_v2",
    "publish_koru_premium_economics_authority_v4",
    "publish_koru_premium_overlay_set_authority_v4",
    "publish_koru_premium_preflight_authority_v2",
    "publish_koru_premium_reader_set_authority_v2",
]
