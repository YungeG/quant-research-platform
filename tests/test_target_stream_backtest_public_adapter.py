from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import crypto_quant_backtest as backtest
from crypto_quant_domain import (
    ArtifactEnvelope,
    ArtifactReadResult,
    ArtifactRef,
    CurrencyId,
    ExecutionStyle,
    InstrumentCatalog,
    InstrumentDefinition,
    InstrumentId,
    InstrumentType,
    Money,
    PositionEffect,
    Price,
    PricePurpose,
    Scale,
    SourceSequence,
    StrategySleeveId,
    TimelinePhase,
    TimeInForce,
    UtcInstant,
    VenueId,
    canonical_bytes,
    canonical_sha256,
)
from crypto_quant_market_data import InMemoryMarketBundleReader, MarketEvent
from crypto_quant_research import TrialExecution
from crypto_quant_trading import (
    MarkObservation,
    OrderCapabilityKey,
    OrderCapabilitySet,
    OrderStyleCapability,
    PriceConstraintShape,
    QuantityLattice,
)

_VENUE = VenueId("synthetic")
_USD = CurrencyId("USD")
_INSTRUMENT = InstrumentId(_VENUE, "cash:btc-usd")


def _plain(value: object) -> object:
    return json.loads(canonical_bytes(value))


class _Cas:
    def __init__(self) -> None:
        self.values: dict[ArtifactRef, ArtifactEnvelope] = {}

    def put(self, *, envelope: ArtifactEnvelope) -> ArtifactRef:
        ref = ArtifactRef.from_envelope(envelope)
        self.values[ref] = envelope
        return ref

    def read(self, *, ref: ArtifactRef) -> ArtifactReadResult:
        envelope = self.values[ref]
        source = canonical_bytes(envelope)
        return ArtifactReadResult(envelope, None, source, canonical_sha256(envelope))


def _catalog() -> InstrumentCatalog:
    btc = CurrencyId("BTC")
    return InstrumentCatalog(
        currencies=(btc, _USD),
        instruments=(
            InstrumentDefinition(
                _INSTRUMENT, InstrumentType.SPOT, btc, _USD, _USD
            ),
        ),
        symbol_timelines=(),
    )


def _target_event() -> MarketEvent:
    return MarketEvent(
        event_id="target-100",
        stream_key="targets",
        event_type=backtest.TARGET_STREAM_EVENT_TYPE,
        capability=backtest.TARGET_STREAM_CAPABILITY,
        instrument_id=None,
        event_time=UtcInstant(100),
        available_time=UtcInstant(100),
        phase=TimelinePhase(30, "strategy_decision"),
        source_sequence=SourceSequence(1),
        revision_id="rev-1",
        supersedes_revision_id=None,
        source_key="targets.v1",
        source_hash="sha256:" + "1" * 64,
        payload={
            "schema_version": 1,
            "candidate": {
                "schema_version": 1,
                "strategy_id": "trend-v1",
                "sleeve_id": "trend.primary",
                "decision_time": 100,
                "observed_through": 99,
                "effective_time": 100,
                "expires_at": 250,
                "targets": [
                    {
                        "instrument_id": {
                            "venue": _VENUE.value,
                            "stable_key": _INSTRUMENT.stable_key,
                        },
                        "value": "0.5",
                    }
                ],
                "confidence": "1",
                "reason": "public adapter smoke",
                "evidence": {"source": "fixed"},
            },
        },
    )


def _bar_event() -> MarketEvent:
    return MarketEvent(
        event_id="bar-200",
        stream_key="bars.open",
        event_type=backtest.BAR_OPEN_EVENT_TYPE,
        capability=backtest.BAR_OPEN_CAPABILITY,
        instrument_id=_INSTRUMENT,
        event_time=UtcInstant(200),
        available_time=UtcInstant(200),
        phase=TimelinePhase(60, "bar_open"),
        source_sequence=SourceSequence(2),
        revision_id="rev-1",
        supersedes_revision_id=None,
        source_key="bars.open.v1",
        source_hash="sha256:" + "2" * 64,
        payload={
            "schema_version": 1,
            "bar_kind": "real",
            "open_price": {"units": 10_000, "scale": 2, "quote_currency": "USD"},
        },
    )


def _manifest() -> backtest.BuildArtifactManifest:
    roles = (
        (backtest.BuildArtifactRole.DECISION_SOURCE, "target-source", "1"),
        (backtest.BuildArtifactRole.TRADING_DOMAIN, "domain", "2"),
        (backtest.BuildArtifactRole.TRADING_KERNEL, "trading", "3"),
        (backtest.BuildArtifactRole.MARKET_DATA_CONTRACTS, "market", "4"),
        (backtest.BuildArtifactRole.BACKTEST_RUNTIME, "backtest", "5"),
    )
    return backtest.BuildArtifactManifest(
        schema_version=1,
        build_key="research.target-adapter.v1",
        artifacts=tuple(
            backtest.BuildArtifactRef(
                role,
                key,
                "0.1.0",
                backtest.ArtifactInstallMode.WHEEL,
                backtest.SourceTreeState.CLEAN,
                "sha256:" + marker * 64,
                None,
            )
            for role, key, marker in roles
        ),
        dependency_lock_hash="sha256:" + "6" * 64,
        runtime_libraries=(
            backtest.RuntimeLibraryRef(
                "python", "3.13.5", "sha256:" + "7" * 64
            ),
        ),
        container_image_digest=None,
        provenance=backtest.BuildProvenance(
            "f73d068d24ffb7ecc0b7d78194fcbc96908d3c04",
            "public-adapter",
            "/workspace/research",
            UtcInstant(1_000),
        ),
    )


def _lattice() -> QuantityLattice:
    return QuantityLattice.create(
        instrument_id=_INSTRUMENT,
        lattice_key="lattice.v1",
        lattice_version=1,
        atomic_scale=Scale(3),
        step_units=1,
        buy_lot_units=1,
        sell_lot_units=1,
        min_quantity_units=1,
        min_notional=Money(100, Scale(2), "USD"),
        odd_lot_close_permitted=False,
    )


def _capabilities() -> OrderCapabilitySet:
    return OrderCapabilitySet.create(
        capability_set_key="capabilities.v1",
        capability_set_version=1,
        style_capabilities=(
            OrderStyleCapability(
                ExecutionStyle.MARKET,
                (PriceConstraintShape.NONE,),
                (TimeInForce.DAY,),
            ),
        ),
        supports_reduce_only=True,
        supported_position_effects=(
            PositionEffect.AUTO,
            PositionEffect.OPEN,
            PositionEffect.CLOSE,
        ),
        declared_capability_keys=tuple(value.value for value in OrderCapabilityKey),
    )


def _mark(units: int, at: int, source: str) -> MarkObservation:
    return MarkObservation(
        instrument_id=_INSTRUMENT,
        quote_currency_id=_USD,
        price_purpose=PricePurpose.VALUATION,
        price=Price(units, Scale(2), str(_INSTRUMENT), "USD"),
        observed_at=UtcInstant(at),
        available_at=UtcInstant(at),
        stream_id=f"marks.{source}",
        source_event_id=f"mark-{source}",
        revision_id="rev-1",
    )


class _PublicTargetAdapter:
    def __init__(self, root: Path) -> None:
        self.cas = _Cas()
        self.repository = backtest.BacktestTargetStreamRepository(
            reader=self.cas, publisher=self.cas
        )
        self.root = root
        self.stream = backtest.PrecomputedTargetStream("targets", (_target_event(),))
        bar = _bar_event()
        self.market_reader = InMemoryMarketBundleReader.build(
            bundle_key="market-only-v1",
            schema_version=1,
            coverage_start=UtcInstant(0),
            coverage_end_exclusive=UtcInstant(400),
            instrument_catalog_hash=canonical_sha256(_catalog()),
            capabilities=(bar.capability,),
            streams={"bars.open": (bar,)},
        )

    def publish_target(self, context: dict[str, object], stream: dict[str, object]) -> object:
        assert canonical_bytes(stream) == canonical_bytes(self.stream)
        return _plain(
            self.repository.publish(
                ArtifactRef(
                    context["artifact_type"],
                    context["schema_version"],
                    context["content_hash"],
                ),
                self.stream,
            )
        )

    def load_target(self, ref: dict[str, object]) -> dict[str, object]:
        artifact = ref["artifact_ref"]
        loaded = self.repository.load(
            backtest.BacktestTargetStreamRef(
                ArtifactRef(
                    artifact["artifact_type"],
                    artifact["schema_version"],
                    artifact["content_hash"],
                )
            )
        )
        return {
            "ref": _plain(loaded.ref),
            "producer_context_ref": _plain(loaded.producer_context_ref),
            "target_stream": _plain(loaded.target_stream),
            "digest": loaded.digest,
        }

    def prepare_trials(self, trials: tuple[object, ...], target_ref: dict[str, object]):
        artifact = target_ref["artifact_ref"]
        prepared = backtest.prepare_cash_target_stream_backtest(
            request_intent=backtest.CashDevelopmentRequestIntent(
                1,
                "research:target-adapter",
                backtest.TimelineWindow(UtcInstant(0), UtcInstant(90), UtcInstant(300)),
                "account:primary",
                _USD,
                7,
            ),
            provider_inputs=backtest.CashDevelopmentProviderInputs(
                1,
                _manifest(),
                _catalog(),
                "trend-v1",
                StrategySleeveId("trend.primary"),
                Money(100_000, Scale(2), "USD"),
                _lattice(),
                _mark(10_000, 100, "decision"),
                _mark(8_000, 299, "final"),
                _capabilities(),
            ),
            target_stream_ref=backtest.BacktestTargetStreamRef(
                ArtifactRef(
                    artifact["artifact_type"],
                    artifact["schema_version"],
                    artifact["content_hash"],
                )
            ),
            artifact_reader=self.cas,
            artifact_publisher=self.cas,
            market_reader=self.market_reader,
            publication_root=self.root,
        )
        return (
            TrialExecution(
                trials[0].ref,
                {"binding": trials[0].ref},
                _plain(prepared.request_ref),
            ),
        )


def test_public_backtest_target_repository_and_preparation_adapter(tmp_path: Path) -> None:
    adapter = _PublicTargetAdapter(tmp_path)
    context = ArtifactRef(
        "trial_declaration", 1, "sha256:" + "a" * 64
    ).to_canonical_dict()
    target_ref = adapter.publish_target(context, _plain(adapter.stream))
    loaded = adapter.load_target(target_ref)
    trial = SimpleNamespace(ref="rp-core:trial_declaration@1:sha256:" + "b" * 64)
    executions = adapter.prepare_trials((trial,), target_ref)

    assert loaded["ref"] == target_ref
    assert loaded["producer_context_ref"] == context
    assert loaded["digest"] == canonical_sha256(adapter.stream)
    assert len(loaded["target_stream"]["events"]) == 1
    assert len(executions) == 1
    assert executions[0].trial_declaration_ref == trial.ref
