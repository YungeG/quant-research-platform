"""Staged real owner-log mechanics with synthetic declarations; no market reads."""
from __future__ import annotations

import json
from dataclasses import replace

import pytest
from crypto_quant_domain import ArtifactRef, canonical_bytes, canonical_sha256
from crypto_quant_foundation import FoundationFailure, LocalFoundation
from crypto_quant_research import (
    ExperimentParameterCombination,
    PublishedStrategyCandidate,
    execute_target_experiment,
    preregister_target_experiment,
)
from crypto_quant_validation import SampleConsumptionLedger

from .test_target_stream_research import (
    ARTIFACT_LOG,
    EXECUTION_LOG,
    RECEIVED_AT,
    SAMPLE_LOG,
    _Backtest,
    _inputs,
    _Materializer,
)


def _two_inputs():
    inputs = _inputs()
    spec = replace(inputs.experiment_spec, parameter_combinations=(
        ExperimentParameterCombination((("arm", "A"),)),
        ExperimentParameterCombination((("arm", "B"),))), budget={"max_trials": 2})
    return replace(inputs, experiment_spec=spec)


def _payload(foundation, ref):
    return json.loads(foundation.read(ref=ref).source_bytes)["payload"]


def test_real_trial_publication_precedes_discovery_append_without_any_execution(tmp_path, monkeypatch):
    foundation = LocalFoundation(tmp_path / "foundation", clock=lambda: RECEIVED_AT)
    inputs = _two_inputs()
    original_reserve = SampleConsumptionLedger.reserve
    observed = []
    def reserve(ledger, record, producer):
        publications = foundation.entries(ARTIFACT_LOG)
        assert any(ArtifactRef.from_envelope(
            foundation.read(ref=producer).envelope) == producer for _ in publications)
        assert _payload(foundation, producer)["experiment_ref"]["artifact_type"] == "experiment_spec"
        observed.append(producer)
        return original_reserve(ledger, record, producer)
    monkeypatch.setattr(SampleConsumptionLedger, "reserve", reserve)
    def forbidden(*args, **kwargs):
        pytest.fail("discovery preregistration may not reserve/assess/freeze holdout")
    monkeypatch.setattr(SampleConsumptionLedger, "freeze_snapshot", forbidden)
    monkeypatch.setattr(SampleConsumptionLedger, "assess_holdout", forbidden)
    registration = preregister_target_experiment(inputs, foundation)
    assert observed == list(registration.trial_declaration_refs)
    assert len(registration.trial_declaration_refs) == len(registration.target_build_task_refs) == 2
    samples = foundation.entries(SAMPLE_LOG)
    assert len(samples) == 2 and foundation.entries(EXECUTION_LOG) == ()
    artifacts = foundation.entries(ARTIFACT_LOG)
    for trial_ref, sample, receipt in zip(registration.trial_declaration_refs, samples,
                                          registration.discovery_reservation_refs, strict=True):
        publication = next(e for e in artifacts if json.loads(e.payload)["artifact_type"] == "trial_declaration"
            and ArtifactRef.from_envelope(foundation.read(ref=trial_ref).envelope).content_hash == json.loads(e.payload)["content_hash"])
        assert publication.ledger_sequence < sample.ledger_sequence
        raw = json.loads(sample.payload)["payload"]
        assert raw["producer_ref"] == trial_ref.to_canonical_dict()
        assert raw["record"]["purpose"] == "discovery"
        assert raw["record"]["consumer_id"] == canonical_sha256(("sample-consumer-v1", trial_ref))
        assert raw["record"]["consumed_at"] == inputs.reservation_at
        assert sample.entry_ref == receipt
    wire = registration.to_canonical_dict()
    assert not wire["operation_market_read_performed"] and not wire["operation_execution_performed"]
    assert not wire["holdout_reserved"] and not wire["untouched_holdout_established"]
    assert wire["validation_report"] is None


def test_registration_retry_reuses_actual_refs_receipts_and_governance_age(tmp_path):
    foundation = LocalFoundation(tmp_path / "foundation", clock=lambda: RECEIVED_AT)
    first = preregister_target_experiment(_two_inputs(), foundation)
    before = (foundation.entries(ARTIFACT_LOG), foundation.entries(SAMPLE_LOG))
    replay = preregister_target_experiment(_two_inputs(), foundation)
    assert canonical_bytes(first) == canonical_bytes(replay)
    assert before == (foundation.entries(ARTIFACT_LOG), foundation.entries(SAMPLE_LOG))


def test_reservation_time_cannot_be_refreshed_for_identical_trial(tmp_path):
    foundation = LocalFoundation(tmp_path / "foundation", clock=lambda: RECEIVED_AT)
    inputs = _inputs()
    preregister_target_experiment(inputs, foundation)
    before = foundation.entries(SAMPLE_LOG)
    with pytest.raises(FoundationFailure) as error:
        preregister_target_experiment(replace(inputs, reservation_at=RECEIVED_AT), foundation)
    assert error.value.code == "LOG_CONFLICT"
    assert foundation.entries(SAMPLE_LOG) == before


def test_partial_append_failure_preserves_owner_facts_and_can_retry_identical_inputs(tmp_path, monkeypatch):
    foundation = LocalFoundation(tmp_path / "foundation", clock=lambda: RECEIVED_AT)
    original = SampleConsumptionLedger.reserve
    def reserve(ledger, record, producer):
        if len(foundation.entries(SAMPLE_LOG)) == 1:
            raise RuntimeError("second append failed")
        return original(ledger, record, producer)
    monkeypatch.setattr(SampleConsumptionLedger, "reserve", reserve)
    with pytest.raises(RuntimeError, match="second append failed"):
        preregister_target_experiment(_two_inputs(), foundation)
    assert len(foundation.entries(SAMPLE_LOG)) == 1
    assert foundation.entries(EXECUTION_LOG) == ()
    monkeypatch.setattr(SampleConsumptionLedger, "reserve", original)
    replay = preregister_target_experiment(_two_inputs(), foundation)
    assert len(replay.discovery_reservation_refs) == 2
    assert len(foundation.entries(SAMPLE_LOG)) == 2


@pytest.mark.parametrize("bad", ["inputs", "foundation", "grade"])
def test_invalid_stage_inputs_fail_before_any_publication(tmp_path, bad):
    path = tmp_path / "not-created"
    foundation = LocalFoundation(path, clock=lambda: RECEIVED_AT)
    inputs = _inputs()
    if bad == "inputs":
        inputs = object()
    elif bad == "foundation":
        foundation = object()
    else:
        inputs = replace(inputs, selection_policy=replace(
            inputs.selection_policy, accepted_backtest_grades=("decision_grade",)))
    with pytest.raises((TypeError, ValueError)):
        preregister_target_experiment(inputs, foundation)
    assert not path.exists()


def test_registered_source_can_continue_existing_target_execution_but_never_reregister_after_start(tmp_path):
    foundation = LocalFoundation(tmp_path / "foundation", clock=lambda: RECEIVED_AT)
    inputs = _inputs()
    registered = preregister_target_experiment(inputs, foundation)
    materializer, backtest = _Materializer(foundation), _Backtest()
    candidate = execute_target_experiment(inputs, foundation, SampleConsumptionLedger(foundation), materializer, backtest)
    assert isinstance(candidate, PublishedStrategyCandidate)
    assert materializer.calls == backtest.economic_run_calls == 1
    assert foundation.entries(SAMPLE_LOG)[0].entry_ref == registered.discovery_reservation_refs[0]
    before = foundation.entries(SAMPLE_LOG)
    with pytest.raises(ValueError, match="no execution entries"):
        preregister_target_experiment(inputs, foundation)
    assert foundation.entries(SAMPLE_LOG) == before
