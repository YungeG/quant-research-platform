"""Public capture-bound genuine Target runtime, entirely synthetic and source-free."""
from dataclasses import replace

import pytest
from crypto_quant_domain import ArtifactEnvelope, ArtifactRef, canonical_bytes, canonical_sha256
from crypto_quant_foundation import LocalFoundation
from crypto_quant_research import (
    DataSlice, ExperimentParameterCombination, InputQualificationDeclarationV1, InputQualificationReadTaskV1,
    InputQualificationCaptureBindingV1, PublishedStrategyCandidate,
    publish_input_qualification_declaration, publish_input_qualification_read_task,
    publish_input_qualification_capture_binding, preregister_input_capture_target_experiment,
    execute_input_capture_target_experiment, preregister_target_experiment, execute_target_experiment,
)
from crypto_quant_validation import (
    InputQualificationError, InputQualificationReadBindingV1, InputQualificationVerifierV1,
    SampleConsumptionLedger,
)
from .test_target_stream_research import _inputs, _Materializer, _Backtest, RECEIVED_AT

H = canonical_sha256("synthetic-identity")
START = "2026-01-01T00:00:00.000000Z"
END = "2026-02-01T00:00:00.000000Z"


def capture_fixture(tmp_path, *, scopes=None, metadata=None):
    foundation = LocalFoundation(tmp_path / "foundation", clock=lambda: RECEIVED_AT)
    ledger = SampleConsumptionLedger(foundation)
    verifier = InputQualificationVerifierV1(foundation, ledger)
    declaration = InputQualificationDeclarationV1("synthetic", H, H, H, H, H,
        scopes or (("prices", START, END),))
    published = publish_input_qualification_declaration(declaration, foundation)
    revision = "local-capture:" + canonical_sha256(("input-qualification-capture-v1", published.ref))
    proofs, receipts, tasks = [], [], []
    for scope, start, end in declaration.source_intervals:
        for phase in ("QUALIFY", "CAPTURE"):
            task = InputQualificationReadTaskV1(published.ref, scope, phase, None if phase == "QUALIFY" else H)
            task_pub = publish_input_qualification_read_task(task, foundation, verifier)
            receipt = verifier.reserve(published.publication, task_pub.publication, RECEIVED_AT,
                InputQualificationReadBindingV1(revision, scope, phase, H, H, H, start, end))
            tasks.append(task_pub)
            receipts.append(receipt)
            if phase == "CAPTURE":
                proofs.append((scope, task_pub.ref, receipt.reservation_entry_ref))
    manifest = {"type": "market_bundle_manifest", "bundle_key": "synthetic", "schema_version": 1,
        "coverage_start": START, "coverage_end_exclusive": END, "instrument_catalog_hash": H,
        "capabilities": (), "streams": (), "content_hash": H}
    ref = {"type": "market_bundle_ref", "bundle_key": "synthetic", "manifest_hash": canonical_sha256(manifest)}
    if metadata is not None:
        ref, manifest = metadata
    binding = InputQualificationCaptureBindingV1(published.ref, tuple(proofs), H, ref, manifest)
    binding_pub = publish_input_qualification_capture_binding(binding, foundation, verifier)
    data = DataSlice(ref, revision, START, END)
    inputs = _inputs()
    inputs = replace(inputs, experiment_spec=replace(inputs.experiment_spec, data_slices=(data,)), reservation_at=RECEIVED_AT)
    return foundation, ledger, verifier, published, tasks, receipts, binding, binding_pub, inputs


def execute(f, ledger, verifier, pub, inputs, materializer, backtest):
    return execute_input_capture_target_experiment(inputs, pub.publication, f, ledger, verifier, materializer, backtest)


def no_reads(materializer, backtest):
    assert materializer.calls == backtest.store_calls == backtest.load_target_calls == backtest.prepare_calls == backtest.run_calls == 0


def test_T8_genuine_vertical_and_completed_execution_replay(tmp_path):
    f, ledger, verifier, declaration, tasks, receipts, binding, pub, inputs = capture_fixture(tmp_path)
    view = verifier.verify_capture(pub.publication)
    assert view.capture_revision == inputs.experiment_spec.data_slices[0].dataset_revision
    registration = preregister_input_capture_target_experiment(inputs, pub.publication, f, verifier)
    for trial in registration.trial_declaration_refs:
        verifier.require_trial_binding(pub.publication, trial)
    materializer, backtest = _Materializer(f), _Backtest()
    result = execute(f, ledger, verifier, pub, inputs, materializer, backtest)
    assert isinstance(result, PublishedStrategyCandidate)
    assert materializer.calls == backtest.economic_run_calls == 1
    before = tuple(f.entries(name) for name in ("research.artifacts.v1", "research.execution.v1", "validation.sample-consumption.v1"))
    replay = execute(f, ledger, verifier, pub, inputs, materializer, backtest)
    assert result == replay
    assert f.read(ref=result.strategy_candidate_ref).source_bytes == f.read(ref=replay.strategy_candidate_ref).source_bytes
    assert materializer.calls == backtest.economic_run_calls == 1
    assert before == tuple(f.entries(name) for name in ("research.artifacts.v1", "research.execution.v1", "validation.sample-consumption.v1"))
    with pytest.raises(ValueError, match="no execution entries"):
        preregister_input_capture_target_experiment(inputs, pub.publication, f, verifier)


@pytest.mark.parametrize("change", ["revision", "bundle", "range"])
@pytest.mark.parametrize("operation", ["preregister", "execute"])
def test_T7_first_C_B_mismatch_sentinel_before_any_reader(tmp_path, change, operation):
    f, ledger, verifier, _, _, _, _, pub, inputs = capture_fixture(tmp_path)
    data = inputs.experiment_spec.data_slices[0]
    if change == "revision":
        data = replace(data, dataset_revision="different-nonempty-revision")
    elif change == "bundle":
        data = replace(data, market_bundle_ref={**dict(data.market_bundle_ref), "bundle_key": "another"})
    else:
        data = replace(data, interval_end="2026-02-02T00:00:00.000000Z")
    inputs = replace(inputs, experiment_spec=replace(inputs.experiment_spec, data_slices=(data,)))
    materializer, backtest = _Materializer(f), _Backtest()
    before = f.entries("research.artifacts.v1"), f.entries("validation.sample-consumption.v1")
    with pytest.raises(InputQualificationError, match="TRIAL_CAPTURE_MISMATCH"):
        if operation == "preregister":
            preregister_input_capture_target_experiment(inputs, pub.publication, f, verifier)
        else:
            execute(f, ledger, verifier, pub, inputs, materializer, backtest)
    no_reads(materializer, backtest)
    assert before == (f.entries("research.artifacts.v1"), f.entries("validation.sample-consumption.v1"))


@pytest.mark.parametrize("change", ["revision", "bundle", "range"])
def test_T7_original_real_Trial_DataSlice_not_caller_compiler_value(tmp_path, change):
    from crypto_quant_research.integration import build_trial_declarations
    f, ledger, verifier, _, _, _, _, pub, inputs = capture_fixture(tmp_path)
    data = inputs.experiment_spec.data_slices[0]
    if change == "revision":
        data = replace(data, dataset_revision="another")
    elif change == "bundle":
        data = replace(data, market_bundle_ref={**dict(data.market_bundle_ref), "manifest_hash": H})
    else:
        data = replace(data, interval_start="2025-12-31T00:00:00.000000Z")
    other_inputs = replace(inputs, experiment_spec=replace(inputs.experiment_spec, data_slices=(data,)))
    original = preregister_target_experiment(other_inputs, f)
    with pytest.raises(InputQualificationError, match="TRIAL_CAPTURE_MISMATCH"):
        verifier.require_trial_binding(pub.publication, original.trial_declaration_refs[0])
    with pytest.raises(TypeError):
        verifier.require_trial_binding(pub.publication, build_trial_declarations(inputs.experiment_spec)[0])


@pytest.mark.parametrize("replay", [False, True])
def test_T7_foreign_Experiment_trial_replacement_before_target_or_result_cold_read(tmp_path, monkeypatch, replay):
    import crypto_quant_research.runtime as runtime
    f, ledger, verifier, _, _, _, _, pub, inputs = capture_fixture(tmp_path)
    materializer, backtest = _Materializer(f), _Backtest()
    if replay:
        execute(f, ledger, verifier, pub, inputs, materializer, backtest)
    foreign_inputs = replace(inputs, experiment_spec=replace(inputs.experiment_spec,
        parameter_combinations=(ExperimentParameterCombination((("foreign", "experiment"),)),)))
    foreign = preregister_input_capture_target_experiment(foreign_inputs, pub.publication, f, verifier)
    original_publish = runtime._publish_target_base
    def replace_trial(*args):
        base = original_publish(*args)
        base.refs[base.trials[0].ref] = foreign.trial_declaration_refs[0]
        return base
    monkeypatch.setattr(runtime, "_publish_target_base", replace_trial)
    before = (materializer.calls, backtest.store_calls, backtest.load_target_calls, backtest.prepare_calls,
              backtest.run_calls, backtest.cache_calls)
    with pytest.raises(InputQualificationError, match="TRIAL_CAPTURE_MISMATCH"):
        execute(f, ledger, verifier, pub, inputs, materializer, backtest)
    assert before == (materializer.calls, backtest.store_calls, backtest.load_target_calls, backtest.prepare_calls,
                      backtest.run_calls, backtest.cache_calls)


def test_T7_scope_cover_cannot_be_assembled_across_gap(tmp_path):
    scopes = (("a", START, "2026-01-10T00:00:00.000000Z"),
              ("b", "2026-01-20T00:00:00.000000Z", END))
    f, ledger, verifier, _, _, _, _, pub, inputs = capture_fixture(tmp_path, scopes=scopes)
    with pytest.raises(InputQualificationError, match="TRIAL_CAPTURE_MISMATCH"):
        preregister_input_capture_target_experiment(inputs, pub.publication, f, verifier)


def test_T8_all_data_slices_and_multiple_Trials_are_bound(tmp_path):
    f, ledger, verifier, _, _, _, _, pub, inputs = capture_fixture(tmp_path)
    data = inputs.experiment_spec.data_slices[0]
    spec = replace(inputs.experiment_spec, data_slices=(data, replace(data, interval_start="2026-01-02T00:00:00.000000Z")),
        budget={"max_trials": 2})
    inputs = replace(inputs, experiment_spec=spec)
    registered = preregister_input_capture_target_experiment(inputs, pub.publication, f, verifier)
    assert len(registered.trial_declaration_refs) == 2
    for ref in registered.trial_declaration_refs:
        verifier.require_trial_binding(pub.publication, ref)
    bad = replace(spec, data_slices=tuple(sorted((data, replace(data, dataset_revision="foreign")), key=lambda item: canonical_bytes(item.payload))))
    with pytest.raises(InputQualificationError, match="TRIAL_CAPTURE_MISMATCH"):
        preregister_input_capture_target_experiment(replace(inputs, experiment_spec=bad), pub.publication, f, verifier)


def test_T10_C01_mixed_new_owner_facts_then_old_target_execution_and_replay(tmp_path):
    f, ledger, verifier, _, _, _, _, pub, inputs = capture_fixture(tmp_path)
    materializer, backtest = _Materializer(f), _Backtest()
    old_inputs = _inputs()
    result = execute_target_experiment(old_inputs, f, ledger, materializer, backtest)
    assert execute_target_experiment(old_inputs, f, ledger, materializer, backtest) == result
    assert materializer.calls == backtest.economic_run_calls == 1


@pytest.mark.parametrize("corruption", ["generic_task", "generic_binding", "wrong_event", "future", "execution_log"])
@pytest.mark.parametrize("guarded", [True, False])
def test_T10_C01_malformed_custom_owner_events_are_not_skipped(tmp_path, corruption, guarded):
    f, ledger, verifier, declaration, tasks, _, binding, pub, inputs = capture_fixture(tmp_path)
    if corruption == "generic_binding":
        envelope = ArtifactEnvelope.create("input_qualification_capture_binding", 1, binding.payload)
    else:
        task = InputQualificationReadTaskV1(declaration.ref, "prices", "QUALIFY", None)
        envelope = ArtifactEnvelope.create("input_qualification_read_task", 2 if corruption == "future" else 1, task.payload)
    ref = f.put(envelope=envelope)
    log = "research.execution.v1" if corruption == "execution_log" else "research.artifacts.v1"
    event = canonical_sha256(("artifact-publication-v1", log, ref))
    if corruption == "wrong_event":
        event = H
    f.append(log, event, canonical_bytes(envelope))
    materializer, backtest = _Materializer(f), _Backtest()
    before = f.entries("validation.sample-consumption.v1")
    with pytest.raises((InputQualificationError, ValueError)):
        if guarded:
            execute(f, ledger, verifier, pub, inputs, materializer, backtest)
        else:
            execute_target_experiment(_inputs(), f, ledger, materializer, backtest)
    no_reads(materializer, backtest)
    assert f.entries("validation.sample-consumption.v1") == before


def test_T1_new_target_foundation_guards_precede_all_input_and_downstream_access(tmp_path, monkeypatch):
    from unittest.mock import Mock
    f = LocalFoundation(tmp_path, clock=lambda: RECEIVED_AT)
    other = LocalFoundation(tmp_path, clock=lambda: RECEIVED_AT)
    ledger = SampleConsumptionLedger(f)
    verifier = InputQualificationVerifierV1(f, ledger)
    for name in ("put", "read", "append", "entries", "checkpoint"):
        monkeypatch.setattr(f, name, Mock(side_effect=AssertionError("I/O")))
        monkeypatch.setattr(other, name, Mock(side_effect=AssertionError("I/O")))
    with pytest.raises(InputQualificationError, match="FOUNDATION_MISMATCH"):
        preregister_input_capture_target_experiment(object(), (), other, verifier)
    with pytest.raises(InputQualificationError, match="FOUNDATION_MISMATCH"):
        execute_input_capture_target_experiment(object(), (), f, SampleConsumptionLedger(other), verifier, object(), object())
    with pytest.raises(InputQualificationError, match="FOUNDATION_MISMATCH"):
        execute_input_capture_target_experiment(object(), (), f, ledger, object(), object(), object())
