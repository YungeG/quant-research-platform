"""AR-01 authority binding; fixture execution only, never market providers."""
from __future__ import annotations

import subprocess
import sys

import pytest
from crypto_quant_foundation import LocalFoundation
from crypto_quant_research import (
    FrozenModelExperimentInputs,
    execute_experiment,
    execute_model_experiment,
    execute_target_experiment,
)
from crypto_quant_validation import SampleConsumptionLedger, ValidationCoreFailure

from . import test_model_build_core as model
from . import test_research_shell as ordinary
from . import test_target_stream_research as target


def test_foreign_ledger_rejected_before_any_io(tmp_path, monkeypatch):
    """Retained audit regression: authority A=0 / foreign B=5 was accepted."""
    foundation, _, port = ordinary._runtime(tmp_path / "authority")
    other = LocalFoundation(tmp_path / "other", clock=lambda: ordinary.RECEIVED_AT)
    ledger = SampleConsumptionLedger(other)
    inputs = ordinary._inputs()
    calls = []
    for store in (foundation, other):
        for name in ("read", "entries", "put", "append", "checkpoint"):
            original = getattr(store, name)
            def counted(*args, _original=original, _name=name, **kwargs):
                calls.append(_name)
                return _original(*args, **kwargs)
            monkeypatch.setattr(store, name, counted)
    failure = None
    try:
        execute_experiment(inputs, foundation, ledger, port)
    except ValidationCoreFailure as error:
        failure = error
    # Observe the historical split without adding to the guarded call counters.
    a = len(LocalFoundation.entries(foundation, ordinary.SAMPLE_LOG))
    b = len(LocalFoundation.entries(other, ordinary.SAMPLE_LOG))
    assert failure is not None, f"foreign ledger accepted: A={a}/B={b}"
    assert failure.code == "SAMPLE_LEDGER_CONFLICT"
    assert calls == []
    assert port.run_requests == [] and port.derive_calls == []
    assert a == b == 0


@pytest.mark.parametrize("mode", ("ordinary", "model", "target"))
@pytest.mark.parametrize("same_path", (False, True))
def test_all_research_guards_precede_storage_and_port_access(tmp_path, monkeypatch, mode, same_path):
    inputs = ordinary._inputs()
    if mode == "model":
        feature, trainer, plan = model._recipes()
        spec = model._spec(plan)
        inputs = FrozenModelExperimentInputs(
            spec, feature, trainer, plan, ordinary._policy(spec),
            {"type": "actor_ref", "actor_id": "research"}, ordinary.RESERVED_AT,
        )
    elif mode == "target":
        inputs = target._inputs()
    calls = []
    def forbidden(*args, **kwargs):
        calls.append("io")
        raise AssertionError("binding must precede I/O and clock")
    foundation = LocalFoundation(tmp_path / "authority", clock=forbidden)
    other = LocalFoundation(tmp_path / ("authority" if same_path else "other"), clock=forbidden)
    ledger = SampleConsumptionLedger(other)
    for name in ("read", "entries", "put", "append", "checkpoint"):
        monkeypatch.setattr(LocalFoundation, name, forbidden)
    class UntouchedPort:
        def __getattribute__(self, name):
            forbidden()
    port = UntouchedPort()
    with pytest.raises(ValidationCoreFailure) as raised:
        if mode == "ordinary":
            execute_experiment(inputs, foundation, ledger, port)
        elif mode == "model":
            execute_model_experiment(inputs, foundation, ledger, port, port)
        else:
            execute_target_experiment(inputs, foundation, ledger, port, port)
    assert raised.value.code == "SAMPLE_LEDGER_CONFLICT"
    assert calls == []


def test_research_pure_checks_still_precede_binding(tmp_path):
    foundation = LocalFoundation(tmp_path / "authority")
    foreign = SampleConsumptionLedger(LocalFoundation(tmp_path / "other"))
    for execute in (execute_experiment, execute_model_experiment, execute_target_experiment):
        args = (None, foundation, foreign, object())
        if execute is not execute_experiment:
            args += (object(),)
        with pytest.raises(TypeError, match="frozen_inputs must be"):
            execute(*args)


def test_same_authority_second_ledger_and_reopen_replay(tmp_path):
    foundation, ledger, port = ordinary._runtime(tmp_path)
    first = execute_experiment(ordinary._inputs(), foundation, ledger, port)
    before = (foundation.entries(ordinary.SAMPLE_LOG), foundation.entries(ordinary.ARTIFACT_LOG), foundation.entries(ordinary.EXECUTION_LOG))
    assert execute_experiment(ordinary._inputs(), foundation, SampleConsumptionLedger(foundation), port) == first
    reopened = LocalFoundation(tmp_path, clock=lambda: ordinary.RECEIVED_AT)
    assert execute_experiment(ordinary._inputs(), reopened, SampleConsumptionLedger(reopened), port) == first
    assert before == (reopened.entries(ordinary.SAMPLE_LOG), reopened.entries(ordinary.ARTIFACT_LOG), reopened.entries(ordinary.EXECUTION_LOG))
    assert len(port.run_requests) == 4


def test_retrospective_public_import_keeps_financial_runtime_lazy():
    code = "import sys; from crypto_quant_research import RetrospectiveStudyProtocol, RetrospectiveStudyDeclaration, RetrospectiveReadTask, publish_retrospective_protocol, publish_retrospective_study, publish_retrospective_read_task; assert 'crypto_quant_research.runtime' not in sys.modules"
    subprocess.run([sys.executable, "-B", "-c", code], check=True)
