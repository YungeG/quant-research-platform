"""Frozen public declaration/identity values and owner slot publication."""
from dataclasses import FrozenInstanceError, fields, replace
from types import MappingProxyType
from unittest.mock import Mock

import pytest
from crypto_quant_domain import ArtifactEnvelope, ArtifactRef, canonical_bytes, canonical_sha256
from crypto_quant_foundation import LocalFoundation, LogEntryRef
from crypto_quant_research import (
    InputQualificationDeclarationV1, InputQualificationReadTaskV1,
    InputQualificationCaptureBindingV1, PublishedInputQualificationArtifactV1,
    publish_input_qualification_declaration, publish_input_qualification_read_task,
)
from crypto_quant_validation import InputQualificationReadBindingV1, InputQualificationVerifierV1, SampleConsumptionLedger

H = canonical_sha256("synthetic")
START = "2000-01-01T00:00:00.000000Z"
END = "2000-02-01T00:00:00.000000Z"


def declaration():
    return InputQualificationDeclarationV1("synthetic", H, H, H, H, H, (("a", START, END),))


def binding():
    d = ArtifactRef.from_envelope(ArtifactEnvelope.create("input_qualification_declaration", 1, declaration().payload))
    manifest = {"type": "market_bundle_manifest", "bundle_key": "synthetic", "schema_version": 1,
        "coverage_start": START, "coverage_end_exclusive": END, "instrument_catalog_hash": H,
        "capabilities": (), "streams": (), "content_hash": H}
    ref = {"type": "market_bundle_ref", "bundle_key": "synthetic", "manifest_hash": canonical_sha256(manifest)}
    return InputQualificationCaptureBindingV1(d, (), H, ref, manifest)


def test_T1_exact_frozen_fields_without_defaults_or_extra_authority():
    expected = {
        InputQualificationDeclarationV1: ("qualification_key", "research_design_hash", "reader_code_hash", "normalization_code_hash", "prior_observation_hash", "source_scope_hash", "source_intervals"),
        InputQualificationReadTaskV1: ("declaration_ref", "scope_key", "phase", "input_check_hash"),
        InputQualificationCaptureBindingV1: ("declaration_ref", "capture_proofs", "source_manifest_hash", "market_bundle_ref", "market_bundle_manifest"),
        PublishedInputQualificationArtifactV1: ("ref", "entry_ref", "checkpoint"),
        InputQualificationReadBindingV1: ("capture_revision", "scope_key", "phase", "source_scope_hash", "reader_code_hash", "normalization_code_hash", "interval_start", "interval_end"),
    }
    from dataclasses import MISSING
    for cls, names in expected.items():
        assert tuple(f.name for f in fields(cls)) == names
        assert all(f.default is MISSING and f.default_factory is MISSING for f in fields(cls))
    d = declaration()
    assert set(d.payload) == {*expected[InputQualificationDeclarationV1], "mode"}
    assert d.payload["mode"] == "DEVELOPMENT_INPUT_QUALIFICATION"
    with pytest.raises(FrozenInstanceError):
        d.qualification_key = "changed"
    with pytest.raises(TypeError):
        InputQualificationDeclarationV1(**{**dict(d.payload), "qualified": True})


@pytest.mark.parametrize("changes", [
    {"qualification_key": "Upper"}, {"qualification_key": "a" * 129},
    {"qualification_key": "e\u0301"}, {"reader_code_hash": True}, {"source_scope_hash": 1.0},
    {"source_intervals": ()}, {"source_intervals": [("a", START, END)]},
    {"source_intervals": (("b", START, END), ("a", START, END))},
    {"source_intervals": (("a", START, END), ("a", START, END))},
    {"source_intervals": (("a", END, START),)},
    {"source_intervals": (("a", "2000-01-01T00:00:00Z", END),)},
    {"source_intervals": tuple((f"s{i:02d}", START, END) for i in range(33))},
])
def test_T1_declaration_rejects_noncanonical_fields(changes):
    with pytest.raises((TypeError, ValueError)):
        replace(declaration(), **changes)


@pytest.mark.parametrize("phase,check", [("QUALIFY", H), ("CAPTURE", None), ("UNKNOWN", None), (True, None)])
def test_T1_read_task_phase_fields_are_exact(phase, check):
    with pytest.raises((TypeError, ValueError)):
        InputQualificationReadTaskV1(binding().declaration_ref, "a", phase, check)


@pytest.mark.parametrize("change", ["bool", "float", "object", "nfc", "extra_ref", "extra_manifest", "size"])
def test_T1_T6_native_metadata_is_only_bounded_canonical_wire(change):
    b = binding()
    manifest, ref = dict(b.market_bundle_manifest), dict(b.market_bundle_ref)
    if change == "bool":
        manifest["schema_version"] = True
    elif change == "float":
        manifest["streams"] = ({"value": 1.5},)
    elif change == "object":
        class Encodable:
            def to_canonical_dict(self):
                return {"value": "custom"}
        manifest["streams"] = (Encodable(),)
    elif change == "nfc":
        manifest["streams"] = ({"value": "e\u0301"},)
    elif change == "extra_ref":
        ref["extra"] = "forbidden"
    elif change == "extra_manifest":
        manifest["extra"] = "forbidden"
    else:
        manifest["streams"] = ({"value": "a" * (2 * 1024 * 1024)},)
    with pytest.raises((TypeError, ValueError)):
        replace(b, market_bundle_ref=ref, market_bundle_manifest=manifest)


def test_T1_mapping_inputs_are_deeply_frozen_not_caller_mutable():
    b = binding()
    manifest = {**dict(b.market_bundle_manifest), "streams": [{"opaque": ["original"]}]}
    changed = replace(b, market_bundle_manifest=manifest)
    before = canonical_bytes(changed.payload)
    manifest["streams"][0]["opaque"][0] = "mutated"
    assert canonical_bytes(changed.payload) == before
    assert isinstance(changed.market_bundle_manifest, MappingProxyType)


def test_T5_owner_events_and_original_receipt_age_are_exact(tmp_path):
    now = ["2026-02-01T00:00:00.000000Z"]
    f = LocalFoundation(tmp_path, clock=lambda: now[0])
    ledger = SampleConsumptionLedger(f)
    verifier = InputQualificationVerifierV1(f, ledger)
    d = publish_input_qualification_declaration(declaration(), f)
    t = InputQualificationReadTaskV1(d.ref, "a", "QUALIFY", None)
    p = publish_input_qualification_read_task(t, f, verifier)
    entries = f.entries("research.artifacts.v1")
    assert entries[0].event_id == canonical_sha256(("artifact-publication-v1", "research.artifacts.v1", d.ref))
    assert entries[1].event_id == canonical_sha256(("input-qualification-read-task-v1", d.ref, "QUALIFY", "a"))
    now[0] = "2026-02-02T00:00:00.000000Z"
    assert publish_input_qualification_declaration(declaration(), f) == d
    assert publish_input_qualification_read_task(t, f, verifier) == p
    assert p.publication == (p.ref, p.entry_ref, p.checkpoint)
    assert f.entries("research.artifacts.v1") == entries


def test_T1_envelope_limit_is_exactly_2MiB_not_an_approximate_payload_limit():
    b = binding()
    manifest = {**dict(b.market_bundle_manifest), "streams": ({"value": ""},)}
    empty = replace(b, market_bundle_manifest=manifest)
    overhead = len(canonical_bytes(ArtifactEnvelope.create("input_qualification_capture_binding", 1, empty.payload)))
    manifest["streams"] = ({"value": "a" * (2 * 1024 * 1024 - overhead)},)
    exact = replace(b, market_bundle_manifest=manifest)
    assert len(canonical_bytes(ArtifactEnvelope.create("input_qualification_capture_binding", 1, exact.payload))) == 2 * 1024 * 1024
    manifest["streams"] = ({"value": "a" * (2 * 1024 * 1024 - overhead + 1)},)
    with pytest.raises(ValueError, match="2MiB"):
        replace(b, market_bundle_manifest=manifest)
