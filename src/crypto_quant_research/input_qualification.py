"""Frozen qualification intent and capture identity attestations, not data authority."""
from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from crypto_quant_domain import ArtifactEnvelope, ArtifactRef, canonical_bytes, canonical_sha256
from crypto_quant_foundation import LocalFoundation, LogCheckpoint, LogEntryRef
from crypto_quant_validation import InputQualificationError, InputQualificationVerifierV1

from .statistical_study import _hash, _instant, _mapping, _publish, _ref, _ref_from_wire

_LOG = "research.artifacts.v1"
_DECLARATION = "input_qualification_declaration"
_TASK = "input_qualification_read_task"
_BINDING = "input_qualification_capture_binding"
_LIMIT = 2 * 1024 * 1024
_KEY = re.compile(r"[a-z][a-z0-9._-]{0,127}\Z")
_HASH_FIELDS = ("research_design_hash", "reader_code_hash", "normalization_code_hash",
                "prior_observation_hash", "source_scope_hash")


def _key(value: object) -> str:
    if type(value) is not str or _KEY.fullmatch(value) is None:
        raise ValueError("key must be canonical lowercase text, at most 128 characters")
    return value


def _reference(value: object, name: str) -> ArtifactRef:
    result = _ref(value, name)
    if type(result.schema_version) is not int:
        raise ValueError("reference version must be an exact integer")
    return result


def _wire_reference(value: object, name: str) -> ArtifactRef:
    wire = _mapping(value, {"type", "artifact_type", "schema_version", "content_hash"})
    if type(wire["schema_version"]) is not int:
        raise ValueError("reference version must be an exact integer")
    return _reference(_ref_from_wire(value, name), name)


def _json_value(value: object) -> object:
    if value is None or type(value) in (bool, int, str):
        canonical_bytes(value)
        return value
    if type(value) in (tuple, list):
        return tuple(_json_value(item) for item in value)
    if type(value) in (dict, MappingProxyType):
        if any(type(key) is not str for key in value):
            raise ValueError("canonical mapping keys must be strings")
        result = MappingProxyType({key: _json_value(child) for key, child in value.items()})
        canonical_bytes(result)
        return result
    raise ValueError("metadata must contain only canonical JSON values")


def _intervals(value: object) -> None:
    if type(value) is not tuple or not 1 <= len(value) <= 32:
        raise ValueError("source intervals must contain 1..32 immutable scopes")
    previous = ""
    for item in value:
        if type(item) is not tuple or len(item) != 3:
            raise ValueError("source intervals must be immutable three-tuples")
        key, start, end = item
        if _key(key) <= previous or _instant(start) >= _instant(end):
            raise ValueError("source scopes must be strictly ordered and nonempty")
        previous = key


def _bounded(name: str, payload: object) -> ArtifactEnvelope:
    envelope = ArtifactEnvelope.create(name, 1, payload)
    if len(canonical_bytes(envelope)) > _LIMIT:
        raise ValueError("input qualification envelope exceeds 2MiB")
    return envelope


def _capture_revision(ref: ArtifactRef) -> str:
    return "local-capture:" + canonical_sha256(("input-qualification-capture-v1", ref))


@dataclass(frozen=True, slots=True)
class InputQualificationDeclarationV1:
    qualification_key: str
    research_design_hash: str
    reader_code_hash: str
    normalization_code_hash: str
    prior_observation_hash: str
    source_scope_hash: str
    source_intervals: tuple[tuple[str, str, str], ...]

    def __post_init__(self) -> None:
        _key(self.qualification_key)
        for name in _HASH_FIELDS:
            _hash(getattr(self, name), name)
        _intervals(self.source_intervals)
        _bounded(_DECLARATION, self.payload)

    @property
    def payload(self) -> Mapping:
        return MappingProxyType({"qualification_key": self.qualification_key,
            **{name: getattr(self, name) for name in _HASH_FIELDS},
            "source_intervals": self.source_intervals, "mode": "DEVELOPMENT_INPUT_QUALIFICATION"})


@dataclass(frozen=True, slots=True)
class InputQualificationReadTaskV1:
    declaration_ref: ArtifactRef
    scope_key: str
    phase: str
    input_check_hash: str | None

    def __post_init__(self) -> None:
        _reference(self.declaration_ref, _DECLARATION)
        _key(self.scope_key)
        if type(self.phase) is not str or self.phase not in ("QUALIFY", "CAPTURE"):
            raise ValueError("phase must be QUALIFY or CAPTURE")
        if self.phase == "QUALIFY":
            if self.input_check_hash is not None:
                raise ValueError("QUALIFY cannot carry a check hash")
        else:
            _hash(self.input_check_hash, "input_check_hash")
        _bounded(_TASK, self.payload)

    @property
    def payload(self) -> Mapping:
        return MappingProxyType({"declaration_ref": self.declaration_ref, "scope_key": self.scope_key,
                                 "phase": self.phase, "input_check_hash": self.input_check_hash})


@dataclass(frozen=True, slots=True)
class InputQualificationCaptureBindingV1:
    declaration_ref: ArtifactRef
    capture_proofs: tuple[tuple[str, ArtifactRef, LogEntryRef], ...]
    source_manifest_hash: str
    market_bundle_ref: Mapping
    market_bundle_manifest: Mapping

    def __post_init__(self) -> None:
        _reference(self.declaration_ref, _DECLARATION)
        _hash(self.source_manifest_hash, "source_manifest_hash")
        if type(self.capture_proofs) is not tuple:
            raise ValueError("capture proofs must be an immutable tuple")
        previous = ""
        for item in self.capture_proofs:
            if type(item) is not tuple or len(item) != 3 or _key(item[0]) <= previous:
                raise ValueError("capture proofs must be strictly scope ordered")
            previous = item[0]
            _reference(item[1], _TASK)
            entry = item[2]
            if type(entry) is not LogEntryRef:
                raise TypeError("capture proof must use an exact LogEntryRef")
            if type(entry.log_sequence) is not int or entry.log_sequence <= 0:
                raise ValueError("entry sequence must be a positive exact integer")
            if type(entry.log_name) is not str or not entry.log_name:
                raise ValueError("entry must name its log")
            _hash(entry.receipt_hash, "receipt_hash")
        ref = _mapping(self.market_bundle_ref, {"type", "bundle_key", "manifest_hash"})
        manifest = _mapping(self.market_bundle_manifest, {"type", "bundle_key", "schema_version", "coverage_start",
            "coverage_end_exclusive", "instrument_catalog_hash", "capabilities", "streams", "content_hash"})
        ref, manifest = _json_value(ref), _json_value(manifest)
        if ref["type"] != "market_bundle_ref" or manifest["type"] != "market_bundle_manifest":
            raise ValueError("binding requires native MarketBundle metadata wire")
        if type(manifest["schema_version"]) is not int or manifest["schema_version"] != 1:
            raise ValueError("manifest schema must be exact integer 1")
        _key(ref["bundle_key"])
        _key(manifest["bundle_key"])
        _hash(ref["manifest_hash"], "manifest_hash")
        _hash(manifest["content_hash"], "content_hash")
        object.__setattr__(self, "market_bundle_ref", ref)
        object.__setattr__(self, "market_bundle_manifest", manifest)
        _bounded(_BINDING, self.payload)

    @property
    def payload(self) -> Mapping:
        return MappingProxyType({"declaration_ref": self.declaration_ref,
            "capture_proofs": tuple((scope, task, {"log_name": entry.log_name, "log_sequence": entry.log_sequence,
                "receipt_hash": entry.receipt_hash}) for scope, task, entry in self.capture_proofs),
            "source_manifest_hash": self.source_manifest_hash, "market_bundle_ref": self.market_bundle_ref,
            "market_bundle_manifest": self.market_bundle_manifest})


@dataclass(frozen=True, slots=True)
class PublishedInputQualificationArtifactV1:
    ref: ArtifactRef
    entry_ref: LogEntryRef
    checkpoint: LogCheckpoint

    def __post_init__(self) -> None:
        if type(self.ref) is not ArtifactRef or self.ref.artifact_type not in (_DECLARATION, _TASK, _BINDING):
            raise TypeError("publication ref must address an input qualification artifact")
        _reference(self.ref, self.ref.artifact_type)
        if type(self.entry_ref) is not LogEntryRef or type(self.checkpoint) is not LogCheckpoint:
            raise TypeError("publication must use exact Foundation receipt coordinates")
        if (self.entry_ref.log_name != _LOG or self.checkpoint.log_name != _LOG
                or type(self.entry_ref.log_sequence) is not int or self.entry_ref.log_sequence <= 0
                or type(self.checkpoint.upper_log_sequence) is not int
                or self.checkpoint.upper_log_sequence != self.entry_ref.log_sequence
                or self.checkpoint.head_receipt_hash != self.entry_ref.receipt_hash):
            raise ValueError("publication checkpoint must bind its original owner receipt")
        _hash(self.entry_ref.receipt_hash, "receipt_hash")
        _instant(self.checkpoint.as_of)

    @property
    def publication(self) -> tuple[ArtifactRef, LogEntryRef, LogCheckpoint]:
        return self.ref, self.entry_ref, self.checkpoint


def _decode(envelope: ArtifactEnvelope) -> object:
    if type(envelope) is not ArtifactEnvelope or type(envelope.schema_version) is not int or envelope.schema_version != 1:
        raise ValueError("input qualification requires an exact schema-1 envelope")
    if envelope.artifact_type == _DECLARATION:
        wire = _mapping(envelope.payload, {"qualification_key", "mode", "source_intervals", *_HASH_FIELDS})
        if wire["mode"] != "DEVELOPMENT_INPUT_QUALIFICATION":
            raise ValueError("declaration mode is fixed")
        result = InputQualificationDeclarationV1(wire["qualification_key"], *(wire[name] for name in _HASH_FIELDS), wire["source_intervals"])
    elif envelope.artifact_type == _TASK:
        wire = _mapping(envelope.payload, {"declaration_ref", "scope_key", "phase", "input_check_hash"})
        result = InputQualificationReadTaskV1(_wire_reference(wire["declaration_ref"], _DECLARATION), wire["scope_key"],
                                               wire["phase"], wire["input_check_hash"])
    elif envelope.artifact_type == _BINDING:
        wire = _mapping(envelope.payload, {"declaration_ref", "capture_proofs", "source_manifest_hash", "market_bundle_ref", "market_bundle_manifest"})
        if type(wire["capture_proofs"]) is not tuple:
            raise ValueError("proofs must be an immutable tuple")
        proofs = []
        for item in wire["capture_proofs"]:
            if type(item) is not tuple or len(item) != 3:
                raise ValueError("proof must be an immutable three-tuple")
            entry = _mapping(item[2], {"log_name", "log_sequence", "receipt_hash"})
            proofs.append((item[0], _wire_reference(item[1], _TASK), LogEntryRef(**entry)))
        result = InputQualificationCaptureBindingV1(_wire_reference(wire["declaration_ref"], _DECLARATION),
            tuple(proofs), wire["source_manifest_hash"], wire["market_bundle_ref"], wire["market_bundle_manifest"])
    else:
        raise ValueError("unknown input qualification artifact type")
    if canonical_bytes(_bounded(envelope.artifact_type, result.payload)) != canonical_bytes(envelope):
        raise ValueError("input qualification envelope must be canonical")
    return result


def _event(envelope: ArtifactEnvelope) -> str:
    value = _decode(envelope)
    if type(value) is InputQualificationReadTaskV1:
        return canonical_sha256(("input-qualification-read-task-v1", value.declaration_ref, value.phase, value.scope_key))
    if type(value) is InputQualificationCaptureBindingV1:
        return canonical_sha256(("input-qualification-capture-binding-v1", _capture_revision(value.declaration_ref)))
    return canonical_sha256(("artifact-publication-v1", _LOG, ArtifactRef.from_envelope(envelope)))


def _require_verifier(foundation: LocalFoundation, verifier: InputQualificationVerifierV1) -> None:
    if type(verifier) is not InputQualificationVerifierV1:
        raise InputQualificationError("FOUNDATION_MISMATCH")
    verifier.require_foundation(foundation)


def publish_input_qualification_declaration(declaration: InputQualificationDeclarationV1,
                                            foundation: LocalFoundation) -> PublishedInputQualificationArtifactV1:
    if type(foundation) is not LocalFoundation:
        raise TypeError("foundation must be an exact LocalFoundation")
    if type(declaration) is not InputQualificationDeclarationV1:
        raise TypeError("declaration must be exact InputQualificationDeclarationV1")
    envelope = _bounded(_DECLARATION, declaration.payload)
    _decode(envelope)
    return PublishedInputQualificationArtifactV1(*_publish(envelope, _event(envelope), foundation))


def publish_input_qualification_read_task(task: InputQualificationReadTaskV1, foundation: LocalFoundation,
                                          verifier: InputQualificationVerifierV1) -> PublishedInputQualificationArtifactV1:
    _require_verifier(foundation, verifier)
    if type(task) is not InputQualificationReadTaskV1:
        raise TypeError("task must be exact InputQualificationReadTaskV1")
    envelope = _bounded(_TASK, task.payload)
    _decode(envelope)
    verifier.verify_read_task_inputs(envelope)
    return PublishedInputQualificationArtifactV1(*_publish(envelope, _event(envelope), foundation))


def publish_input_qualification_capture_binding(binding: InputQualificationCaptureBindingV1, foundation: LocalFoundation,
                                                verifier: InputQualificationVerifierV1) -> PublishedInputQualificationArtifactV1:
    _require_verifier(foundation, verifier)
    if type(binding) is not InputQualificationCaptureBindingV1:
        raise TypeError("binding must be exact InputQualificationCaptureBindingV1")
    envelope = _bounded(_BINDING, binding.payload)
    _decode(envelope)
    verifier.verify_capture_inputs(envelope)
    return PublishedInputQualificationArtifactV1(*_publish(envelope, _event(envelope), foundation))
