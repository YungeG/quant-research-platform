from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol

from crypto_quant_bundle_builder import (
    RawBlobSnapshotManifest,
    RawBlobSnapshotSourceMember,
    RawBlobSnapshotView,
    create_raw_blob_snapshot_manifest,
)
from crypto_quant_domain import (
    ArtifactEnvelope,
    ArtifactRef,
    RawBlobRef,
    canonical_bytes,
    canonical_sha256,
)
from crypto_quant_foundation import LogEntryRef

RAW_BLOB_SNAPSHOTS_LOG = "research.raw_snapshots.v1"
_HASH = re.compile(r"sha256:[0-9a-f]{64}\Z")


class RawBlobSnapshotFoundation(Protocol):
    """Public Foundation operations used by the raw-snapshot composition root."""

    def put_raw_blob(self, *, blob: bytes) -> RawBlobRef: ...

    def read_raw_blob(self, *, ref: RawBlobRef) -> bytes: ...

    def raw_blob_path(self, *, ref: RawBlobRef): ...

    def put(self, *, envelope: ArtifactEnvelope) -> ArtifactRef: ...

    def read(self, *, ref: ArtifactRef): ...

    def append(self, log_name: str, event_id: str, payload: bytes): ...

    def entries(self, log_name: str, through: LogEntryRef | None = None): ...


@dataclass(frozen=True, slots=True)
class RawBlobSnapshotPublicationFact:
    manifest_ref: ArtifactRef
    snapshot_id: str

    def __post_init__(self) -> None:
        if type(self.manifest_ref) is not ArtifactRef:
            raise TypeError("manifest_ref must be an ArtifactRef")
        if (
            self.manifest_ref.artifact_type != "raw_blob_snapshot_manifest"
            or self.manifest_ref.schema_version != 1
        ):
            raise ValueError("manifest_ref must address raw_blob_snapshot_manifest@1")
        if (
            type(self.snapshot_id) is not str
            or _HASH.fullmatch(self.snapshot_id) is None
        ):
            raise ValueError("snapshot_id must be a content hash")

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "type": "raw_blob_snapshot_publication",
            "schema_version": 1,
            "manifest_ref": self.manifest_ref.to_canonical_dict(),
            "snapshot_id": self.snapshot_id,
        }


def _event_id(fact: RawBlobSnapshotPublicationFact) -> str:
    return canonical_sha256(
        (
            "raw-blob-snapshot-publication-v1",
            RAW_BLOB_SNAPSHOTS_LOG,
            fact.to_canonical_dict(),
        )
    )


def _fact_bytes(fact: RawBlobSnapshotPublicationFact) -> bytes:
    return canonical_bytes(fact.to_canonical_dict())


def _entry_for_exact_fact(
    foundation: RawBlobSnapshotFoundation,
    fact: RawBlobSnapshotPublicationFact,
    entry_ref: LogEntryRef,
) -> None:
    if (
        type(entry_ref) is not LogEntryRef
        or entry_ref.log_name != RAW_BLOB_SNAPSHOTS_LOG
    ):
        raise ValueError("raw blob snapshot publication entry is invalid")
    try:
        entries = foundation.entries(RAW_BLOB_SNAPSHOTS_LOG, entry_ref)
    except Exception as error:
        raise ValueError(
            "raw blob snapshot publication entry is unavailable"
        ) from error
    if (
        type(entries) is not tuple
        or not entries
        or entries[-1].entry_ref != entry_ref
        or entries[-1].event_id != _event_id(fact)
        or entries[-1].payload != _fact_bytes(fact)
    ):
        raise ValueError("raw blob snapshot publication entry does not bind manifest")


@dataclass(frozen=True, slots=True)
class RawBlobSnapshotPublication:
    manifest: RawBlobSnapshotManifest
    manifest_ref: ArtifactRef
    publication_entry_ref: LogEntryRef

    def __post_init__(self) -> None:
        if type(self.manifest) is not RawBlobSnapshotManifest:
            raise TypeError("manifest must be a RawBlobSnapshotManifest")
        if self.manifest_ref != self.manifest.artifact_ref:
            raise ValueError("manifest_ref does not match manifest")
        if type(self.publication_entry_ref) is not LogEntryRef:
            raise TypeError("publication_entry_ref must be a LogEntryRef")


def publish_raw_blob_snapshot(
    foundation: RawBlobSnapshotFoundation,
    *,
    members: Iterable[RawBlobSnapshotSourceMember],
    provenance: Mapping[str, object],
) -> RawBlobSnapshotPublication:
    """Publishes raw blobs, their manifest, then the one owner-log authority fact."""

    source_members = tuple(members)
    manifest = create_raw_blob_snapshot_manifest(
        members=source_members, provenance=provenance
    )
    sources = {member.member_key: member for member in source_members}
    for member in manifest.members:
        source = sources.get(member.member_key)
        if source is None or source.raw_blob_ref != member.raw_blob_ref:
            raise ValueError("raw blob snapshot source does not match manifest")
        if foundation.put_raw_blob(blob=source.raw_bytes) != member.raw_blob_ref:
            raise ValueError("Foundation raw blob ref does not match manifest")
        if foundation.read_raw_blob(ref=member.raw_blob_ref) != source.raw_bytes:
            raise ValueError("Foundation raw blob readback does not match manifest")

    envelope = manifest.envelope
    manifest_ref = foundation.put(envelope=envelope)
    if manifest_ref != manifest.artifact_ref:
        raise ValueError("Foundation manifest ref does not match manifest")
    try:
        readback = foundation.read(ref=manifest_ref)
    except Exception as error:
        raise ValueError("raw blob snapshot manifest is unpublished") from error
    if getattr(readback, "envelope", None) != envelope:
        raise ValueError("Foundation manifest readback does not match manifest")

    fact = RawBlobSnapshotPublicationFact(manifest_ref, manifest.snapshot_id)
    receipt = foundation.append(
        RAW_BLOB_SNAPSHOTS_LOG,
        _event_id(fact),
        _fact_bytes(fact),
    )
    entry_ref = getattr(receipt, "entry_ref", None)
    if type(entry_ref) is not LogEntryRef:
        raise ValueError("Foundation publication append did not return a LogEntryRef")
    _entry_for_exact_fact(foundation, fact, entry_ref)
    return RawBlobSnapshotPublication(manifest, manifest_ref, entry_ref)


def open_verified_raw_blob_snapshot(
    foundation: RawBlobSnapshotFoundation,
    manifest_ref: ArtifactRef,
    publication_entry_ref: LogEntryRef,
) -> RawBlobSnapshotView:
    """Opens only a manifest that is bound by the supplied exact owner-log fact."""

    if type(manifest_ref) is not ArtifactRef:
        raise TypeError("manifest_ref must be an ArtifactRef")
    try:
        readback = foundation.read(ref=manifest_ref)
        envelope = readback.envelope
        manifest = RawBlobSnapshotManifest.from_envelope(envelope)
    except Exception as error:
        raise ValueError("raw blob snapshot manifest is unavailable") from error
    if manifest.artifact_ref != manifest_ref:
        raise ValueError("raw blob snapshot manifest ref does not match manifest")
    _entry_for_exact_fact(
        foundation,
        RawBlobSnapshotPublicationFact(manifest_ref, manifest.snapshot_id),
        publication_entry_ref,
    )
    return RawBlobSnapshotView.open(manifest, foundation)
