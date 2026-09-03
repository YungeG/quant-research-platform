from __future__ import annotations

from pathlib import Path

import pytest
from crypto_quant_bundle_builder import (
    RawBlobSnapshotSourceMember,
    create_raw_blob_snapshot_manifest,
)
from crypto_quant_foundation import LocalFoundation, LogEntryRef
from crypto_quant_research.raw_blob_snapshots import (
    RAW_BLOB_SNAPSHOTS_LOG,
    open_verified_raw_blob_snapshot,
    publish_raw_blob_snapshot,
)

_CLOCK = lambda: "2026-09-04T00:00:00.000000Z"


def _sources() -> tuple[RawBlobSnapshotSourceMember, ...]:
    return (
        RawBlobSnapshotSourceMember("raw/a.txt", b"alpha", "0644"),
        RawBlobSnapshotSourceMember("raw/b.txt", b"beta", "0644"),
    )


def test_publication_is_idempotent_and_requires_exact_log_fact(
    tmp_path: Path,
) -> None:
    foundation = LocalFoundation(tmp_path, clock=_CLOCK)

    first = publish_raw_blob_snapshot(
        foundation,
        members=_sources(),
        provenance={"source": "fixture"},
    )
    second = publish_raw_blob_snapshot(
        foundation,
        members=tuple(reversed(_sources())),
        provenance={"source": "fixture"},
    )

    assert second.manifest_ref == first.manifest_ref
    assert second.publication_entry_ref == first.publication_entry_ref
    assert len(foundation.entries(RAW_BLOB_SNAPSHOTS_LOG)) == 1
    view = open_verified_raw_blob_snapshot(
        foundation, first.manifest_ref, first.publication_entry_ref
    )
    assert view.member_bytes("raw/a.txt") == b"alpha"

    with pytest.raises(ValueError):
        open_verified_raw_blob_snapshot(
            foundation,
            first.manifest_ref,
            LogEntryRef(RAW_BLOB_SNAPSHOTS_LOG, 2, "sha256:" + "0" * 64),
        )

    different = publish_raw_blob_snapshot(
        foundation,
        members=_sources(),
        provenance={"source": "different"},
    )
    with pytest.raises(ValueError, match="does not bind"):
        open_verified_raw_blob_snapshot(
            foundation, first.manifest_ref, different.publication_entry_ref
        )


def test_published_manifest_without_owner_log_entry_has_no_authority(
    tmp_path: Path,
) -> None:
    foundation = LocalFoundation(tmp_path, clock=_CLOCK)
    sources = _sources()
    manifest = create_raw_blob_snapshot_manifest(
        members=sources, provenance={"source": "unpublished"}
    )
    for source in sources:
        assert foundation.put_raw_blob(blob=source.raw_bytes) == source.raw_blob_ref
    manifest_ref = foundation.put(envelope=manifest.envelope)
    assert manifest_ref == manifest.artifact_ref
    assert foundation.read(ref=manifest_ref).envelope == manifest.envelope
    assert foundation.entries(RAW_BLOB_SNAPSHOTS_LOG) == ()

    with pytest.raises(ValueError, match="unavailable"):
        open_verified_raw_blob_snapshot(
            foundation,
            manifest_ref,
            LogEntryRef(RAW_BLOB_SNAPSHOTS_LOG, 1, "sha256:" + "0" * 64),
        )


def test_orphan_raw_blobs_and_unpublished_manifest_have_no_authority(
    tmp_path: Path,
) -> None:
    foundation = LocalFoundation(tmp_path, clock=_CLOCK)
    orphan = foundation.put_raw_blob(blob=b"orphan")
    assert foundation.read_raw_blob(ref=orphan) == b"orphan"
    unpublished = create_raw_blob_snapshot_manifest(
        members=_sources(), provenance={"source": "unpublished"}
    )
    with pytest.raises(ValueError, match="unavailable"):
        open_verified_raw_blob_snapshot(
            foundation,
            unpublished.artifact_ref,
            LogEntryRef(RAW_BLOB_SNAPSHOTS_LOG, 1, "sha256:" + "0" * 64),
        )

    publication = publish_raw_blob_snapshot(
        foundation,
        members=_sources(),
        provenance={"source": "fixture"},
    )
    entries = foundation.entries(RAW_BLOB_SNAPSHOTS_LOG)
    assert len(entries) == 1
    with pytest.raises(ValueError):
        open_verified_raw_blob_snapshot(
            foundation,
            publication.manifest_ref,
            LogEntryRef(
                RAW_BLOB_SNAPSHOTS_LOG,
                entries[0].log_sequence,
                "sha256:" + "f" * 64,
            ),
        )
