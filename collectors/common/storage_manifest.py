"""Collector compatibility import backed by the reader-wheel implementation.

Keep this module as the established collector import path while placing the
actual contract in the reader wheel's top-level module.  Writers and consumers
therefore validate exactly the same release-manifest shape.
"""

from storage_manifest import (
    MANIFEST_RELATIVE_PATH,
    SCHEMA_VERSION,
    StorageCompatibilityError,
    StorageManifestError,
    assert_loader_compatible,
    read_release_manifest,
    release_manifest_path,
    validate_accepted_release_manifest,
    validate_release_manifest,
    write_release_manifest,
)

__all__ = [
    "MANIFEST_RELATIVE_PATH",
    "SCHEMA_VERSION",
    "StorageCompatibilityError",
    "StorageManifestError",
    "assert_loader_compatible",
    "read_release_manifest",
    "release_manifest_path",
    "validate_accepted_release_manifest",
    "validate_release_manifest",
    "write_release_manifest",
]
