"""Where the files go: the Unity Catalog volume (Files API), or a local folder for trying
the ingest without Databricks."""

from __future__ import annotations

import io
from pathlib import Path

from databricks.sdk.errors import NotFound


class Volume:
    def __init__(self, files, root: str) -> None:
        self._files = files  # WorkspaceClient().files
        self._root = root.rstrip("/")

    def put(self, rel: str, data: bytes) -> None:
        self._files.upload(f"{self._root}/{rel}", io.BytesIO(data), overwrite=True)

    def get(self, rel: str) -> bytes | None:
        try:
            return self._files.download(f"{self._root}/{rel}").contents.read()
        except NotFound:
            return None


class LocalDir:
    def __init__(self, path: Path) -> None:
        self._path = Path(path)

    def put(self, rel: str, data: bytes) -> None:
        target = self._path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

    def get(self, rel: str) -> bytes | None:
        target = self._path / rel
        return target.read_bytes() if target.exists() else None
