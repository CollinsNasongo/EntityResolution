from pathlib import Path

from entityresolver.ingestion.manager import ingest
from entityresolver.ingestion.manifest import Manifest


class DummySource:
    def fetch(self):
        return [b"data"]


def test_ingest_success(tmp_path):
    manifest = Manifest(tmp_path / "manifest.json")
    output = tmp_path / "file.txt"

    result = ingest(DummySource(), output, manifest)

    assert result.exists()
    assert result.read_bytes() == b"data"

    record = manifest.get_file("file.txt")
    assert record["status"] == "completed"


def test_ingest_skip(tmp_path):
    manifest = Manifest(tmp_path / "manifest.json")
    output = tmp_path / "file.txt"

    source = DummySource()

    ingest(source, output, manifest)
    result = ingest(source, output, manifest)

    assert result.exists()


def test_ingest_overwrite(tmp_path):
    manifest = Manifest(tmp_path / "manifest.json")
    output = tmp_path / "file.txt"

    class Source1:
        def fetch(self):
            return [b"one"]

    class Source2:
        def fetch(self):
            return [b"two"]

    ingest(Source1(), output, manifest)
    ingest(Source2(), output, manifest, overwrite=True)

    assert output.read_bytes() == b"two"