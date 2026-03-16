from pathlib import Path
import hashlib
import pytest

from entityresolver.download.downloader import save_stream


def test_save_stream(tmp_path):
    data = [b"hello ", b"world"]

    output = tmp_path / "test.txt"

    result = save_stream(data, output)

    assert result.exists()
    assert result.read_bytes() == b"hello world"


def test_checksum_validation(tmp_path):
    data = [b"test data"]

    output = tmp_path / "file.txt"

    checksum = hashlib.sha256(b"test data").hexdigest()

    save_stream(data, output, checksum=checksum)

    assert output.exists()


def test_checksum_failure(tmp_path):
    data = [b"wrong data"]

    output = tmp_path / "file.txt"

    with pytest.raises(ValueError):
        save_stream(data, output, checksum="invalid")


def test_retry(tmp_path):
    calls = {"count": 0}

    def faulty_stream():
        calls["count"] += 1

        if calls["count"] == 1:
            raise RuntimeError("fail")

        yield b"data"

    output = tmp_path / "retry.txt"

    save_stream(faulty_stream(), output)

    assert output.exists()