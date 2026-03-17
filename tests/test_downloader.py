from pathlib import Path
import hashlib
import pytest

from entityresolver.download.downloader import save_stream


# ---------------------------------------------------------
# BASIC SUCCESS
# ---------------------------------------------------------
def test_save_stream(tmp_path):
    data = [b"hello ", b"world"]

    output = tmp_path / "test.txt"

    result = save_stream(lambda: data, output)

    assert result.exists()
    assert result.read_bytes() == b"hello world"


# ---------------------------------------------------------
# CHECKSUM SUCCESS
# ---------------------------------------------------------
def test_checksum_validation(tmp_path):
    data = [b"test data"]

    output = tmp_path / "file.txt"

    checksum = hashlib.sha256(b"test data").hexdigest()

    save_stream(lambda: data, output, checksum=checksum)

    assert output.exists()


# ---------------------------------------------------------
# CHECKSUM FAILURE
# ---------------------------------------------------------
def test_checksum_failure(tmp_path):
    data = [b"wrong data"]

    output = tmp_path / "file.txt"

    with pytest.raises(ValueError):
        save_stream(lambda: data, output, checksum="invalid")


# ---------------------------------------------------------
# RETRY SUCCESS (stream recreated)
# ---------------------------------------------------------
def test_retry(tmp_path):
    calls = {"count": 0}

    def faulty_stream():
        calls["count"] += 1

        if calls["count"] == 1:
            raise RuntimeError("fail")

        return [b"data"]

    output = tmp_path / "retry.txt"

    save_stream(faulty_stream, output)

    assert output.exists()
    assert calls["count"] == 2


# ---------------------------------------------------------
# STREAM FACTORY CALLED PER RETRY (STRICT)
# ---------------------------------------------------------
def test_stream_factory_called_per_retry(tmp_path):
    calls = {"count": 0}

    def factory():
        calls["count"] += 1

        if calls["count"] == 1:
            raise RuntimeError("fail")

        return [b"ok"]

    output = tmp_path / "file.txt"

    save_stream(factory, output)

    assert calls["count"] == 2
    assert output.read_bytes() == b"ok"


# ---------------------------------------------------------
# EMPTY STREAM (CRITICAL EDGE CASE)
# ---------------------------------------------------------
def test_empty_stream(tmp_path):
    def empty_stream():
        return []

    output = tmp_path / "empty.txt"

    with pytest.raises(ValueError, match="Empty stream"):
        save_stream(empty_stream, output)


# ---------------------------------------------------------
# TEMP FILE CLEANUP ON FAILURE
# ---------------------------------------------------------
def test_temp_cleanup_on_failure(tmp_path):
    def bad_stream():
        raise RuntimeError("boom")

    output = tmp_path / "fail.txt"

    with pytest.raises(RuntimeError):
        save_stream(bad_stream, output, retries=0)

    temp_file = tmp_path / "fail.txt.tmp"
    assert not temp_file.exists()


# ---------------------------------------------------------
# MULTIPLE CHUNKS
# ---------------------------------------------------------
def test_multiple_chunks(tmp_path):
    def stream():
        return [b"a", b"b", b"c"]

    output = tmp_path / "multi.txt"

    save_stream(stream, output)

    assert output.read_bytes() == b"abc"


# ---------------------------------------------------------
# LARGE STREAM SIMULATION
# ---------------------------------------------------------
def test_large_stream(tmp_path):
    def stream():
        for _ in range(1000):
            yield b"x" * 1024  # 1KB * 1000

    output = tmp_path / "large.bin"

    save_stream(stream, output)

    assert output.exists()
    assert output.stat().st_size == 1000 * 1024


# ---------------------------------------------------------
# PROGRESS BAR DISABLED (no total_bytes)
# ---------------------------------------------------------
def test_progress_bar_disabled(tmp_path):
    def stream():
        return [b"data"]

    output = tmp_path / "file.txt"

    save_stream(stream, output, total_bytes=None)

    assert output.exists()


# ---------------------------------------------------------
# INVALID HASH ALGORITHM
# ---------------------------------------------------------
def test_invalid_hash_algorithm(tmp_path):
    def stream():
        return [b"data"]

    output = tmp_path / "file.txt"

    with pytest.raises(ValueError):
        save_stream(stream, output, algorithm="invalid_algo")