from entityresolver.ingestion.manifest import Manifest


def test_manifest_attempt_tracking(tmp_path):
    manifest = Manifest(tmp_path / "manifest.json")

    attempt_id = manifest.mark_started("file.txt", "source")
    manifest.mark_completed(attempt_id, size_bytes=100)

    file_record = manifest.get_file("file.txt")

    assert file_record["status"] == "completed"
    assert file_record["size_bytes"] == 100

    attempts = manifest.attempts_for("file.txt")
    assert len(attempts) == 1


def test_manifest_failure(tmp_path):
    manifest = Manifest(tmp_path / "manifest.json")

    attempt_id = manifest.mark_started("file.txt", "source")
    manifest.mark_failed(attempt_id, "error")

    file_record = manifest.get_file("file.txt")
    assert file_record["status"] == "failed"


def test_multiple_attempts(tmp_path):
    manifest = Manifest(tmp_path / "manifest.json")

    a1 = manifest.mark_started("file.txt", "source")
    manifest.mark_failed(a1, "error")

    a2 = manifest.mark_started("file.txt", "source")
    manifest.mark_completed(a2, size_bytes=200)

    attempts = manifest.attempts_for("file.txt")

    assert len(attempts) == 2

    latest = manifest.get_file("file.txt")
    assert latest["status"] == "completed"
    assert latest["size_bytes"] == 200