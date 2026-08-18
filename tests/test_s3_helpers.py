"""Tests for the S3 helper module."""

from __future__ import annotations

import pytest

from pipeline import s3 as s3_module
from pipeline.s3 import (
    delete_from_staging,
    parse_s3_uri,
    read_s3_bytes,
    upload_to_staging,
)


class TestParseS3Uri:
    """Tests for parse_s3_uri()."""

    def test_valid_uri_with_key(self) -> None:
        bucket, key = parse_s3_uri("s3://my-bucket/pipeline/staging/xtb/report.xlsx")
        assert bucket == "my-bucket"
        assert key == "pipeline/staging/xtb/report.xlsx"

    def test_valid_uri_with_short_key(self) -> None:
        bucket, key = parse_s3_uri("s3://my-bucket/file.txt")
        assert bucket == "my-bucket"
        assert key == "file.txt"

    def test_valid_uri_bucket_only(self) -> None:
        bucket, key = parse_s3_uri("s3://my-bucket")
        assert bucket == "my-bucket"
        assert key == ""

    def test_valid_uri_nested_key(self) -> None:
        bucket, key = parse_s3_uri(
            "s3://bucket-demo/pipeline_demo/staging_demo/xtb/2026-07.xlsx"
        )
        assert bucket == "bucket-demo"
        assert key == "pipeline_demo/staging_demo/xtb/2026-07.xlsx"

    def test_rejects_non_s3_uri(self) -> None:
        with pytest.raises(ValueError, match="Not an S3 URI"):
            parse_s3_uri("/local/path/file.xlsx")

    def test_rejects_http_uri(self) -> None:
        with pytest.raises(ValueError, match="Not an S3 URI"):
            parse_s3_uri("https://bucket.s3.amazonaws.com/key")

    def test_rejects_empty_string(self) -> None:
        with pytest.raises(ValueError, match="Not an S3 URI"):
            parse_s3_uri("")


class _FakeStream:
    """Minimal stream stand-in for ``pyarrow.fs.S3FileSystem.open_input_stream``."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self.closed = False

    def read(self) -> bytes:
        return self._data

    def close(self) -> None:
        self.closed = True


class _FakeS3FileSystem:
    """In-memory stand-in for ``pyarrow.fs.S3FileSystem``.

    Stores objects keyed by ``"bucket/key"``.  Raises on missing keys for
    reads/deletes so the ``delete_from_staging`` swallow-path can be
    exercised.  Records call arguments so tests can assert on them.
    """

    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}
        self.copy_calls: list[tuple[str, str]] = []
        self.copy_kwargs: list[dict[str, object]] = []
        self.delete_calls: list[str] = []
        self.raise_on_delete: bool = False

    def copy_files(self, source: str, destination: str, **kwargs: object) -> None:
        self.copy_calls.append((source, destination))
        self.copy_kwargs.append(kwargs)
        with open(source, "rb") as fh:
            self.store[destination] = fh.read()

    def open_input_stream(self, remote_path: str) -> _FakeStream:
        if remote_path not in self.store:
            raise FileNotFoundError(remote_path)
        return _FakeStream(self.store[remote_path])

    def delete(self, remote_path: str) -> None:
        self.delete_calls.append(remote_path)
        if self.raise_on_delete:
            raise RuntimeError("simulated S3 delete failure")
        if remote_path not in self.store:
            raise FileNotFoundError(remote_path)
        del self.store[remote_path]


@pytest.fixture()
def _fake_s3fs(monkeypatch: pytest.MonkeyPatch):
    """Patch ``_make_s3fs`` and ``pafs.copy_files`` to use an in-memory fake."""
    fake = _FakeS3FileSystem()
    monkeypatch.setattr(s3_module, "_make_s3fs", lambda: fake)
    monkeypatch.setattr(s3_module.pafs, "copy_files", fake.copy_files)
    return fake


class TestUploadToStaging:
    """Tests for ``upload_to_staging`` (s3.py:60-90)."""

    def test_uploads_file_and_returns_uri(self, tmp_path, _fake_s3fs) -> None:
        local = tmp_path / "report.xlsx"
        local.write_bytes(b"payload-bytes")
        uri = "s3://my-bucket/pipeline/staging/xtb/report.xlsx"

        result = upload_to_staging(str(local), uri)

        assert result == uri
        # Object stored under bucket/key (no s3:// scheme) with file content.
        assert _fake_s3fs.store == {
            "my-bucket/pipeline/staging/xtb/report.xlsx": b"payload-bytes"
        }
        assert _fake_s3fs.copy_calls == [
            (str(local), "my-bucket/pipeline/staging/xtb/report.xlsx")
        ]
        # The S3 filesystem is passed as the copy destination.
        assert _fake_s3fs.copy_kwargs == [{"destination_filesystem": _fake_s3fs}]

    def test_missing_local_file_raises_file_not_found(self, _fake_s3fs) -> None:
        with pytest.raises(FileNotFoundError, match="File not found"):
            upload_to_staging("/does/not/exist.xlsx", "s3://bucket/key.xlsx")
        # No upload attempted when the local file is missing.
        assert _fake_s3fs.copy_calls == []

    def test_non_s3_uri_raises_value_error(self, tmp_path, _fake_s3fs) -> None:
        local = tmp_path / "report.xlsx"
        local.write_bytes(b"x")
        with pytest.raises(ValueError, match="Not an S3 URI"):
            upload_to_staging(str(local), "/local/path/key.xlsx")
        assert _fake_s3fs.copy_calls == []


class TestReadS3Bytes:
    """Tests for ``read_s3_bytes`` (s3.py:108-119)."""

    def test_reads_content_and_filename(self, tmp_path, _fake_s3fs) -> None:
        _fake_s3fs.store["my-bucket/pipeline/staging/xtb/report.xlsx"] = b"contents"
        content, filename = read_s3_bytes(
            "s3://my-bucket/pipeline/staging/xtb/report.xlsx"
        )
        assert content == b"contents"
        assert filename == "report.xlsx"

    def test_filename_for_key_without_slash(self, _fake_s3fs) -> None:
        # Key with no slash -> filename is the whole key.
        _fake_s3fs.store["my-bucket/file.txt"] = b"abc"
        _, filename = read_s3_bytes("s3://my-bucket/file.txt")
        assert filename == "file.txt"

    def test_missing_object_raises(self, _fake_s3fs) -> None:
        with pytest.raises(FileNotFoundError):
            read_s3_bytes("s3://my-bucket/missing.bin")

    def test_stream_closed_after_read(self, _fake_s3fs) -> None:
        _fake_s3fs.store["my-bucket/dir/report.xlsx"] = b"data"
        stream_holder: list[_FakeStream] = []

        original_open = _fake_s3fs.open_input_stream

        def capturing_open(remote_path: str) -> _FakeStream:
            stream = original_open(remote_path)
            stream_holder.append(stream)
            return stream

        _fake_s3fs.open_input_stream = capturing_open
        read_s3_bytes("s3://my-bucket/dir/report.xlsx")
        assert stream_holder and stream_holder[0].closed is True

    def test_stream_closed_even_when_read_raises(self, _fake_s3fs) -> None:
        """The ``finally`` branch closes the stream even when ``read`` raises."""

        class _RaisingStream(_FakeStream):
            def read(self) -> bytes:  # type: ignore[override]
                raise RuntimeError("read failure")

        original_open = _fake_s3fs.open_input_stream
        holder: list[_RaisingStream] = []

        def capturing_open(remote_path: str) -> _RaisingStream:
            stream = _RaisingStream(b"")
            holder.append(stream)
            return stream

        _fake_s3fs.open_input_stream = capturing_open
        try:
            with pytest.raises(RuntimeError, match="read failure"):
                read_s3_bytes("s3://my-bucket/dir/report.xlsx")
        finally:
            _fake_s3fs.open_input_stream = original_open
        assert holder and holder[0].closed is True


class TestDeleteFromStaging:
    """Tests for ``delete_from_staging`` (s3.py:128-134)."""

    def test_deletes_existing_object(self, _fake_s3fs) -> None:
        _fake_s3fs.store["my-bucket/pipeline/staging/xtb/old.xlsx"] = b"x"
        delete_from_staging("s3://my-bucket/pipeline/staging/xtb/old.xlsx")
        assert _fake_s3fs.delete_calls == ["my-bucket/pipeline/staging/xtb/old.xlsx"]
        assert "my-bucket/pipeline/staging/xtb/old.xlsx" not in _fake_s3fs.store

    def test_failed_delete_does_not_raise(self, _fake_s3fs) -> None:
        """The broad ``except Exception`` swallow is the documented contract:
        staging cleanup is best-effort and must not block the pipeline."""
        _fake_s3fs.raise_on_delete = True
        # Must not raise even though the underlying delete fails.
        delete_from_staging("s3://my-bucket/pipeline/staging/xtb/gone.xlsx")
        assert _fake_s3fs.delete_calls == ["my-bucket/pipeline/staging/xtb/gone.xlsx"]

    def test_missing_object_does_not_raise(self, _fake_s3fs) -> None:
        # FileNotFoundError is inside the broad except -> swallowed.
        delete_from_staging("s3://my-bucket/pipeline/staging/xtb/missing.xlsx")
        assert _fake_s3fs.delete_calls == [
            "my-bucket/pipeline/staging/xtb/missing.xlsx"
        ]
