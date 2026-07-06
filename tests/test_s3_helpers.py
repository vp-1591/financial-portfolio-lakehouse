"""Tests for the S3 helper module."""

from __future__ import annotations

import pytest

from pipeline.s3 import parse_s3_uri


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
