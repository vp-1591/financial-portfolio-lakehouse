"""Tests for S3 credential configuration in query.py."""

import os
from unittest.mock import Mock, patch

import duckdb
import pytest

from pipeline.query import _configure_s3
from pipeline.secrets import (
    AwsCredentials,
    _boto3_default_chain_credentials,
    resolve_aws_credentials,
    set_mode,
)


def _secret_fields(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Return the per-field values of the DuckDB S3 SECRET.

    Parses the ``secret_string`` column of ``duckdb_secrets()`` (the last
    column) into a ``name=value`` dict.  This reads the SECRET's actual
    field assignment -- unlike ``current_setting('s3_*')`` which reads
    from the AWS environment variables (DuckDB's S3 extension auto-reads
    ``AWS_ACCESS_KEY_ID`` / ``AWS_REGION``) and so cannot detect a
    KEY_ID/REGION swap inside the SECRET.  A substring ``in str(row)``
    assertion (A5 W4/C7) likewise passes when a value lands in the wrong
    field; per-field extraction from ``secret_string`` does not.

    The ``secret`` value is redacted by DuckDB (``secret=redacted``) so it
    cannot be verified here; KEY_ID and REGION are available verbatim.
    """
    rows = conn.execute("SELECT * FROM duckdb_secrets() WHERE type = 's3'").fetchall()
    assert rows, "expected at least one S3 secret"
    secret_string = rows[0][6]
    fields: dict[str, str] = {}
    for pair in secret_string.split(";"):
        if "=" in pair:
            key, value = pair.split("=", 1)
            fields[key] = value
    return fields


class TestConfigureS3:
    """Verify _configure_s3 uses DuckDB SECRET mechanism."""

    def setup_method(self):
        """Clear the functools.cache between tests so each sees fresh env vars."""
        resolve_aws_credentials.cache_clear()

    def test_creates_s3_secret(self):
        """_configure_s3 should create a DuckDB S3 secret when credentials are present."""
        conn = duckdb.connect()
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "test-key-id",
                "AWS_SECRET_ACCESS_KEY": "test-secret",
                "AWS_REGION": "eu-west-1",
            },
        ):
            set_mode("docker")
            _configure_s3(conn)

        secrets = conn.execute("SELECT * FROM duckdb_secrets()").fetchall()
        # DuckDB stores the type as lowercase 's3'
        s3_secrets = [s for s in secrets if s[1] == "s3"]
        assert len(s3_secrets) >= 1, f"Expected at least one S3 secret, got: {secrets}"

        # Per-field verification: extract KEY_ID/REGION from the SECRET's
        # secret_string rather than substring-matching the whole row (A5
        # W4/C7). A KEY_ID/REGION swap mutation would place "test-key-id" in
        # the REGION field and still satisfy a substring ``in str(row)``
        # assertion, but per-field extraction catches the field misassignment.
        fields = _secret_fields(conn)
        assert fields["key_id"] == "test-key-id", f"KEY_ID field: {fields}"
        assert fields["region"] == "eu-west-1", f"REGION field: {fields}"
        conn.close()

    def test_uses_region_from_env(self):
        """_configure_s3 should use AWS_REGION env var."""
        conn = duckdb.connect()
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "test-key-id",
                "AWS_SECRET_ACCESS_KEY": "test-secret",
                "AWS_REGION": "us-east-1",
            },
        ):
            set_mode("docker")
            _configure_s3(conn)

        secrets = conn.execute(
            "SELECT * FROM duckdb_secrets() WHERE type = 's3'"
        ).fetchall()
        assert len(secrets) >= 1
        # Per-field check (A5 W4/C7): region must land in the REGION field,
        # not merely appear somewhere in the row string.
        fields = _secret_fields(conn)
        assert fields["region"] == "us-east-1", f"REGION field: {fields}"
        conn.close()

    def test_default_region(self):
        """_configure_s3 should default to eu-west-1 when AWS_REGION is unset."""
        conn = duckdb.connect()
        env = {
            "AWS_ACCESS_KEY_ID": "test-key-id",
            "AWS_SECRET_ACCESS_KEY": "test-secret",
        }
        # Ensure AWS_REGION is absent so default kicks in
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("AWS_REGION", None)
            set_mode("docker")
            _configure_s3(conn)

        secrets = conn.execute(
            "SELECT * FROM duckdb_secrets() WHERE type = 's3'"
        ).fetchall()
        assert len(secrets) >= 1
        # Per-field check (A5 W4/C7): default region must be in REGION field.
        fields = _secret_fields(conn)
        assert fields["region"] == "eu-west-1", f"REGION field: {fields}"
        conn.close()

    def test_secret_propagates_to_s3_settings(self):
        """DuckDB SECRET credentials should be accessible to delta_scan().

        Verify that CREATE SECRET propagates to the s3_* settings that
        extensions can read, unlike the legacy SET approach which only
        affected DuckDB's built-in httpfs.
        """
        conn = duckdb.connect()
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "test-key-id",
                "AWS_SECRET_ACCESS_KEY": "test-secret",
                "AWS_REGION": "eu-west-1",
            },
        ):
            set_mode("docker")
            _configure_s3(conn)

        # After CREATE SECRET, DuckDB should propagate credentials
        # so they're available to extensions like delta_scan()
        key_id = conn.execute("SELECT current_setting('s3_access_key_id')").fetchone()[
            0
        ]
        assert key_id == "test-key-id", f"S3 key should come from secret, got: {key_id}"

        region = conn.execute("SELECT current_setting('s3_region')").fetchone()[0]
        assert region == "eu-west-1", (
            f"S3 region should come from secret, got: {region}"
        )
        conn.close()

    def test_raises_when_credentials_absent(self):
        """When AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are not set,
        _configure_s3 should raise RuntimeError with an actionable message.

        DuckDB's delta_scan() cannot resolve credentials from
        ~/.aws/credentials or AWS SSO, so silently skipping SECRET
        creation leads to confusing IMDS timeout errors.  Raising an
        error with a clear message tells the user exactly what to do.
        """
        conn = duckdb.connect()
        with patch.dict(os.environ, {"AWS_REGION": "eu-west-1"}, clear=False):
            os.environ.pop("AWS_ACCESS_KEY_ID", None)
            os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
            set_mode("docker")
            with pytest.raises(RuntimeError, match="AWS credentials not found"):
                _configure_s3(conn)
        conn.close()

    def test_raises_when_credentials_empty(self):
        """When AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are set to empty
        strings, _configure_s3 should raise RuntimeError.

        Empty-string credentials are normalized to None by
        resolve_aws_credentials(), so the same missing-credential
        error applies.
        """
        conn = duckdb.connect()
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "",
                "AWS_SECRET_ACCESS_KEY": "",
                "AWS_REGION": "eu-west-1",
            },
        ):
            set_mode("docker")
            with pytest.raises(RuntimeError, match="AWS credentials not found"):
                _configure_s3(conn)
        conn.close()

    def test_staging_mode_uses_credentials(self):
        """In staging mode, _configure_s3 uses AWS credentials from env vars."""
        conn = duckdb.connect()
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "staging-key-id",
                "AWS_SECRET_ACCESS_KEY": "staging-secret",
                "AWS_REGION": "eu-west-1",
            },
        ):
            set_mode("staging")
            _configure_s3(conn)

        secrets = conn.execute(
            "SELECT * FROM duckdb_secrets() WHERE type = 's3'"
        ).fetchall()
        assert len(secrets) >= 1, f"Expected S3 secret in staging mode, got: {secrets}"
        # Per-field verification (A5 W4/C7): KEY_ID and REGION must land in
        # their own fields, not merely appear somewhere in the row string.
        fields = _secret_fields(conn)
        assert fields["key_id"] == "staging-key-id", f"KEY_ID field: {fields}"
        assert fields["region"] == "eu-west-1", f"REGION field: {fields}"
        conn.close()

    def test_staging_mode_no_credentials_creates_empty_secret(self):
        """In staging mode with missing credentials, a SECRET with empty
        credentials is created to prevent DuckDB from falling back to
        any credentials in environment variables.

        This tests the core isolation guarantee (A5 C4): if AWS credentials
        are missing, the pipeline must NOT fall back to credentials from a
        different environment. We verify the CONTENT of the SECRET's
        credential fields is empty -- not merely that a SECRET exists -- so
        a mutation leaking production credentials into the staging SECRET fails.
        """
        conn = duckdb.connect()
        with patch.dict(os.environ, {"AWS_REGION": "eu-west-1"}, clear=False):
            os.environ.pop("AWS_ACCESS_KEY_ID", None)
            os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
            set_mode("staging")
            _configure_s3(conn)

        # A SECRET should be created with EMPTY credentials (content check,
        # not just existence -- A5 C4/W1), preventing DuckDB from falling
        # back to any env vars. A mutation that injects non-empty (e.g.
        # leaked production) credentials here must fail this assertion.
        secrets = conn.execute(
            "SELECT * FROM duckdb_secrets() WHERE type = 's3'"
        ).fetchall()
        assert len(secrets) >= 1, (
            f"Expected at least one S3 secret with empty credentials, got: {secrets}"
        )
        # Content check (A5 C4/W1): the SECRET's KEY_ID field must be EMPTY
        # (not merely present), so a mutation that injects non-empty (e.g.
        # leaked production) credentials into the staging SECRET fails. DuckDB
        # redacts the SECRET value (``secret=redacted``) so only KEY_ID can
        # be verified verbatim from the secret_string.
        fields = _secret_fields(conn)
        assert fields["key_id"] == "", (
            "staging SECRET KEY_ID must be empty so DuckDB cannot fall back to "
            f"production credentials; got: {fields!r}"
        )
        assert fields["region"] == "eu-west-1", f"REGION field: {fields}"
        conn.close()

    def test_prod_mode_falls_back_to_boto3(self):
        """In prod with env vars absent, _configure_s3 discovers credentials
        via boto3's default chain (e.g. AWS SSO / ~/.aws/credentials) and
        creates a DuckDB SECRET with them — including the SESSION_TOKEN —
        instead of raising.
        """
        conn = duckdb.connect()
        frozen = Mock()
        frozen.access_key = "sso-key"
        frozen.secret_key = "sso-secret"
        frozen.token = "sso-token"
        creds = Mock()
        creds.get_frozen_credentials.return_value = frozen
        session = Mock()
        session.get_credentials.return_value = creds
        with patch.dict(os.environ, {"AWS_REGION": "eu-west-1"}, clear=False):
            os.environ.pop("AWS_ACCESS_KEY_ID", None)
            os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
            set_mode("prod")
            with patch("boto3.Session", return_value=session):
                _configure_s3(conn)

        fields = _secret_fields(conn)
        assert fields["key_id"] == "sso-key", (
            f"KEY_ID should come from boto3 chain; got: {fields!r}"
        )
        # DuckDB redacts the SESSION_TOKEN value (like SECRET), so we can only
        # verify the field is present here; the actual value/emission is
        # verified in TestAwsCredentialsSessionToken.
        assert "session_token" in fields, (
            f"SESSION_TOKEN should be registered in the SECRET; got: {fields!r}"
        )
        conn.close()

    def test_prod_mode_raises_when_boto3_also_empty(self):
        """Prod with env vars absent AND boto3 finds no credentials -> the
        RuntimeError is raised (final fallback, actionable message)."""
        conn = duckdb.connect()
        session = Mock()
        session.get_credentials.return_value = None
        with patch.dict(os.environ, {"AWS_REGION": "eu-west-1"}, clear=False):
            os.environ.pop("AWS_ACCESS_KEY_ID", None)
            os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
            set_mode("prod")
            with (
                patch("boto3.Session", return_value=session),
                pytest.raises(RuntimeError, match="AWS credentials not found"),
            ):
                _configure_s3(conn)
        conn.close()

    def test_prod_mode_env_vars_skip_boto3(self):
        """Prod with env vars set: env vars win, the boto3 fallback helper is
        never invoked."""
        conn = duckdb.connect()
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "env-key",
                "AWS_SECRET_ACCESS_KEY": "env-secret",
                "AWS_REGION": "eu-west-1",
            },
        ):
            set_mode("prod")
            with patch("pipeline.secrets._boto3_default_chain_credentials") as helper:
                _configure_s3(conn)
                helper.assert_not_called()
        conn.close()


class TestBoto3DefaultChainCredentials:
    """Unit tests for the shared boto3 default-chain credential helper."""

    def _session(
        self,
        access_key: str,
        secret_key: str,
        token: str | None = None,
        *,
        has_creds: bool = True,
    ) -> Mock:
        """Build a mock boto3.Session with the given frozen credentials."""
        frozen = Mock()
        frozen.access_key = access_key
        frozen.secret_key = secret_key
        frozen.token = token
        creds = Mock()
        creds.get_frozen_credentials.return_value = frozen
        session = Mock()
        session.get_credentials.return_value = creds if has_creds else None
        return session

    def test_returns_credentials_with_token(self):
        with patch(
            "boto3.Session",
            return_value=self._session("ak", "sk", "tok"),
        ) as session_cls:
            result = _boto3_default_chain_credentials("eu-west-1")
        assert result == ("ak", "sk", "tok")
        session_cls.assert_called_once_with(region_name="eu-west-1")

    def test_returns_credentials_without_token(self):
        with patch(
            "boto3.Session",
            return_value=self._session("ak", "sk", None),
        ):
            result = _boto3_default_chain_credentials("eu-west-1")
        assert result == ("ak", "sk", None)

    def test_returns_none_when_no_credentials(self):
        with patch(
            "boto3.Session",
            return_value=self._session("", "", has_creds=False),
        ):
            result = _boto3_default_chain_credentials("eu-west-1")
        assert result is None

    def test_returns_none_when_access_key_empty(self):
        with patch("boto3.Session", return_value=self._session("", "sk")):
            result = _boto3_default_chain_credentials("eu-west-1")
        assert result is None


class TestAwsCredentialsSessionToken:
    """The session_token (SSO/temporary creds) threads through all adapters."""

    def _creds(self, session_token: str | None) -> AwsCredentials:
        return AwsCredentials(
            key_id="k",
            secret_key="s",
            region="eu-west-1",
            endpoint_url=None,
            allow_http=False,
            session_token=session_token,
        )

    def test_to_storage_options_includes_session_token(self):
        opts = self._creds("tok").to_storage_options()
        assert opts["aws_session_token"] == "tok"

    def test_to_storage_options_omits_session_token_when_none(self):
        opts = self._creds(None).to_storage_options()
        assert "aws_session_token" not in opts

    def test_to_pyarrow_kwargs_includes_session_token(self):
        assert self._creds("tok").to_pyarrow_kwargs()["session_token"] == "tok"

    def test_to_pyarrow_kwargs_omits_session_token_when_none(self):
        assert "session_token" not in self._creds(None).to_pyarrow_kwargs()

    def test_to_duckdb_secret_parts_includes_session_token(self):
        assert "SESSION_TOKEN 'tok'" in self._creds("tok").to_duckdb_secret_parts()

    def test_to_duckdb_secret_parts_omits_session_token_when_none(self):
        parts = self._creds(None).to_duckdb_secret_parts()
        assert not any(p.startswith("SESSION_TOKEN") for p in parts)

    def test_to_duckdb_secret_parts_escapes_single_quotes(self):
        parts = self._creds("a'b").to_duckdb_secret_parts()
        assert "SESSION_TOKEN 'a''b'" in parts
