# Trading 212 Connector Setup

## API Access

Trading 212 provides a REST API for retrieving account data, positions, and
instruments. Requires an API key and secret.

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `T212_API_KEY` | API key |
| `T212_API_SECRET` | API secret |
| `T212_BASE_URL` | Base URL (auto-derived from `DEMO` setting) |
| `T212_ENABLED` | Enable/disable connector (default: enabled) |

### Demo mode

When `DEMO=true`, the connector uses `T212_API_KEY_DEMO` and
`T212_API_SECRET_DEMO` instead. The base URL is automatically set to the
demo endpoint.

## API key permissions

For the required API key permissions, see
[API Key Permissions](../trading212/api-key-permissions.md).
