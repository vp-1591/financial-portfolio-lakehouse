# Trading 212 Connector Setup

## API Access

Trading 212 provides a REST API for retrieving account data, positions, and
instruments. Requires an API key and secret.

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `T212_API_KEY` | API key |
| `T212_API_SECRET` | API secret |
| `T212_BASE_URL` | Base URL (auto-derived from `--mode`) |

### Staging mode

In `--mode staging`, the connector uses `T212_API_KEY` and
`T212_API_SECRET` (injected from `/portfolio/demo/` SSM parameters in ECS,
or set in `.env` locally). The base URL is automatically set to the
demo endpoint.

## API key permissions

For the required API key permissions, see
[API Key Permissions](../trading212/api-key-permissions.md).
