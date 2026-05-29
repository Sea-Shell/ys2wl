# ys2wl — YouTube Subscription to Watch Later

Web service that scrapes YouTube subscriptions and routes new videos to
playlists based on configurable rules.

## Quick start

```sh
uv sync --dev
uv run python -m ys2wl
```

Open http://localhost:8080

## Configuration

Runtime config is stored in the SQLite DB and editable via the web UI at
`/ui#config`. Environment variables seed the DB on first run. Ignore lists
(subscription, video, words) are managed in the web UI — no more `.ignore` files.

| Variable | Default | Description |
|---|---|---|
| `YS2WL_API_PORT` | `8080` | HTTP listen port |
| `YS2WL_LOG_LEVEL` | `warning` | Log level |
| `YS2WL_LOG_FILE` | `stream` | Log output (`stream` for stdout) |
| `YS2WL_PICKLE_FILE` | `credentials.pickle` | OAuth token file path |
| `YS2WL_CREDENTIALS_FILE` | `client_secret.json` | Google OAuth client JSON |
| `YS2WL_DATABASE_FILE` | `ys2wl.db` | SQLite database path |
| `YS2WL_SCHEDULE` | `0 */6 * * *` | Cron expression for pipeline |
| `YS2WL_COMPARE_DISTANCE` | `80` | Title similarity threshold (0-100) |
| `YS2WL_REPROCESS_DAYS` | `2` | Days before re-processing a sub |
| `YS2WL_PLAYLIST_SLEEP` | `10` | Seconds between playlist API inserts |
| `YS2WL_SUBSCRIPTION_SLEEP` | `30` | Seconds between sub processing |
| `YS2WL_ACTIVITY_LIMIT` | `0` | Max activities per sub (0=unlimited) |
| `YS2WL_SUBSCRIPTION_LIMIT` | `0` | Max subs per run (0=unlimited) |
| `YS2WL_MINIMUM_LENGTH` | `0s` | Min video duration |
| `YS2WL_MAXIMUM_LENGTH` | `0s` | Max video duration |
| `YS2WL_PUBLISHED_AFTER` | — | ISO8601 date filter |
| `YS2WL_NO_WEBBROWSER` | `false` | Skip browser auth (headless mode) |
| `YS2WL_PIPELINE_CONCURRENCY` | `1` | Parallel pipelines |

## API

| Method | Path | Description |
|---|---|---|
| GET | `/api/health` | Health check |
| GET/PUT | `/api/config` | Get/update runtime config (DB-backed) |
| GET/POST | `/api/config/ignores` | List/add ignore entries |
| DELETE | `/api/config/ignores/{id}` | Delete an ignore entry |
| GET | `/api/auth/status` | OAuth status |
| POST | `/api/auth/device` | Start device auth flow |
| POST | `/api/auth/poll` | Poll for auth completion |
| GET | `/api/subscriptions` | List subscriptions |
| GET | `/api/subscriptions/{cid}/activity` | Channel activity |
| CRUD | `/api/rules` | Routing rules |
| POST | `/api/pipeline/trigger` | Trigger pipeline run |
| GET | `/api/pipeline/runs` | Pipeline run history |
| GET | `/api/pipeline/runs/{id}` | Run details |
| GET | `/metrics` | Prometheus metrics |

## Docker

```sh
make docker
# or
docker build -t ys2wl .
docker run -p 8080:8080 -v /path/to/data:/data ys2wl
```

## Development

```sh
make sync    # install dependencies
make test    # run tests
make lint    # ruff check
make format  # ruff format
make check   # lint + format check
```

Lint and format run automatically on commit via pre-commit:

```sh
uv tool install pre-commit
pre-commit install
```

## Kubernetes

Manifests in `k8s/` — deploy with `kubectl apply -f k8s/`.
