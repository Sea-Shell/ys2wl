# sortarr — YouTube Subscription to Watch Later

Web service that scrapes YouTube subscriptions and routes new videos to
playlists based on configurable rules.

## Quick start

```sh
uv sync --dev
uv run python -m sortarr
```

Open http://localhost:8080

## Configuration

Runtime config is stored in the SQLite DB and editable via the web UI at
`/ui#config`. Environment variables seed the DB on first run. Ignore lists
(subscription, video, words) are managed in the web UI — no more `.ignore` files.

| Variable | Default | Description |
|---|---|---|
| `SORTARR_API_PORT` | `8080` | HTTP listen port |
| `SORTARR_LOG_LEVEL` | `warning` | Log level |
| `SORTARR_LOG_FILE` | `stream` | Log output (`stream` for stdout) |
| `SORTARR_PICKLE_FILE` | `credentials.pickle` | OAuth token file path |
| `SORTARR_CREDENTIALS_FILE` | `client_secret.json` | Google OAuth client JSON |
| `SORTARR_DATABASE_FILE` | `sortarr.db` | SQLite database path |
| `SORTARR_SCHEDULE` | `0 */6 * * *` | Cron expression for pipeline |
| `SORTARR_COMPARE_DISTANCE` | `80` | Title similarity threshold (0-100) |
| `SORTARR_REPROCESS_DAYS` | `2` | Days before re-processing a sub |
| `SORTARR_PLAYLIST_SLEEP` | `10` | Seconds between playlist API inserts |
| `SORTARR_SUBSCRIPTION_SLEEP` | `30` | Seconds between sub processing |
| `SORTARR_ACTIVITY_LIMIT` | `0` | Max activities per sub (0=unlimited) |
| `SORTARR_SUBSCRIPTION_LIMIT` | `0` | Max subs per run (0=unlimited) |
| `SORTARR_MINIMUM_LENGTH` | `0s` | Min video duration |
| `SORTARR_MAXIMUM_LENGTH` | `0s` | Max video duration |
| `SORTARR_PUBLISHED_AFTER` | — | ISO8601 date filter |
| `SORTARR_NO_WEBBROWSER` | `false` | Skip browser auth (headless mode) |
| `SORTARR_PIPELINE_CONCURRENCY` | `1` | Parallel pipelines |

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
| GET | `/api/videos/{video_id}` | Lookup video by ID |
| GET | `/metrics` | Prometheus metrics |

## Docker

```sh
make docker
# or
docker build -t sortarr .
docker run -p 8080:8080 -v /path/to/data:/data sortarr
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

## 🕑 Scheduling & Pipeline

- **Automated schedule:** The pipeline now runs via an internal scheduler (APScheduler) started with the app process. It is configured using the `SORTARR_SCHEDULE` environment variable (default: every 6 hours, `0 */6 * * *`).
- **No external cron job required:** You do not need a separate Kubernetes CronJob — the web service handles scheduled pipeline runs automatically.
- **Manual triggers:** Hitting the `/api/pipeline/trigger` endpoint or using the UI runs the exact same pipeline logic as the scheduler.
- **Shared logic:** Both automatic and manual runs use the same core execution pathway for reliability and DRYness.

## Type Checking

To run mypy type checks (if installed):

```sh
uv run mypy src/sortarr/
# Or if defined: make type-check
```

## Kubernetes

Manifests in `k8s/` — deploy with `kubectl apply -f k8s/`.