# TrackTramReliability

A Python project to collect and store reliability data (delays, cancellations) for Munich MVG public transportation. It fetches station metadata and real-time departures from MVG endpoints and stores normalized records in a database for later analysis.

Sources:
- Stations API: https://www.mvg.de/.rest/zdm/stations
- Departures API: https://www.mvg.de/api/bgw-pt/v3/departures?globalId={station_id}

Status: MVP with CLI to fetch, cache, ingest, poll, and aggregate.

## Requirements
- Python 3.9+
- Internet access to MVG endpoints

## Installation
Important: The project folder name is `TrackTramReliablilty` (note the spelling).

From the repo root:

- Create a virtual environment and activate it
  - Linux/macOS:
    - `python -m venv .venv`
    - `source .venv/bin/activate`
  - Windows (PowerShell):
    - `python -m venv .venv`
    - `.venv\Scripts\Activate.ps1`

- Upgrade build tools and install the package in editable mode
  - `pip install -U pip setuptools wheel`
  - `pip install -e ./TrackTramReliablilty`

After install, the CLI command `ttr` becomes available.

Alternative install (from inside the project directory):
- `cd TrackTramReliablilty`
- `pip install -e .`

## Configuration
A sample config is provided at `config/example.yml`. Copy it to `config/local.yml` and adjust as needed.

- Default DB: `sqlite:///./data/reliability.db` (created automatically)
- Default cadence: `polling_interval_seconds: 300`
- Station selection: by names or by global IDs (IDs are used later when filtering by products)
- Log level: `INFO` (or `DEBUG`)

Environment variable overrides (take precedence over YAML):
- `TTR_DB_URL`
- `TTR_POLLING_INTERVAL_SECONDS`
- `TTR_LOG_LEVEL`
- `TTR_STATION_NAMES` (comma-separated)
- `TTR_STATION_IDS` (comma-separated)

## CLI Commands
Run `ttr --help` to see all commands.

- Show effective configuration
  - `ttr show-config [--config-file PATH]`

- Fetch and cache all station metadata
  - `ttr load-stations [--cache PATH]`
  - Default cache path: `TrackTramReliablilty/data/stations.json`

- Initialize the database schema
  - `ttr initdb [--config-file PATH]`

- Sync cached stations into the database
  - `ttr sync-stations [--config-file PATH] [--cache PATH]`

- Fetch departures for a single station (for inspection)
  - `ttr get-departures --station-id "de:09162:1" [--json-out]`

- Build a label->station_ids index from GTFS (speeds up label-specific ingests)
  - `ttr build-label-index --products TRAM --labels 27,28`
  - Options: `--gtfs URL_OR_PATH` (defaults to MVG GTFS), `--out data/label_index.json`, `--cache PATH`

- One-shot ingestion (filter by products, labels, and/or stations)
  - `ttr ingest --products ALL`
  - `ttr ingest --products TRAM`
  - `ttr ingest --products BUS,TRAM`
  - Restrict to specific lines: `ttr ingest --products BUS --labels 53,164`
  - Use GTFS index to avoid scanning all stations: `ttr ingest --products TRAM --labels 27,28 --use-label-index`
  - Limit to specific stations: `--station-names "Sendlinger Tor,Marienplatz"` or `--station-ids "de:09162:1,de:09162:2"`
  - Tune concurrency: `--max-workers 16`
  - Options: `--config-file PATH`, `--cache PATH`

- Continuous polling with graceful shutdown (Ctrl+C)
  - `ttr poll --products ALL`
  - `ttr poll --products BUS,TRAM --interval 300`
  - Poll with labels plus GTFS index: `ttr poll --products TRAM --labels 27,28 --use-label-index --interval 300`
  - Note: poll currently limits scope by products and labels but uses the generic poller loop; for heavy scoping, prefer one-shot ingests in cron, or we can extend the poller.
  - Options: `--config-file PATH`, `--cache PATH`, `--interval SECONDS`

- Aggregate basic reliability metrics
  - `ttr aggregate --scope line`
  - `ttr aggregate --scope station`
  - Options: `--config-file PATH`, `--no-json-out`

## Typical Workflow
1) Install and activate the environment (see Installation)
2) Cache stations: `ttr load-stations`
3) Initialize DB: `ttr initdb`
4) Sync stations to DB: `ttr sync-stations`
5) Ingest once or start polling:
   - Once: `ttr ingest --products TRAM`
   - Poll: `ttr poll --products BUS,TRAM --interval 300`
6) Compute metrics:
   - `ttr aggregate --scope line`
   - `ttr aggregate --scope station`

## Data Model (summary)
- `stations` (station_id PK, name, place, coordinates, products JSON, etc.)
- `departures_raw` (id, station_id FK, transport_type, label, destination, planned_ts, realtime_ts, delay_min, cancelled, platform, realtime, fetched_at)
  - Idempotency: unique constraint on (station_id, transport_type, label, destination, planned_departure_time)

## Notes and caveats
- Unofficial MVG endpoints; can change or be rate-limited.
- Delay is computed if not provided by API (`realtime - planned` in minutes).
- Stations cache (`data/stations.json`) is required for product-based filtering; refresh with `ttr load_stations`.

## Running Tests
From the repo root, after installation:
- `python -m unittest discover -s TrackTramReliablilty/tests -p 'test_*.py' -t . -v`

Or from within the project directory:
- `cd TrackTramReliablilty`
- `python -m unittest discover -s tests -p 'test_*.py' -v`

## Troubleshooting
- Editable install fails: Use the exact path `./TrackTramReliablilty` (note spelling) or `cd TrackTramReliablilty && pip install -e .`
- unittest discovery error "Start directory is not importable": either `cd TrackTramReliablilty` first, or add `-t .` when running discovery from repo root.
- SQLite file errors: ensure `data/` exists (created automatically) and you have write permissions.

## Roadmap ideas
- Date-range filtering and percentile (p90/p95) metrics in aggregations
- Station selection by names/IDs in config for targeted polling
- Dockerfile and CI workflow
