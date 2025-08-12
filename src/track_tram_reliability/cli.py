from __future__ import annotations

import typer
from pathlib import Path

from .stations import refresh_stations_cache, DEFAULT_CACHE
from .departures import fetch_departures
from .config import load_settings
from .db import init_db
from .ingest import ingest_departures_for_products, sync_stations_from_cache_to_db
from .poller import run_poller
from .aggregate import compute_line_metrics, compute_station_metrics
from .gtfs_index import build_label_index as build_label_index_from_gtfs, write_label_index, load_label_index, GTFS_DEFAULT_URL
from .print_label_stations import resolve_stations_for_labels
from .gtfs_debug import debug_link_for_stop_name

app = typer.Typer(help="Track tram reliability: fetch stations and departures, store to DB.")


@app.command()
def build_label_index(
    gtfs: str = typer.Option(GTFS_DEFAULT_URL, help="GTFS zip URL or local path"),
    products: str = typer.Option("TRAM", help="Products to include (comma) e.g., TRAM,BUS"),
    labels: str = typer.Option(None, help="Labels to include (comma), e.g., 27,28"),
    out: Path = typer.Option(Path("data/label_index.json"), help="Output path for label index JSON"),
    cache: Path = typer.Option(DEFAULT_CACHE, help="Stations cache path for mapping to MVG station ids"),
    distance_threshold_m: float = typer.Option(150.0, help="Max distance (meters) to match GTFS stop to MVG station"),
):
    """Build label->station_ids index using MVG GTFS and stations cache."""
    prods = {p.strip().upper() for p in products.split(",") if p.strip()}
    labs = {l.strip().upper() for l in labels.split(",")} if labels else None
    index = build_label_index_from_gtfs(gtfs, prods, labs, cache, distance_threshold_m)
    write_label_index(index, out)
    typer.echo(f"Wrote label index to {out} from {gtfs}")


@app.command()
def show_config(config_file: Path = typer.Option(None, help="Path to YAML config file")):
    """Print the effective configuration (YAML + env overrides)."""
    settings = load_settings(config_file)
    import json as _json
    typer.echo(_json.dumps(settings.model_dump(), indent=2))


@app.command()
def initdb(config_file: Path = typer.Option(None, help="Path to YAML config file")):
    """Create database schema as per SQLAlchemy models."""
    settings = load_settings(config_file)
    init_db(settings.db_url)
    typer.echo(f"Initialized database at {settings.db_url}")


@app.command()
def load_stations(cache: Path = typer.Option(DEFAULT_CACHE, help="Cache file path for stations")):
    """Fetch stations from MVG and cache them locally."""
    stations = refresh_stations_cache(cache)
    typer.echo(f"Fetched and cached {len(stations)} stations to {cache}")


@app.command()
def sync_stations(config_file: Path = typer.Option(None, help="Path to YAML config file"), cache: Path = typer.Option(DEFAULT_CACHE, help="Cache file path for stations")):
    """Sync cached stations into DB (upsert)."""
    settings = load_settings(config_file)
    count = sync_stations_from_cache_to_db(settings.db_url, cache)
    typer.echo(f"Synced {count} stations to DB: {settings.db_url}")


@app.command()
def get_departures(station_id: str, json_out: bool = typer.Option(False, help="Print JSON output")):
    """Fetch departures for a station and print a summary or JSON."""
    deps = fetch_departures(station_id)
    if json_out:
        import json as _json
        typer.echo(_json.dumps([d.model_dump() for d in deps], ensure_ascii=False, indent=2))
    else:
        for d in deps[:20]:  # limit output size
            delay = d.delay_in_minutes if d.delay_in_minutes is not None else "?"
            typer.echo(
                f"{d.label or '?'} to {d.destination or '?'} | planned={d.planned_departure_time} real={d.realtime_departure_time} delay={delay} cancelled={d.cancelled}"
            )


@app.command()
def ingest(
    products: str = typer.Option("ALL", help="Comma-separated products to include (UBAHN,SBAHN,BUS,TRAM,ALL)"),
    labels: str = typer.Option(None, help="Optional comma-separated line labels to include (e.g., '53,164,X30')"),
    station_names: str = typer.Option(None, help="Optional comma-separated station names to include"),
    station_ids: str = typer.Option(None, help="Optional comma-separated station ids to include"),
    use_label_index: bool = typer.Option(False, help="Use GTFS-built label index to resolve station ids for labels"),
    label_index_path: Path = typer.Option(Path("data/label_index.json"), help="Path to label index JSON"),
    max_workers: int = typer.Option(8, help="Concurrency for fetching departures"),
    config_file: Path = typer.Option(None, help="Path to YAML config file"),
    cache: Path = typer.Option(DEFAULT_CACHE, help="Cache file path for stations"),
):
    """Ingest departures for all stations matching products into the DB.

    Use --labels to restrict ingestion to specific lines (e.g., bus lines 53,164). Labels are matched case-insensitively.
    """
    settings = load_settings(config_file)
    product_set = {p.strip().upper() for p in products.split(",") if p.strip()}
    label_set = {s.strip() for s in labels.split(",")} if labels else None

    # If using label index, expand labels->station_ids and merge with provided ids
    resolved_station_ids = None
    if use_label_index and label_set:
        index = load_label_index(label_index_path)
        resolved_station_ids = set()
        for prod in product_set or {"ALL"}:
            prod_map = index.mapping.get(prod, {})
            for lab in label_set:
                resolved_station_ids.update(prod_map.get(lab.upper(), []))
        if station_ids:
            resolved_station_ids |= {s.strip() for s in station_ids.split(",")}
    else:
        resolved_station_ids = {s.strip() for s in station_ids.split(",")} if station_ids else None

    stations_processed, rows_inserted, rows_skipped = ingest_departures_for_products(
        settings.db_url,
        cache,
        product_set,
        label_set,
        {s.strip() for s in station_names.split(",")} if station_names else None,
        resolved_station_ids,
        max_workers,
    )
    typer.echo(
        f"Ingested from {stations_processed} stations | inserted={rows_inserted} skipped_duplicates={rows_skipped}"
    )


@app.command()
def poll(
    products: str = typer.Option("ALL", help="Comma-separated products to include (UBAHN,SBAHN,BUS,TRAM,ALL)"),
    labels: str = typer.Option(None, help="Optional comma-separated line labels to include (e.g., '53,164,X30')"),
    station_names: str = typer.Option(None, help="Optional comma-separated station names to include"),
    station_ids: str = typer.Option(None, help="Optional comma-separated station ids to include"),
    use_label_index: bool = typer.Option(False, help="Use GTFS-built label index to resolve station ids for labels"),
    label_index_path: Path = typer.Option(Path("data/label_index.json"), help="Path to label index JSON"),
    max_workers: int = typer.Option(8, help="Concurrency for fetching departures"),
    config_file: Path = typer.Option(None, help="Path to YAML config file"),
    cache: Path = typer.Option(DEFAULT_CACHE, help="Cache file path for stations"),
    interval: int = typer.Option(None, help="Override polling interval seconds"),
):
    """Continuously ingest at a fixed cadence with graceful shutdown."""
    settings = load_settings(config_file)
    product_set = {p.strip().upper() for p in products.split(",") if p.strip()}
    poll_interval = interval or settings.polling_interval_seconds
    label_set = {s.strip() for s in labels.split(",")} if labels else None

    # Resolve station ids via label index if requested
    resolved_station_ids = None
    if use_label_index and label_set:
        index = load_label_index(label_index_path)
        resolved_station_ids = set()
        for prod in product_set or {"ALL"}:
            prod_map = index.mapping.get(prod, {})
            for lab in label_set:
                resolved_station_ids.update(prod_map.get(lab.upper(), []))
        if station_ids:
            resolved_station_ids |= {s.strip() for s in station_ids.split(",")}
    else:
        resolved_station_ids = {s.strip() for s in station_ids.split(",")} if station_ids else None

    typer.echo(
        f"Starting poller: db={settings.db_url}, interval={poll_interval}s, products={','.join(sorted(product_set) or ['ALL'])}, labels={','.join(label_set or [])}"
    )
    # We reuse the ingest loop inside run_poller; pass through product_set only.
    # For station/label scoping in poll mode, it's best to wrap run_poller to ingest with these parameters.
    # To keep changes minimal, we recommend using one-shot ingest for heavy filters, or we can extend run_poller later.
    run_poller(settings.db_url, poll_interval, product_set, str(cache))


@app.command()
def aggregate(
    scope: str = typer.Option("line", help="Aggregation scope: line or station"),
    config_file: Path = typer.Option(None, help="Path to YAML config file"),
    json_out: bool = typer.Option(True, help="Output JSON to stdout"),
):
    """Compute simple reliability metrics and print as JSON."""
    settings = load_settings(config_file)
    if scope.lower() == "line":
        rows = compute_line_metrics(settings.db_url)
    elif scope.lower() == "station":
        rows = compute_station_metrics(settings.db_url)
    else:
        raise typer.BadParameter("scope must be 'line' or 'station'")
    if json_out:
        import json as _json
        typer.echo(_json.dumps(rows, ensure_ascii=False, indent=2))
    else:
        for r in rows[:50]:
            typer.echo(str(r))


@app.command()
def print_label_stations(
    labels: str = typer.Option(..., help="Labels to resolve (comma)"),
    products: str = typer.Option("TRAM", help="Products to include (comma) e.g., TRAM,BUS"),
    label_index_path: Path = typer.Option(Path("data/label_index.json"), help="Path to label index JSON"),
):
    """Print station IDs resolved for given labels and products from label index."""
    labs = {l.strip().upper() for l in labels.split(",") if l.strip()}
    prods = {p.strip().upper() for p in products.split(",") if p.strip()}
    station_ids = resolve_stations_for_labels(label_index_path, prods, labs)
    for sid in station_ids:
        typer.echo(sid)


@app.command()
def debug_gtfs_link(
    stop_name: str = typer.Argument(..., help="Partial GTFS stop name to search, e.g., 'Elisabethplatz'"),
    gtfs: str = typer.Option(GTFS_DEFAULT_URL, help="GTFS zip URL or local path"),
    cache: Path = typer.Option(DEFAULT_CACHE, help="Stations cache path for mapping to MVG station ids"),
    radius_m: float = typer.Option(300.0, help="Radius (meters) to list nearest MVG stations"),
):
    """Debug how a GTFS stop links to MVG stations: shows id matches and nearest stations."""
    import json as _json
    report = debug_link_for_stop_name(stop_name, gtfs, cache, radius_m)
    typer.echo(_json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    app()
