# NYC Yellow Taxi — Top Trips by Distance

For each monthly NYC TLC yellow-taxi parquet file, compute the 90th percentile
of `trip_distance` and emit every trip above that threshold. Output is
partitioned parquet plus a summary table.

```
input  : yellow_tripdata_YYYY-MM.parquet  (TLC, public CloudFront URL)
output : output/top_trips/year_month=YYYY-MM/part.parquet
         output/summary.parquet
```

## Quickstart

```bash
git clone https://github.com/Morpho4444/nyc-taxi-top-trips.git && cd nyc-taxi-top-trips
make setup     # creates .venv, installs deps
make test      # 17 tests, ~0.2s, no network
make run       # processes the last 12 published months
```

Or with Docker:

```bash
make docker-build
make docker-run    # output/ on the host is mounted into the container
```

That's it.

## Decisions and assumptions

The prompt is intentionally ambiguous. Questions I'd send to the client on
day one, and the assumption I made for each if they hadn't replied:

| Question | Assumption |
|---|---|
| Is the percentile computed **per file** or **globally** across all files? | **Per file.** Reads naturally as "for any of the parquet files," parallelizes cleanly, and yields more meaningful thresholds since the distance distribution shifts over time. |
| Which files count as "any of the parquet files you can find"? | **Last 12 published months** (configurable). Easy to widen to all of 2009→today via `config.yaml` or `--months`. |
| What does "give me all the trips" mean — full rows, IDs, aggregates? | **Full original rows**, partitioned by month. A small `summary.parquet` gives per-month threshold and counts at a glance. |
| Strict `>` or inclusive `>=`? | **Strict `>`** ("over 0.9 percentile"). |
| Should bad data (zero/negative distance, impossible distances) be cleaned? | **Left untouched.** The task is purely about distance ranking. A 269,097-mile trip survived in Jan 2026 — see Sample run. |
| One unified schema across years, or preserve each file's columns? | **Preserved per partition.** TLC's schema changes over time; forcing a union loses information and isn't needed for the ask. |

## How it works

```
src/taxi_top_trips/
├── urls.py       URL building + default-month math
├── config.py     YAML + CLI flag loading, validation
├── pipeline.py   Per-month processing loop
└── main.py       argparse CLI
```

The work is three DuckDB SQL statements per file:

```sql
-- 1. Threshold + total in one pass
SELECT quantile_cont(trip_distance, 0.9), COUNT(*)
FROM read_parquet('https://.../yellow_tripdata_YYYY-MM.parquet');

-- 2. Filter and write
COPY (
    SELECT * FROM read_parquet('https://.../yellow_tripdata_YYYY-MM.parquet')
    WHERE trip_distance > :threshold
) TO 'output/top_trips/year_month=YYYY-MM/part.parquet' (FORMAT PARQUET);

-- 3. Stats sidecar (also acts as the "done" marker for idempotency)
COPY (SELECT 'YYYY-MM' AS year_month, ...) TO '.../_stats.parquet';
```

After every month processes, a glob over the `_stats.parquet` sidecars
produces `output/summary.parquet`.

**Why DuckDB.** `quantile_cont` is built-in; reads parquet over HTTPS via
`httpfs` using HTTP range requests, so a 50 MB file isn't downloaded — only
the column bytes needed are streamed. Embedded library, not a service —
`pip install duckdb` is the entire setup. Spark/Dask/Polars would all work
but add reproducibility cost without measurable benefit at this data size.

**Idempotency.** Each partition writes `part.parquet` + `_stats.parquet`. If
both exist, the month is skipped on re-runs. Use `--force` to reprocess.

**Missing or unpublished months.** TLC's publishing lag varies (I observed
5–6 months at time of testing, not the 2 months I initially assumed). HTTP
403/404 responses are logged as warnings and skipped; the loop continues.

## Sample run

Run on May 17, 2026, default config (last 12 months):

| year_month | threshold (mi) | total trips | filtered trips |
|------------|---------------:|------------:|---------------:|
| 2025-04 | 8.32 | 3,970,553 | 396,977 |
| 2025-05 | 8.75 | 4,591,845 | 458,916 |
| 2025-06 | 8.81 | 4,322,960 | 432,115 |
| 2025-07 | 8.90 | 3,898,963 | 389,655 |
| 2025-08 | 9.50 | 3,574,091 | 357,036 |
| 2025-09 | 8.96 | 4,251,015 | 424,877 |
| 2025-10 | 8.94 | 4,428,699 | 442,485 |
| 2025-11 | 8.83 | 4,181,444 | 417,816 |
| 2025-12 | 8.70 | 4,305,006 | 428,712 |
| 2026-01 | 8.56 | 3,724,889 | 372,333 |
| 2026-02 | 8.36 | 3,399,866 | 339,881 |
| 2026-03 | 8.62 | 3,952,451 | 394,869 |

**Observations:**

- Per-month p90 is stable at **8.3–9.5 miles** all year — roughly the
  distance from Manhattan to JFK/LGA, consistent with the long tail being
  airport runs.
- August has the highest threshold (9.50 mi); worth investigating whether
  this is seasonal travel or a data artifact.
- Filtered count is ~10% of total every month, as expected for strict `>p90`.
- Total output: ~126 MB across 12 months.
- **Data quality.** The maximum recorded `trip_distance` in Jan 2026 is
  **269,097.48 miles** — about ten times Earth's circumference. Clearly a
  sensor or data-entry error. Preserved in the output by design, since the
  task is "trips over the 0.9 percentile of recorded distance," not "of
  cleaned distance." A `--clean` flag would be a one-day add.

## Output format

```
output/
├── summary.parquet                         one row per processed month
└── top_trips/
    ├── year_month=2025-04/
    │   ├── part.parquet                    filtered trips for that month
    │   └── _stats.parquet                  threshold + counts (sidecar)
    └── ...
```

**`summary.parquet` schema:**

| column          | type      | notes                                  |
|-----------------|-----------|----------------------------------------|
| year_month      | VARCHAR   | `YYYY-MM`                              |
| source_url      | VARCHAR   | exact URL the data was read from       |
| taxi_color      | VARCHAR   | `yellow` or `green`                    |
| percentile      | DOUBLE    | e.g. 0.9                               |
| threshold_miles | DOUBLE    | p90 of `trip_distance` for that month  |
| total_trips     | BIGINT    | rows in the source file                |
| filtered_trips  | BIGINT    | rows in the output `part.parquet`      |
| runtime_seconds | DOUBLE    | wall clock to process that month       |
| processed_at    | TIMESTAMP | UTC                                    |

**`part.parquet`** preserves the source file's schema as-is. See the
[TLC Yellow Trips Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
for column meanings.

## Configuration

`config.yaml` is the canonical source; CLI flags override anything in it.

```yaml
percentile: 0.9
taxi_color: yellow
output_dir: output
force: false
# months:                # optional; defaults to last 12 published months
#   - 2024-01
```

CLI examples:

```bash
python -m taxi_top_trips --months 2024-01 2024-02 2024-03
python -m taxi_top_trips --percentile 0.95
python -m taxi_top_trips --taxi-color green
python -m taxi_top_trips --force
python -m taxi_top_trips --output-dir /tmp/run-01
python -m taxi_top_trips --help
```

## Testing

```bash
make test
```

17 tests, ~0.2s, no network required. Fixtures generate a tiny synthetic
parquet with the TLC schema so the pipeline can be exercised end-to-end
without hitting CloudFront.

## Known limitations and next steps

Each is a deliberate scope decision for the MVP, and each is a 1-day add:

- **Local cache.** Source files are streamed from CloudFront on every run.
  A `data/raw/` cache would make re-runs instant; skipped because streaming
  is already fast (~2–3s per file).
- **Cross-month parallelism.** DuckDB already parallelizes within a file;
  cross-month parallelism would help on slow networks but matters less on
  fast ones.
- **Data-quality cleaning.** A `--clean` flag with configurable rules (cap
  distance, drop non-positive, sanity-check timestamps).
- **Schema-drift unification.** A unified cross-year output table would need
  a column-projection step (TLC added `cbd_congestion_fee` in 2025, for
  example).
- **S3/GCS sink.** Output is local; remote sinks would need DuckDB's S3
  credentials and a small path change.
- **Other taxi types.** Green works out of the box (`--taxi-color green`).
  FHV and HVFHV have different schemas and would need separate handling.

## File map

```
.
├── README.md
├── Makefile                  setup / run / test / docker-* / clean
├── Dockerfile
├── pyproject.toml
├── config.yaml
├── src/taxi_top_trips/
│   ├── main.py               CLI entrypoint
│   ├── pipeline.py           core processing loop
│   ├── config.py             config dataclass + loader
│   └── urls.py               URL builder + default-month math
└── tests/                    17 tests, no network required
```