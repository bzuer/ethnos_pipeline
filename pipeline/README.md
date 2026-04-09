# data — Bibliographic Data Pipeline

ELT pipeline (Extract, Load, Transform) for bibliographic metadata into MariaDB.

Sources: **Crossref**, **OpenAlex**, **Scopus**, **SCImago**.
Entities: **works**, **publications**, **persons**, **organizations**, **venues**, **subjects**.

## Setup

```bash
python -m venv venv
venv/bin/pip install -e .
cp config.ini.example config.ini   # edit with DB/API credentials
```

Requires: Python 3.11+, MariaDB with database `data`.

## Pipeline Overview

```
EXTRACT → JSON cache → LOAD → MariaDB → TRANSFORM → clean, deduplicated, ranked data
```

| Stage | Language | What it does |
|-------|----------|-------------|
| **Extract** | Python | Fetch metadata from APIs → save as JSON files |
| **Load** | Python | Read JSON/CSV → insert/update DB records |
| **Transform** | Python + SQL | Clean text, remove junk, deduplicate, calculate metrics, rank venues |

## Full Pipeline Execution

### Extract

```bash
# E1. Find missing DOIs (compare Crossref catalog vs DB)
venv/bin/python pipeline/extract/works/missing_dois.py -i venues_list.csv --from-year 2026

# E2. Fetch work metadata (can run in parallel)
venv/bin/python pipeline/extract/works/crossref.py works/doi/missing
venv/bin/python pipeline/extract/works/openalex.py works/doi/missing

# E3. Fetch venue metadata (can run in parallel)
venv/bin/python pipeline/extract/venues/scopus.py
venv/bin/python pipeline/extract/venues/openalex.py
```

### Load

```bash
# L1. Load works (can run in parallel)
venv/bin/python pipeline/load/works/openalex.py works/openalex
venv/bin/python pipeline/load/works/crossref.py works/crossref

# L2. Load venues (can run in parallel)
venv/bin/python pipeline/load/venues/openalex.py --json-dir venues/openalex
venv/bin/python pipeline/load/venues/scopus.py --json-dir venues/scopus
venv/bin/python pipeline/load/venues/scimago.py venues/scimago
```

### Transform

```bash
# T0. Procedure setup (run once, or when procedures change)
mariadb data < pipeline/transform/procedures.sql

# T1. Text normalization (HTML cleanup, sentinels, identifiers, capitalization)
venv/bin/python pipeline/transform/cleanup.py

# T2. Structural cleanup (junk deletion, orphan removal, OA propagation)
mariadb data < pipeline/transform/delete_from_works.sql

# T3. Pre-dedup metrics + sphinx summaries (REQUIRED before T4)
venv/bin/python pipeline/transform/metrics.py --full --works --sphinx

# T4. Work deduplication (uses work_author_summary from T3)
mariadb data < pipeline/transform/deduplicate_works.sql

# T5. Person deduplication, signatures, name parts
mariadb data < pipeline/transform/persons_full_process.sql

# T6. Full metrics recalculation (post-dedup, all entities)
venv/bin/python pipeline/transform/metrics.py --full

# T7. Venue ranking (run once, or when tier weights change)
mariadb data < pipeline/transform/venue_ranking_setup.sql
```

**Key dependency**: T3 must run before T4 (dedup needs sphinx summary tables).

## Project Structure

```
pipeline/
├── common.py                  # DB connection (get_connection, normalize_issn)
├── extract/                   # Fetch from APIs → JSON cache
│   ├── config.py              # Config management
│   ├── retry.py               # Retry/backoff, rate-limit gate
│   ├── http.py                # Unified httpx client factory
│   ├── io_utils.py            # DOI file I/O, .filtered/.404 markers
│   ├── works_common.py        # Work filtering, save_work, process_issn_generic
│   ├── venues_common.py       # Venue loading, batch processing
│   ├── common.py              # Re-export shim
│   ├── works/
│   │   ├── missing_dois.py    # Crossref catalog vs DB → missing DOI lists
│   │   ├── crossref.py        # Fetch works from Crossref API
│   │   └── openalex.py        # Fetch works from OpenAlex API
│   └── venues/
│       ├── scopus.py          # Fetch venues from Scopus API
│       └── openalex.py        # Fetch venues from OpenAlex API
├── load/                      # Read JSON/CSV → insert into DB
│   ├── db.py                  # DB connection + ensure_connection + safe_rollback
│   ├── normalize.py           # DOI, identifier, name normalization
│   ├── filtering.py           # EXCLUDED_WORK_TYPES (17), title/subtitle patterns
│   ├── entities.py            # get_or_create: person, organization, venue, subject
│   ├── io_utils.py            # Cache, file discovery
│   ├── constants.py           # STATUS_*, LICENSE_VERSION_RANK, type constants
│   ├── runner.py              # Shared main loop (savepoint/batch/reconnect)
│   ├── common.py              # Re-export shim
│   ├── works/
│   │   ├── shared.py          # DOI skip/mark helpers
│   │   ├── crossref.py        # Load Crossref works into DB
│   │   └── openalex.py        # Load OpenAlex works into DB
│   └── venues/
│       ├── shared.py          # Venue merge logic, conflict detection
│       ├── openalex.py        # Load OpenAlex venues
│       ├── scopus.py          # Load Scopus venues
│       └── scimago.py         # Load SCImago CSV venues
└── transform/                 # Clean/normalize data in DB
    ├── common.py              # Re-export shim
    ├── cleanup.py             # Text cleanup (5 phases) + DB profile
    ├── procedures.sql         # Stored procedures (version-controlled source)
    ├── delete_from_works.sql  # Junk deletion, orphan removal, OA propagation
    ├── deduplicate_works.sql  # Work dedup (3 strategies)
    ├── persons_full_process.sql # Person dedup, signatures, name parts
    ├── venue_ranking_setup.sql  # Venue ranking (7-component scoring)
    └── metrics.py             # Metrics calculation + sphinx summaries
```

### Data directories

```
works/crossref/        # Cached Crossref work JSONs (per ISSN subdir)
works/openalex/        # Cached OpenAlex work JSONs (per ISSN subdir)
works/doi/cache/       # DOI lists per ISSN/year (Crossref catalog)
works/doi/missing/     # Missing DOIs per ISSN/year
venues/scopus/         # Cached Scopus venue JSONs
venues/openalex/       # Cached OpenAlex venue JSONs
venues/scimago/        # SCImago CSV exports
```

### Root files

| File | Purpose |
|------|---------|
| `config.ini` | DB/API credentials (never commit) |
| `venues_list.csv` | Main input: venue list with ISSNs |
| `pyproject.toml` | Package metadata for `pip install -e .` |
| `total_corrigido.jsonl` | LLM-assessed venue relevance (0-5 scale) |

## Common Flags

### Extract

| Flag | Description |
|------|-------------|
| `--force` | Re-fetch even if cached |
| `--limit N` | Max ISSNs/venues to process |
| `--config PATH` | Path to config.ini |

### Load

| Flag | Description |
|------|-------------|
| `--mode {new,full}` | `new` = skip existing DOIs; `full` = reprocess all |
| `--limit N` | Max files per folder |
| `--commit-batch N` | Commit every N files (0 = per folder) |
| `--dry-run` | Simulate without writing (venues only) |

### Transform

| Flag | Description |
|------|-------------|
| `--mode {profile,run,both}` | cleanup.py: profile DB, run cleanup, or both |
| `--phase PHASE` | cleanup.py: run specific phase only |
| `--full` / `--partial` | metrics.py: full recalculation vs fill gaps |
| `--works/--persons/--venues/--sphinx` | metrics.py: entity selection |
| `--dry-run` | Preview steps without executing |

## Safety

- **Always back up** the database before running transform scripts.
- **Always `--dry-run` first** for Python scripts.
- **Always `--limit`** for first runs of extract/load scripts.
- Never commit `config.ini` or secrets.
- Never embed SQL regex in Python strings (see CLAUDE.md for details).

## Architecture Decisions

- **Single source of truth**: `EXCLUDED_WORK_TYPES` lives in `load/filtering.py`; extract imports it.
- **HTTP client**: All extract scripts use `httpx` via `extract/http.create_client`.
- **DB connection**: All scripts use `pipeline.common.get_connection` (socket discovery + TCP fallback).
- **Retry**: Unified `retry_request` with capped 429 backoff: `min(11 * 2^attempt, 120)` seconds.
- **Stored procedures**: Source-controlled in `transform/procedures.sql` + inline in dedup/ranking SQL files.
- **Python vs SQL**: Python for text processing, batch orchestration, API calls. SQL for bulk data operations, dedup, ranking.

Full technical documentation: [CLAUDE.md](CLAUDE.md)
