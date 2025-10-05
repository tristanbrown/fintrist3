# 📦 Fintrist3 Local Data Cache Design

> This document defines the architecture for storing and managing the local dataset cache used by `fintrist3`, including metadata indexing, file layout, and platform-specific file paths.

---

## 🎯 Purpose

To cache financial datasets (e.g. OHLCV, fundamentals, news, estimates, etc.) on-demand, locally and deterministically, using:

- A **DuckDB database** for all structured metadata and indexing
- A **flat directory of UUID-named data blobs**
- A platform-agnostic application data root directory

This enables:

- Fast, local analytics and filtering
- Versionable, hash-addressed datasets
- Decoupling of data structure from physical storage
- Easy backups, diffs, and metadata-driven loading

---

## 📁 File Layout

All data is stored under the platform-specific user app data directory, resolved using a cross-platform library (e.g. [`platformdirs`](https://pypi.org/project/platformdirs/) in Python):

```
<user_app_data>/fintrist3/
├── db/
│   └── registry.duckdb       ← DuckDB database file
├── cache/
│   ├── a1b2c3d4.parquet
│   ├── e5f6g7h8.json
│   ├── i9j0k1l2.csv
│   └── ... (flat UUID-named blobs)
```

- `db/registry.duckdb`: Contains all metadata and indexing tables
- `cache/`: Flat storage of all actual data blobs (Parquet, JSON, CSV, etc.)

> ✅ No data semantics are implied by filenames or folders.  
> ✅ All semantics are stored in the registry database.

---

## 🧱 Metadata Tables (DuckDB Schema)

### `data_registry`

Tracks all known datasets and their associated files:

```sql
CREATE TABLE data_registry (
    id TEXT PRIMARY KEY,               -- UUID or hash (filename stem)
    dataset_type TEXT,                 -- 'ohlcv', 'fundamentals', 'trades', etc.
    symbol TEXT,                       -- 'AAPL', 'MSFT', etc.
    frequency TEXT,                    -- 'daily', '5min', NULL if not applicable
    source TEXT,                       -- 'tiingo', 'alpha', etc.
    start_date DATE,
    end_date DATE,
    file_path TEXT,                    -- Relative path, e.g. 'cache/a1b2c3d4.parquet'
    format TEXT,                       -- 'parquet', 'json', 'csv', etc.
    schema TEXT,                       -- Optional: JSON or fingerprint of schema
    file_hash TEXT,                    -- Optional: MD5/SHA256 of file content
    row_count INTEGER,
    last_updated TIMESTAMP
);
```

All filtering and grouping (e.g. by symbol, date range, frequency) is done via SQL queries on this table.

---

## 🧠 Design Principles

- **Single Source of Truth**: All dataset semantics live in the DuckDB registry, not in paths or filenames.
- **Flat File Store**: All cached data blobs are stored in a flat directory, named by UUID or hash, to avoid hierarchical constraints.
- **Decoupling of Metadata and Storage**: File organization is optimized for simplicity and tooling, not human readability.
- **Hashable + Diffable**: File hashes and schema fingerprints support deduplication, verification, and version tracking.
- **Cross-Platform Safe**: Files are stored under a platform-specific app data directory using standard conventions.

---

## 📌 Platform-Agnostic Storage Location

The base path `<user_app_data>/fintrist3/` is resolved using:

| OS      | Resolved Path                                      |
|---------|----------------------------------------------------|
| Linux   | `$XDG_DATA_HOME/fintrist3/` (usually `~/.local/share/`) |
| macOS   | `~/Library/Application Support/fintrist3/`         |
| Windows | `%LOCALAPPDATA%\fintrist3\`                        |

Use a library like [`platformdirs`](https://pypi.org/project/platformdirs/) to resolve this safely.

---

## 🧪 Example Usage Flow

1. Pull OHLCV data for AAPL from Tiingo
2. Save it as a Parquet file:
   - `a1b2c3d4.parquet` in `cache/`
3. Compute metadata:
   - symbol, frequency, date range, row count, hash
4. Insert into `data_registry`:

```sql
INSERT INTO data_registry VALUES (
    'a1b2c3d4', 'ohlcv', 'AAPL', '5min', 'tiingo',
    '2025-01-01', '2025-10-04', 'cache/a1b2c3d4.parquet',
    'parquet', '{"timestamp": "TIMESTAMP", "open": "DOUBLE", ...}',
    'abc123...', 182400, now()
);
```

5. Later: query the registry for all `5min` AAPL data, get file paths, and pass to `read_parquet([...])` for columnar analytics.

---

## 🧩 Future Extensions

- **Schema evolution detection**
- **Soft deletion or versioning (add `is_latest`, `history_id`)**
- **Tracking derived datasets (e.g. aggregates, indicators)**
- **Dependency tracking between blobs (e.g. joins or transforms)**

---

## 🧠 Summary

This design gives you:

- ✅ Strong separation of concerns
- ✅ Fully queryable, introspectable metadata
- ✅ Clean local cache management
- ✅ Platform portability
- ✅ Support for multiple data formats and sources
- ✅ Future-proofing for versioning and analytics

It forms the foundation for a robust, local-first financial dataset caching layer in `fintrist3`.

