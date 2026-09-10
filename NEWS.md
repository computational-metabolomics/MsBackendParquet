# MsBackendParquet 0.99.0

## Initial Bioconductor submission

### Backend

- `MsBackendParquet` class extending `Spectra::MsBackendCached` and backed by on-disk columnar datasets. Datasets are written with the Apache Arrow R bindings and queried with DuckDB (`DBI` + `duckdb`), keeping spectra metadata and the m/z / intensity peak values in a columnar format with compression, dictionary encoding and optional Hive-style partitioning.

- `createMsBackendParquetDataset()` imports raw MS data files (mzML, mzXML, netCDF) into a new on-disk dataset, optionally Hive-partitioned, or writes an existing `DataFrame` (e.g. the output of `Spectra::spectraData()`) directly.

- `mzMLToParquet()` convenience wrapper converts mzML / mzXML / netCDF files into a store, validates the inputs and returns an initialised backend ready to wrap in a `Spectra` object.

- `engine = "mzr"` on `mzMLToParquet()` and `createMsBackendParquetDataset()` streams spectra directly from `mzR::openMSfile()` into Apache Arrow's incremental Parquet writer, bounding memory use to `batch_size` spectra at a time, for converting multi-GB mzML files on memory-constrained machines.

- Tunable write layout via `compression`, `row_group_size` and `partitioning`, letting datasets be sized for the expected query pattern.

- `backendInitialize()` attaches the backend to an existing dataset or creates one from a `DataFrame`; `setBackend()` switches a `Spectra` object to a `MsBackendParquet`.

- Filter methods (`filterMsLevel`, `filterRt`, `filterDataOrigin`, `filterPrecursorMzRange`, `filterPrecursorMzValues`) use DuckDB predicate push-down and Parquet row-group / partition pruning.

- A bounded in-memory index of the non-peak spectra variables, shared across all backends over the same dataset, keeps filtering and metadata access fast while peak data stays on disk. Configurable through the `MsBackendParquet.cacheMetadata`, `MsBackendParquet.cacheMaxCells` and `MsBackendParquet.cacheColumns` options.

- Peak matrices are packed and unpacked in C for fast `peaksData()`, `mz()` and `intensity()` access.

### On-disk format (mzStack)

- The dataset format is an implementation of the **mzStack** specification (work in progress). A dataset is identified by an **`mzStack.json`** manifest declaring `format: "mzStack"` and a semantic `version`, and recording the Hive `partitioning` and per-run spectrum counts so `backendInitialize()` derives spectrum ids arithmetically without scanning the dataset.

- One manifest covers both dataset kinds. A dataset converted from raw MS data files registers a run with `kind: "native"`; a dataset indexing mzPeak archives registers its runs with `kind: "mzpeak"`. The kind is recorded in the manifest rather than inferred from the directory layout.

- Both kinds store mzPeak's column vocabulary and value encodings on disk. A natively converted dataset's `spectra/` files use mzPeak names (`ms_level`, `time` in minutes, `scan_polarity` ±1, `spectrum_representation` as a CURIE, `isolation_window_target` plus offsets, `data_origin`, ...), so a single SQL translation view serves both kinds and either can be exported back to an archive mechanically.

- `spectrum_index` is the dataset's unique, monotonic 0-based key; a spectrum's position within its source file (`Spectra`'s `scanIndex`, which restarts per file) is kept separately in `scan_index`. Reserved columns are `spectrum_id_` (the row key), `acquisition_num_` (the instrument scan number, which mzPeak has no column for) and the `n_scans` / `n_selected_ions` / `n_precursors` counts. `mz` and `intensity` keep mzPeak's short names.

- Spectrum ids are allocated in one contiguous block per run, so adding a run never renumbers existing spectra.

### mzPeak archives

- `createMzPeakDataset()` and `addMzPeakArchives()` register a collection of HUPO-PSI mzPeak archives as a single queryable dataset. The archives are not modified and not copied: only a small derived index of the spectrum metadata is written (~3% of the archive size), and peak data is read from the archives on demand, so exporting a run back to mzPeak is a directory copy.

- Archives are resolved through their `mzpeak_index.json` and their Parquet array index, as the specification requires, rather than by file or column name. Archives with differing schemas — normal, since conformant writers promote different parameters to columns — combine into one dataset.

- mzPeak's relational side tables (scans, precursors, selected ions) are flattened into one row per spectrum, with `n_scans`, `n_selected_ions` and `n_precursors` recording where more detail remains in the archive.

- Column names and units are translated to `Spectra` conventions in a single SQL view, including mzPeak's `time` in minutes to `rtime` in seconds.

- `backendInitialize()` takes a `representation` argument selecting profile or centroid signal, which mzPeak stores in separate files.

### Projections

- `buildProjection()` / `dropProjection()` maintain an optional m/z-ordered copy of the signal for mzPeak datasets. `filterContainsMz()` selects spectra containing a peak at a given m/z inside the storage engine; with a projection it measured 8.3x faster than reading the archives, and returns the same result either way.
