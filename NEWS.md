# MsBackendParquet 0.99.5

## New features

- Typed error conditions. Failures the mzStack specification classifies are raised as conditions of class `mzstack_format`, `mzstack_archive`, `mzstack_stale`, `mzstack_unsupported`, `mzstack_semantic`, `mzstack_capability` or `mzstack_resource`, each inheriting `mzstack_error` and `error`, so a caller can handle one kind of failure without matching message text. A missing or foreign manifest, an unsupported major version and mixed run kinds are `mzstack_format`; an unreadable mzPeak archive is `mzstack_archive`; filtering on sample metadata a dataset does not carry, or asking a run for a representation it does not hold, is `mzstack_capability`; a sample variable that would shadow a spectra variable is `mzstack_semantic`. `mzstackError()` and `mzstackCondition()` are exported so that packages layered on this one raise the same classes. Messages are unchanged.

- An exported manifest API for packages layered on MsBackendParquet: `newManifest()`, `readManifest()`, `writeManifest()`, `manifestRuns()` and `invalidateDatasetCache()`. `writeManifest()` replaces the manifest atomically, increments `generation`, refuses to overwrite a manifest another writer committed after it was read, and drops the caches held for the dataset. Given a directory with no manifest, it commits the first one, which is how a dataset with no runs of its own (a results dataset holding only tables) is created.

- Peak-annotation variables. A native dataset can carry list columns beside `mz` and `intensity`, one value per peak, such as a merged spectrum's per-peak signal-to-noise or contributor counts (mzStack-4 §6). Pass them as list columns in `data` to `createMsBackendParquetDataset()`; every non-`NULL` element must be as long as that spectrum's `mz`. They are reported by `peaksVariables()` and returned by `peaksData(columns = )` and `spectraData()`. A spectrum that does not carry a variable returns `NA` for it, aligned with its peaks. Requests for `mz` and `intensity` alone take the existing fast path.

- An explicit `run_id` column in `data` names the runs `createMsBackendParquetDataset()` writes, in place of names derived from `dataOrigin`. Each run must be one contiguous block of spectra, and every id must be a valid run id. `dataOrigin`, when given, is still recorded as the run's source. This lets a derived spectrum keep its source run's `dataOrigin` while its run is named after the aggregation that produced it.

## Bug fixes

- Manifest keys this package does not interpret, such as the results layer's `sources`, `provenance` and `results`, are now carried through a read/write cycle exactly as parsed. Previously a one-element array in them came back as a scalar, so `runData<-()` on a results dataset rewrote `"sorted_by": ["x"]` as `"sorted_by": "x"`. A one-element `signal.partitioning` is likewise written as an array.

- Numbers in the manifest are written with the shortest decimal form that reads back exactly, rather than jsonlite's 15 significant digits.

- The parsed-manifest cache notices a manifest replaced by another process or package, by its modification time and size, and drops every cache held for the dataset when it does.

# MsBackendParquet 0.99.4

## New features

- Per-run sample metadata. `runData()` reads and `runData<-()` writes a table of whatever the experiment records about the sample a run came from, one row per run, keyed by `run_id` and stored in `index/samples.parquet`. Its columns become ordinary spectra variables, `runVariables()` lists them, and `filterSampleData()` selects on them. Because a run owns a contiguous block of `spectrum_id_`, such a filter resolves to an id range over the run table rather than a read of a per-spectrum column, and the resulting predicate is carried into any later `peaksData()`. Replacing the table rewrites a few kilobytes and does not touch the signal, so projections survive a correction.

## Documentation

- The vignette is rewritten around a real data set. It converts `MsDataHub::PestMix1_DDA.mzML()` with `mzMLToParquet()` and works with the result throughout, and the mzPeak, projection and sample metadata sections run against the `QC01.mzpeak` / `QC02.mzpeak` archives shipped in `inst/extdata`. Every chunk is now evaluated when the package is built; none is marked `eval = FALSE`.

- Filtering is demonstrated with the `Spectra` filter methods (`filterMsLevel()`, `filterRt()`, `filterPrecursorMzRange()`, `filterPrecursorMzValues()`, `filterDataOrigin()`) and peak access with `peaksData()`, `tic()` and `containsMz()`, in place of the previous MassQL section built on `SpectraQL`.

# MsBackendParquet 0.99.3

## Internal

- Package code now calls imported functions directly instead of through `::`, with the corresponding selective `importFrom()` / `importMethodsFrom()` directives declared in the roxygen blocks.

- `mzR` moves from `Suggests` to `Imports`, since the `engine = "mzr"` read path in `createMsBackendParquetDataset()` and `mzMLToParquet()` depends on it.

- `tests/testthat.R` additionally runs the `MsBackend` compliance test suite shipped in `Spectra` (`inst/test_backends/test_MsBackend`) against a dataset created from `MsDataHub::MS3TMT11.mzML()`.

# MsBackendParquet 0.99.2

## Internal

- The SQL numeric literal round-trip test now covers the numeric range mass spectrometry data occupies, rather than the extremes of the double range where `sprintf()` and DuckDB's literal parser can disagree by one ulp on some platforms.

# MsBackendParquet 0.99.1

## Bug fixes

- Numeric SQL literals are always written with 17 significant digits, the round-trip width of an IEEE-754 double, instead of being narrowed by checking them with `as.numeric()`. That check mis-rejected exact literals on macOS arm64, where `long double` is a plain double, and the generated SQL now no longer varies between platforms.

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
