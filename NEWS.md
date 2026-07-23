# MsBackendParquet 0.99.0

## Initial Bioconductor submission

* `MsBackendParquet` class extending `Spectra::MsBackendCached` and
  backed by on-disk Apache Parquet datasets. Datasets are written with
  the Apache Arrow R bindings and queried with DuckDB (`DBI` +
  `duckdb`), keeping spectra metadata and the m/z / intensity peak
  values in a columnar format with compression, dictionary encoding
  and optional Hive-style partitioning.
* `createMsBackendParquetDataset()` to import raw MS data files (mzML,
  mzXML, netCDF) into a new on-disk Parquet dataset, optionally
  Hive-partitioned, or to write an existing `DataFrame` (e.g. the
  output of `Spectra::spectraData()`) directly.
* `mzMLToParquet()` convenience wrapper to convert mzML / mzXML /
  netCDF files into a Parquet store, validate the inputs and return an
  initialised backend ready to wrap in a `Spectra` object.
* `engine = "mzr"` option on `mzMLToParquet()` and
  `createMsBackendParquetDataset()` streams spectra directly from
  `mzR::openMSfile()` into Apache Arrow's incremental Parquet writer,
  bounding memory use to `batch_size` spectra at a time. Useful for
  converting multi-GB mzML files on memory-constrained machines.
* Tunable Parquet write layout via `compression`, `row_group_size` and
  `partitioning`, letting datasets be sized for the expected query
  pattern.
* `backendInitialize()` to attach the backend to an existing dataset or
  to create one from a `DataFrame`.
* `setBackend()` support to switch a `Spectra` object to a
  `MsBackendParquet`.
* Filter methods (`filterMsLevel`, `filterRt`, `filterDataOrigin`,
  `filterPrecursorMzRange`, `filterPrecursorMzValues`) optimised via
  DuckDB predicate push-down and Parquet row-group / partition pruning.
* Bounded in-memory index of the non-peak spectra variables, shared
  across all backends over the same dataset, so filtering and metadata
  access stay fast while peak data remains on disk. Configurable
  through the `MsBackendParquet.cacheMetadata`,
  `MsBackendParquet.cacheMaxCells` and `MsBackendParquet.cacheColumns`
  options.
* Peak matrices are packed and unpacked in C for fast `peaksData()`,
  `mz()` and `intensity()` access.
