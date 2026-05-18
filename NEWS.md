# MsBackendParquet 0.99.0

## Initial Bioconductor submission

* `MsBackendParquet` class extending `Spectra::MsBackendCached` and
  backed by Apache Parquet datasets accessed via the Apache Arrow R
  package.
* `createMsBackendParquetDataset()` to import raw MS data files into a
  new on-disk Parquet dataset, optionally Hive-partitioned.
* `mzMLToParquet()` convenience wrapper to convert mzML / mzXML /
  netCDF files into a Parquet store and return an initialised
  backend.
* `engine = "mzr"` option on `mzMLToParquet()` and
  `createMsBackendParquetDataset()` streams spectra directly from
  `mzR::openMSfile()` into Apache Arrow's incremental Parquet writer,
  bounding memory use to `batch_size` spectra at a time (default
  1000). Useful for converting multi-GB mzML files on memory-
  constrained machines.
* `backendInitialize()` to attach the backend to an existing dataset
  or to create one from a `DataFrame`.
* `setBackend()` support to switch a `Spectra` object to a
  `MsBackendParquet`.
* Filter methods (`filterMsLevel`, `filterRt`, `filterDataOrigin`,
  `filterPrecursorMzRange`, `filterPrecursorMzValues`) optimised via
  Arrow predicate push-down.
