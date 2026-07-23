# MsBackendParquet

[![License: Artistic-2.0](https://img.shields.io/badge/license-Artistic--2.0-brightgreen.svg)](https://opensource.org/licenses/Artistic-2.0)

`MsBackendParquet` is a [Spectra](https://github.com/RforMassSpectrometry/Spectra)
backend that stores mass spectrometry (MS) data in an on-disk
[Apache Parquet](https://parquet.apache.org/) dataset. Datasets are written
with [Apache Arrow](https://arrow.apache.org/docs/r) and queried with
[DuckDB](https://duckdb.org/).

## Overview

Compared with the in-memory or mzML-on-disk backends, the Parquet
backend:

- writes spectra metadata together with `mz` and `intensity` values as
  Parquet `list<double>` columns, benefiting from columnar compression
  and dictionary encoding;
- keeps the (small) spectra variables in a lazily-loaded in-memory index and
  reads only peak data from disk, so metadata filters cost about as much as an
  in-memory backend while peak storage stays out of core;
- pushes range and set predicates down to DuckDB, which prunes Parquet row
  groups using their min/max statistics;
- supports Hive-style partitioning (e.g. by `dataOrigin`) for
  efficient access patterns;
- stores only a file system path inside the backend object, so
  `MsBackendParquet` instances are fully serialisable and parallel-
  processing friendly.

### Performance

Against the other on-disk `Spectra` backends over 76,020 spectra (10 mzML
files), running the SpectraQL query suite in
[`inst/benchmarks/`](inst/benchmarks/). Medians in ms, lower is better:

| query | parquet | mzR | HDF5 | SQL |
|---|---|---|---|---|
| `RTMIN/RTMAX` range | **3.7** | 5.3 | 9.3 | 144.8 |
| narrow RT range | 7.5 | **4.0** | 6.7 | 81.8 |
| precursor m/z ± ppm | **7.0** | 11.3 | 12.7 | 100.3 |
| RT and precursor | **6.2** | 9.8 | 16.7 | 114.6 |
| MS1 peak data | **94.5** | 558.4 | 732.9 | 196.1 |
| MS1 TIC | **101.5** | 623.8 | 661.8 | 286.4 |
| scan info | 13.8 | 11.8 | **11.2** | 273.7 |

Reproduce with `Rscript inst/benchmarks/benchmark-spectraql-multifile.R`. See
[`inst/benchmarks/performance.md`](inst/benchmarks/performance.md) for the design
considerations behind these numbers.

The in-memory index is on by default and bounded; set
`options(MsBackendParquet.cacheMetadata = "off")` to disable it, or
`MsBackendParquet.cacheColumns` / `MsBackendParquet.cacheMaxCells` to control
what it holds. Over budget, queries fall back to reading from disk.

## Installation

```r
if (!requireNamespace("BiocManager", quietly = TRUE))
    install.packages("BiocManager")
BiocManager::install("MsBackendParquet")
```

## Usage

The quickest way to convert raw mzML files into a Parquet store is
the `mzMLToParquet()` helper, which validates the inputs and returns
an initialised backend in one step:

```r
library(Spectra)
library(MsBackendParquet)

files <- c("sample-1.mzML", "sample-2.mzML")
be <- mzMLToParquet(files, path = tempfile(), partitioning = "dataOrigin")
sps <- Spectra(be)
```

For more control (custom backends, chunk sizes, in-memory inputs) use
[`createMsBackendParquetDataset()`](R/MsBackendParquet-creators.R)
directly.

### Memory-constrained conversion

For very large mzML files, pass `engine = "mzr"` to stream spectra
directly from `mzR::openMSfile()` into Apache Arrow's incremental
Parquet writer. Memory use is bounded by `batch_size` spectra at a
time (default 1000) instead of scaling with the chunk size:

```r
be <- mzMLToParquet(files, path = tempfile(),
                    engine = "mzr",
                    batch_size = 1000L)
```

`engine = "mzr"` requires the `mzR` package and processes files
serially.

## Contributions

Contributions follow the
[R for Mass Spectrometry contribution guidelines](https://rformassspectrometry.github.io/RforMassSpectrometry/articles/RforMassSpectrometry.html#contributions).
