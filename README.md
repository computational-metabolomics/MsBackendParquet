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

Against the other on-disk `Spectra` backends over 380,100 spectra (50 mzML
files), running the SpectraQL query suite in
[`inst/benchmarks/`](inst/benchmarks/). Medians in ms, lower is better:

| query | parquet | mzR | HDF5 | SQL |
|---|---|---|---|---|
| `RTMIN/RTMAX` range | **12.24** | 15.44 | 24.05 | 380.19 |
| narrow RT range | 11.23 | **5.33** | 14.76 | 348.17 |
| precursor m/z ± ppm | **14.09** | 21.66 | 28.66 | 336.33 |
| RT and precursor | **12.56** | 17.25 | 27.75 | 389.22 |
| MS1 peak data | **314.82** | 1510 | 1440 | 919.04 |
| MS1 TIC | **506.79** | 1390 | 1490 | 890.82 |
| scan info | 26.09 | **17.27** | 22.30 | 718.91 |

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
