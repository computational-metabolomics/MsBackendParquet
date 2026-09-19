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
- stores one run per source file, so selecting whole files prunes whole
  directories, and supports Hive-style partitioning (e.g. by `msLevel`)
  below that for finer access patterns;
- stores only a file system path inside the backend object, so
  `MsBackendParquet` instances are fully serialisable and parallel-
  processing friendly;
- supports [mzML](https://github.com/HUPO-PSI/mzml) and [mzPeak](https://github.com/HUPO-PSI/mzPeak) as data soruces.

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
[`inst/benchmarks/performance.Rmd`](inst/benchmarks/performance.Rmd) for the design
considerations behind these numbers.

`Rscript inst/benchmarks/benchmark-mzpeak.R` covers the mzPeak path on a
synthetic fixture of 20 archives / 10,000 spectra / 5,000,000 points. Ingest
reads metadata only and writes a derived index of 3.4% of the archive size;
metadata filters are answered from it in about a millisecond; and an m/z-ordered
projection makes `filterContainsMz()` **8.3x** faster than searching the
archives (331 to 40 ms), for the same answer.

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
be <- mzMLToParquet(files, path = tempfile())
sps <- Spectra(be)
```

For more control (custom backends, chunk sizes, in-memory inputs) use
[`createMsBackendParquetDataset()`](R/MsBackendParquet-creators.R)
directly.

### Sample metadata

Each run — one mzPeak archive, or one converted source file — owns a
contiguous block of the dataset's spectrum ids. `runData()` attaches the
experiment's own sample metadata to those runs:

```r
runData(be) <- data.frame(run_id   = c("QC01", "QC02"),
                          subject   = c("S3", "S3"),
                          timepoint = c(0, 6))

be$timepoint                          # an ordinary spectra variable
filterSampleData(be, timepoint == 6)  # resolved as an id range
```

The sample metadata is stored once per run, not copied onto every spectrum, so a
filter on it becomes a range predicate on the column the files are sorted by
— the case Parquet's row-group statistics prune best — without reading a
per-spectrum column. It can also be corrected at any time without rewriting
the signal or invalidating projections.

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

### Querying mzPeak archives

An mzPeak archive contains a single MS run, making analyses that span thousands of runs challenging. `createMzPeakDataset()` registers a collection of them as a single
queryable dataset:

```r
path <- tempfile()
createMzPeakDataset(c("QC01.mzpeak", "QC02.mzpeak"), path = path)

be <- backendInitialize(MsBackendParquet(), path = path)
hits <- filterContainsMz(be, 278.093, ppm = 20)

buildProjection(path) # optional m/z-ordered copy; makes that search fast
```

The archives are neither modified nor copied: only a small index of their
spectrum metadata is written, and peaks are read from the archives on demand, so
they stay readable by every other mzPeak tool. `filterContainsMz()` runs the
peak search inside the storage engine; `buildProjection()` adds a droppable
m/z-ordered copy of the signal that speeds it up without changing its result.
See the vignette for the detail.
