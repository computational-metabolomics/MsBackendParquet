# MsBackendParquet

[![License: Artistic-2.0](https://img.shields.io/badge/license-Artistic--2.0-brightgreen.svg)](https://opensource.org/licenses/Artistic-2.0)

`MsBackendParquet` is a [Spectra](https://github.com/RforMassSpectrometry/Spectra)
backend that stores mass spectrometry (MS) data in an on-disk
[Apache Parquet](https://parquet.apache.org/) dataset, accessed through
the [Apache Arrow](https://arrow.apache.org/docs/r) R package.

Compared with the in-memory or mzML-on-disk backends, the Parquet
backend:

- writes spectra metadata together with `mz` and `intensity` values as
  Parquet `list<double>` columns, benefiting from columnar compression
  and dictionary encoding;
- exploits Arrow's lazy evaluation and predicate push-down to filter
  large datasets without loading them into memory;
- supports Hive-style partitioning (e.g. by `dataOrigin`) for
  efficient access patterns;
- stores only a file system path inside the backend object, so
  `MsBackendParquet` instances are fully serialisable and parallel-
  processing friendly.

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
be    <- mzMLToParquet(files, path = tempfile(),
                       partitioning = "dataOrigin")
sps   <- Spectra(be)
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
