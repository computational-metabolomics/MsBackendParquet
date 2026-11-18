## Generate the example data shipped in inst/extdata.
##
## Run by hand from the package root, not at build or check time:
##
##     Rscript inst/scripts/make-extdata.R
##
## The files are deliberately tiny (tens of kB in total): they exist so that
## the examples in the manual pages actually run, offline and without a data
## package, not to be representative of real acquisitions.

library(Spectra)

pkg_root <- normalizePath(".")
extdata <- file.path(pkg_root, "inst", "extdata")
dir.create(extdata, recursive = TRUE, showWarnings = FALSE)

## ---------------------------------------------------------------------------
## demo.mzML
##
## Written through `MsBackendMzR()`, i.e. by proteowizard itself, so the file
## is valid mzML by construction rather than by hand-assembly.
## ---------------------------------------------------------------------------

sd <- S4Vectors::DataFrame(
    msLevel = c(1L, 2L, 1L),
    rtime = c(1.1, 1.4, 2.2),
    precursorMz = c(NA_real_, 120.5, NA_real_))
sd$mz <- IRanges::NumericList(c(100.1, 110.2, 120.3), c(50.5, 60.6),
                              c(101.1, 111.2, 121.3), compress = FALSE)
sd$intensity <- IRanges::NumericList(c(10, 20, 30), c(5, 6),
                                     c(11, 21, 31), compress = FALSE)

mzml <- file.path(extdata, "demo.mzML")
unlink(mzml)
export(Spectra(sd), MsBackendMzR(), file = mzml)

## ---------------------------------------------------------------------------
## QC01.mzpeak, QC02.mzpeak
##
## mzPeak archives, built by the same synthesiser the test suite uses. Spectrum
## `i` (0-based) carries peaks `100 * (i + 1) + 1:4 + mz_offset` with
## intensities `10 * (i + 1) + 1:4`, and MS levels alternate 1/2.
## ---------------------------------------------------------------------------

source(file.path(pkg_root, "tests", "testthat", "helper-mzpeak.R"))

## Zip from inside the archive directory so the members are stored under
## relative paths; `.mzpeak_unpack()` expects the index at the archive root.
.zip_archive <- function(dir, zipfile) {
    unlink(zipfile)
    owd <- setwd(dir)
    on.exit(setwd(owd), add = TRUE)
    utils::zip(zipfile, list.files(dir), flags = "-qr9X")
    invisible(zipfile)
}

staging <- tempfile("mzpeak-staging")
dir.create(staging)

.make_mzpeak_archive(file.path(staging, "QC01"), n = 6L, run_id = "QC01",
                     mz_offset = 0)
.make_mzpeak_archive(file.path(staging, "QC02"), n = 4L, run_id = "QC02",
                     mz_offset = 10000)

.zip_archive(file.path(staging, "QC01"), file.path(extdata, "QC01.mzpeak"))
.zip_archive(file.path(staging, "QC02"), file.path(extdata, "QC02.mzpeak"))

unlink(staging, recursive = TRUE)

## ---------------------------------------------------------------------------
## msn.mzML.gz
##
## Not generated here: a copy of msPurity's
## inst/extdata/msn-standards/msPurity_L_tryptophan_6305_MSn.mzML.gz
## (computational-metabolomics/msPurity#119), a reduced Thermo MS1-MS4
## acquisition of an L-tryptophan standard converted by msconvert. Its 176
## spectra have scan numbers that differ from their positions, MS2-MS4
## precursor references, and CID and HCD activation, so it covers what
## demo.mzML cannot. Copy it again rather than regenerating it.
## ---------------------------------------------------------------------------

for (f in list.files(extdata, full.names = TRUE))
    message(basename(f), ": ", file.size(f), " bytes")
