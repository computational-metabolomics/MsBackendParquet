## Benchmark: querying a collection of mzPeak archives.
##
## Measures the three costs that decide whether the architecture works:
##
##   1. ingest   -- reads metadata only, so it should scale with the number
##                  of spectra, not with the size of the archives;
##   2. index    -- the derived index should be a small fraction of the
##                  archives it indexes;
##   3. queries  -- metadata filters should be fast without any help;
##                  peak-level searches should be slow against the archives
##                  and fast against an m/z-ordered projection.
##
## Archives are synthetic (see tests/testthat/helper-mzpeak.R) so the script
## runs anywhere. Absolute numbers are therefore indicative; the ratios are
## the point. Re-run against real archives from the public corpus linked at
## https://www.mzpeak.org/ before drawing conclusions about real data.
##
##   Rscript inst/benchmarks/benchmark-mzpeak.R

N_RUNS <- 20L
N_SPECTRA <- 500L
N_POINTS <- 500L

required <- c("Spectra", "devtools", "arrow", "jsonlite")
missing <- required[!vapply(required, requireNamespace, logical(1L),
                            quietly = TRUE)]
if (length(missing))
    stop("Missing packages: ", paste(missing, collapse = ", "), call. = FALSE)

suppressPackageStartupMessages({
    library(Spectra)
})
devtools::load_all(quiet = TRUE)
source(system.file("..", "tests", "testthat", "helper-mzpeak.R",
                   package = "MsBackendParquet", mustWork = FALSE))
if (!exists(".make_mzpeak_archive"))
    source(file.path("tests", "testthat", "helper-mzpeak.R"))

du <- function(p)
    sum(file.info(list.files(p, recursive = TRUE, full.names = TRUE))$size,
        na.rm = TRUE)
tm <- function(f, reps = 5L)
    median(replicate(reps, system.time(f())[["elapsed"]])) * 1000

root <- tempfile("mzpeak_bench_")
dir.create(root)
on.exit(unlink(root, recursive = TRUE), add = TRUE)

message("Building ", N_RUNS, " synthetic archives (",
        format(N_RUNS * N_SPECTRA * N_POINTS, big.mark = ","), " points) ...")
archives <- vapply(seq_len(N_RUNS), function(i) {
    d <- file.path(root, sprintf("R%02d", i))
    .make_mzpeak_archive(d, n = N_SPECTRA, run_id = sprintf("R%02d", i),
                         npeaks = N_POINTS)
    d
}, character(1))
archive_bytes <- du(root)

ds <- file.path(root, "dataset")
t0 <- Sys.time()
createMzPeakDataset(archives, path = ds, verbose = FALSE)
ingest_s <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
index_bytes <- du(ds)

be <- backendInitialize(MsBackendParquet(), path = ds)

## A peak value that exists in exactly one spectrum per run.
target <- 100 * (N_SPECTRA / 2 + 1) + 7

res <- data.frame(
    query = c("filterRt", "filterMsLevel", "filterPrecursorMzRange",
              "spectraData (metadata)", "peaksData (200 spectra)",
              "filterContainsMz (archives)"),
    ms = c(
        tm(function() filterRt(be, c(100, 200))),
        tm(function() filterMsLevel(be, 2L)),
        tm(function() filterPrecursorMzRange(be, c(400, 450))),
        ## Metadata only: `spectraData(be)` with no `columns` asks for every
        ## spectra variable, which includes mz and intensity and therefore
        ## reads all the peaks.
        tm(function() spectraData(be, c("msLevel", "rtime", "precursorMz",
                                        "totIonCurrent")), 3L),
        tm(function() peaksData(be[seq(1, length(be),
                                       length.out = 200)]), 3L),
        tm(function() filterContainsMz(be, target, tolerance = 0.2,
                                       ppm = 0))),
    stringsAsFactors = FALSE)

message("Building the m/z-ordered projection ...")
t0 <- Sys.time()
buildProjection(ds, verbose = FALSE)
proj_s <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
proj_bytes <- du(file.path(ds, "index", "projections"))

be2 <- backendInitialize(MsBackendParquet(), path = ds)
res <- rbind(res, data.frame(
    query = "filterContainsMz (projection)",
    ms = tm(function() filterContainsMz(be2, target, tolerance = 0.2,
                                        ppm = 0)),
    stringsAsFactors = FALSE))

## The projection is a cache, so it must not change the answer.
stopifnot(identical(
    filterContainsMz(be, target, tolerance = 0.2, ppm = 0)@spectraIds,
    filterContainsMz(be2, target, tolerance = 0.2, ppm = 0)@spectraIds))

cat("\n== storage ==\n")
cat(sprintf("archives         %8.1f MB\n", archive_bytes / 1e6))
cat(sprintf("derived index    %8.1f MB  (%.2f%% of archives)\n",
            index_bytes / 1e6, 100 * index_bytes / archive_bytes))
cat(sprintf("mzsorted proj.   %8.1f MB  (%.2f%% of archives)\n",
            proj_bytes / 1e6, 100 * proj_bytes / archive_bytes))

cat("\n== build ==\n")
cat(sprintf("ingest (metadata only) %6.2f s for %s spectra\n",
            ingest_s, format(length(be), big.mark = ",")))
cat(sprintf("projection build       %6.2f s\n", proj_s))

cat("\n== queries (median ms) ==\n")
print(res, row.names = FALSE, digits = 4)

speedup <- res$ms[res$query == "filterContainsMz (archives)"] /
    res$ms[res$query == "filterContainsMz (projection)"]
cat(sprintf("\nprojection speedup on peak search: %.1fx\n", speedup))
