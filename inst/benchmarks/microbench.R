## Per-operation microbenchmark for MsBackendParquet.
##
## The SpectraQL scripts measure whole queries, which is the right end-to-end
## scoreboard but too coarse to attribute a cost to a single operation. This one
## times the individual backend operations a query is built from, and separates
## the *fixed* cost of an operation from its *per-spectrum* cost by running at
## several dataset sizes and fitting time against spectrum count.
##
## A fixed cost that does not fall with dataset size is per-query overhead:
## Parquet footer parsing, SQL planning, view resolution. A cost proportional to
## spectra is real work. The two have completely different fixes, so the split
## is the point of this script.
##
## Results are written to inst/benchmarks/results/microbench-<date>.csv. Set
## LABEL to tag a run when comparing configurations.
##
##   Rscript inst/benchmarks/microbench.R
##   N_SAMPLES=1,10,50 LABEL=nothreads Rscript inst/benchmarks/microbench.R

N_SAMPLES <- as.integer(strsplit(Sys.getenv("N_SAMPLES", "1,10"), ",")[[1L]])
LABEL <- Sys.getenv("LABEL", "current")
ITER <- as.integer(Sys.getenv("ITER", "10"))

required <- c("bench", "MsDataHub", "Spectra", "devtools", "mzR")
missing <- required[!vapply(required, requireNamespace, logical(1L),
                            quietly = TRUE)]
if (length(missing))
    stop("Missing packages: ", paste(missing, collapse = ", "), call. = FALSE)

suppressPackageStartupMessages({
    library(Spectra); library(bench)
})
devtools::load_all(quiet = TRUE)

fmt_size <- function(n)
    format(structure(as.numeric(n), class = "object_size"), units = "auto")

dir_size <- function(p) {
    fi <- file.info(list.files(p, recursive = TRUE, full.names = TRUE))
    if (!nrow(fi)) 0 else sum(fi$size, na.rm = TRUE)
}

src <- MsDataHub::PestMix1_DDA.mzML()

## Copies rather than symlinks: normalizePath() collapses symlinks back to the
## source, which would give every spectrum the same dataOrigin.
make_files <- function(k) {
    if (k == 1L) return(src)
    d <- tempfile("samples_"); dir.create(d)
    vapply(seq_len(k), function(i) {
        dst <- file.path(d, sprintf("sample_%02d.mzML", i))
        file.copy(src, dst, overwrite = TRUE)
        dst
    }, character(1L))
}

## The operations under test. Each takes an initialised backend and is timed
## in isolation. `setup` values are computed once, outside the timing, so the
## measurement is of the operation and not of building its arguments.
operations <- list(
    backendInitialize = function(be, path, sps)
        backendInitialize(MsBackendParquet(), path = path),
    filterRt = function(be, path, sps)
        filterRt(be, c(200, 300)),
    filterRt_narrow = function(be, path, sps)
        filterRt(be, c(250, 260)),
    filterMsLevel = function(be, path, sps)
        filterMsLevel(be, 1L),
    filterPrecursorMzValues = function(be, path, sps)
        filterPrecursorMzValues(be, 278.093, ppm = 30),
    uniqueMsLevels = function(be, path, sps)
        uniqueMsLevels(be),
    spectraData_1col = function(be, path, sps)
        spectraData(be, "rtime"),
    spectraData_all = function(be, path, sps)
        spectraData(be),
    peaksData = function(be, path, sps)
        peaksData(be),
    peaksData_subset = function(be, path, sps)
        peaksData(filterRt(be, c(200, 300))),
    mz = function(be, path, sps)
        mz(be),
    intensity = function(be, path, sps)
        intensity(be),
    ionCount = function(be, path, sps)
        ionCount(sps),
    subset_scattered = function(be, path, sps)
        be[seq(1L, length(be), by = 7L)]
)

rows <- list()
for (k in N_SAMPLES) {
    files <- make_files(k)
    path <- tempfile()
    cat(sprintf("\n=== %d sample(s) ===\n", k))

    t_write <- system.time(
        be <- mzMLToParquet(files, path = path, engine = "mzr",
                            batch_size = 1000L, overwrite = TRUE,
                            verbose = FALSE))[["elapsed"]]
    sps <- Spectra(be)
    n <- length(be)
    on_disk <- dir_size(path)
    cat(sprintf("  %d spectra, %s on disk, written in %.1fs\n",
                n, fmt_size(on_disk), t_write))

    rows[[length(rows) + 1L]] <- data.frame(
        label = LABEL, n_samples = k, n_spectra = n,
        operation = "convert", median_ms = round(t_write * 1000, 1),
        mem_mb = NA_real_, on_disk_mb = round(on_disk / 1024^2, 2))

    for (nm in names(operations)) {
        fn <- operations[[nm]]
        ## One untimed call: the first touch of a dataset pays for view
        ## registration and the Parquet footer read, which would otherwise be
        ## charged to whichever operation happens to run first.
        try(fn(be, path, sps), silent = TRUE)
        b <- tryCatch(
            bench::mark(fn(be, path, sps), iterations = ITER, check = FALSE,
                        filter_gc = FALSE, memory = FALSE),
            error = function(e) {
                cat(sprintf("  %-24s ERROR %s\n", nm, conditionMessage(e)))
                NULL
            })
        if (is.null(b)) next
        ms <- as.numeric(b$median) * 1000
        cat(sprintf("  %-24s %8.2f ms\n", nm, ms))
        rows[[length(rows) + 1L]] <- data.frame(
            label = LABEL, n_samples = k, n_spectra = n,
            operation = nm, median_ms = round(ms, 2),
            mem_mb = NA_real_, on_disk_mb = round(on_disk / 1024^2, 2))
    }

    unlink(path, recursive = TRUE)
    if (k > 1L) unlink(dirname(files[1L]), recursive = TRUE)
}

res <- do.call(rbind, rows)

## Fixed vs per-spectrum split. With >= 2 sizes, fit median_ms ~ n_spectra per
## operation: the intercept is per-query overhead, the slope is real work.
if (length(N_SAMPLES) > 1L) {
    cat("\n== Fixed vs per-spectrum cost ==\n")
    split_tbl <- do.call(rbind, lapply(split(res, res$operation), function(d) {
        if (nrow(d) < 2L) return(NULL)
        fit <- stats::lm(median_ms ~ n_spectra, data = d)
        data.frame(operation = d$operation[1L],
                   fixed_ms = round(unname(coef(fit)[1L]), 2),
                   us_per_1k_spectra =
                       round(unname(coef(fit)[2L]) * 1000 * 1000, 1))
    }))
    split_tbl <- split_tbl[order(-split_tbl$fixed_ms), ]
    print(split_tbl, row.names = FALSE)
}

outdir <- file.path("inst", "benchmarks", "results")
dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
out <- file.path(outdir, sprintf("microbench-%s.csv", Sys.Date()))
write.csv(res, out, row.names = FALSE)
cat(sprintf("\nWrote %s\n", out))
