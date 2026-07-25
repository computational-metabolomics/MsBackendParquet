## Configuration sweeps for MsBackendParquet.
##
## The SpectraQL scoreboard (benchmark-spectraql-multifile.R) fixes every
## MsBackendParquet knob at its default and compares backends. This script does
## the orthogonal thing: it holds the backend fixed (Parquet only -- competitor
## backends are irrelevant to these axes) and sweeps the tunables, to show how
## query latency and on-disk size move with them.
##
## Two axes are runtime-only and need no rewrite, so they run at the full
## scoreboard scale (50 samples): DuckDB `threads` and the metadata cache
## (`MsBackendParquet.cacheMetadata`). Two are dataset-creation params that
## force a full rewrite per value, so they run at a smaller scale (10 samples)
## to keep total runtime bounded: `row_group_size` and `compression`.
##
## Tidy results are written to inst/benchmarks/results/sweep-<axis>-<date>.csv
## (build-ignored). Two committed summary figures are written to
## inst/benchmarks/figures/ and embedded in performance.md.
##
##   Rscript inst/benchmarks/benchmark-sweeps.R
##   N_SAMPLES_RUNTIME=50 N_SAMPLES_REWRITE=10 ITER=5 Rscript inst/benchmarks/benchmark-sweeps.R

N_RUNTIME <- as.integer(Sys.getenv("N_SAMPLES_RUNTIME", "50"))
N_REWRITE <- as.integer(Sys.getenv("N_SAMPLES_REWRITE", "10"))
ITER      <- as.integer(Sys.getenv("ITER", "5"))
THREADS    <- as.integer(strsplit(Sys.getenv("THREADS", "1,2,4,8,12"), ",")[[1L]])
ROW_GROUPS <- as.integer(strsplit(Sys.getenv("ROW_GROUPS", "50,100,250,1000,5000"), ",")[[1L]])
CODECS     <- strsplit(Sys.getenv("CODECS", "uncompressed,snappy,gzip,zstd,lz4"), ",")[[1L]]

required <- c("bench", "SpectraQL", "MsDataHub", "Spectra", "devtools", "DBI")
missing <- required[!vapply(required, requireNamespace, logical(1L),
                            quietly = TRUE)]
if (length(missing))
    stop("Missing packages: ", paste(missing, collapse = ", "),
         "\nInstall with: BiocManager::install(c(",
         paste0("'", missing, "'", collapse = ", "), "))", call. = FALSE)

has_plots <- requireNamespace("ggplot2", quietly = TRUE) &&
             requireNamespace("patchwork", quietly = TRUE)
if (!has_plots)
    message("ggplot2/patchwork not installed: will write CSVs but skip figures.")

suppressPackageStartupMessages({
    library(Spectra)
    library(SpectraQL)
    library(MsDataHub)
    library(bench)
})
devtools::load_all(quiet = TRUE)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

dir_size <- function(p) {
    fi <- file.info(list.files(p, recursive = TRUE, full.names = TRUE))
    if (!nrow(fi)) 0 else sum(fi$size, na.rm = TRUE)
}

src <- MsDataHub::PestMix1_DDA.mzML()

## Copies, not symlinks: normalizePath() collapses symlinks back to the source,
## which would give every spectrum the same dataOrigin. Track dirs for cleanup.
temp_dirs <- character()
make_files <- function(k) {
    d <- tempfile("samples_"); dir.create(d)
    temp_dirs <<- c(temp_dirs, d)
    vapply(seq_len(k), function(i) {
        dst <- file.path(d, sprintf("sample_%02d.mzML", i))
        file.copy(src, dst, overwrite = TRUE)
        dst
    }, character(1L))
}

temp_paths <- character()
new_dataset <- function(files, ...) {
    path <- tempfile()
    temp_paths <<- c(temp_paths, path)
    be <- mzMLToParquet(files, path = path, engine = "mzr", batch_size = 1000L,
                        overwrite = TRUE, verbose = FALSE, ...)
    list(sps = Spectra(be), path = path)
}

on.exit({
    unlink(temp_paths, recursive = TRUE)
    unlink(temp_dirs, recursive = TRUE)
}, add = TRUE)

## The queries the sweeps are read through. Two metadata-only filters and two
## peak-heavy pulls -- the split is what makes the thread- and cache-sensitivity
## visible (peak reads hit DuckDB; metadata filters are answered from the
## in-memory index when the cache is on).
Q <- list(
    rt_range      = "QUERY * WHERE RTMIN = 200 AND RTMAX = 300",
    precursor_ppm = "QUERY * WHERE MS2PREC = 278.093:TOLERANCEPPM=30",
    ms1_peaks     = "QUERY MS1DATA WHERE RTMIN = 200 AND RTMAX = 300",
    ms1_tic       = "QUERY scansum(MS1DATA) WHERE RTMIN = 200 AND RTMAX = 300"
)

## Median wall-clock of one query in ms. One untimed warm-up first so the first
## touch (view registration, Parquet footer read) is not charged to the timing.
time_query <- function(sps, qn, iter = ITER) {
    try(suppressWarnings(query(sps, Q[[qn]])), silent = TRUE)
    b <- bench::mark(query(sps, Q[[qn]]), iterations = iter,
                     check = FALSE, filter_gc = FALSE, memory = FALSE)
    as.numeric(b$median) * 1000
}

# ------------------------------------------------------------------------------
# Runtime-only sweeps (one dataset, re-queried) -- at the full 50-sample scale
# ------------------------------------------------------------------------------

cat(sprintf("== Building runtime-sweep dataset (%d samples) ==\n", N_RUNTIME))
rt <- new_dataset(make_files(N_RUNTIME))
sps_rt <- rt$sps
cat(sprintf("  %d spectra, %s on disk\n", length(sps_rt),
            format(structure(dir_size(rt$path), class = "object_size"),
                   units = "auto")))

## The DuckDB connection is created once per process and `SET threads` runs only
## at connection setup (.duckdb_configure in R/duckdb-connection.R), so setting
## the option alone is a no-op on the live handle. Issue SET on the connection
## directly; keep the option in sync so the value is also recorded the usual way.
con <- MsBackendParquet:::.duckdb_con()
set_threads <- function(k) {
    options(MsBackendParquet.threads = as.integer(k))
    DBI::dbExecute(con, sprintf("SET threads = %d", as.integer(k)))
}
get_threads <- function()
    as.integer(DBI::dbGetQuery(con, "SELECT current_setting('threads') AS t")$t)

cat("\n== Sweep: DuckDB threads ==\n")
thr_queries <- c("rt_range", "ms1_peaks", "ms1_tic")
threads_rows <- list()
for (k in THREADS) {
    set_threads(k)
    actual <- get_threads()
    for (qn in thr_queries) {
        ms <- time_query(sps_rt, qn)
        cat(sprintf("  threads=%2d (%2d) %-10s %8.2f ms\n", k, actual, qn, ms))
        threads_rows[[length(threads_rows) + 1L]] <- data.frame(
            threads = k, actual_threads = actual, query = qn,
            median_ms = round(ms, 2))
    }
}
threads_df <- do.call(rbind, threads_rows)

cat("\n== Sweep: metadata cache ==\n")
invisible(set_threads(max(THREADS)))    # fix threads so the axes don't interact
cache_queries <- c("rt_range", "precursor_ppm", "ms1_peaks")
cache_rows <- list()
for (mode in c("auto", "off")) {
    options(MsBackendParquet.cacheMetadata = mode)
    for (qn in cache_queries) {
        ms <- time_query(sps_rt, qn)
        cat(sprintf("  cache=%-4s %-14s %8.2f ms\n", mode, qn, ms))
        cache_rows[[length(cache_rows) + 1L]] <- data.frame(
            cache = mode, query = qn, median_ms = round(ms, 2))
    }
}
options(MsBackendParquet.cacheMetadata = "auto")
cache_df <- do.call(rbind, cache_rows)

# ------------------------------------------------------------------------------
# Creation-param sweeps (rewrite per value) -- at the smaller 10-sample scale
# ------------------------------------------------------------------------------

cat(sprintf("\n== Building rewrite-sweep source files (%d samples) ==\n",
            N_REWRITE))
files_rw <- make_files(N_REWRITE)

cat("\n== Sweep: row_group_size ==\n")
rg_rows <- list()
for (r in ROW_GROUPS) {
    ds <- new_dataset(files_rw, row_group_size = r)
    size_mb <- dir_size(ds$path) / 1024^2
    for (qn in c("rt_range", "ms1_peaks")) {
        ms <- time_query(ds$sps, qn)
        cat(sprintf("  row_group=%5d %-10s %8.2f ms  (%.1f MB)\n",
                    r, qn, ms, size_mb))
        rg_rows[[length(rg_rows) + 1L]] <- data.frame(
            row_group_size = r, query = qn, median_ms = round(ms, 2),
            on_disk_mb = round(size_mb, 2))
    }
    unlink(ds$path, recursive = TRUE)
}
rg_df <- do.call(rbind, rg_rows)

cat("\n== Sweep: compression codec ==\n")
cz_rows <- list()
for (codec in CODECS) {
    ds <- tryCatch(new_dataset(files_rw, compression = codec),
                   error = function(e) {
                       message(sprintf("  codec '%s' skipped: %s",
                                       codec, conditionMessage(e)))
                       NULL
                   })
    if (is.null(ds)) next
    size_mb <- dir_size(ds$path) / 1024^2
    ms <- time_query(ds$sps, "ms1_peaks")
    cat(sprintf("  codec=%-12s %8.2f ms  (%.1f MB)\n", codec, ms, size_mb))
    cz_rows[[length(cz_rows) + 1L]] <- data.frame(
        codec = codec, median_ms = round(ms, 2),
        on_disk_mb = round(size_mb, 2))
    unlink(ds$path, recursive = TRUE)
}
cz_df <- do.call(rbind, cz_rows)

# ------------------------------------------------------------------------------
# Write tidy CSVs
# ------------------------------------------------------------------------------

outdir <- file.path("inst", "benchmarks", "results")
dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
today <- Sys.Date()
csv <- function(df, axis)
    write.csv(df, file.path(outdir, sprintf("sweep-%s-%s.csv", axis, today)),
              row.names = FALSE)
csv(threads_df, "threads")
csv(cache_df, "cache")
csv(rg_df, "rowgroup")
csv(cz_df, "compression")
cat(sprintf("\nWrote 4 CSVs to %s\n", outdir))

# ------------------------------------------------------------------------------
# Figures (ggplot2 + patchwork). Palette: dataviz reference slots 1-3
# (blue/orange/aqua), validated colorblind-safe on the light surface. No
# dual-axis anywhere: latency and on-disk size are always separate panels.
# ------------------------------------------------------------------------------

if (has_plots) {
    suppressPackageStartupMessages({ library(ggplot2); library(patchwork) })

    BLUE <- "#2a78d6"; ORANGE <- "#eb6834"; AQUA <- "#1baf7a"
    INK <- "#0b0b0b"; INK2 <- "#52514e"; MUTED <- "#898781"
    GRID <- "#e1e0d9"; SURFACE <- "#fcfcfb"

    theme_sweep <- theme_minimal(base_size = 11) + theme(
        plot.background   = element_rect(fill = SURFACE, colour = NA),
        panel.background  = element_rect(fill = SURFACE, colour = NA),
        panel.grid.major  = element_line(colour = GRID, linewidth = 0.3),
        panel.grid.minor  = element_blank(),
        axis.title        = element_text(colour = INK2),
        axis.text         = element_text(colour = MUTED),
        plot.title        = element_text(colour = INK, face = "bold", size = 12),
        plot.subtitle     = element_text(colour = INK2, size = 9),
        legend.title      = element_text(colour = INK2, size = 9),
        legend.text       = element_text(colour = INK2, size = 9),
        legend.position   = "top"
    )

    ## -- threads: line, one series per query, direct end-labels (relief rule
    ##    for aqua's sub-3:1 contrast) plus a legend.
    q_lab <- c(rt_range = "RT range filter", ms1_peaks = "MS1 peak data",
               ms1_tic = "MS1 TIC")
    td <- threads_df
    td$query <- factor(td$query, levels = names(q_lab), labels = q_lab)
    end <- td[td$threads == max(THREADS), ]
    p_threads <- ggplot(td, aes(threads, median_ms, colour = query)) +
        geom_line(linewidth = 0.7) + geom_point(size = 2) +
        geom_text(data = end, aes(label = query), hjust = 0, nudge_x = 0.4,
                  size = 3, show.legend = FALSE) +
        scale_colour_manual(values = setNames(c(BLUE, ORANGE, AQUA),
                                              levels(td$query))) +
        scale_x_continuous(breaks = THREADS,
                           limits = c(min(THREADS), max(THREADS) + 6)) +
        labs(title = "DuckDB threads",
             subtitle = sprintf("query latency vs thread count (%d samples)",
                                N_RUNTIME),
             x = "threads", y = "median latency (ms)", colour = NULL) +
        theme_sweep + theme(legend.position = "none")

    ## -- metadata cache: grouped bar, auto vs off, per query.
    cd <- cache_df
    cd$query <- factor(cd$query, levels = cache_queries,
                       labels = c("RT range", "precursor ppm", "MS1 peaks"))
    cd$cache <- factor(cd$cache, levels = c("auto", "off"))
    p_cache <- ggplot(cd, aes(query, median_ms, fill = cache)) +
        geom_col(position = position_dodge(width = 0.72), width = 0.64,
                 colour = SURFACE, linewidth = 0.6) +
        scale_fill_manual(values = c(auto = BLUE, off = ORANGE)) +
        labs(title = "Metadata cache",
             subtitle = sprintf("filter latency: in-memory index vs SQL (%d samples)",
                                N_RUNTIME),
             x = NULL, y = "median latency (ms)", fill = "cacheMetadata") +
        theme_sweep

    ## -- row_group_size: two panels (latency, storage). Never dual-axis.
    rgl <- rg_df
    rgl$query <- factor(rgl$query, levels = c("rt_range", "ms1_peaks"),
                        labels = c("RT range", "MS1 peaks"))
    p_rg_lat <- ggplot(rgl, aes(factor(row_group_size), median_ms,
                                colour = query, group = query)) +
        geom_line(linewidth = 0.7) + geom_point(size = 2) +
        scale_colour_manual(values = c("RT range" = BLUE, "MS1 peaks" = ORANGE)) +
        labs(title = "Row-group size — latency",
             subtitle = sprintf("%d samples", N_REWRITE),
             x = "row_group_size (spectra)", y = "median latency (ms)",
             colour = NULL) +
        theme_sweep
    rg_size <- unique(rg_df[, c("row_group_size", "on_disk_mb")])
    p_rg_size <- ggplot(rg_size, aes(factor(row_group_size), on_disk_mb)) +
        geom_col(fill = BLUE, width = 0.64) +
        geom_text(aes(label = sprintf("%.0f", on_disk_mb)), vjust = -0.4,
                  size = 3, colour = INK2) +
        labs(title = "Row-group size — storage", x = "row_group_size (spectra)",
             y = "on-disk size (MB)") +
        theme_sweep

    ## -- compression: two panels (storage, latency), one bar per codec.
    cz_df$codec <- factor(cz_df$codec, levels = CODECS)
    p_cz_size <- ggplot(cz_df, aes(codec, on_disk_mb)) +
        geom_col(fill = BLUE, width = 0.64) +
        geom_text(aes(label = sprintf("%.0f", on_disk_mb)), vjust = -0.4,
                  size = 3, colour = INK2) +
        labs(title = "Compression — storage",
             subtitle = sprintf("%d samples", N_REWRITE),
             x = NULL, y = "on-disk size (MB)") +
        theme_sweep
    p_cz_lat <- ggplot(cz_df, aes(codec, median_ms)) +
        geom_col(fill = ORANGE, width = 0.64) +
        geom_text(aes(label = sprintf("%.0f", median_ms)), vjust = -0.4,
                  size = 3, colour = INK2) +
        labs(title = "Compression — MS1 peak latency", x = NULL,
             y = "median latency (ms)") +
        theme_sweep

    figdir <- file.path("inst", "benchmarks", "figures")
    dir.create(figdir, showWarnings = FALSE, recursive = TRUE)
    dev <- if (requireNamespace("ragg", quietly = TRUE))
               ragg::agg_png else grDevices::png

    fig_runtime <- (p_threads | p_cache) +
        patchwork::plot_annotation(
            title = "MsBackendParquet runtime-config sweeps",
            theme = theme(plot.title = element_text(colour = INK, face = "bold")))
    ggsave(file.path(figdir, "sweeps-runtime.png"), fig_runtime,
           width = 11, height = 4.6, dpi = 150, bg = SURFACE, device = dev)

    fig_storage <- (p_rg_lat | p_rg_size) / (p_cz_size | p_cz_lat) +
        patchwork::plot_annotation(
            title = "MsBackendParquet storage-config sweeps",
            theme = theme(plot.title = element_text(colour = INK, face = "bold")))
    ggsave(file.path(figdir, "sweeps-storage.png"), fig_storage,
           width = 11, height = 8.2, dpi = 150, bg = SURFACE, device = dev)

    cat(sprintf("Wrote 2 figures to %s\n", figdir))
}

cat("\nDone.\n")
