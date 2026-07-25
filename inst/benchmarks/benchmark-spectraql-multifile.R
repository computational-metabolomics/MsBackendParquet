## Benchmark (multi-file): MsBackendParquet vs MsBackendMzR vs
##   MsBackendHdf5Peaks vs MsBackendOfflineSql
##
## Sibling to benchmark-spectraql.R. The single-file benchmark is too
## small to exercise the regime where Parquet+DuckDB wins (per-query
## overhead dominates and the in-memory metadata backends always win
## narrow filters). This script copies PestMix1_DDA.mzML N_SAMPLES
## times so each copy gets a distinct dataOrigin path, then runs the
## same 7-query SpectraQL suite against every backend built from those
## files.
##
## At N_SAMPLES = 50 (380,100 spectra) Parquet leads 5 of the 7 queries.
## Measured 2026-07-25, medians in ms:
##
##            query   parquet     mzr    hdf5     sql
##         rt_range     12.24   15.44   24.05  380.19
##        rt_narrow     11.23    5.33   14.76  348.17
##    precursor_ppm     14.09   21.66   28.66  336.33
## rt_and_precursor     12.56   17.25   27.75  389.22
##        ms1_peaks    314.82 1510.00 1440.00  919.04
##          ms1_tic    506.79 1390.00 1490.00  890.82
##         scaninfo     26.09   17.27   22.30  718.91
##
## The metadata-only filters are competitive because the backend keeps the
## non-peak columns in an in-memory index -- see R/metadata-cache.R -- just as
## mzr and hdf5 keep the whole spectra table cache-resident, so those queries
## are answered in R and only peak data goes to disk. `rt_narrow` and
## `scaninfo` are the two mzr edges out, both by a couple of ms.
##
## Design considerations behind these numbers:
##   inst/benchmarks/performance.md
##
## Parquet is written *unpartitioned*. An ad-hoc comparison at this
## scale showed `partitioning = "msLevel"`, `partitioning = "dataOrigin"`
## and the combined `c("dataOrigin","msLevel")` each lost to no
## partitioning on most queries (the combined form was strictly worse);
## the mzr-streaming writer already produces one Parquet file per input
## mzML, so DuckDB already gets file-level parallelism for free.
##
##   Rscript inst/benchmarks/benchmark-spectraql-multifile.R

N_SAMPLES <- 50L

required <- c("bench", "SpectraQL", "MsDataHub", "Spectra", "devtools",
              "dplyr", "rlang")
missing <- required[!vapply(required, requireNamespace, logical(1L),
                            quietly = TRUE)]
if (length(missing)) {
    stop("Missing core packages: ", paste(missing, collapse = ", "),
         "\nInstall with: BiocManager::install(c(",
         paste0("'", missing, "'", collapse = ", "), "))",
         call. = FALSE)
}

suppressPackageStartupMessages({
    library(Spectra)
    library(SpectraQL)
    library(MsDataHub)
    library(bench)
})

devtools::load_all(quiet = TRUE)

has_hdf5 <- requireNamespace("rhdf5", quietly = TRUE)
has_sql  <- requireNamespace("MsBackendSql", quietly = TRUE) &&
            requireNamespace("RSQLite", quietly = TRUE)
if (!has_hdf5)
    message("Skipping MsBackendHdf5Peaks: install 'rhdf5' (BiocManager::install('rhdf5')).")
if (!has_sql)
    message("Skipping MsBackendSql: install 'MsBackendSql' and 'RSQLite' ",
            "(BiocManager::install(c('MsBackendSql', 'RSQLite'))).")

## Build N_SAMPLES copies of the source mzML so each has a distinct
## dataOrigin. Symlinks would not work — normalizePath() collapses them
## back to the source file, so all spectra would share one dataOrigin.
src <- MsDataHub::PestMix1_DDA.mzML()
copy_dir <- tempfile("samples_")
dir.create(copy_dir)
files <- vapply(seq_len(N_SAMPLES), function(i) {
    dst <- file.path(copy_dir, sprintf("sample_%02d.mzML", i))
    file.copy(src, dst, overwrite = TRUE)
    dst
}, character(1L))
cat(sprintf("Prepared %d sample file(s), %s on disk.\n",
            length(files),
            format(structure(sum(file.info(files)$size),
                             class = "object_size"),
                   units = "auto")))

parquet_path <- tempfile()
hdf5_dir <- tempfile(); dir.create(hdf5_dir)
sql_file <- tempfile(fileext = ".sqlite")
on.exit({
    unlink(copy_dir, recursive = TRUE)
    unlink(parquet_path, recursive = TRUE)
    unlink(hdf5_dir, recursive = TRUE)
    unlink(sql_file)
}, add = TRUE)

cat("== Building backends ==\n")

t_parquet <- system.time({
    be <- mzMLToParquet(files, path = parquet_path, engine = "mzr",
                        partitioning = character(),
                        batch_size = 1000L, overwrite = TRUE)
    sps_parquet <- Spectra(be)
})

t_mzr <- system.time({
    sps_mzr <- Spectra(files, source = MsBackendMzR())
})

sps_hdf5 <- NULL
t_hdf5 <- NULL
if (has_hdf5) {
    ## MsBackendHdf5Peaks does not import raw mzML on its own — it needs
    ## an in-memory Spectra (e.g. from MsBackendMzR) handed off via
    ## setBackend(). The setBackend pass dominates t_hdf5.
    t_hdf5 <- system.time({
        sps_hdf5 <- setBackend(sps_mzr, MsBackendHdf5Peaks(),
                               hdf5path = hdf5_dir)
    })
}

sps_sql <- NULL
t_sql <- NULL
if (has_sql) {
    t_sql <- system.time({
        sql_con <- DBI::dbConnect(RSQLite::SQLite(), sql_file)
        MsBackendSql::createMsBackendSqlDatabase(sql_con, files)
        DBI::dbDisconnect(sql_con)
        sps_sql <- Spectra(sql_file,
                           source = MsBackendSql::MsBackendOfflineSql(),
                           drv = RSQLite::SQLite())
    })
}

dir_size <- function(p) {
    fi <- file.info(list.files(p, recursive = TRUE, full.names = TRUE))
    if (!nrow(fi)) 0 else sum(fi$size, na.rm = TRUE)
}
fmt_size <- function(n) format(structure(n, class = "object_size"),
                               units = "auto")

backends <- list(parquet = sps_parquet, mzr = sps_mzr)
if (has_hdf5) backends$hdf5 <- sps_hdf5
if (has_sql)  backends$sql  <- sps_sql

backend_syms <- list(parquet = quote(sps_parquet), mzr = quote(sps_mzr))
if (has_hdf5) backend_syms$hdf5 <- quote(sps_hdf5)
if (has_sql)  backend_syms$sql  <- quote(sps_sql)

elapsed <- c(parquet = t_parquet[["elapsed"]], mzr = t_mzr[["elapsed"]])
if (has_hdf5) elapsed["hdf5"] <- t_hdf5[["elapsed"]]
if (has_sql)  elapsed["sql"]  <- t_sql[["elapsed"]]

storage_size <- c(
    parquet = dir_size(parquet_path),
    mzr     = sum(file.info(files)$size, na.rm = TRUE)
)
if (has_hdf5) storage_size["hdf5"] <- dir_size(hdf5_dir)
if (has_sql)  storage_size["sql"]  <- unname(file.info(sql_file)$size)

construction <- data.frame(
    backend   = names(backends),
    elapsed_s = elapsed[names(backends)],
    n_spectra = vapply(backends, length, integer(1L)),
    on_disk   = vapply(storage_size[names(backends)], fmt_size, character(1L))
)
print(construction, row.names = FALSE)

## Sanity: all backends should return the same number of spectra
## for a deterministic predicate. At N_SAMPLES = 50 we expect
## 717 * 50 == 35850.
sanity_q <- "QUERY * WHERE RTMIN = 200 AND RTMAX = 300"
sanity_n <- vapply(backends, function(s) length(query(s, sanity_q)),
                   integer(1L))
if (length(unique(sanity_n)) != 1L) {
    warning("Cross-backend result lengths disagree: ",
            paste(names(sanity_n), sanity_n, sep = "=", collapse = ", "))
} else {
    cat(sprintf("\nCross-backend sanity OK (%d spectra match RT 200-300).\n",
                sanity_n[[1L]]))
}

queries <- list(
    rt_range         = "QUERY * WHERE RTMIN = 200 AND RTMAX = 300",
    rt_narrow        = "QUERY * WHERE RTMIN = 250 AND RTMAX = 260",
    precursor_ppm    = "QUERY * WHERE MS2PREC = 278.093:TOLERANCEPPM=30",
    rt_and_precursor = "QUERY * WHERE RTMIN = 200 AND RTMAX = 300 AND MS2PREC = 278.093:TOLERANCEPPM=30",
    ms1_peaks        = "QUERY MS1DATA WHERE RTMIN = 200 AND RTMAX = 300",
    ms1_tic          = "QUERY scansum(MS1DATA) WHERE RTMIN = 200 AND RTMAX = 300",
    scaninfo         = "QUERY scaninfo(*) WHERE RTMIN = 200 AND RTMAX = 300"
)

run_one <- function(q) {
    exprs <- lapply(backend_syms, function(sym) bquote(query(.(sym), .(q))))
    ## memory = FALSE is required: DuckDB's scan kernel runs across
    ## multiple threads on this larger dataset and bench::mark's memory
    ## profiler can't sample parallel code.
    rlang::inject(bench::mark(
        !!!exprs,
        iterations = 5,
        check = FALSE,
        filter_gc = FALSE,
        memory = FALSE
    ))
}

cat("\n== Running query benchmarks ==\n")
results <- lapply(names(queries), function(name) {
    cat(sprintf("  - %s\n", name))
    res <- run_one(queries[[name]])
    res$expression <- factor(names(backends), levels = names(backends))
    dplyr::mutate(res, query = name, .before = 1)
})

results_df <- do.call(rbind, results)
report <- dplyr::select(results_df, query, expression, min, median,
                        n_itr, n_gc)

cat("\n== Results ==\n")
print(report, n = Inf)

cat(sprintf("\nParquet dataset: %s\n", parquet_path))
