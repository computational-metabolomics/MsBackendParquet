# Per-run metadata: `index/samples.parquet`.
#
# One row per run, keyed by `run_id`, holding whatever the experiment calls a
# sample -- subject, timepoint, group, injection order. It is stored once per
# run rather than once per spectrum, and it never reaches DuckDB.
#
# That is not a space argument: a constant column is dictionary-encoded to
# almost nothing, and copying it into the per-spectrum index would cost little
# on disk. It is a consequence of what a run already is. Ids are handed out in
# one contiguous block per run (see `R/mzstack-manifest.R`), so "timepoint ==
# 6" is exactly "spectrum_id_ in these ranges" -- a range predicate on the
# column the data is ordered by, which prunes row groups exactly. Resolving it
# against a table of a few thousand rows in R is cheaper than reading a column
# of a few million, and it leaves the bounded in-memory index
# (`R/metadata-cache.R`) for variables that genuinely vary per spectrum.
#
# The other half is mutability. Sample metadata is the one thing that
# reliably changes after ingest -- a mislabelled vial, a clinical variable
# that arrives months later. Held here, a correction rewrites a few kilobytes
# and bumps the manifest generation. Copied into the index, it would rewrite
# the index and invalidate that run's projections for a change that never
# touched the signal.

.SAMPLES_FILE <- "samples.parquet"

#' @noRd
.samples_path <- function(path) {
    file.path(.index_path(path), .SAMPLES_FILE)
}

# path -> data.frame. Read-through, like `.manifest_cache`: backend objects
# stay small and serialisable, and every object over one dataset shares a
# copy. Dropped by `.invalidate_dataset_cache()`.
.samples_cache <- new.env(parent = emptyenv())

#' @noRd
.samples_cache_drop <- function(path) {
    for (k in unique(path))
        if (exists(k, envir = .samples_cache, inherits = FALSE))
            rm(list = k, envir = .samples_cache)
    invisible()
}

#' The sample metadata for a dataset, read at most once per process.
#'
#' Returns a zero-row `data.frame` with just `run_id` when the dataset has
#' none, so callers never have to special-case its absence.
#'
#' @noRd
.samples <- function(path) {
    hit <- .samples_cache[[path]]
    if (!is.null(hit))
        return(hit)
    fl <- .samples_path(path)
    out <- if (file.exists(fl))
        as.data.frame(arrow::read_parquet(fl), stringsAsFactors = FALSE)
    else data.frame(run_id = character(), stringsAsFactors = FALSE)
    out$run_id <- as.character(out$run_id)
    assign(path, out, envir = .samples_cache)
    out
}

#' Sample metadata variables, i.e. the columns other than the key.
#'
#' @noRd
.sample_var_names <- function(path) {
    setdiff(names(.samples(path)), "run_id")
}

#' Resolve a dataset argument that may be a path or an open backend.
#'
#' A backend's `path` is taken verbatim rather than re-normalised. Every cache
#' in the package is keyed on the string the backend holds, and
#' `normalizePath()` is not idempotent across the lifetime of a dataset: it
#' leaves a path that does not exist yet alone, then resolves symlinks and
#' duplicated separators once it does. Re-normalising here would hand
#' `.invalidate_dataset_cache()` a key that no cache was ever filed under, and
#' a writer would silently leave readers holding the old table.
#'
#' @noRd
.as_dataset_path <- function(x) {
    if (is(x, "MsBackendParquet"))
        x <- .path(x)
    else if (is.character(x) && length(x) == 1L && nzchar(x))
        x <- normalizePath(x, mustWork = FALSE)
    if (!is.character(x) || length(x) != 1L || !nzchar(x))
        stop("'x' must be a dataset path or an 'MsBackendParquet'.",
             call. = FALSE)
    if (!.is_parquet_dataset(x))
        stop("'", x, "' is not an mzStack dataset.", call. = FALSE)
    x
}

#' Which run each spectrum belongs to, as a row index into `runs`.
#'
#' `runs` must be ordered by `uid_base`. Because the blocks are contiguous and
#' ascending this is a binary search, and -- the point -- it needs no
#' per-spectrum column at all: the manifest plus the ids are enough.
#'
#' @noRd
.run_of <- function(runs, ids) {
    findInterval(ids, runs$uid_base)
}

#' @noRd
.runs_by_uid <- function(path) {
    r <- .manifest_runs(.dataset_manifest(path))
    r[order(r$uid_base), , drop = FALSE]
}

#' A predicate selecting the ids of the runs flagged in `sel`.
#'
#' Adjacent selected runs are one contiguous id range, so they are merged
#' before rendering: selecting half a cohort should produce a handful of
#' `BETWEEN`s, not a thousand-term `OR`.
#'
#' @noRd
.runs_where <- function(runs, sel) {
    rr <- rle(sel)
    end <- as.integer(cumsum(rr$lengths))
    start <- end - as.integer(rr$lengths) + 1L
    k <- which(rr$values)
    if (!length(k))
        return("FALSE")
    do.call(.pred_or, lapply(k, function(i)
        .pred_range("spectrum_id_", runs$uid_base[start[i]],
                    runs$uid_base[end[i]] + runs$n_spectra[end[i]] - 1L)))
}

#' Restrict `object` to the spectra of `run_ids`.
#'
#' The shared primitive behind `filterSampleData()` and `filterDataOrigin()`.
#' The surviving ids are computed in R from the manifest, and the equivalent
#' SQL rides along so a later `peaksData()` can push it down -- on
#' `spectrum_id_`, which is the on-disk sort key, so DuckDB prunes row groups
#' instead of matching a long `IN` list.
#'
#' @noRd
.filter_runs <- function(object, run_ids) {
    runs <- .runs_by_uid(.path(object))
    sel <- runs$run_id %in% run_ids
    keep <- .run_of(runs, .ids(object)) %in% which(sel)
    .filter_cached(object, keep, .runs_where(runs, sel))
}

#' Values of sample metadata variables, one row per spectrum of `x`.
#'
#' @noRd
.sample_values <- function(x, columns) {
    runs <- .runs_by_uid(.path(x))
    sd <- .samples(.path(x))
    out <- sd[match(runs$run_id[.run_of(runs, .ids(x))], sd$run_id),
              columns, drop = FALSE]
    rownames(out) <- NULL
    out
}

#' @title Per-run sample metadata
#'
#' @description
#'
#' `runData()` reads, and `runData<-()` writes, a dataset's per-run sample
#' metadata: one row per run, keyed by `run_id`, holding whatever the
#' experiment records about the sample that run came from.
#'
#' A run is one MS run -- one mzPeak archive, or one converted source file --
#' and owns a contiguous block of the dataset's `spectrum_id_`, so the sample
#' metadata's columns become ordinary spectra variables and
#' [filterSampleData()] can select on them by id range rather than by reading
#' a per-spectrum column.
#'
#' The sample metadata is written to `index/samples.parquet` and can be
#' replaced at any time after ingest. Replacing it does not invalidate
#' projections: it does not touch the signal.
#'
#' `runVariables()` lists the sample metadata columns.
#'
#' @details
#'
#' Column names are checked against the dataset's existing spectra variables
#' and against the mzPeak column vocabulary. A sample metadata column called
#' `time` or `id` would otherwise silently shadow the source of a core
#' variable, so it is refused rather than accepted and later mis-read.
#'
#' Runs with no row in the table yield `NA`, so metadata covering only some
#' of a dataset's runs is legal.
#'
#' @param x dataset path, or an [MsBackendParquet()] opened on one.
#'
#' @param value `data.frame` with a `run_id` column naming runs of the
#'     dataset, plus one column per sample metadata variable.
#'
#' @return `runData()` a `data.frame` with one row per described run;
#'     `runVariables()` a `character` of sample metadata column names.
#'
#' @seealso [filterSampleData()] to select spectra on it.
#'
#' @md
#'
#' @export
#'
#' @examples
#' library(Spectra)
#'
#' sd <- S4Vectors::DataFrame(
#'     msLevel = c(1L, 1L, 2L),
#'     rtime = c(1.0, 2.0, 3.0),
#'     dataOrigin = c("QC01", "QC01", "QC02"))
#' sd$mz <- IRanges::NumericList(c(100, 110), c(101, 111),
#'                               c(102, 112), compress = FALSE)
#' sd$intensity <- IRanges::NumericList(c(10, 20), c(11, 21),
#'                                      c(12, 22), compress = FALSE)
#' path <- tempfile()
#' createMsBackendParquetDataset(path = path, data = sd)
#'
#' ## One row per run; `run_id` is the source file's name.
#' runData(path) <- data.frame(run_id = c("QC01", "QC02"),
#'                             subject = c("S3", "S3"),
#'                             timepoint = c(0, 6))
#'
#' be <- backendInitialize(MsBackendParquet(), path = path)
#' runVariables(be)
#' spectraData(be, c("rtime", "timepoint"))
#'
#' ## Selecting on it is a range query over the runs' id blocks.
#' filterSampleData(be, timepoint == 6)
runData <- function(x) {
    .samples(.as_dataset_path(x))
}

#' @rdname runData
#'
#' @export
runVariables <- function(x) {
    .sample_var_names(.as_dataset_path(x))
}

#' @rdname runData
#'
#' @export
`runData<-` <- function(x, value) {
    path <- .as_dataset_path(x)
    value <- as.data.frame(value, stringsAsFactors = FALSE)
    if (!"run_id" %in% names(value))
        stop("'value' must have a 'run_id' column naming the runs it ",
             "describes.", call. = FALSE)
    value$run_id <- as.character(value$run_id)
    if (anyNA(value$run_id) || anyDuplicated(value$run_id))
        stop("'run_id' must be unique and non-missing.", call. = FALSE)

    known <- .manifest_runs(.dataset_manifest(path))$run_id
    unknown <- setdiff(value$run_id, known)
    if (length(unknown))
        stop("No such run(s) in '", path, "': ",
             paste(utils::head(unknown, 5L), collapse = ", "),
             ". Known runs: ", paste(utils::head(known, 5L), collapse = ", "),
             ".", call. = FALSE)

    vars <- setdiff(names(value), "run_id")
    # A column that collides with a spectra variable, or with a name the read
    # view consumes to build one, would be shadowed rather than reported.
    clash <- intersect(vars, unique(c(.dataset_var_names(path),
                                      names(Spectra::coreSpectraVariables()),
                                      .MZPEAK_CONSUMED, "mz", "intensity")))
    if (length(clash))
        stop("Sample metadata column(s) would shadow a spectra variable: ",
             paste(clash, collapse = ", "), ".", call. = FALSE)

    dir.create(.index_path(path), recursive = TRUE, showWarnings = FALSE)
    fl <- .samples_path(path)
    # Same tmp-then-rename as the manifest: a reader never sees a half-written
    # table, and an interrupted write leaves the previous one intact.
    tmp <- paste0(fl, ".tmp-", Sys.getpid())
    arrow::write_parquet(value, sink = tmp)
    if (!file.rename(tmp, fl)) {
        unlink(tmp)
        stop("Could not write '", fl, "'.", call. = FALSE)
    }

    # Bump the generation so other processes drop what they hold, but leave
    # every run's `ingested_at` alone: projection currency is keyed on that,
    # and correcting a timepoint must not throw away an m/z-sorted copy of
    # the signal.
    .manifest_write(path, .manifest_bump_generation(.manifest_read(path)))
    .invalidate_dataset_cache(path)

    # When the target is an open backend, hand back one that already knows
    # about the new columns: a replacement function that returned the stale
    # object would leave `spectraVariables()` disagreeing with the dataset
    # until the caller thought to re-open it.
    if (is(x, "MsBackendParquet")) {
        x@.sample_vars <- vars
        x@spectraVariables <- union(x@spectraVariables, vars)
    }
    invisible(x)
}

#' @title Select spectra by their run's sample metadata
#'
#' @description
#'
#' `filterSampleData()` keeps the spectra whose run satisfies `expr`, an
#' expression evaluated against the dataset's [runData()] table.
#'
#' Because every run owns a contiguous block of `spectrum_id_`, the condition
#' is resolved against the (small) sample metadata table in R and turned into a
#' range predicate over those blocks. No per-spectrum column is read, and the
#' predicate is carried forward so a subsequent `peaksData()` prunes Parquet
#' row groups on the dataset's sort key.
#'
#' @param object [MsBackendParquet()] object.
#'
#' @param expr expression evaluated in the [runData()] table, returning a
#'     `logical` with one element per row. Variables not found there are
#'     looked up in the calling frame.
#'
#' @return `object` restricted to the matching spectra.
#'
#' @seealso [runData()] to set the sample metadata being filtered on.
#'
#' @md
#'
#' @export
#'
#' @examples
#' ## See `?runData` for a worked example.
filterSampleData <- function(object, expr) {
    if (!is(object, "MsBackendParquet"))
        stop("'object' must be an 'MsBackendParquet'.", call. = FALSE)
    sd <- .samples(.path(object))
    if (!nrow(sd))
        stop("Dataset '", .path(object), "' has no sample metadata. Set it ",
             "with runData().", call. = FALSE)
    keep <- eval(substitute(expr), sd, parent.frame())
    if (!is.logical(keep) || length(keep) != nrow(sd))
        stop("'expr' must give one logical value per sample metadata row.",
             call. = FALSE)
    # NA is not a match, matching `filterValues()` and R's own `%in%`.
    .filter_runs(object, sd$run_id[which(keep)])
}
