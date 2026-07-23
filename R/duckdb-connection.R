# Lifecycle and configuration of the package-level DuckDB connection used by
# every read path, plus the per-dataset view registry.
#
# One in-memory connection per R process is enough: DuckDB serialises queries
# internally and a `:memory:` connection has no file overhead. The connection
# is created lazily by `.duckdb_con()` and closed by `.onUnload()` in zzz.R.

# Package-level state for the DuckDB read path:
#   $con     - lazy in-memory DuckDB connection
#   $pid     - the process that created $con, so a forked worker can tell
#              that the handle it inherited is not its own
#   $views   - env mapping normalised dataset path -> SQL view name
#   $counter - monotonic counter for view names
# Caches are invalidated by writers via `.invalidate_dataset_cache()`.
.duckdb_state <- new.env(parent = emptyenv())
.duckdb_state$con <- NULL
.duckdb_state$pid <- NA_integer_
.duckdb_state$views <- new.env(parent = emptyenv())

#' Session settings applied once per connection.
#'
#' `parquet_metadata_cache` is off by default in DuckDB, which means the
#' Thrift footer of every Parquet file is re-decoded on every query. Measured
#' on this package's own benchmark that is ~4.6 ms per call on a single file
#' and 25 ms on ten -- pure overhead, since the footer cannot have changed.
#'
#' Note what is deliberately *not* set here: `preserve_insertion_order`.
#' DuckDB's guidance recommends disabling it for bulk import/export of
#' larger-than-memory data, but this backend's reads are the opposite shape --
#' every fetch has to come back in `spectraIds` order. Measured with it off,
#' rows arrive scrambled, which misses the ordered fast path in
#' `.reorder_to_ids()` / `.fetch_peaks_data()` and pays instead for a
#' `match()` over every spectrum plus a full reindex of the peak list. The
#' sort DuckDB avoids is cheaper than the one R then has to do.
#'
#' @noRd
.duckdb_configure <- function(con) {
    settings <- "SET parquet_metadata_cache = true"
    threads <- getOption("MsBackendParquet.threads")
    if (!is.null(threads))
        settings <- c(settings,
                      sprintf("SET threads = %d", as.integer(threads)))
    mem <- getOption("MsBackendParquet.memoryLimit")
    if (!is.null(mem))
        settings <- c(settings, sprintf("SET memory_limit = '%s'", mem))
    for (s in settings)
        try(DBI::dbExecute(con, s), silent = TRUE)
    invisible(con)
}

#' Lazily create (or return) the package-level DuckDB connection.
#'
#' Guarded by the creating process id. `BiocParallel::MulticoreParam` forks,
#' and a forked child inherits the parent's external pointer: `dbIsValid()`
#' still reports `TRUE` for it, but the DuckDB instance behind it belongs to
#' the parent and must not be used or shut down from the child. When the pid
#' differs we therefore *abandon* the inherited handle rather than
#' disconnecting it (disconnecting would tear down the parent's database)
#' and open a fresh one, dropping the view registry with it since view names
#' are per-connection.
#'
#' This is what makes `backendBpparam()`'s promise true: the backend object
#' itself still stores nothing but a path, and each process ends up with its
#' own connection.
#'
#' @noRd
.duckdb_con <- function() {
    con <- .duckdb_state$con
    pid <- Sys.getpid()
    if (!is.null(con) && identical(.duckdb_state$pid, pid) &&
        DBI::dbIsValid(con))
        return(con)
    if (!is.null(con) && !identical(.duckdb_state$pid, pid)) {
        # Inherited across a fork: drop every reference without touching the
        # parent's database.
        .duckdb_state$views <- new.env(parent = emptyenv())
    }
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    .duckdb_configure(con)
    .duckdb_state$con <- con
    .duckdb_state$pid <- pid
    con
}

#' Return a fresh, SQL-safe view name from a per-process counter. The
#' path -> view mapping lives in `.duckdb_state$views`, so a name never has to
#' be re-derived from a path.
#'
#' @noRd
.view_name <- function() {
    n <- .duckdb_state$counter
    if (is.null(n)) n <- 0L
    n <- n + 1L
    .duckdb_state$counter <- n
    sprintf("spectra_%d", n)
}

#' Register (once) a DuckDB view over the Parquet files of a dataset and
#' return its name.
#'
#' `backendInitialize()` normalises the path once so the hot path is a bare
#' environment lookup. A non-normalised path still resolves, but pays a
#' `normalizePath()` on the cache miss only.
#'
#' @noRd
.dataset_view <- function(path) {
    con <- .duckdb_con()
    view <- .duckdb_state$views[[path]]
    if (!is.null(view)) {
        return(view)
    }
    norm <- normalizePath(path, mustWork = FALSE)
    if (!identical(norm, path)) {
        view <- .duckdb_state$views[[norm]]
        if (!is.null(view)) {
            assign(path, view, envir = .duckdb_state$views)
            return(view)
        }
        path <- norm
    }
    sp <- .spectra_path(path)
    if (!dir.exists(sp)) {
        stop("Parquet dataset directory '", sp, "' does not exist.",
             call. = FALSE)
    }
    view <- .view_name()
    glob <- file.path(sp, "**", "*.parquet")
    DBI::dbExecute(con, sprintf(
        "CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet(%s, hive_partitioning = TRUE)",
        DBI::dbQuoteIdentifier(con, view),
        DBI::dbQuoteString(con, glob)))
    assign(path, view, envir = .duckdb_state$views)
    view
}

#' Drop the cached DuckDB view for a path so the next access re-registers it
#' against the (possibly mutated) on-disk files. Writers call this after any
#' change to the dataset directory.
#'
#' @noRd
.invalidate_dataset_cache <- function(path) {
    if (!length(path)) {
        return(invisible())
    }
    key <- normalizePath(path, mustWork = FALSE)
    views <- .duckdb_state$views
    con <- .duckdb_state$con
    for (k in unique(c(path, key))) {
        if (exists(k, envir = views, inherits = FALSE)) {
            view <- get(k, envir = views)
            if (!is.null(con) && DBI::dbIsValid(con))
                try(DBI::dbExecute(con, sprintf(
                    "DROP VIEW IF EXISTS %s",
                    DBI::dbQuoteIdentifier(con, view))), silent = TRUE)
            rm(list = k, envir = views)
        }
    }
    .meta_cache_drop(unique(c(path, key)))
    invisible()
}

# ------------------------------------------------------------------------------
# SQL helpers
# ------------------------------------------------------------------------------

#' Pre-quote a column identifier. SQL identifier names in this backend are
#' well-behaved (ASCII alphanumeric plus `_` and `.`), so a simple `"<name>"`
#' wrap is equivalent to `DBI::dbQuoteIdentifier()` without the S4 dispatch
#' cost -- checked once per name and cached.
#'
#' @noRd
.quoted_idents <- new.env(parent = emptyenv())

.quote_ident <- function(name) {
    cached <- .quoted_idents[[name]]
    if (!is.null(cached)) return(cached)
    # Allow ASCII letters, digits, underscore, and dot -- Spectra variables
    # sometimes have dotted names like `acquisitionNum.1`. Reject anything
    # else (incl. double-quote, which would break the naive wrap) and fall
    # back to DBI.
    if (grepl("^[A-Za-z_][A-Za-z0-9_.]*$", name)) {
        quoted <- paste0("\"", name, "\"")
    } else {
        quoted <- as.character(
            DBI::dbQuoteIdentifier(.duckdb_con(), name))
    }
    assign(name, quoted, envir = .quoted_idents)
    quoted
}

#' Render a SQL WHERE-clause body (no leading "WHERE") restricting a scan to
#' `ids`, or `NULL` when no restriction is needed.
#'
#' A contiguous ascending run becomes `BETWEEN`, which DuckDB can answer from
#' row-group min/max statistics; an arbitrary set becomes an `IN` list. The
#' `IN` form is capped: past `.MAX_INLINE_IDS` the SQL text itself becomes the
#' bottleneck (hundreds of KB to build in R and re-parse in DuckDB on every
#' call), so callers switch to `.with_id_table()` instead.
#'
#' @noRd
.MAX_INLINE_IDS <- 1024L

.ids_where <- function(ids, full = FALSE) {
    if (isTRUE(full)) {
        return(NULL)
    }
    n <- length(ids)
    if (!n) {
        return("FALSE")
    }
    if (n > 1L) {
        lo <- ids[1L]
        hi <- ids[n]
        if (hi - lo + 1L == n && !is.unsorted(ids)) {
            return(sprintf("spectrum_id_ BETWEEN %d AND %d",
                           as.integer(lo), as.integer(hi)))
        }
    }
    if (n > .MAX_INLINE_IDS) {
        return(NA_character_)
    }
    sprintf("spectrum_id_ IN (%s)", paste(as.integer(ids), collapse = ","))
}

#' Run `f(tbl_name)` with `ids` registered as a temporary DuckDB table so a
#' large id set can be joined rather than inlined as SQL text.
#'
#' The registered table carries an `ord_` column, so the join can also do the
#' reordering that would otherwise be a `match()` on the R side.
#'
#' @noRd
.with_id_table <- function(ids, f) {
    con <- .duckdb_con()
    nm <- sprintf("__ids_%d", sample.int(.Machine$integer.max, 1L))
    duckdb::duckdb_register(
        con, nm, data.frame(spectrum_id_ = as.integer(ids),
                            ord_ = seq_along(ids)))
    on.exit(try(duckdb::duckdb_unregister(con, nm), silent = TRUE), add = TRUE)
    f(nm)
}
