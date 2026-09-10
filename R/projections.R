# Projections: rebuildable, optional copies of signal data.
#
# An mzPeak archive stores signal one row per data point, ordered by
# spectrum. That is the right order for "give me these spectra", and the
# wrong one for "which spectra contain a peak near m/z X".
#
# Parquet records the minimum and maximum of each column per block of rows,
# and a query skips blocks that cannot match. In spectrum order, every block
# spans nearly the whole mass range (each block holds a few complete
# spectra, and each spectrum covers the full range) so a filter on m/z
# skips nothing. Re-sorting the same rows by m/z makes each block cover a
# narrow slice, and the same filter then reads a handful of blocks.
#
# Measured through `filterContainsMz()` on 5 million points: the m/z-ordered
# copy answered a narrow m/z window about eight times faster than the
# archive's own ordering. It is a second copy of the signal, so it costs
# roughly as much disk again.
#
# A projection is a cache. It is derived from an archive, it can be dropped
# and rebuilt, and no query depends on it for correctness.

# Rows per block in a projection. Much larger than the metadata index uses:
# these are individual data points, so a block of 100,000 still covers only
# a narrow m/z slice once sorted.
.PROJECTION_ROW_GROUP_SIZE <- 100000L

#' @noRd
.projection_run_dir <- function(path, type, run_id) {
    file.path(.projection_path(path, type), paste0("run_id=", run_id))
}

#' SQL source over a projection, restricted to the given runs.
#'
#' @noRd
.projection_source <- function(path, type) {
    con <- .duckdb_con()
    paste0("read_parquet(",
           DBI::dbQuoteString(con, file.path(.projection_path(path, type),
                                             "**", "*.parquet")),
           ", hive_partitioning = TRUE)")
}

#' @title Build or drop signal projections
#'
#' @description
#'
#' `buildProjection()` writes an m/z-ordered copy of a dataset's signal
#' data, which makes queries that search for peaks at a given m/z across
#' many runs substantially faster. `dropProjection()` deletes it again.
#'
#' @details
#'
#' A projection is a **cache**: it is derived from the mzPeak archives, it
#' can be deleted at any time, and no result depends on it for correctness.
#' Queries that can use it do; queries that cannot fall back to reading the
#' archives directly.
#'
#' The cost is disk space: a projection is a second copy of the signal data,
#' of broadly the same size (sorting compresses better, but how much better
#' depends entirely on the data).
#'
#' @param path `character(1)` with the dataset directory.
#'
#' @param type `character(1)` with the projection type. Currently only
#'     `"mzsorted"` is available.
#'
#' @param runs `character` with the run identifiers to build (or drop). The
#'     default, `NULL`, means every run in the dataset.
#'
#' @param verbose `logical(1)` whether to report progress.
#'
#' @return `path`, invisibly.
#'
#' @md
#'
#' @export
#'
#' @examples
#' \dontrun{
#' buildProjection(path)
#' filterContainsMz(be, 278.093, ppm = 20)
#' }
buildProjection <- function(path, type = "mzsorted", runs = NULL,
                            verbose = TRUE) {
    type <- match.arg(type, "mzsorted")
    path <- normalizePath(path, mustWork = TRUE)
    if (.dataset_kind(path) != "mzpeak")
        stop("Projections are built from mzPeak archives; '", path,
             "' holds natively converted data. See createMzPeakDataset().",
             call. = FALSE)
    m <- .manifest_read(path)
    con <- .duckdb_con()
    rs <- .manifest_runs(m)
    if (!is.null(runs))
        rs <- rs[rs$run_id %in% runs, , drop = FALSE]
    if (!nrow(rs))
        stop("No matching runs in '", path, "'.", call. = FALSE)

    for (i in seq_len(nrow(rs))) {
        run <- rs[i, , drop = FALSE]
        f <- .run_signal_file(run, "auto")
        cols <- .mzpeak_signal_columns(f)
        dest_dir <- .projection_run_dir(path, type, run$run_id)
        unlink(dest_dir, recursive = TRUE)
        dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
        DBI::dbExecute(con, paste0(
            "COPY (SELECT CAST(", run$uid_base, " + ", cols$index,
            " AS INTEGER) AS ", .quote_ident("spectrum_id_"), ", ",
            cols$mz, " AS ", .quote_ident("mz"), ", ",
            cols$intensity, " AS ", .quote_ident("intensity"),
            " FROM read_parquet(", DBI::dbQuoteString(con, f), ")",
            " ORDER BY ", cols$mz, ") TO ",
            DBI::dbQuoteString(con, file.path(dest_dir, "part-0.parquet")),
            " (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE ",
            .PROJECTION_ROW_GROUP_SIZE, ")"))
        m <- .manifest_set_projection(m, run$run_id, type)
        if (verbose)
            message("  ", run$run_id, ": ", type, " projection built")
    }
    .manifest_write(path, m)
    .invalidate_dataset_cache(path)
    invisible(path)
}

#' @rdname buildProjection
#'
#' @export
dropProjection <- function(path, type = "mzsorted", runs = NULL,
                           verbose = TRUE) {
    type <- match.arg(type, "mzsorted")
    path <- normalizePath(path, mustWork = TRUE)
    m <- .manifest_read(path)
    rs <- .manifest_runs(m)
    if (!is.null(runs))
        rs <- rs[rs$run_id %in% runs, , drop = FALSE]
    for (id in rs$run_id) {
        unlink(.projection_run_dir(path, type, id), recursive = TRUE)
        m <- .manifest_drop_projection(m, id, type)
    }
    if (is.null(runs))
        unlink(.projection_path(path, type), recursive = TRUE)
    .manifest_write(path, m)
    .invalidate_dataset_cache(path)
    if (verbose)
        message("Dropped '", type, "' projection for ", nrow(rs), " run(s).")
    invisible(path)
}

#' Spectrum ids holding at least one peak inside any of the given windows.
#'
#' Uses the `mzsorted` projection when every involved run has a current one,
#' and otherwise reads the archives. Both give the same answer; only the
#' cost differs.
#'
#' @noRd
.ids_containing_mz <- function(x, los, his) {
    con <- .duckdb_con()
    path <- .path(x)
    m <- .dataset_manifest(path)
    ids <- .ids(x)
    if (!length(ids))
        return(integer())

    windows <- do.call(.pred_or, lapply(seq_along(los), function(i)
        .pred_range("mz", los[i], his[i])))

    parts <- .manifest_split_ids(m, ids)
    run_ids <- vapply(parts, function(p) p$run$run_id, character(1))

    if (.manifest_has_projection(m, "mzsorted", run_ids)) {
        # One scan over the sorted copy: the m/z predicate prunes blocks,
        # and the id restriction prunes runs.
        idw <- .ids_where(ids, full = .is_full(x))
        where <- if (is.null(idw)) windows
                 else if (is.na(idw)) windows       # too many to inline
                 else .pred_and(idw, windows)
        res <- DBI::dbGetQuery(con, paste0(
            "SELECT DISTINCT ", .quote_ident("spectrum_id_"), " FROM ",
            .projection_source(path, "mzsorted"), " WHERE ", where))
        return(sort(intersect(as.integer(res$spectrum_id_), ids)))
    }

    # No projection: ask each archive in turn. Correct, just slower.
    out <- lapply(parts, function(p) {
        run <- p$run
        f <- .run_signal_file(run, .representation(x))
        cols <- .mzpeak_signal_columns(f)
        w <- do.call(.pred_or, lapply(seq_along(los), function(i)
            paste0("(", cols$mz, " >= ", .sql_num(los[i]), " AND ",
                   cols$mz, " <= ", .sql_num(his[i]), ")")))
        idw <- .ids_where(p$local, full = FALSE, col = cols$index)
        if (!is.null(idw) && !is.na(idw))
            w <- .pred_and(idw, w)
        as.integer(DBI::dbGetQuery(con, paste0(
            "SELECT DISTINCT CAST(", run$uid_base, " + ", cols$index,
            " AS INTEGER) AS ", .quote_ident("spectrum_id_"),
            " FROM read_parquet(", DBI::dbQuoteString(con, f), ")",
            " WHERE ", w))$spectrum_id_)
    })
    sort(intersect(unlist(out, use.names = FALSE), ids))
}

#' @title Select spectra containing a peak at a given m/z
#'
#' @description
#'
#' `filterContainsMz()` keeps the spectra that hold at least one peak within
#' a tolerance of any of the given m/z values. Unlike the peak-level filters
#' in `Spectra`, the search happens inside the storage engine, so it does
#' not read every spectrum's peaks into R.
#'
#' @details
#'
#' The query is much faster when the dataset has an `mzsorted` projection
#' (see [buildProjection()]); without one it reads the archives directly and
#' returns the same answer.
#'
#' @param object a [MsBackendParquet()] over an mzPeak dataset.
#'
#' @param mz `numeric` with the m/z values to look for.
#'
#' @param tolerance `numeric` with the absolute m/z tolerance.
#'
#' @param ppm `numeric` with the m/z-relative tolerance in parts per million.
#'
#' @return `object` subset to the matching spectra, in their original order.
#'
#' @md
#'
#' @export
#'
#' @importFrom MsCoreUtils ppm
filterContainsMz <- function(object, mz = numeric(), tolerance = 0,
                             ppm = 20) {
    if (!length(mz))
        return(object)
    if (.dataset_kind(.path(object)) != "mzpeak")
        stop("'filterContainsMz()' needs a dataset built from mzPeak ",
             "archives; see createMzPeakDataset().", call. = FALSE)
    lmz <- length(mz)
    if (length(ppm) != lmz) ppm <- rep(ppm[1L], lmz)
    if (length(tolerance) != lmz) tolerance <- rep(tolerance[1L], lmz)
    d <- MsCoreUtils::ppm(mz, ppm) + tolerance
    keep <- .ids_containing_mz(object, mz - d, mz + d)
    res <- extractByIndex(object, which(.ids(object) %in% keep))
    # The surviving ids are not described by any predicate over the metadata
    # index, so the carried-forward predicate must be cleared.
    res@.pending_predicate <- NULL
    res@.predicate_clean <- FALSE
    res
}
