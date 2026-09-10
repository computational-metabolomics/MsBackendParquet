# ------------------------------------------------------------------------------
# Internal helpers for the MsBackendParquet backend.
#
# File layout of a `MsBackendParquet` dataset directory:
#
# <path>/
#   spectra/                Hive-partitioned Parquet dataset with one row
#                           per spectrum. Columns are the spectra
#                           variables plus `spectrum_id_` (integer
#                           primary key) and `mz` / `intensity` stored
#                           as `list<double>` columns.
#
# The backend object only stores the path to <path> and the (integer)
# vector of primary keys for the spectra currently *seen* by the
# backend. All data access is performed by opening the dataset lazily
# with `arrow::open_dataset()` and filtering by `spectrum_id_`.
# ------------------------------------------------------------------------------

# Name of the sub-directory storing the spectra dataset.
.SPECTRA_DIR <- "spectra"

# Default Parquet compression. `snappy` is widely supported and
# reasonably fast; users can override with `compression`.
.DEFAULT_COMPRESSION <- "snappy"

# Default number of spectra per Parquet row group. Smaller row groups
# give finer-grained pruning for narrow range queries (e.g. retention
# time) at the cost of slightly more file metadata; larger groups
# compress better and reduce per-group scan setup. 250 is a balance
# that works well for typical LC-MS files (where rtime is roughly
# monotonic per row group).
.DEFAULT_ROW_GROUP_SIZE <- 250L

# ------------------------------------------------------------------------------
# Path / metadata helpers
# ------------------------------------------------------------------------------

#' @noRd
.spectra_path <- function(path) {
    file.path(path, .SPECTRA_DIR)
}

#' Is `path` a dataset this backend can open?
#'
#' Presence of the mzStack manifest is the whole test. Earlier versions also
#' accepted any directory containing a `spectra/` sub-directory; that
#' fallback is gone, so a dataset written before mzStack naming fails with a
#' clear message instead of half-opening.
#'
#' @noRd
.is_parquet_dataset <- function(path) {
    .is_mzstack_dataset(path)
}

# ------------------------------------------------------------------------------
# Accessors on the object
# ------------------------------------------------------------------------------

#' @noRd
.path <- function(x) {
    x@path
}

#' @noRd
.ids <- function(x) {
    x@spectraIds
}

#' Which signal representation this backend reads: `"auto"`, `"profile"` or
#' `"centroid"`. Only applicable to mzPeak-backed datasets.
#'
#' @noRd
.representation <- function(x) {
    r <- x@representation
    if (!length(r)) "auto" else r[1L]
}

#' Variables actually present as columns in the on-disk dataset (excluding
#' the peak list columns).
#'
#' @noRd
.dataset_variables <- function(x) {
    x@.dataset_vars
}

#' Peak variables stored in the dataset (always `c("mz", "intensity")`
#' for the current implementation; kept as a slot for forward
#' compatibility).
#'
#' @noRd
.peaks_variables <- function(x) {
    pv <- x@.peaks_vars
    if (!length(pv)) c("mz", "intensity") else pv
}

#' Check whether a variable is cached locally in the `MsBackendCached`
#' parent class.
#'
#' @noRd
.has_local_variable <- function(x, variable = character()) {
    all(variable %in% colnames(x@localData))
}

# ------------------------------------------------------------------------------
# SQL predicate construction
# ------------------------------------------------------------------------------

#' Format a numeric literal at full double precision. `as.character()` on a
#' double drops digits (15 significant), which for an m/z bound is a silent
#' correctness bug.
#'
#' @noRd
.sql_num <- function(x) {
    if (is.na(x)) return("NULL")
    if (is.infinite(x))
        return(if (x > 0) sprintf("%.17g", .Machine$double.xmax)
               else sprintf("%.17g", -.Machine$double.xmax))
    sprintf("%.17g", x)
}

#' @noRd
.sql_str <- function(x)
    paste0("'", gsub("'", "''", x, fixed = TRUE), "'")

#' `col BETWEEN lo AND hi`, with infinite bounds dropped rather than emitted.
#'
#' @noRd
.pred_range <- function(col, lo, hi) {
    col <- .quote_ident(col)
    parts <- character()
    if (is.finite(lo)) parts <- c(parts, paste0(col, " >= ", .sql_num(lo)))
    if (is.finite(hi)) parts <- c(parts, paste0(col, " <= ", .sql_num(hi)))
    if (!length(parts)) return("TRUE")
    paste0("(", paste(parts, collapse = " AND "), ")")
}

#' `col IN (...)` with R's `%in%` NA semantics.
#'
#' @noRd
.pred_in <- function(col, values, quote = FALSE) {
    if (!length(values)) return("FALSE")
    vals <- if (quote) vapply(values, .sql_str, character(1))
            else vapply(values, .sql_num, character(1))
    paste0("COALESCE(", .quote_ident(col), " IN (",
           paste(vals, collapse = ", "), "), FALSE)")
}

#' @noRd
.pred_and <- function(...) {
    p <- Filter(nzchar, c(...))
    if (!length(p)) return("TRUE")
    if (length(p) == 1L) return(p[[1L]])
    paste0("(", paste(p, collapse = " AND "), ")")
}

#' @noRd
.pred_or <- function(...) {
    p <- Filter(nzchar, c(...))
    if (!length(p)) return("FALSE")
    if (length(p) == 1L) return(p[[1L]])
    paste0("(", paste(p, collapse = " OR "), ")")
}

#' @noRd
.pred_not <- function(p) paste0("(NOT ", p, ")")

# ------------------------------------------------------------------------------
# Core data extraction
# ------------------------------------------------------------------------------

#' @noRd
.is_full <- function(x) {
    isTRUE(x@.full)
}

#' Render the SQL WHERE-clause body (no leading "WHERE") matching the
#' backend's current row restriction, or `NULL` when it covers the whole
#' dataset.
#'
#' A backend carrying a "clean" predicate -- one that exactly describes its
#' surviving ids, set by `.subset_filter()` -- pushes that predicate instead
#' of an id list, so DuckDB can prune row groups on the predicate columns.
#' Returns `NA_character_` when the id set is too large to inline, signalling
#' the caller to join against a registered id table instead.
#'
#' @noRd
.where_sql <- function(x) {
    if (isTRUE(x@.predicate_clean) && !is.null(x@.pending_predicate)) {
        return(x@.pending_predicate)
    }
    .ids_where(.ids(x), full = .is_full(x))
}

#' Direct-SQL fetch. Returns a `data.frame` with the requested columns from
#' the backend's view, restricted by the current backend state.
#'
#' Two forms: an inline WHERE clause for the common cases, and -- when the id
#' set is too large to inline as SQL text -- a join against a temporarily
#' registered id table, which also carries the ordering so the R side does not
#' have to `match()` afterwards.
#'
#' @noRd
.fetch_sql <- function(x, cols) {
    con <- .duckdb_con()
    view <- .quote_ident(.dataset_view(.path(x)))
    select_sql <- paste(vapply(cols, .quote_ident, character(1)),
                        collapse = ", ")
    where <- .where_sql(x)
    if (!is.null(where) && is.na(where)) {
        return(.with_id_table(.ids(x), function(nm) {
            DBI::dbGetQuery(con, paste0(
                "SELECT ", paste0("s.", vapply(cols, .quote_ident,
                                               character(1)),
                                  collapse = ", "),
                " FROM ", view, " s JOIN ", .quote_ident(nm), " i",
                " ON s.\"spectrum_id_\" = i.\"spectrum_id_\"",
                " ORDER BY i.\"ord_\""))
        }))
    }
    sql <- if (is.null(where))
        paste0("SELECT ", select_sql, " FROM ", view)
    else
        paste0("SELECT ", select_sql, " FROM ", view, " WHERE ", where)
    DBI::dbGetQuery(con, sql)
}

#' Fetch a `data.frame` with the requested *non-peak* spectra variables
#' for the spectra of `x`, in the order of `x@spectraIds`.
#'
#' @noRd
.fetch_spectra_data <- function(x, columns = "spectrum_id_") {
    if (!length(.ids(x))) {
        out <- data.frame(matrix(NA, nrow = 0L, ncol = length(columns)))
        colnames(out) <- columns
        return(out)
    }
    # The object already carries the dataset schema from
    # `backendInitialize()`; re-deriving it per fetch would cost a DuckDB
    # round trip for information that cannot have changed.
    schema_names <- union(.dataset_variables(x), .peaks_variables(x))
    cols <- unique(c("spectrum_id_", columns))
    available <- intersect(cols, schema_names)
    miss <- setdiff(cols, available)

    # Served entirely from the in-memory index when it holds these columns:
    # already in `spectraIds` order, so no fetch and no reorder.
    wanted <- setdiff(available, "spectrum_id_")
    res <- if (length(wanted)) .meta_frame(x, wanted) else NULL
    if (!is.null(res)) {
        # The index is keyed by spectrum_id_ rather than storing it, so
        # rebuild the column when the caller asked for it.
        if ("spectrum_id_" %in% columns) res[["spectrum_id_"]] <- .ids(x)
        for (m in miss) res[[m]] <- NA
        rownames(res) <- NULL
        return(res[, columns, drop = FALSE])
    }

    res <- .fetch_sql(x, available)
    if (length(miss))
        for (m in miss) res[[m]] <- NA
    res <- .reorder_to_ids(res, .ids(x), columns)
    rownames(res) <- NULL
    res
}

#' Reorder a `data.frame` returned by Arrow so its rows align with the
#' backend's `spectraIds` order. The match() pass is skipped when the
#' result already comes back in that exact order (the common
#' single-file, non-subsetted case).
#'
#' @noRd
.reorder_to_ids <- function(res, ids, columns) {
    sid <- res$spectrum_id_
    if (length(sid) == length(ids) && identical(as.integer(sid), ids)) {
        res[, columns, drop = FALSE]
    } else {
        res[match(ids, sid), columns, drop = FALSE]
    }
}

#' Fetch peaks data (list of two-column matrices, one per spectrum) for
#' the spectra of `x` in the order of `x@spectraIds`.
#'
#' @noRd
.fetch_peaks_data <- function(x, columns = c("mz", "intensity"), drop = FALSE) {
    pv <- .peaks_variables(x)
    miss <- setdiff(columns, pv)
    if (length(miss)) {
        stop("Unsupported peaks variable(s): ",
             paste0("'", miss, "'", collapse = ", "), call. = FALSE)
    }
    if (!length(.ids(x))) {
        return(list())
    }

    # Read only the peak columns actually asked for
    want <- intersect(pv, columns)
    res <- if (.dataset_kind(.path(x)) == "mzpeak")
        # Signal lives in the archives, one row per data point.
        .fetch_peaks_point(x, want)
    else
        # Native dataset: peaks are list columns beside the metadata.
        .fetch_sql(x, unique(c("spectrum_id_", want)))
    sid <- res$spectrum_id_
    if (length(sid) != length(.ids(x)) ||
        !identical(as.integer(sid), .ids(x))) {
        idx <- match(.ids(x), sid)
        for (nm in want) {
            res[[nm]] <- res[[nm]][idx]
        }
    }
    mz_list <- if ("mz" %in% want) res$mz else NULL
    int_list <- if ("intensity" %in% want) res$intensity else NULL
    .pack_peaks(mz_list, int_list, columns = columns, drop = drop)
}

#' Which signal file of a run to read.
#'
#' mzPeak stores profile and centroid signal in separate members, and both
#' may be present for the same spectra. `"auto"` prefers centroids, because
#' that is what nearly all downstream `Spectra` code expects; the explicit
#' settings ask for one or the other and fail loudly when it is absent,
#' rather than quietly handing back the other representation.
#'
#' @noRd
.run_signal_file <- function(run, representation = "auto") {
    prof <- run$profile
    cent <- run$centroid
    pick <- switch(
        representation,
        profile = if (is.na(prof))
            stop("Run '", run$run_id, "' holds no profile data. Open the ",
                 "dataset with representation = \"centroid\" or \"auto\".",
                 call. = FALSE) else prof,
        centroid = if (is.na(cent))
            stop("Run '", run$run_id, "' holds no centroid data. Open the ",
                 "dataset with representation = \"profile\" or \"auto\".",
                 call. = FALSE) else cent,
        if (!is.na(cent)) cent else prof)
    file.path(run$path, pick)
}

#' Fetch peaks for an mzPeak-backed dataset.
#'
#' The signal lives in the archives, one row per data point, so the peaks of
#' a spectrum have to be gathered back up. DuckDB does that with `list()`,
#' which returns exactly the two list columns `C_pack_peaks()` already
#' consumes.
#'
#' One query per involved run: the archives are separate files, and querying
#' them separately also keeps each `WHERE` clause small and lets each run
#' choose its own id-restriction strategy.
#'
#' @return `data.frame` with `spectrum_id_` and the requested list columns,
#'     in `spectraIds` order.
#'
#' @noRd
.fetch_peaks_point <- function(x, want) {
    con <- .duckdb_con()
    m <- .dataset_manifest(.path(x))
    ids <- .ids(x)
    parts <- .manifest_split_ids(m, ids)
    rep <- .representation(x)

    out <- lapply(parts, function(p) {
        run <- p$run
        f <- .run_signal_file(run, rep)
        cols <- .mzpeak_signal_columns(f)
        # The specification guarantees the sorting-rank-0 array (m/z) is
        # ascending, so ordering inside the aggregate should be a no-op --
        # but `list()` gives no order guarantee of its own, and m/z paired
        # with the wrong intensity is a silent, unrecoverable error.
        agg <- character()
        if ("mz" %in% want)
            agg <- c(agg, paste0("list(", cols$mz, " ORDER BY ", cols$mz,
                                 ") AS ", .quote_ident("mz")))
        if ("intensity" %in% want)
            agg <- c(agg, paste0("list(", cols$intensity, " ORDER BY ",
                                 cols$mz, ") AS ",
                                 .quote_ident("intensity")))
        sel <- paste0(
            "SELECT CAST(", run$uid_base, " + ", cols$index,
            " AS INTEGER) AS ", .quote_ident("spectrum_id_"), ", ",
            paste(agg, collapse = ", "),
            " FROM read_parquet(", DBI::dbQuoteString(con, f), ")")
        grp <- paste0(" GROUP BY ", cols$index)

        where <- .ids_where(p$local, full = FALSE, col = cols$index)
        if (!is.null(where) && is.na(where))
            # Too many scattered spectra to inline; join against a
            # temporarily registered table instead.
            return(.with_id_table(p$local, function(nm)
                DBI::dbGetQuery(con, paste0(
                    sel, " SEMI JOIN ", .quote_ident(nm), " i ON ",
                    cols$index, " = i.", .quote_ident("spectrum_id_"),
                    grp))))
        DBI::dbGetQuery(con, paste0(sel, " WHERE ", where, grp))
    })

    res <- do.call(rbind, out)
    if (is.null(res))
        res <- data.frame(spectrum_id_ = integer())
    res
}

#' Fetch a single peaks column as a `CompressedNumericList`.
#'
#' Used by `mz()` / `intensity()`. Building the compressed form -- one shared
#' values vector plus a partitioning -- costs 106 ms where
#' `NumericList(compress = FALSE)` costs 519 ms, because the latter has to keep
#' a separate R vector per spectrum.
#'
#' @noRd
.fetch_peaks_column <- function(x, column) {
    if (!length(.ids(x))) {
        return(IRanges::NumericList(compress = TRUE))
    }
    vals <- .fetch_peaks_data(x, columns = column, drop = TRUE)
    IRanges::NumericList(vals, compress = TRUE)
}

#' Combine separate `mz` / `intensity` list columns into the list-of-
#' matrices structure expected by `peaksData()`.
#'
#' @noRd
.pack_peaks <- function(mz, intensity, columns = c("mz", "intensity"),
                        drop = FALSE) {
    n <- max(length(mz), length(intensity))
    if (!n) {
        return(list())
    }

    if (drop && length(columns) == 1L) {
        out <- switch(columns,
                      mz = mz,
                      intensity = intensity,
                      stop("Unsupported single column '", columns, "'.",
                           call. = FALSE))
        # DuckDB returns doubles already; only a NULL (empty spectrum) needs
        # replacing, so skip the blanket as.numeric() pass over every vector.
        empty <- vapply(out, is.null, logical(1))
        if (any(empty)) {
            out[empty] <- list(numeric())
        }
        return(out)
    }

    # Canonical c("mz", "intensity") layout: one allocation and two memcpy
    # per spectrum in C, with the dimnames built once and shared. The R
    # equivalent (Map over cbind) is slow.
    if (identical(columns, c("mz", "intensity"))) {
        return(.Call(C_pack_peaks, mz, intensity))
    }

    # Single requested column, but the caller wants a matrix back.
    single <- switch(columns, mz = mz, intensity = intensity, NULL)
    if (length(columns) == 1L && !is.null(single)) {
        res <- .Call(C_pack_peaks,
                     if (identical(columns, "mz")) single else NULL,
                     if (identical(columns, "intensity")) single else NULL)
        return(lapply(res, function(m) m[, columns, drop = FALSE]))
    }
    stop("Unsupported peaks column selection: ",
         paste0("'", columns, "'", collapse = ", "), call. = FALSE)
}


#' Combine local cached data, dataset-resident variables and Spectra
#' *core* variables to produce a `DataFrame` for `spectraData()`.
#'
#' Mirrors `MsBackendSql`'s `.spectra_data_sql()`.
#'
#' @noRd
.spectra_data_parquet <- function(x, columns = spectraVariables(x)) {
    res <- methods::getMethod(
        "spectraData", "MsBackendCached")(x, columns = columns)
    if (is.null(res)) {
        res <- S4Vectors::make_zero_col_DFrame(length(x))
    }
    ds_cols <- intersect(columns, x@spectraVariables)
    ds_cols <- ds_cols[!ds_cols %in% c("mz", "intensity", colnames(res))]
    mz_cols <- intersect(columns, c("mz", "intensity"))

    if (length(ds_cols)) {
        res <- cbind(res, methods::as(
            .fetch_spectra_data(x, columns = ds_cols), "DataFrame"))
    }

    # Each peak column is fetched and wrapped on its own: building matrices
    # only to slice a column back out of them would pay the packing cost
    # twice and force both columns to be read even when one was asked for.
    for (col in mz_cols) {
        res[[col]] <- .fetch_peaks_column(x, col)
    }

    if (any(columns == "centroided") && !is.logical(res$centroided)) {
        res$centroided <- as.logical(res$centroided)
    }

    if (any(columns == "smoothed") && !is.logical(res$smoothed)) {
        res$smoothed <- as.logical(res$smoothed)
    }

    S4Vectors::extractCOLS(res, columns)
}

# ------------------------------------------------------------------------------
# Writing the dataset
# ------------------------------------------------------------------------------

#' Internal: write a `data.frame` (spectra metadata) plus a list of
#' peaks matrices into the Parquet spectra dataset at `path`. The data
#' frame must already contain a `spectrum_id_` column.
#'
#' @noRd
.write_spectra_chunk <- function(
    path,
    data,
    peaks,
    partitioning = character(),
    compression = .DEFAULT_COMPRESSION,
    row_group_size = .DEFAULT_ROW_GROUP_SIZE,
    append = FALSE
) {
    if (!nrow(data)) {
        return(invisible(FALSE))
    }
    if (length(peaks) != nrow(data)) {
        stop("'peaks' must have the same length as 'data' has rows.",
             call. = FALSE)
    }
    if (!"spectrum_id_" %in% colnames(data)) {
        stop("'data' must contain a 'spectrum_id_' column.", call. = FALSE)
    }
    mz <- lapply(peaks, function(m) {
        if (is.null(m) || !nrow(m)) numeric() else as.numeric(m[, "mz"])
    })
    intensity <- lapply(peaks, function(m) {
        if (is.null(m) || !nrow(m)) numeric()
        else as.numeric(m[, "intensity"])
    })
    data$mz <- mz
    data$intensity <- intensity
    sp <- .spectra_path(path)
    if (!dir.exists(sp)) {
        dir.create(sp, recursive = TRUE)
    }

    rgs <- max(1L, as.integer(row_group_size))
    tbl <- arrow::as_arrow_table(data)
    if (length(partitioning)) {
        # Use a unique basename template per chunk to avoid overwriting
        # previous Parquet parts when appending to an existing
        # partitioned dataset.
        token <- paste0(format(Sys.time(), "%H%M%S"), "-",
                        paste(sample(c(letters, 0:9), 6, TRUE),
                              collapse = ""))
        arrow::write_dataset(
            tbl,
            path = sp,
            format = "parquet",
            partitioning = partitioning,
            compression = compression,
            max_rows_per_group = rgs,
            basename_template = paste0("part-", token, "-{i}.parquet"),
            existing_data_behavior = "overwrite")
    } else {
        # Single-file or chunked write: append a uniquely-named file in
        # the spectra directory to keep multiple chunks separate.
        base <- sprintf("part-%s-%s.parquet",
                        format(Sys.time(), "%Y%m%d%H%M%S"),
                        paste(sample(c(letters, 0:9), 6, TRUE),
                              collapse = ""))
        arrow::write_parquet(
            tbl,
            sink = file.path(sp, base),
            compression = compression,
            chunk_size = rgs)
    }
    .invalidate_dataset_cache(path)
    invisible(TRUE)
}

#' Return the column names of the on-disk dataset. `LIMIT 0` asks DuckDB for
#' the schema only; it never reads a data page.
#'
#' @noRd
.dataset_col_names <- function(path) {
    con <- .duckdb_con()
    view <- .quote_ident(.dataset_view(path))
    names(DBI::dbGetQuery(con, paste0("SELECT * FROM ", view, " LIMIT 0")))
}

#' List the non-peak variable names present in the dataset.
#'
#' @noRd
.dataset_var_names <- function(path) {
    setdiff(.dataset_col_names(path), c("mz", "intensity"))
}

#' List the peak variable names present in the dataset.
#'
#' @noRd
.dataset_peak_names <- function(path) {
    intersect(.dataset_col_names(path), c("mz", "intensity"))
}

#' Compute the (sorted) integer vector of all spectrum IDs in the
#' on-disk dataset.
#'
#' @noRd
.dataset_spectra_ids <- function(path) {
    con <- .duckdb_con()
    ids <- DBI::dbGetQuery(con, paste0(
        "SELECT \"spectrum_id_\" FROM ", .quote_ident(.dataset_view(path)),
        " ORDER BY \"spectrum_id_\""))
    as.integer(ids$spectrum_id_)
}

#' Number of spectra in a native dataset.
#'
#' Counted from the written Parquet files rather than threaded through the
#' three different writers. `count(*)` is answered from row-group metadata,
#' so it never reads a data page.
#'
#' @noRd
.dataset_n_spectra <- function(path) {
    con <- .duckdb_con()
    as.integer(DBI::dbGetQuery(con, paste0(
        "SELECT count(*) AS n FROM ",
        .quote_ident(.dataset_view(path))))$n)
}

#' @noRd
.dataset_unique_ms_levels <- function(path) {
    con <- .duckdb_con()
    res <- DBI::dbGetQuery(con, paste0(
        "SELECT DISTINCT \"msLevel\" FROM ",
        .quote_ident(.dataset_view(path)), " ORDER BY \"msLevel\""))
    sort(as.integer(res$msLevel))
}

# ------------------------------------------------------------------------------
# Filter helpers (DuckDB pushdown)
# ------------------------------------------------------------------------------

#' Collect the `spectrum_id_` values matching a SQL predicate, optionally
#' restricted to the backend's current ids.
#'
#' @noRd
.matching_ids <- function(object, where, restrict = TRUE) {
    con <- .duckdb_con()
    view <- .quote_ident(.dataset_view(.path(object)))
    sel <- paste0("SELECT \"spectrum_id_\" FROM ", view, " WHERE ")
    if (!restrict) {
        res <- DBI::dbGetQuery(con, paste0(sel, where))
        return(as.integer(res$spectrum_id_))
    }
    idw <- .ids_where(.ids(object), full = .is_full(object))
    if (!is.null(idw) && is.na(idw))
        return(.with_id_table(.ids(object), function(nm) {
            as.integer(DBI::dbGetQuery(con, paste0(
                "SELECT s.\"spectrum_id_\" FROM ", view, " s JOIN ",
                .quote_ident(nm), " i",
                " ON s.\"spectrum_id_\" = i.\"spectrum_id_\"",
                " WHERE ", where))$spectrum_id_)
        }))
    res <- DBI::dbGetQuery(
        con, paste0(sel, if (is.null(idw)) where
                         else .pred_and(idw, where)))
    as.integer(res$spectrum_id_)
}

#' Apply a SQL predicate to the dataset, restricted to the backend's current
#' spectra ids, and return the subset of `object` holding the matching ids in
#' their original order.
#'
#' If the object is in a "clean predicate" state (`.full = TRUE`, or a stored
#' predicate that exactly describes its surviving ids), the new predicate is
#' AND-combined with the stored one and evaluated against the **full** dataset
#' in one scan. The combined predicate is then stored on the result so a later
#' fetch can push it down instead of a long `spectrum_id_ IN (...)` list --
#' which both keeps the SQL small and lets DuckDB prune row groups on the
#' predicate columns.
#'
#' @param object MsBackendParquet
#'
#' @param where `character(1)` SQL predicate, built with the `.pred_*`
#'     helpers.
#'
#' @noRd
.subset_filter <- function(object, where) {
    if (!length(.ids(object))) {
        return(object)
    }

    stored <- if (isTRUE(object@.predicate_clean))
                  object@.pending_predicate else NULL
    chain_clean <- isTRUE(object@.full) || !is.null(stored)

    if (chain_clean) {
        combined <- if (is.null(stored)) where else .pred_and(stored, where)
        ids <- .matching_ids(object, combined, restrict = FALSE)
        new <- extractByIndex(object, which(.ids(object) %in% ids))
        new@.pending_predicate <- combined
        new@.predicate_clean <- TRUE
        return(new)
    }

    ids <- .matching_ids(object, where, restrict = TRUE)
    extractByIndex(object, which(.ids(object) %in% ids))
}

#' Merge multiple `MsBackendParquet` objects. All must point to the same
#' dataset path.
#'
#' @noRd
.combine <- function(objects) {
    if (length(objects) == 1L) {
        return(objects[[1L]])
    }
    cls <- vapply(objects, function(z) class(z)[1L], character(1))
    if (length(unique(cls)) != 1L) {
        stop("Can only merge backends of the same type: ", cls[1L],
             call. = FALSE)
    }
    paths <- vapply(objects, .path, character(1))
    if (length(unique(paths)) != 1L) {
        stop("Can only merge backends pointing to the same dataset path.",
             call. = FALSE)
    }
    res <- objects[[1L]]
    res@spectraIds <- unlist(lapply(objects, .ids), use.names = FALSE)
    res@localData <- do.call(
        MsCoreUtils::rbindFill,
        lapply(objects, function(z) z@localData))
    if (!nrow(res@localData)) {
        res@localData <- data.frame(row.names = seq_along(res@spectraIds))
    }
    res@spectraVariables <- unique(unlist(
        lapply(objects, function(z) z@spectraVariables),
        use.names = FALSE))
    res@nspectra <- length(res@spectraIds)
    # Merging combines distinct ID sets so the per-object stored
    # predicate no longer describes the union; force the fetch path
    # back to the spectrum_id_ filter.
    res@.full <- FALSE
    res@.predicate_clean <- FALSE
    res@.pending_predicate <- NULL
    res
}
