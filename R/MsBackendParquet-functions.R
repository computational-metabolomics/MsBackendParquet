#' @noRd
##
## Internal helpers for the MsBackendParquet backend.
##
## File layout of a `MsBackendParquet` dataset directory:
##
## <path>/
##   spectra/                Hive-partitioned Parquet dataset with one row
##                           per spectrum. Columns are the spectra
##                           variables plus `spectrum_id_` (integer
##                           primary key) and `mz` / `intensity` stored
##                           as `list<double>` columns.
##
## The backend object only stores the path to <path> and the (integer)
## vector of primary keys for the spectra currently *seen* by the
## backend. All data access is performed by opening the dataset lazily
## with `arrow::open_dataset()` and filtering by `spectrum_id_`.

## Name of the sub-directory storing the spectra dataset.
.SPECTRA_DIR <- "spectra"

## File written next to the spectra dataset that marks the directory as a
## valid `MsBackendParquet` dataset and stores a small bit of metadata
## that is cheap to read.
.META_FILE <- "MsBackendParquet.json"

## Default Parquet compression. `snappy` is widely supported and
## reasonably fast; users can override with `compression`.
.DEFAULT_COMPRESSION <- "snappy"

## ----- path / metadata helpers ---------------------------------------------

#' @noRd
.spectra_path <- function(path) {
    file.path(path, .SPECTRA_DIR)
}

#' @noRd
.meta_path <- function(path) {
    file.path(path, .META_FILE)
}

#' @noRd
.is_parquet_dataset <- function(path) {
    length(path) == 1L && dir.exists(path) && dir.exists(.spectra_path(path))
}

#' Write a small metadata file alongside the dataset.
#'
#' @noRd
.write_meta <- function(path, partitioning = character(),
                        peaksVariables = c("mz", "intensity")) {
    info <- list(
        package = "MsBackendParquet",
        version = as.character(utils::packageVersion("MsBackendParquet")),
        created = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
        partitioning = as.character(partitioning),
        peaksVariables = as.character(peaksVariables))
    writeLines(.to_json(info), .meta_path(path))
    invisible(TRUE)
}

#' Minimal JSON writer to avoid a hard jsonlite dependency.
#'
#' @noRd
.to_json <- function(x) {
    quote_str <- function(s) {
        s <- gsub("\\\\", "\\\\\\\\", s)
        s <- gsub("\"", "\\\\\"", s)
        paste0("\"", s, "\"")
    }
    fmt <- function(v) {
        if (is.character(v))
            paste0("[", paste0(quote_str(v), collapse = ","), "]")
        else if (is.logical(v))
            paste0("[", paste0(tolower(as.character(v)), collapse = ","), "]")
        else
            paste0("[", paste0(v, collapse = ","), "]")
    }
    parts <- vapply(names(x), function(nm) {
        v <- x[[nm]]
        if (length(v) == 1L && is.character(v))
            paste0(quote_str(nm), ":", quote_str(v))
        else
            paste0(quote_str(nm), ":", fmt(v))
    }, character(1))
    paste0("{", paste0(parts, collapse = ","), "}")
}

#' Open the spectra dataset lazily.
#'
#' @noRd
.open_spectra <- function(path) {
    sp <- .spectra_path(path)
    if (!dir.exists(sp))
        stop("Parquet dataset directory '", sp, "' does not exist.",
             call. = FALSE)
    arrow::open_dataset(sp)
}

## ----- accessors on the object --------------------------------------------

#' @noRd
.path <- function(x) {
    x@path
}

#' @noRd
.ids <- function(x) {
    x@spectraIds
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

## ----- core data extraction ----------------------------------------------

#' Apply a subsetting expression to the (lazy) dataset using the spectra
#' IDs of the backend. Assumes `ids` is non-empty; callers must
#' short-circuit for empty inputs.
#'
#' @noRd
.filter_by_ids <- function(ds, ids) {
    ## `%in%` on integer columns is pushed down by arrow.
    dplyr::filter(ds, .data$spectrum_id_ %in% !!ids)
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
    ds <- .open_spectra(.path(x))
    schema_names <- ds$schema$names
    cols <- unique(c("spectrum_id_", columns))
    available <- intersect(cols, schema_names)
    miss <- setdiff(cols, available)
    res <- ds |>
        .filter_by_ids(.ids(x)) |>
        dplyr::select(dplyr::all_of(available)) |>
        dplyr::collect()
    res <- as.data.frame(res)
    if (length(miss))
        for (m in miss) res[[m]] <- NA
    ## Re-order to match the backend's spectrum order.
    idx <- match(.ids(x), res$spectrum_id_)
    res <- res[idx, columns, drop = FALSE]
    rownames(res) <- NULL
    res
}

#' Fetch peaks data (list of two-column matrices, one per spectrum) for
#' the spectra of `x` in the order of `x@spectraIds`.
#'
#' @noRd
.fetch_peaks_data <- function(x, columns = c("mz", "intensity"),
                              drop = FALSE) {
    pv <- .peaks_variables(x)
    miss <- setdiff(columns, pv)
    if (length(miss)) {
        stop("Unsupported peaks variable(s): ",
             paste0("'", miss, "'", collapse = ", "), call. = FALSE)
    }
    if (!length(.ids(x))) {
        return(list())
    }
    ds <- .open_spectra(.path(x))
    sel <- unique(c("spectrum_id_", pv))
    res <- ds |>
        .filter_by_ids(.ids(x)) |>
        dplyr::select(dplyr::all_of(sel)) |>
        dplyr::collect()
    res <- as.data.frame(res)
    idx <- match(.ids(x), res$spectrum_id_)
    mz_list <- if ("mz" %in% pv) res$mz[idx] else NULL
    int_list <- if ("intensity" %in% pv) res$intensity[idx] else NULL
    .pack_peaks(mz_list, int_list, columns = columns, drop = drop)
}

#' Combine separate `mz` / `intensity` list columns into the list-of-
#' matrices structure expected by `peaksData()`.
#'
#' @noRd
.pack_peaks <- function(mz, intensity, columns = c("mz", "intensity"),
                        drop = FALSE) {
    n <- max(length(mz), length(intensity))
    if (!n) return(list())
    if (drop && length(columns) == 1L) {
        col <- columns
        out <- switch(col,
                      mz = mz,
                      intensity = intensity,
                      stop("Unsupported single column '", col, "'.",
                           call. = FALSE))
        out <- lapply(out, function(z) {
            if (is.null(z)) numeric() else as.numeric(z)
        })
        return(out)
    }
    empty <- matrix(NA_real_, nrow = 0L, ncol = length(columns),
                    dimnames = list(NULL, columns))
    lapply(seq_len(n), function(i) {
        m <- if (!is.null(mz)) as.numeric(mz[[i]]) else NULL
        ii <- if (!is.null(intensity)) as.numeric(intensity[[i]]) else NULL
        if (is.null(m) && is.null(ii)) return(empty)
        len <- if (!is.null(m)) length(m) else length(ii)
        if (!len) return(empty)
        cells <- vapply(columns, function(cc) {
            switch(cc,
                   mz = if (is.null(m)) rep(NA_real_, len) else m,
                   intensity = if (is.null(ii)) rep(NA_real_, len) else ii,
                   rep(NA_real_, len))
        }, numeric(len))
        mat <- matrix(cells, nrow = len, ncol = length(columns),
                      dimnames = list(NULL, columns))
        mat
    })
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

    if (length(mz_cols)) {
        peaks <- .fetch_peaks_data(x, columns = mz_cols, drop = FALSE)
        if (any(mz_cols == "mz")) {
            vals <- lapply(peaks, function(m) {
                if (is.null(m) || !nrow(m)) numeric() else m[, "mz"]
            })
            res$mz <- IRanges::NumericList(vals, compress = FALSE)
        }
        if (any(mz_cols == "intensity")) {
            vals <- lapply(peaks, function(m) {
                if (is.null(m) || !nrow(m)) numeric() else m[, "intensity"]
            })
            res$intensity <- IRanges::NumericList(vals, compress = FALSE)
        }
    }

    if (any(columns == "centroided") && !is.logical(res$centroided)) {
        res$centroided <- as.logical(res$centroided)
    }

    if (any(columns == "smoothed") && !is.logical(res$smoothed)) {
        res$smoothed <- as.logical(res$smoothed)
    }

    S4Vectors::extractCOLS(res, columns)
}

## ----- writing the dataset ------------------------------------------------

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

    tbl <- arrow::as_arrow_table(data)
    if (length(partitioning)) {
        ## Use a unique basename template per chunk to avoid overwriting
        ## previous Parquet parts when appending to an existing
        ## partitioned dataset.
        token <- paste0(format(Sys.time(), "%H%M%S"), "-",
                        paste(sample(c(letters, 0:9), 6, TRUE),
                              collapse = ""))
        arrow::write_dataset(
            tbl,
            path = sp,
            format = "parquet",
            partitioning = partitioning,
            compression = compression,
            basename_template = paste0("part-", token, "-{i}.parquet"),
            existing_data_behavior = "overwrite_or_ignore")
    } else {
        ## Single-file or chunked write: append a uniquely-named file in
        ## the spectra directory to keep multiple chunks separate.
        base <- sprintf("part-%s-%s.parquet",
                        format(Sys.time(), "%Y%m%d%H%M%S"),
                        paste(sample(c(letters, 0:9), 6, TRUE),
                              collapse = ""))
        arrow::write_parquet(
            tbl,
            sink = file.path(sp, base),
            compression = compression)
    }
    invisible(TRUE)
}

#' Read the schema (column names + types) of the on-disk dataset.
#'
#' @noRd
.dataset_schema <- function(path) {
    ds <- .open_spectra(path)
    ds$schema
}

#' List the non-peak variable names present in the dataset.
#'
#' @noRd
.dataset_var_names <- function(path) {
    sch <- .dataset_schema(path)
    setdiff(sch$names, c("mz", "intensity"))
}

#' List the peak variable names present in the dataset.
#'
#' @noRd
.dataset_peak_names <- function(path) {
    sch <- .dataset_schema(path)
    intersect(sch$names, c("mz", "intensity"))
}

#' Compute the (sorted) integer vector of all spectrum IDs in the
#' on-disk dataset.
#'
#' @noRd
.dataset_spectra_ids <- function(path) {
    ds <- .open_spectra(path)
    ids <- ds |>
        dplyr::select(dplyr::all_of("spectrum_id_")) |>
        dplyr::arrange(.data$spectrum_id_) |>
        dplyr::collect()
    as.integer(ids$spectrum_id_)
}

#' @noRd
.dataset_unique_ms_levels <- function(path) {
    ds <- .open_spectra(path)
    res <- ds |>
        dplyr::select(dplyr::all_of("msLevel")) |>
        dplyr::distinct() |>
        dplyr::collect()
    sort(as.integer(res$msLevel))
}

## ----- filter helpers (Arrow-pushdown) ------------------------------------

#' Run a `dplyr` filter expression on the lazy dataset, restricted to
#' the current spectra IDs of the backend, and return the subset of
#' object containing only the matching IDs (preserving their original
#' order).
#'
#' @param object MsBackendParquet
#'
#' @param expr quosure produced via `rlang::enquo()`.
#'
#' @noRd
.subset_filter <- function(object, expr) {
    if (!length(.ids(object))) {
        return(object)
    }

    ds <- .open_spectra(.path(object))
    keep <- ds |>
        .filter_by_ids(.ids(object)) |>
        dplyr::filter(!!expr) |>
        dplyr::select(dplyr::all_of("spectrum_id_")) |>
        dplyr::collect()
    ids <- as.integer(keep$spectrum_id_)
    extractByIndex(object, which(.ids(object) %in% ids))
}

#' Merge multiple `MsBackendParquet` objects. All must point to the same
#' dataset path.
#'
#' @noRd
.combine <- function(objects) {
    if (length(objects) == 1L) return(objects[[1L]])
    cls <- vapply(objects, function(z) class(z)[1L], character(1))
    if (length(unique(cls)) != 1L)
        stop("Can only merge backends of the same type: ", cls[1L],
             call. = FALSE)
    paths <- vapply(objects, .path, character(1))
    if (length(unique(paths)) != 1L)
        stop("Can only merge backends pointing to the same dataset path.",
             call. = FALSE)
    res <- objects[[1L]]
    res@spectraIds <- unlist(lapply(objects, .ids), use.names = FALSE)
    res@localData <- do.call(
        MsCoreUtils::rbindFill,
        lapply(objects, function(z) z@localData))
    if (!nrow(res@localData))
        res@localData <- data.frame(row.names = seq_along(res@spectraIds))
    res@spectraVariables <- unique(unlist(
        lapply(objects, function(z) z@spectraVariables),
        use.names = FALSE))
    res@nspectra <- length(res@spectraIds)
    res
}
