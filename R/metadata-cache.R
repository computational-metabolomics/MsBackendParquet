# In-memory index of the (small) non-peak spectra variables.
#
# The cache is package-level and keyed by normalised dataset path, NOT stored
# on the backend object. That keeps `MsBackendParquet` instances small and
# serialisable, lets every object over one dataset share a single copy, and
# lets a forked worker rebuild transparently. It is a read-through cache of
# immutable on-disk columns, entirely separate from `MsBackendCached`'s
# `localData`, which holds user modifications and always takes precedence.

# path -> list(ids = <integer>, dense = <logical>, data = <data.frame>)
.meta_state <- new.env(parent = emptyenv())

# Columns worth holding by default: the ones every optimised filter method
# needs. Everything else is loaded only if `spectraData()` asks for it and the
# budget allows.
.META_DEFAULT_COLUMNS <- c("msLevel", "rtime", "precursorMz", "dataOrigin")

# Budget in cells (rows x columns). 5e7 cells is ~400 MB for doubles, and
# covers 12.5M spectra for the four default columns.
.META_MAX_CELLS <- 5e7

#' @noRd
.meta_cache_enabled <- function() {
    opt <- getOption("MsBackendParquet.cacheMetadata", "auto")
    if (is.character(opt)) return(!identical(opt, "off"))
    isTRUE(opt)
}

#' @noRd
.meta_max_cells <- function() {
    as.numeric(getOption("MsBackendParquet.cacheMaxCells", .META_MAX_CELLS))
}

#' @noRd
.meta_default_columns <- function() {
    getOption("MsBackendParquet.cacheColumns", .META_DEFAULT_COLUMNS)
}

#' Drop the cached metadata for a dataset path. Called by
#' `.invalidate_dataset_cache()` whenever the on-disk files change.
#'
#' @noRd
.meta_cache_drop <- function(path) {
    for (k in unique(path)) {
        if (exists(k, envir = .meta_state, inherits = FALSE)) {
            rm(list = k, envir = .meta_state)
        }
    }
    invisible()
}

#' @noRd
.meta_cache_clear <- function() {
    rm(list = ls(.meta_state, all.names = TRUE), envir = .meta_state)
    invisible()
}

#' Load `columns` for the whole dataset into the cache and return its entry,
#' or `NULL` when caching is disabled or the request exceeds the budget.
#'
#' @param path normalised dataset path.
#'
#' @param columns character vector of *non-peak* column names.
#'
#' @param nspectra expected number of spectra, used for the budget check
#'     before any I/O happens.
#'
#' @noRd
.meta_cache_get <- function(path, columns, nspectra = NA_integer_) {
    if (!.meta_cache_enabled()) {
        return(NULL)
    }
    columns <- setdiff(unique(columns), c("mz", "intensity", "spectrum_id_"))
    if (!length(columns)) {
        return(NULL)
    }

    st <- .meta_state[[path]]
    have <- if (is.null(st)) character() else names(st$data)
    need <- setdiff(columns, have)
    if (!length(need)) {
        return(st)
    }

    n <- if (!is.null(st)) length(st$ids) else nspectra
    if (!is.na(n) && n * (length(have) + length(need)) > .meta_max_cells()) {
        return(NULL)
    }

    con <- .duckdb_con()
    view <- .quote_ident(.dataset_view(path))
    sel <- paste(c("\"spectrum_id_\"",
                   vapply(need, .quote_ident, character(1))), collapse = ", ")
    fetched <- DBI::dbGetQuery(con, paste0(
        "SELECT ", sel, " FROM ", view, " ORDER BY \"spectrum_id_\""))
    ids <- as.integer(fetched$spectrum_id_)
    fetched$spectrum_id_ <- NULL

    # `dataOrigin` / `dataStorage` repeat one long path per spectrum. Holding
    # them as factors keeps the cache a few bytes per row instead of a CHARSXP
    # pointer plus string per row.
    for (nm in names(fetched)) {
        if (is.character(fetched[[nm]])) {
            fetched[[nm]] <- factor(fetched[[nm]])
        }
    }

    if (is.null(st)) {
        st <- list(ids = ids,
                   dense = identical(ids, seq_along(ids)),
                   data = fetched)
    } else {
        if (!identical(ids, st$ids)) {
            return(NULL) # dataset changed
        }
        st$data <- cbind(st$data, fetched)
    }
    assign(path, st, envir = .meta_state)
    st
}

#' Row positions in a cache entry for a set of `spectrum_id_` values.
#'
#' Dense ascending ids make this a direct index rather than a hash join.
#'
#' @noRd
.meta_rows <- function(st, ids) {
    if (isTRUE(st$dense)) ids else match(ids, st$ids)
}

#' Values of a single cached spectra variable for the spectra of `x`, in
#' `spectraIds` order. Returns `NULL` when the value is not available from the
#' cache, which tells the caller to fall back to the SQL path.
#'
#' Character columns are held as factors in the cache; they are converted back
#' on the way out so callers see the type the dataset has.
#'
#' @noRd
.meta_values <- function(x, name) {
    st <- .meta_cache_get(.path(x), name, nspectra = length(x@spectraIds))
    if (is.null(st) || !name %in% names(st$data)) {
        return(NULL)
    }
    v <- st$data[[name]][.meta_rows(st, .ids(x))]
    if (is.factor(v)) as.character(v) else v
}

#' A `data.frame` of several cached spectra variables for the spectra of `x`,
#' in `spectraIds` order, or `NULL` if any is unavailable.
#'
#' @noRd
.meta_frame <- function(x, columns) {
    if (!length(columns)) return(NULL)
    st <- .meta_cache_get(.path(x), columns, nspectra = length(x@spectraIds))
    if (is.null(st) || !all(columns %in% names(st$data))) return(NULL)
    rows <- .meta_rows(st, .ids(x))
    out <- lapply(columns, function(nm) {
        v <- st$data[[nm]][rows]
        if (is.factor(v)) as.character(v) else v
    })
    names(out) <- columns
    as.data.frame(out, stringsAsFactors = FALSE, optional = TRUE)
}

#' Warm the cache with the default filter columns. Called once from
#' `backendInitialize()` so the first filter does not pay for the load.
#'
#' @noRd
.meta_cache_warm <- function(path, available, nspectra) {
    cols <- intersect(.meta_default_columns(), available)
    if (length(cols)) {
        try(.meta_cache_get(path, cols, nspectra = nspectra), silent = TRUE)
    }
    invisible()
}

#' Subset `object` to the spectra where `keep` is `TRUE`, recording `where` as
#' the equivalent SQL predicate.
#'
#' `keep` is computed in R from cached columns, but the SQL predicate is still
#' worth carrying: a later `peaksData()` can push it down so DuckDB prunes row
#' groups on the predicate columns, instead of matching a long
#' `spectrum_id_ IN (...)` list. Only valid while the predicate exactly
#' describes the surviving ids, which is what `.predicate_clean` tracks.
#'
#' @noRd
.filter_cached <- function(object, keep, where) {
    stored <- if (isTRUE(object@.predicate_clean))
                  object@.pending_predicate else NULL
    chain_clean <- isTRUE(object@.full) || !is.null(stored)
    new <- extractByIndex(object, which(keep))
    if (chain_clean) {
        new@.pending_predicate <-
            if (is.null(stored)) where else .pred_and(stored, where)
        new@.predicate_clean <- TRUE
    }
    new
}
