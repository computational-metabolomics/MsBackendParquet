#' @include MsBackendParquet-functions.R

#' @title Apache Parquet-based MS data backend
#'
#' @aliases MsBackendParquet-class
#' @aliases backendInitialize,MsBackendParquet-method
#' @aliases backendMerge,MsBackendParquet-method
#' @aliases dataStorage,MsBackendParquet-method
#' @aliases extractByIndex,MsBackendParquet,ANY-method
#' @aliases filterDataOrigin,MsBackendParquet-method
#' @aliases filterMsLevel,MsBackendParquet-method
#' @aliases filterPrecursorMzRange,MsBackendParquet-method
#' @aliases filterPrecursorMzValues,MsBackendParquet-method
#' @aliases filterRt,MsBackendParquet-method
#' @aliases intensity,MsBackendParquet-method
#' @aliases intensity<-,MsBackendParquet-method
#' @aliases mz,MsBackendParquet-method
#' @aliases mz<-,MsBackendParquet-method
#' @aliases peaksData,MsBackendParquet-method
#' @aliases peaksVariables,MsBackendParquet-method
#' @aliases reset,MsBackendParquet-method
#' @aliases show,MsBackendParquet-method
#' @aliases spectraData,MsBackendParquet-method
#' @aliases spectraNames,MsBackendParquet-method
#' @aliases spectraNames<-,MsBackendParquet-method
#' @aliases supportsSetBackend,MsBackendParquet-method
#' @aliases tic,MsBackendParquet-method
#' @aliases uniqueMsLevels,MsBackendParquet-method
#'
#' @description
#'
#' The `MsBackendParquet` is an implementation of [Spectra::MsBackend()] for
#' [Spectra::Spectra()] objects that stores and retrieves mass spectrometry
#' (MS) data from an on-disk **mzStack** dataset, built on
#' [Apache Parquet](https://parquet.apache.org/).
#'
#' An mzStack dataset is a directory identified by an `mzStack.json`
#' manifest. It holds one or more *runs*, of either kind:
#'
#' - `native` - converted from raw MS data files by this package, with m/z
#'   and intensity stored as Parquet `list<double>` columns beside the
#'   spectra metadata;
#' - `mzpeak` - an index over external
#'   [HUPO-PSI mzPeak](https://github.com/HUPO-PSI/mzPeak) archives, which
#'   are read where they are and never modified.
#'
#' The manifest records which kind each run is, so a dataset never has to be
#' identified by guessing from its directory layout.
#'
#' The backend uses the [Apache Arrow](https://arrow.apache.org/docs/r)
#' R bindings to read and write Parquet files. Spectra metadata together
#' with the m/z and intensity values (stored as Parquet `list<double>`
#' columns) live in a single directory that can optionally be
#' Hive-partitioned (for example by `dataOrigin`) to enable Arrow's
#' partition pruning and parallel processing.
#'
#' New datasets can be created from raw MS data files using
#' [createMsBackendParquetDataset()] or by changing the backend of an
#' existing `Spectra` object with [Spectra::setBackend()].
#'
#' A backend can also be opened over a collection of
#' [HUPO-PSI mzPeak](https://github.com/HUPO-PSI/mzPeak) archives registered
#' with [createMzPeakDataset()]. In that case the archives are read where
#' they are and are never modified: the dataset holds only a small derived
#' index of the spectrum metadata, and peak data is read from the archives
#' on demand. See [createMzPeakDataset()] and [filterContainsMz()].
#'
#' @details
#'
#' The `MsBackendParquet` is principally a *read-only* backend; m/z and
#' intensity values cannot be modified once they have been written to
#' disk. However, by extending [Spectra::MsBackendCached()], the backend
#' allows users to *cache* additional or modified spectra variables
#' locally within the object without rewriting the on-disk dataset.
#'
#' Because the backend stores only a file system path (and not a live
#' connection), `MsBackendParquet` objects can be serialised to disk
#' with [save()] / [base::saveRDS()] and reused across parallel workers.
#' The DuckDB connection used to read the dataset is held at package
#' level, not on the object, and is keyed by process id, so a forked
#' worker transparently opens its own. Accordingly, [backendBpparam()]
#' returns the requested parallel processing setup unchanged.
#'
#' @section Creation of backend objects:
#'
#' New backend objects can be created with the `MsBackendParquet()`
#' constructor. To attach the backend to an existing Parquet dataset
#' (created previously with `createMsBackendParquetDataset()` or
#' `backendInitialize()` with the `data` argument) use:
#'
#' ```
#' be <- backendInitialize(MsBackendParquet(), path = "/path/to/dataset")
#' ```
#'
#' - `createMsBackendParquetDataset()`: import raw MS data files into a
#'   new Parquet dataset. See the dedicated help page for details.
#'
#' - `backendInitialize()`: attach the backend to an existing dataset.
#'   With the optional `data` argument (a `DataFrame` such as returned
#'   by [Spectra::spectraData()]), a **new** dataset is created at
#'   `path` from in-memory data. The dataset must not already exist.
#'   Additional arguments such as `partitioning` or `compression` are
#'   forwarded to [createMsBackendParquetDataset()].
#'
#' - `supportsSetBackend()`: returns `TRUE`. Changing to a
#'   `MsBackendParquet` is supported through [Spectra::setBackend()],
#'   passing the destination `path` (and optional `partitioning` /
#'   `compression`) as named arguments.
#'
#' @section Subsetting, merging and filtering data:
#'
#' `MsBackendParquet` objects can be subsetted with `[` or
#' `extractByIndex()`. Internally this only subsets the in-memory
#' vector of primary keys; the on-disk dataset is never modified. Any
#' subsetting can be undone with `reset()`.
#'
#' Multiple `MsBackendParquet` objects pointing to the **same** dataset
#' path can be combined with `backendMerge()` (mostly useful after a
#' `split()`).
#'
#' Filter methods optimised for `MsBackendParquet` (via Arrow's predicate
#' push-down) are:
#'
#' - `filterDataOrigin()`: keep spectra with matching `dataOrigin`.
#' - `filterMsLevel()`: keep spectra with the requested MS levels.
#' - `filterPrecursorMzRange()`: keep spectra with `precursorMz` in the
#'   given range.
#' - `filterPrecursorMzValues()`: keep spectra with `precursorMz`
#'   matching one of the provided values (with `ppm` / `tolerance`).
#' - `filterRt()`: keep spectra with retention times in the given range,
#'   optionally restricted to selected MS levels.
#'
#' If a filter variable has been modified locally with `$<-` the default
#' [Spectra::MsBackendCached()] implementation is used instead so that
#' the cached values take precedence over the on-disk ones.
#'
#' @section Accessing data:
#'
#' - `dataStorage()`: returns a `character` vector with the dataset
#'   path, one element per spectrum.
#' - `intensity()` / `mz()`: return [IRanges::NumericList()] objects.
#' - `intensity<-` / `mz<-`: not supported.
#' - `peaksData()`: returns a `list` of two-column matrices.
#' - `peaksVariables()`: returns `c("mz", "intensity")`.
#' - `reset()`: re-initialises the backend from disk and drops cached
#'   spectra variables.
#' - `spectraData()`: returns a `DataFrame` with the requested spectra
#'   variables. Locally cached variables are merged with on-disk ones.
#' - `spectraNames()`: returns the (character) `spectrum_id_` values.
#' - `tic()`: returns the original `totIonCurrent` (for `initial = TRUE`)
#'   or computes it from the intensity values.
#' - `uniqueMsLevels()`: returns the unique MS levels in the dataset.
#'
#' @param BPPARAM for `backendBpparam()`: parallel processing setup.
#'
#' @param columns For `spectraData()`: `character` with the names of the
#'     spectra variables to return. Defaults to all available variables.
#'     For `peaksData()`: `character` with the peaks variables to
#'     return.
#'
#' @param data For `backendInitialize()`: optional `DataFrame` with the
#'     spectra data that should be written to a new Parquet dataset.
#'
#' @param dataOrigin For `filterDataOrigin()`: `character` with the
#'     data origin values to keep.
#'
#' @param drop For `[`: ignored.
#'
#' @param i For `[`: `integer`, `logical` or `character` to subset the
#'     object.
#'
#' @param initial For `tic()`: `logical(1)` whether the original total
#'     ion current should be returned (`TRUE`, default) or computed
#'     from the intensities (`FALSE`).
#'
#' @param j For `[`: ignored.
#'
#' @param msLevel For `filterMsLevel()`: `integer` with the MS levels to
#'     keep. For `uniqueMsLevels()`: ignored.
#'
#' @param msLevel. For `filterRt()`: `integer` with the MS levels on
#'     which the retention time filter should be applied.
#'
#' @param mz For `filterPrecursorMzRange()`: `numeric(2)` with the lower
#'     and upper precursor m/z bounds. For `filterPrecursorMzValues()`:
#'     `numeric` with the m/z values to keep.
#'
#' @param name For `$<-`: `character(1)` with the variable name.
#'
#' @param object A `MsBackendParquet` instance.
#'
#' @param path `character(1)` with the path to the Parquet dataset
#'     directory.
#'
#' @param ppm For `filterPrecursorMzValues()`: `numeric` with the
#'     m/z-relative tolerance in parts-per-million.
#'
#' @param representation For `backendInitialize()` on a dataset built from
#'     mzPeak archives: `character(1)` selecting which signal to read.
#'     mzPeak keeps profile and centroid data in separate files, and an
#'     archive may hold both. `"auto"` (the default) prefers centroids and
#'     falls back to profile data; `"profile"` and `"centroid"` ask for one
#'     specifically and fail if the archive does not have it. Ignored for
#'     datasets written by [createMsBackendParquetDataset()].
#'
#' @param rt For `filterRt()`: `numeric(2)` with the retention time
#'     range.
#'
#' @param tolerance For `filterPrecursorMzValues()`: `numeric` with the
#'     absolute m/z tolerance.
#'
#' @param value Replacement value.
#'
#' @param x A `MsBackendParquet` instance (or a `character` with file
#'     paths for `createMsBackendParquetDataset()`).
#'
#' @param ... Additional arguments. For `backendInitialize()` with
#'     `data`: passed to [createMsBackendParquetDataset()].
#'
#' @name MsBackendParquet
#'
#' @useDynLib MsBackendParquet, .registration = TRUE
#'
#' @return See the description of the individual methods.
#'
#' @md
#'
#' @exportClass MsBackendParquet
#'
#' @examples
#' library(MsBackendParquet)
#'
#' # Create a tiny in-memory `Spectra`-like data frame.
#' sd <- S4Vectors::DataFrame(
#'     msLevel = c(1L, 1L, 2L),
#'     rtime = c(1.0, 2.0, 3.0),
#'     dataOrigin = "memory")
#' sd$mz <- IRanges::NumericList(c(100, 110), c(101, 111),
#'                               c(102, 112), compress = FALSE)
#' sd$intensity <- IRanges::NumericList(c(10, 20), c(11, 21),
#'                                      c(12, 22), compress = FALSE)
#'
#' path <- tempfile()
#' be <- backendInitialize(MsBackendParquet(), path = path, data = sd)
#' be
#' spectraVariables(be)
#' peaksData(be)
NULL

#' @importClassesFrom S4Vectors DataFrame
#'
#' @importClassesFrom Spectra MsBackendCached
setClass(
    "MsBackendParquet",
    contains = "MsBackendCached",
    slots = c(
        path = "character",
        spectraIds = "integer",
        .dataset_vars = "character",
        .peaks_vars = "character",
        partitioning = "character",
        representation = "character",
        .full = "logical",
        .pending_predicate = "ANY",
        .predicate_clean = "logical"),
    prototype = prototype(
        path = character(),
        spectraIds = integer(),
        .dataset_vars = character(),
        .peaks_vars = c("mz", "intensity"),
        partitioning = character(),
        representation = "auto",
        .full = FALSE,
        .pending_predicate = NULL,
        .predicate_clean = FALSE,
        readonly = TRUE, version = "0.1"))

#' @importFrom methods validObject
setValidity("MsBackendParquet", function(object) {
    msg <- NULL
    if (length(object@path) > 1L) {
        msg <- c(msg, "'path' must be a length-one character or empty.")
    }
    if (length(object@spectraIds) != object@nspectra) {
        msg <- c(msg, paste0("Number of spectra IDs (",
                             length(object@spectraIds),
                             ") does not match nspectra (",
                             object@nspectra, ")."))
    }
    if (length(object@path) && !.is_parquet_dataset(object@path)) {
        msg <- c(msg,
                 paste0("'", object@path,
                        "' is not a valid mzStack dataset."))
    }
    if (is.null(msg)) TRUE else msg
})

#' @rdname MsBackendParquet
#'
#' @export MsBackendParquet
MsBackendParquet <- function() {
    methods::new("MsBackendParquet")
}

#' @importMethodsFrom Spectra show
#'
#' @exportMethod show
#'
#' @rdname MsBackendParquet
setMethod("show", "MsBackendParquet", function(object) {
    methods::callNextMethod()
    if (length(.path(object))) {
        cat("Dataset: ", .path(object), "\n", sep = "")
        if (.dataset_kind(.path(object)) == "mzpeak") {
            m <- .dataset_manifest(.path(object))
            cat("mzPeak archives: ", length(m$runs), " run(s), signal read ",
                "as: ", .representation(object), "\n", sep = "")
        }
    }
})

#' @exportMethod backendInitialize
#'
#' @importMethodsFrom ProtGenerics backendInitialize
#'
#' @rdname MsBackendParquet
setMethod(
    "backendInitialize", "MsBackendParquet",
    function(object, path = character(), data,
             representation = c("auto", "profile", "centroid"), ...) {
        if (!length(path)) {
            stop("Parameter 'path' is required for 'MsBackendParquet'.",
                 call. = FALSE)
        }

        if (length(path) != 1L || !is.character(path)) {
            stop("'path' must be a length-one character vector.",
                 call. = FALSE)
        }
        representation <- match.arg(representation)

        path <- normalizePath(path, mustWork = FALSE)
        # If `data` is given, materialise a new dataset at `path`.
        if (!missing(data)) {
            if (.is_parquet_dataset(path)) {
                stop("An mzStack dataset already exists at '",
                     path, "'.", call. = FALSE)
            }
            createMsBackendParquetDataset(path = path, data = data, ...)
        }
        if (!.is_parquet_dataset(path)) {
            stop("'", path, "' is not an mzStack dataset: no ",
                 .MZSTACK_MANIFEST, ". Datasets written before mzStack ",
                 "naming must be re-created.", call. = FALSE)
        }
        object@path <- path
        object@representation <- representation
        # Ids are handed out in one contiguous block per run, in the order
        # runs were written, so the full set is exactly `seq_len(N)`;
        # computable from the manifest without reading the dataset. True of
        # both kinds: the converters allocate `spectrum_id_` from 1 upwards.
        object@spectraIds <- seq_len(
            .manifest_n_spectra(.dataset_manifest(path)))
        # Both kinds present `Spectra` names through the same translating
        # view. They differ only in where the peaks live: an mzPeak-backed
        # dataset reads them from the archives (so the view has no peak
        # columns), a native one from `mz` / `intensity` list columns in the
        # view.
        object@.dataset_vars <- .dataset_var_names(path)
        object@.peaks_vars <- if (.dataset_kind(path) == "mzpeak")
            c("mz", "intensity") else .dataset_peak_names(path)
        object@.full <- TRUE
        sv <- union(object@.dataset_vars, object@.peaks_vars)
        object <- methods::callNextMethod(
            object,
            nspectra = length(object@spectraIds),
            spectraVariables = sv)
        methods::validObject(object)

        # Load the filter columns once so the first filter does not pay for it.
        .meta_cache_warm(path, object@.dataset_vars, length(object@spectraIds))
        object
    })

#' @exportMethod dataStorage
#'
#' @importMethodsFrom ProtGenerics dataStorage
#'
#' @rdname MsBackendParquet
setMethod("dataStorage", "MsBackendParquet", function(object) {
    if (length(.path(object)) && length(object)) {
        rep(.path(object), length(object))
    } else {
        character()
    }
})

#' @exportMethod [
#'
#' @importFrom MsCoreUtils i2index
#'
#' @importFrom methods slot<-
#'
#' @rdname MsBackendParquet
setMethod("[", "MsBackendParquet", function(x, i, j, ..., drop = FALSE) {
    if (missing(i)) {
        return(x)
    }
    i <- MsCoreUtils::i2index(i, length(x), as.character(x@spectraIds))
    extractByIndex(x, i)
})

#' @rdname MsBackendParquet
#'
#' @importMethodsFrom ProtGenerics extractByIndex
#'
#' @exportMethod extractByIndex
setMethod(
    "extractByIndex", c("MsBackendParquet", "ANY"),
    function(object, i) {
        new_ids <- object@spectraIds[i]
        changed <- length(new_ids) != length(object@spectraIds) ||
            is.unsorted(new_ids) ||
            !identical(new_ids, object@spectraIds)
        if (changed) {
            object@.full <- FALSE
            object@.predicate_clean <- FALSE
        }
        methods::slot(object, "spectraIds", check = FALSE) <- new_ids
        methods::callNextMethod(object, i = i)
    })

#' @importMethodsFrom ProtGenerics peaksData
#'
#' @exportMethod peaksData
#'
#' @rdname MsBackendParquet
setMethod(
    "peaksData", "MsBackendParquet",
    function(object, columns = c("mz", "intensity")) {
        .fetch_peaks_data(object, columns = columns)
    })

#' @importMethodsFrom ProtGenerics peaksVariables
#'
#' @exportMethod peaksVariables
#'
#' @rdname MsBackendParquet
setMethod("peaksVariables", "MsBackendParquet", function(object) {
    .peaks_variables(object)
})

#' @exportMethod intensity
#'
#' @importMethodsFrom ProtGenerics intensity
#'
#' @rdname MsBackendParquet
setMethod("intensity", "MsBackendParquet", function(object) {
    .fetch_peaks_column(object, "intensity")
})

#' @exportMethod intensity<-
#'
#' @importMethodsFrom ProtGenerics intensity<-
#'
#' @rdname MsBackendParquet
setReplaceMethod("intensity", "MsBackendParquet", function(object, value) {
    stop("Cannot replace intensity values in a 'MsBackendParquet': data ",
         "are read-only.", call. = FALSE)
})

#' @exportMethod mz
#'
#' @importMethodsFrom ProtGenerics mz
#'
#' @rdname MsBackendParquet
setMethod("mz", "MsBackendParquet", function(object) {
    .fetch_peaks_column(object, "mz")
})

#' @exportMethod mz<-
#'
#' @importMethodsFrom ProtGenerics mz<-
#'
#' @rdname MsBackendParquet
setReplaceMethod("mz", "MsBackendParquet", function(object, value) {
    stop("Cannot replace m/z values in a 'MsBackendParquet': data are ",
         "read-only.", call. = FALSE)
})

#' @rdname MsBackendParquet
#'
#' @export
setReplaceMethod("$", "MsBackendParquet", function(x, name, value) {
    if (name == "spectrum_id_") {
        stop("'spectrum_id_' cannot be modified.", call. = FALSE)
    }
    methods::callNextMethod()
})

#' @importMethodsFrom ProtGenerics spectraData spectraVariables
#'
#' @exportMethod spectraData
#'
#' @rdname MsBackendParquet
setMethod(
    "spectraData", "MsBackendParquet",
    function(object, columns = spectraVariables(object)) {
        .spectra_data_parquet(object, columns = columns)
    })

#' @exportMethod reset
#'
#' @importMethodsFrom Spectra reset
#'
#' @rdname MsBackendParquet
setMethod("reset", "MsBackendParquet", function(object) {
    message("Restoring original data ...", appendLF = FALSE)
    if (length(.path(object)) && .is_parquet_dataset(.path(object))) {
        object <- backendInitialize(MsBackendParquet(),
                                    path = .path(object),
                                    representation = .representation(object))
    }
    message("DONE")
    object
})

#' @exportMethod spectraNames
#'
#' @importMethodsFrom ProtGenerics spectraNames
#'
#' @rdname MsBackendParquet
setMethod("spectraNames", "MsBackendParquet", function(object) {
    as.character(.ids(object))
})

#' @exportMethod spectraNames<-
#'
#' @importMethodsFrom ProtGenerics spectraNames<-
#'
#' @rdname MsBackendParquet
setReplaceMethod(
    "spectraNames", "MsBackendParquet", function(object, value) {
        stop("Replacing spectraNames is not supported for ",
             class(object)[1L], ".", call. = FALSE)
    })

#' @importMethodsFrom ProtGenerics filterMsLevel uniqueMsLevels
#'
#' @rdname MsBackendParquet
#'
#' @exportMethod filterMsLevel
setMethod(
    "filterMsLevel", "MsBackendParquet",
    function(object, msLevel = uniqueMsLevels(object)) {
        if (!length(msLevel)) {
            return(object)
        }

        if (.has_local_variable(object, "msLevel")) {
            return(methods::callNextMethod())
        }

        msLevel <- as.integer(msLevel)
        where <- .pred_in("msLevel", msLevel)
        v <- .meta_values(object, "msLevel")
        if (is.null(v)) {
            return(.subset_filter(object, where))
        }
        .filter_cached(object, v %in% msLevel, where)
    })

#' @importMethodsFrom ProtGenerics filterRt msLevel rtime
#'
#' @rdname MsBackendParquet
#'
#' @exportMethod filterRt
setMethod(
    "filterRt", "MsBackendParquet",
    function(object, rt = numeric(), msLevel. = integer()) {
        if (!length(rt) || all(is.infinite(rt))) {
            return(object)
        }

        rt <- range(rt)
        if (.has_local_variable(object, "rtime")) {
            if (length(msLevel.) && !.has_local_variable(object, "msLevel")) {
                object$msLevel <- msLevel(object)
            }
            return(methods::callNextMethod())
        }
        if (length(msLevel.) && .has_local_variable(object, "msLevel")) {
            if (!.has_local_variable(object, "rtime")) {
                object$rtime <- rtime(object)
            }
            return(methods::callNextMethod())
        }
        rng <- .pred_range("rtime", rt[1L], rt[2L])
        rtv <- .meta_values(object, "rtime")
        if (!length(msLevel.)) {
            if (is.null(rtv))
                return(.subset_filter(object, rng))
            return(.filter_cached(
                object, !is.na(rtv) & rtv >= rt[1L] & rtv <= rt[2L], rng))
        }
        # Spectra of other MS levels pass through untouched.
        ms <- as.integer(msLevel.)
        in_ms <- .pred_in("msLevel", ms)
        where <- .pred_or(.pred_and(rng, in_ms), .pred_not(in_ms))
        msv <- .meta_values(object, "msLevel")
        if (is.null(rtv) || is.null(msv))
            return(.subset_filter(object, where))
        sel <- msv %in% ms
        .filter_cached(
            object,
            (sel & !is.na(rtv) & rtv >= rt[1L] & rtv <= rt[2L]) | !sel,
            where)
    })

#' @importMethodsFrom ProtGenerics filterDataOrigin dataOrigin
#'
#' @rdname MsBackendParquet
#'
#' @exportMethod filterDataOrigin
setMethod(
    "filterDataOrigin", "MsBackendParquet",
    function(object, dataOrigin = character()) {
        if (!length(dataOrigin)) {
            return(object)
        }

        if (.has_local_variable(object, "dataOrigin")) {
            return(methods::callNextMethod())
        }

        dataOrigin <- as.character(dataOrigin)
        where <- .pred_in("dataOrigin", dataOrigin, quote = TRUE)
        v <- .meta_values(object, "dataOrigin")
        object <- if (is.null(v)) .subset_filter(object, where)
                  else .filter_cached(object, v %in% dataOrigin, where)
        if (length(dataOrigin) > 1L && length(object)) {
            object <- extractByIndex(
                object,
                order(match(dataOrigin(object), dataOrigin)))
        }
        object
    })

#' @importMethodsFrom ProtGenerics filterPrecursorMzRange
#'
#' @rdname MsBackendParquet
#'
#' @exportMethod filterPrecursorMzRange
setMethod(
    "filterPrecursorMzRange", "MsBackendParquet",
    function(object, mz = numeric()) {
        if (!length(mz)) {
            return(object)
        }

        if (.has_local_variable(object, "precursorMz")) {
            return(methods::callNextMethod())
        }

        mz <- range(mz)
        where <- .pred_range("precursorMz", mz[1L], mz[2L])
        v <- .meta_values(object, "precursorMz")
        if (is.null(v)) {
            return(.subset_filter(object, where))
        }
        .filter_cached(object, !is.na(v) & v >= mz[1L] & v <= mz[2L], where)
    })

#' @importMethodsFrom ProtGenerics filterPrecursorMzValues
#'
#' @importFrom MsCoreUtils ppm
#'
#' @rdname MsBackendParquet
#'
#' @exportMethod filterPrecursorMzValues
setMethod(
    "filterPrecursorMzValues", "MsBackendParquet",
    function(object, mz = numeric(), ppm = 20, tolerance = 0) {
        if (!length(mz)) {
            return(object)
        }

        if (.has_local_variable(object, "precursorMz")) {
            return(methods::callNextMethod())
        }

        lmz <- length(mz)
        if (length(ppm) != lmz) {
            ppm <- rep(ppm[1L], lmz)
        }
        if (length(tolerance) != lmz) {
            tolerance <- rep(tolerance[1L], lmz)
        }
        diffs <- MsCoreUtils::ppm(mz, ppm) + tolerance
        los <- mz - diffs
        his <- mz + diffs
        # Per-value tolerance windows, OR-ed together; DuckDB pushes the
        # disjunction down to row-group statistics on precursorMz.
        where <- do.call(.pred_or,
                         lapply(seq_along(mz), function(i)
                             .pred_range("precursorMz", los[i], his[i])))
        v <- .meta_values(object, "precursorMz")
        if (is.null(v))
            return(.subset_filter(object, where))
        keep <- rep(FALSE, length(v))
        ok <- !is.na(v)
        for (i in seq_along(mz)) {
            keep <- keep | (ok & v >= los[i] & v <= his[i])
        }
        .filter_cached(object, keep, where)
    })

#' @rdname MsBackendParquet
#'
#' @exportMethod uniqueMsLevels
setMethod("uniqueMsLevels", "MsBackendParquet", function(object, ...) {
    if (!length(.path(object)) || !.is_parquet_dataset(.path(object))) {
        return(integer())
    }
    v <- .meta_values(object, "msLevel")
    if (!is.null(v)) {
        return(sort(unique(as.integer(v))))
    }
    .dataset_unique_ms_levels(.path(object))
})

#' @rdname MsBackendParquet
#'
#' @importMethodsFrom ProtGenerics backendMerge
#'
#' @exportMethod backendMerge
setMethod("backendMerge", "MsBackendParquet", function(object, ...) {
    object <- unname(c(object, ...))
    not_empty <- lengths(object) > 0
    if (any(not_empty)) {
        res <- .combine(object[not_empty])
    } else {
        res <- object[[1L]]
    }
    methods::validObject(res)
    res
})

#' @rdname MsBackendParquet
#'
#' @importMethodsFrom ProtGenerics precScanNum
#'
#' @exportMethod precScanNum
setMethod("precScanNum", "MsBackendParquet", function(object) {
    spectraData(object, "precScanNum")[, 1L]
})

#' @rdname MsBackendParquet
#'
#' @importMethodsFrom ProtGenerics centroided
#'
#' @exportMethod centroided
setMethod("centroided", "MsBackendParquet", function(object) {
    as.logical(methods::callNextMethod())
})

#' @rdname MsBackendParquet
#'
#' @importMethodsFrom ProtGenerics smoothed
#'
#' @exportMethod smoothed
setMethod("smoothed", "MsBackendParquet", function(object) {
    as.logical(methods::callNextMethod())
})

#' @importMethodsFrom ProtGenerics tic
#'
#' @importFrom MsCoreUtils vapply1d
#'
#' @importFrom Spectra intensity
#'
#' @exportMethod tic
#'
#' @rdname MsBackendParquet
setMethod("tic", "MsBackendParquet", function(object, initial = TRUE) {
    if (initial) {
        spectraData(object, "totIonCurrent")[, 1L]
    } else {
        MsCoreUtils::vapply1d(intensity(object), sum, na.rm = TRUE)
    }
})

#' @importMethodsFrom Spectra supportsSetBackend
#'
#' @exportMethod supportsSetBackend
#'
#' @rdname MsBackendParquet
setMethod("supportsSetBackend", "MsBackendParquet", function(object, ...) {
    TRUE
})

#' @importMethodsFrom Spectra backendBpparam
#'
#' @importFrom BiocParallel SerialParam bpparam
#'
#' @rdname MsBackendParquet
setMethod(
    "backendBpparam", signature = "MsBackendParquet",
    function(object, BPPARAM = bpparam()) {
        # The backend stores no live connection, so any BPPARAM is fine.
        BPPARAM
    })

#' @importMethodsFrom ProtGenerics setBackend
#'
#' @importFrom Spectra processingChunkFactor
#'
#' @noRd
setMethod(
    "setBackend", c("Spectra", "MsBackendParquet"),
    function(object, backend, f = processingChunkFactor(object),
             path = character(), ..., BPPARAM = BiocParallel::SerialParam()) {
        if (!length(path))
            stop("Parameter 'path' is required for 'MsBackendParquet'.",
                 call. = FALSE)
        backend_class <- class(object@backend)[1L]
        if (length(object)) {
            .set_backend_insert_data(object, f = f, path = path, ...)
            object@backend <- backendInitialize(backend, path = path)
        } else {
            object@backend <- backendInitialize(
                backend, path = path,
                data = spectraData(object@backend), ...)
        }
        object@processing <- Spectra:::.logging(
            object@processing,
            "Switch backend from ", backend_class, " to ",
            class(object@backend))
        object
    })
