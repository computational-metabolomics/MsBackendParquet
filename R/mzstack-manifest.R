# The mzStack manifest: `mzStack.json`.
#
# An mzStack dataset is a directory holding a manifest, a derived index, and
# (optionally) the signal itself:
#
#   <dataset>/
#     mzStack.json                              this file's subject
#     index/spectra/run_id=<id>/part-0.parquet  one flat row per spectrum
#     index/column_map.json                     CV terms obtained from archives
#     index/projections/<type>/run_id=<id>/     rebuildable signal caches
#     spectra/                                  signal, for `native` runs
#     runs/run_id=<id>/                         only when a ZIP had to be unpacked
#
# Everything under `index/` is derived. Source mzPeak archives are
# *referenced* by path and never written to.
#
# A dataset holds runs of one of two kinds, and the distinction is recorded
# per run rather than inferred from the directory:
#
#   kind = "mzpeak"   signal lives in an external mzPeak archive, one row per
#                     data point;
#   kind = "native"   signal was converted from mzML by this package and lives
#                     in <dataset>/spectra as list columns beside the metadata.
#
# Recording the kind *in* the manifest matters. Both kinds keep their manifest
# at the same place, so anything that guessed the kind from a file name would
# route a native dataset into the mzPeak read path and look for archives that
# do not exist.

.MZSTACK_MANIFEST <- "mzStack.json"
.MZSTACK_FORMAT <- "mzStack"
.MZSTACK_VERSION <- "0.1.0"
.MZSTACK_KINDS <- c("mzpeak", "native")

#' @noRd
.manifest_path <- function(path) {
    file.path(path, .MZSTACK_MANIFEST)
}

#' @noRd
.index_path <- function(path) {
    file.path(path, "index")
}

#' @noRd
.index_spectra_path <- function(path) {
    file.path(.index_path(path), "spectra")
}

#' @noRd
.projection_path <- function(path, type) {
    file.path(.index_path(path), "projections", type)
}

#' Is `path` an mzStack dataset?
#'
#' Answers only that question: does a manifest exist. What *kind* of signal
#' the dataset holds is a separate question, answered by `.dataset_kind()`
#' from the manifest's contents.
#'
#' @noRd
.is_mzstack_dataset <- function(path) {
    length(path) == 1L && !is.na(path) && dir.exists(path) &&
        file.exists(.manifest_path(path))
}

# Read-through cache of parsed manifests, keyed by normalised dataset path.
# Held at package level rather than on the backend object, for the same
# reason the metadata index is: backend objects stay small and serialisable,
# and every object over one dataset shares a single copy. Dropped by
# `.invalidate_dataset_cache()`.
.manifest_cache <- new.env(parent = emptyenv())

#' The manifest for a dataset, parsed at most once per process.
#'
#' @noRd
.dataset_manifest <- function(path) {
    m <- .manifest_cache[[path]]
    if (!is.null(m))
        return(m)
    m <- .manifest_read(path)
    assign(path, m, envir = .manifest_cache)
    m
}

#' @noRd
.manifest_cache_drop <- function(path) {
    for (k in unique(path))
        if (exists(k, envir = .manifest_cache, inherits = FALSE))
            rm(list = k, envir = .manifest_cache)
    invisible()
}

#' What kind of signal the dataset at `path` holds: `"mzpeak"` or `"native"`.
#'
#' Read from the manifest's run entries, never guessed from the directory
#' layout: both kinds keep their manifest in the same place, so the file name
#' says nothing about the contents.
#'
#' Returns `"native"` when there is no manifest yet. That is what lets the
#' converters work: they write the spectra first, query the dataset to count
#' them, and only then write the manifest.
#'
#' @noRd
.dataset_kind <- function(path) {
    if (!.is_mzstack_dataset(path))
        return("native")
    kinds <- unique(vapply(.dataset_manifest(path)$runs,
                           function(r) as.character(r$kind %||% "native"),
                           character(1)))
    if (!length(kinds))
        return("native")
    if (length(kinds) > 1L)
        stop("Dataset '", path, "' mixes run kinds (",
             paste(kinds, collapse = ", "),
             "), which is not supported.", call. = FALSE)
    kinds
}

#' An empty manifest.
#'
#' @noRd
.manifest_new <- function() {
    list(format = .MZSTACK_FORMAT,
         version = .MZSTACK_VERSION,
         generation = 1L,
         created = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
         runs = list())
}

#' Major component of a semantic version string.
#'
#' @noRd
.semver_major <- function(v) {
    suppressWarnings(as.integer(sub("\\..*$", "", as.character(v)[1L])))
}

#' Read an mzStack manifest.
#'
#' @noRd
.manifest_read <- function(path) {
    fl <- .manifest_path(path)
    if (!file.exists(fl))
        stop("'", path, "' is not an mzStack dataset: no ",
             .MZSTACK_MANIFEST, ". Datasets written before mzStack ",
             "naming must be re-created.", call. = FALSE)
    m <- tryCatch(jsonlite::fromJSON(fl, simplifyVector = TRUE,
                                     simplifyDataFrame = FALSE),
                  error = function(e)
                      stop("Could not parse '", fl, "': ",
                           conditionMessage(e), call. = FALSE))
    if (!identical(as.character(m$format)[1L], .MZSTACK_FORMAT))
        stop("'", fl, "' declares format '", m$format %||% "<missing>",
             "'; expected '", .MZSTACK_FORMAT, "'.", call. = FALSE)
    # Same major version means the layout is one this code understands; a
    # later minor version may add fields, which is harmless.
    have <- .semver_major(m$version)
    want <- .semver_major(.MZSTACK_VERSION)
    if (is.na(have) || !identical(have, want))
        stop("Dataset '", path, "' is mzStack version ",
             m$version %||% "<missing>", "; this version of ",
             "MsBackendParquet reads ", want, ".x.", call. = FALSE)
    m$generation <- as.integer(m$generation)
    if (is.null(m$runs))
        m$runs <- list()
    m
}

#' Write a dataset manifest atomically.
#'
#' Written to a temporary file in the same directory and renamed into place,
#' so a reader never observes a half-written manifest and an interrupted
#' write leaves the previous one intact.
#'
#' @noRd
.manifest_write <- function(path, m) {
    if (!dir.exists(path))
        dir.create(path, recursive = TRUE)
    fl <- .manifest_path(path)
    tmp <- paste0(fl, ".tmp-", Sys.getpid())
    writeLines(jsonlite::toJSON(m, auto_unbox = TRUE, pretty = TRUE,
                                null = "null", na = "null"), tmp)
    if (!file.rename(tmp, fl)) {
        unlink(tmp)
        stop("Could not write '", fl, "'.", call. = FALSE)
    }
    invisible(fl)
}

#' Increment the generation counter.
#'
#' Every cache in the package is keyed on `(path, generation)`, so bumping
#' this is how a writer tells readers -- including readers in other processes
#' -- that what they hold is stale.
#'
#' @noRd
.manifest_bump_generation <- function(m) {
    m$generation <- as.integer(m$generation) + 1L
    m
}

#' Run entries as a `data.frame`, one row per run.
#'
#' Flattening the nested `signal` object here keeps every caller from having
#' to walk the list structure.
#'
#' @noRd
.manifest_runs <- function(m) {
    runs <- m$runs
    if (!length(runs))
        return(data.frame(run_id = character(), kind = character(),
                          path = character(), n_spectra = integer(),
                          uid_base = integer(), layout = character(),
                          profile = character(), centroid = character(),
                          stringsAsFactors = FALSE))
    chr <- function(x) if (is.null(x) || !length(x)) NA_character_
                       else as.character(x)[1L]
    do.call(rbind, lapply(runs, function(r) data.frame(
        run_id = chr(r$run_id),
        kind = chr(r$kind),
        path = chr(r$path),
        n_spectra = as.integer(r$n_spectra %||% 0L),
        uid_base = as.integer(r$uid_base %||% 1L),
        ingested_at = as.integer(r$ingested_at %||% 1L),
        layout = chr(r$signal$layout),
        profile = chr(r$signal$profile),
        centroid = chr(r$signal$centroid),
        stringsAsFactors = FALSE)))
}

#' Total number of spectra across all runs.
#'
#' @noRd
.manifest_n_spectra <- function(m) {
    if (!length(m$runs))
        return(0L)
    sum(vapply(m$runs, function(r) as.integer(r$n_spectra %||% 0L),
               integer(1)))
}

#' The `spectrum_id_` a new run's spectra start at.
#'
#' Ids are allocated in contiguous blocks, one block per run, in the order
#' runs were added: run k covers `uid_base .. uid_base + n_spectra - 1`. Two
#' properties follow, and both matter:
#'
#' - the full id vector of an untouched dataset is exactly `seq_len(N)`, so
#'   `backendInitialize()` can build it arithmetically instead of scanning
#'   every row on disk;
#' - adding a run never renumbers an existing one, so ids stay stable and any
#'   external annotation keyed on them survives.
#'
#' @noRd
.manifest_next_uid_base <- function(m) {
    if (!length(m$runs)) {
        return(1L)
    }
    r <- .manifest_runs(m)
    max(r$uid_base + r$n_spectra)
}

#' Append a run entry.
#'
#' @noRd
.manifest_add_run <- function(m, run_id, kind, path, n_spectra, layout,
                              profile = NA_character_,
                              centroid = NA_character_,
                              partitioning = character()) {
    kind <- match.arg(kind, .MZSTACK_KINDS)
    if (run_id %in% vapply(m$runs, function(r) as.character(r$run_id),
                           character(1)))
        stop("A run with id '", run_id, "' is already in this dataset.",
             call. = FALSE)
    signal <- list(layout = layout, profile = profile, centroid = centroid)
    if (length(partitioning))
        signal$partitioning <- as.character(partitioning)
    m$runs[[length(m$runs) + 1L]] <- list(
        run_id = run_id,
        kind = kind,
        path = path,
        n_spectra = as.integer(n_spectra),
        uid_base = .manifest_next_uid_base(m),
        # The generation at which this run's index was built. Projections
        # record the value they were derived from, so re-ingesting one run
        # invalidates only that run's caches -- adding an unrelated run does
        # not.
        ingested_at = as.integer(m$generation),
        signal = signal,
        projections = stats::setNames(list(), character()))
    m
}

#' Write the manifest for a dataset converted from raw MS data files.
#'
#' A native dataset is one run covering the whole `<dataset>/spectra`
#' directory. Individual source files stay distinguishable through the
#' `dataOrigin` column, so the writers do not have to track per-file
#' boundaries.
#'
#' Called at the end of every conversion path, replacing the sentinel file
#' that earlier versions wrote and never read.
#'
#' @param n_spectra total number of spectra written.
#'
#' @noRd
.manifest_write_native <- function(path, n_spectra,
                                   partitioning = character()) {
    m <- .manifest_add_run(
        .manifest_new(), run_id = "native", kind = "native",
        path = normalizePath(.spectra_path(path), mustWork = FALSE),
        n_spectra = n_spectra,
        layout = "list", partitioning = partitioning)
    .manifest_write(path, m)
    .manifest_cache_drop(unique(c(path, normalizePath(path,
                                                      mustWork = FALSE))))
    invisible(path)
}

#' Hive partitioning columns recorded for a run, or `character()`.
#'
#' Kept out of `.manifest_runs()` because it is a vector per run rather than
#' a scalar, and it is informational: nothing in the read path needs it,
#' since DuckDB discovers partitions from the directory names.
#'
#' @noRd
.manifest_partitioning <- function(m, run_id) {
    for (r in m$runs)
        if (identical(as.character(r$run_id), run_id))
            return(as.character(r$signal$partitioning %||% character()))
    character()
}

#' Map `spectrum_id_` values onto the runs that hold them.
#'
#' Used by the peak-reading path, which must issue one query per source
#' archive. `findInterval()` on the block starts turns this into a binary
#' search rather than a scan over runs.
#'
#' @return `list` with one element per involved run: `run` (the manifest
#'     row) and `local` (the archive-local `spectrum_index` values,
#'     0-based), in the order the runs first appear in `ids`.
#'
#' @noRd
.manifest_split_ids <- function(m, ids) {
    r <- .manifest_runs(m)
    if (!nrow(r) || !length(ids))
        return(list())
    o <- order(r$uid_base)
    r <- r[o, , drop = FALSE]
    pos <- findInterval(ids, r$uid_base)
    pos[pos < 1L] <- NA_integer_
    bad <- is.na(pos) | ids >= (r$uid_base[pmax(pos, 1L)] +
                                r$n_spectra[pmax(pos, 1L)])
    if (any(bad))
        stop("spectrum id(s) outside the dataset: ",
             paste(utils::head(ids[bad], 5L), collapse = ", "), call. = FALSE)
    lapply(unique(pos), function(k) {
        sel <- pos == k
        list(run = r[k, , drop = FALSE],
             ids = ids[sel],
             local = as.integer(ids[sel] - r$uid_base[k]))
    })
}

#' Record that a projection has been built for a run.
#'
#' @noRd
.manifest_set_projection <- function(m, run_id, type) {
    for (i in seq_along(m$runs)) {
        if (identical(as.character(m$runs[[i]]$run_id), run_id)) {
            p <- m$runs[[i]]$projections
            if (is.null(p)) p <- list()
            p[[type]] <- list(
                generation = as.integer(m$runs[[i]]$ingested_at %||% 1L))
            m$runs[[i]]$projections <- p
            return(m)
        }
    }
    stop("No run '", run_id, "' in this dataset.", call. = FALSE)
}

#' @noRd
.manifest_drop_projection <- function(m, run_id, type) {
    for (i in seq_along(m$runs)) {
        if (identical(as.character(m$runs[[i]]$run_id), run_id)) {
            m$runs[[i]]$projections[[type]] <- NULL
            return(m)
        }
    }
    m
}

#' Which runs have a usable projection of `type`?
#'
#' A projection is usable only if it was built from the run as it stands now,
#' i.e. from the same ingest. Re-ingesting a run invalidates its projections
#' and leaves every other run's alone.
#'
#' @noRd
.manifest_has_projection <- function(m, type, run_ids = NULL) {
    ok <- vapply(m$runs, function(r) {
        p <- r$projections[[type]]
        !is.null(p) &&
            identical(as.integer(p$generation),
                      as.integer(r$ingested_at %||% 1L))
    }, logical(1))
    ids <- vapply(m$runs, function(r) as.character(r$run_id), character(1))
    have <- ids[ok]
    if (is.null(run_ids))
        return(have)
    all(run_ids %in% have)
}
