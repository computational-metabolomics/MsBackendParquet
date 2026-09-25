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

# The top-level keys this package interprets. Everything else in a manifest
# (the results layer's `uid`, `role`, `sources`, `provenance` and `results`,
# or keys a later version adds) is carried through a read/write cycle
# untouched, which mzStack-0 requires of every reader that rewrites a
# manifest.
.MZSTACK_OWNED_KEYS <- c("format", "version", "generation", "created", "runs")

#' Reduce an identifier to characters that survive every file system.
#'
#' A `run_id` becomes a directory name (`run_id=<id>`), so it has to lose the
#' Hive separators along with anything a file system might reject, and it must
#' not begin with a dot: `.` and `..` are not names, and a leading dot hides the
#' run from anything that lists a dataset.
#'
#' @noRd
.sanitise_run_id <- function(id) {
    id <- gsub("[^A-Za-z0-9._-]+", "_", as.character(id))
    id <- sub("^[._]+", "", id)
    id[is.na(id) | !nzchar(id)] <- "run"
    id
}

#' The run id for a natively converted source file.
#'
#' @importFrom tools file_path_sans_ext
#'
#' @noRd
.native_run_id <- function(file) {
    .sanitise_run_id(tools::file_path_sans_ext(basename(file)))
}

#' Make run ids unique, comparing them case-insensitively.
#'
#' A run id becomes a `run_id=<id>` directory name, so it must be unique within
#' a dataset. Ids taken from file basenames can repeat across source
#' directories, and on macOS and Windows `QC01` and `qc01` name the same
#' directory even though they are distinct strings.
#'
#' Either case would put two runs in one directory with their Parquet parts
#' interleaved. The `spectrum_id_` values stay correct, but a run's recorded
#' `n_spectra` stops matching what its directory actually holds, so later
#' per-run operations read or overwrite part of the other run.
#'
#' Ids are therefore folded to lower case before deduplication, and the suffix
#' `make.unique()` chose is appended to the original spelling so the caller's
#' capitalisation survives.
#'
#' @param ids candidate run ids, in write order.
#'
#' @param taken ids already recorded in the manifest, reserved so a separately
#'     written chunk cannot reuse one.
#'
#' @return `ids`, with a suffix added where one was needed.
#'
#' @noRd
.unique_run_ids <- function(ids, taken = character()) {
    if (!length(ids))
        return(ids)
    folded <- tolower(c(taken, ids))
    i <- seq_along(ids) + length(taken)
    paste0(ids, substring(make.unique(folded, sep = "-")[i],
                          nchar(folded[i]) + 1L))
}

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

#' Modification time and size of a dataset's manifest, `NULL` if it has none.
#'
#' The manifest is only ever replaced by an atomic rename, so a change of
#' either value means another writer, in this process or another, has
#' committed a new version.
#'
#' @noRd
.manifest_stamp <- function(path) {
    info <- file.info(.manifest_path(path), extra_cols = FALSE)
    if (is.na(info$size))
        return(NULL)
    c(as.numeric(info$mtime), info$size)
}

#' The manifest for a dataset, parsed once per version of the file.
#'
#' Checking the file's stamp costs one `stat()`, and is what lets a manifest
#' rewritten by another package or another process reach this one. When the
#' manifest has changed, every other cache held for the dataset is dropped
#' too, since the views, metadata and sample tables all derive from it.
#'
#' @noRd
.dataset_manifest <- function(path) {
    hit <- .manifest_cache[[path]]
    stamp <- .manifest_stamp(path)
    if (!is.null(hit)) {
        if (identical(hit$stamp, stamp))
            return(hit$manifest)
        .invalidate_dataset_cache(path)
    }
    m <- .manifest_read(path)
    assign(path, list(manifest = m, stamp = stamp), envir = .manifest_cache)
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
        mzstackError("format",
                     "Dataset '", path, "' mixes run kinds (",
                     paste(kinds, collapse = ", "),
                     "), which is not supported.")
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
        mzstackError("format",
                     "'", path, "' is not an mzStack dataset: no ",
                     .MZSTACK_MANIFEST, ". Datasets written before mzStack ",
                     "naming must be re-created.")
    txt <- tryCatch(paste(readLines(fl, warn = FALSE, encoding = "UTF-8"),
                          collapse = "\n"),
                    error = function(e)
                        mzstackError("format",
                                     "Could not read '", fl, "': ",
                                     conditionMessage(e)))
    parse <- function(simplify)
        tryCatch(jsonlite::fromJSON(txt, simplifyVector = simplify,
                                    simplifyDataFrame = FALSE),
                 error = function(e)
                     mzstackError("format",
                                  "Could not parse '", fl, "': ",
                                  conditionMessage(e)))
    m <- parse(TRUE)
    # Keys this package does not interpret are kept exactly as parsed, arrays
    # as lists. Simplified, a one-element array would come back as a scalar
    # and be written out as one, silently changing another layer's data.
    other <- setdiff(names(m), .MZSTACK_OWNED_KEYS)
    if (length(other)) {
        raw <- parse(FALSE)
        m[other] <- raw[other]
    }
    if (!identical(as.character(m$format)[1L], .MZSTACK_FORMAT))
        mzstackError("format",
                     "'", fl, "' declares format '", m$format %||% "<missing>",
                     "'; expected '", .MZSTACK_FORMAT, "'.")
    # Same major version means the layout is one this code understands; a
    # later minor version may add fields, which is harmless.
    have <- .semver_major(m$version)
    want <- .semver_major(.MZSTACK_VERSION)
    if (is.na(have) || !identical(have, want))
        mzstackError("format",
                     "Dataset '", path, "' is mzStack version ",
                     m$version %||% "<missing>", "; this version of ",
                     "MsBackendParquet reads ", want, ".x.")
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
    # `auto_unbox` would write a one-element array as a scalar; `I()` exempts
    # the fields the format defines as arrays.
    for (i in seq_along(m$runs))
        if (!is.null(m$runs[[i]]$signal$partitioning))
            m$runs[[i]]$signal$partitioning <-
                I(as.character(unlist(m$runs[[i]]$signal$partitioning)))
    writeLines(jsonlite::toJSON(.json_exact_doubles(m), auto_unbox = TRUE,
                                pretty = TRUE, null = "null", na = "null",
                                json_verbatim = TRUE),
               tmp)
    if (!file.rename(tmp, fl)) {
        unlink(tmp)
        stop("Could not write '", fl, "'.", call. = FALSE)
    }
    invisible(fl)
}

#' The shortest decimal rendering of each double that reads back exactly.
#'
#' jsonlite writes 15 significant digits by default, which does not
#' round-trip a binary64 value; mzStack requires numbers in the manifest to
#' round-trip exactly (a tolerance recorded as `0.01` when the code used
#' `0.010000000000000002` misstates the parameter). 17 digits always
#' suffice, and trying 15 and 16 first keeps `0.1` from being written as
#' `0.10000000000000001`. Non-finite values have no JSON form and become
#' `null`.
#'
#' @noRd
.shortest_double <- function(x) {
    out <- rep("null", length(x))
    ok <- is.finite(x)
    todo <- ok
    for (d in 15:17) {
        if (!any(todo))
            break
        s <- sprintf(paste0("%.", d, "g"), x[todo])
        back <- as.numeric(s) == x[todo]
        if (d == 17L)
            back[] <- TRUE
        idx <- which(todo)[back]
        out[idx] <- s[back]
        todo[idx] <- FALSE
    }
    out
}

#' Replace every double in a manifest by its exact JSON text.
#'
#' Doubles become pre-rendered `json` values, which jsonlite inserts
#' verbatim. A length-one vector is written as a scalar, as `auto_unbox`
#' would, unless it is wrapped in `I()`.
#'
#' @noRd
.json_exact_doubles <- function(x) {
    if (is.list(x)) {
        x[] <- lapply(x, .json_exact_doubles)
        return(x)
    }
    if (!is.double(x) || inherits(x, "json"))
        return(x)
    s <- .shortest_double(as.vector(x))
    if (length(s) != 1L || inherits(x, "AsIs"))
        s <- paste0("[", paste(s, collapse = ","), "]")
    structure(s, class = "json")
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
#' One column at a time rather than one `data.frame` per run bound together:
#' this is on the read path of `.manifest_split_ids()`, and a dataset holds one
#' run per source file, so `rbind`ing N single-row frames would make every call
#' quadratic in the number of runs.
#'
#' @noRd
.manifest_runs <- function(m) {
    runs <- m$runs
    if (!length(runs))
        return(data.frame(run_id = character(), kind = character(),
                          path = character(), n_spectra = integer(),
                          uid_base = integer(), ingested_at = integer(),
                          layout = character(), profile = character(),
                          centroid = character(),
                          stringsAsFactors = FALSE))
    chr <- function(f) vapply(runs, function(r) {
        x <- f(r)
        if (is.null(x) || !length(x)) NA_character_ else as.character(x)[1L]
    }, character(1))
    int <- function(f, default) vapply(runs, function(r) {
        x <- f(r)
        if (is.null(x) || !length(x)) default else as.integer(x)[1L]
    }, integer(1))
    kind_v <- chr(function(r) r$kind)
    path_v <- chr(function(r) r$path)
    src_v <- chr(function(r) r$signal$source)
    data.frame(
        run_id = chr(function(r) r$run_id),
        kind = kind_v,
        path = path_v,
        n_spectra = int(function(r) r$n_spectra, 0L),
        uid_base = int(function(r) r$uid_base, 1L),
        ingested_at = int(function(r) r$ingested_at, 1L),
        layout = chr(function(r) r$signal$layout),
        profile = chr(function(r) r$signal$profile),
        centroid = chr(function(r) r$signal$centroid),
        source = ifelse(!is.na(src_v), src_v,
                        ifelse(kind_v == "mzpeak", path_v, NA_character_)),
        stringsAsFactors = FALSE)
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
#'   external metadata keyed on them survives.
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
                              partitioning = character(),
                              uid_base = .manifest_next_uid_base(m),
                              source = NA_character_) {
    kind <- match.arg(kind, .MZSTACK_KINDS)
    if (run_id %in% vapply(m$runs, function(r) as.character(r$run_id),
                           character(1)))
        stop("A run with id '", run_id, "' is already in this dataset.",
             call. = FALSE)
    signal <- list(layout = layout, profile = profile, centroid = centroid)
    if (length(partitioning))
        signal$partitioning <- as.character(partitioning)
    if (length(source) && !is.na(source))
        signal$source <- as.character(source)[1L]
    m$runs[[length(m$runs) + 1L]] <- list(
        run_id = run_id,
        kind = kind,
        path = path,
        n_spectra = as.integer(n_spectra),
        uid_base = as.integer(uid_base),
        ingested_at = as.integer(m$generation),
        signal = signal,
        projections = stats::setNames(list(), character()))
    m
}

#' Write the manifest for a dataset converted from raw MS data files.
#'
#' One run per source file, each covering its own `spectra/run_id=<id>`
#' directory, in the order they were written.
#'
#' @param runs `list` of `list(run_id, path, n_spectra)`, in write order.
#'
#' @noRd
.manifest_write_native <- function(path, runs, partitioning = character()) {
    m <- .manifest_new()
    base <- 1L
    for (r in runs) {
        n <- as.integer(r$n_spectra)
        # A zero-spectrum run would share its `uid_base` with the next one,
        # leaving `findInterval()` in `.manifest_split_ids()` to pick between
        # them on tie-breaking alone.
        if (!length(n) || is.na(n) || !n)
            next
        m <- .manifest_add_run(
            m, run_id = r$run_id, kind = "native",
            path = normalizePath(r$path, mustWork = FALSE),
            n_spectra = n, layout = "list", partitioning = partitioning,
            uid_base = base, source = r$source %||% NA_character_)
        base <- base + n
    }
    n_disk <- .dataset_n_spectra(path)
    if (!identical(.manifest_n_spectra(m), as.integer(n_disk)))
        stop("Wrote ", n_disk, " spectra but accounted for ",
             .manifest_n_spectra(m), " across ", length(m$runs),
             " run(s); refusing to write an inconsistent manifest.",
             call. = FALSE)
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

#' Read and write an mzStack manifest
#'
#' @description
#'
#' Every mzStack dataset is described by a manifest, `mzStack.json`, at the
#' root of its directory. These functions give packages layered on
#' MsBackendParquet, such as a results layer that adds its own top-level keys,
#' access to it without re-implementing the format's rules:
#'
#' - `newManifest()` returns the manifest of an empty dataset: no runs, at
#'   generation 1.
#'
#' - `readManifest()` parses and validates the manifest. Keys this package
#'   does not interpret are returned exactly as parsed, arrays as `list`s, so
#'   they survive a read/write cycle unchanged.
#'
#' - `writeManifest()` replaces the manifest atomically: the new version is
#'   written to a temporary file and renamed into place, so a reader never
#'   observes a half-written manifest. It increments `generation`, refuses to
#'   overwrite a manifest another writer has committed since `manifest` was
#'   read, and drops every cache this package holds for the dataset.
#'   Numbers are written so that they read back exactly. Where `path` holds
#'   no manifest yet, the directory is created if needed and `manifest` is
#'   written as the dataset's first, without incrementing `generation`.
#'
#' - `manifestRuns()` returns the run entries as a `data.frame`, one row per
#'   run: `run_id`, `kind`, `path`, `n_spectra`, `uid_base`, `ingested_at`,
#'   `layout`, `profile`, `centroid` and `source`. The tuple
#'   `(run_id, uid_base, n_spectra, ingested_at)` is the run's fingerprint:
#'   while it is unchanged, the run's `spectrum_id_` values address the same
#'   spectra.
#'
#' - `invalidateDatasetCache()` drops the parsed manifest, the DuckDB view,
#'   the metadata cache and the sample metadata this process holds for a
#'   dataset. `writeManifest()` calls it; call it directly after changing a
#'   dataset's files by other means. A manifest replaced by another process is
#'   detected without it.
#'
#' The manifest must be written last, after every file it declares is
#' durably on disk: its atomic replacement is the commit point of any write.
#'
#' @param path `character(1)`, the dataset directory, or an
#'     `MsBackendParquet` over it.
#'
#' @param manifest `list`, as returned by `readManifest()` and modified.
#'
#' @param bump `logical(1)`, whether to increment `generation`. Every change
#'     to a dataset's content must, so leave this `TRUE` unless the change is
#'     one the specification exempts.
#'
#' @return `newManifest()` and `readManifest()` return a `list`. `writeManifest()` returns the
#'     written manifest invisibly, with its new `generation`.
#'     `manifestRuns()` returns a `data.frame`. `invalidateDatasetCache()`
#'     returns `NULL` invisibly.
#'
#' @author Ossama Edbali
#'
#' @name mzstack-manifest
#'
#' @examples
#' fl <- system.file("extdata", "QC01.mzpeak", package = "MsBackendParquet")
#' ds <- createMzPeakDataset(fl, file.path(tempdir(), "manifest-example"))
#' m <- readManifest(ds)
#' m$generation
#' manifestRuns(ds)[, c("run_id", "uid_base", "n_spectra", "ingested_at")]
#'
#' ## Add a key of another layer. It survives this package's own writes.
#' m$role <- "study"
#' m <- writeManifest(ds, m)
#' m$generation
NULL

#' @rdname mzstack-manifest
#'
#' @export
newManifest <- function() {
    .manifest_new()
}

#' @rdname mzstack-manifest
#'
#' @export
readManifest <- function(path) {
    .manifest_read(.as_dataset_path(path))
}

#' @rdname mzstack-manifest
#'
#' @export
writeManifest <- function(path, manifest, bump = TRUE) {
    if (!is.list(manifest) ||
        !identical(as.character(manifest$format)[1L], .MZSTACK_FORMAT))
        stop("'manifest' must be an mzStack manifest as returned by ",
             "readManifest() or newManifest().", call. = FALSE)
    if (is.character(path) && length(path) == 1L && !is.na(path) &&
        !.is_mzstack_dataset(path)) {
        # The first manifest is what makes the directory a dataset, so there
        # is no earlier version to conflict with or to count on from.
        .manifest_write(path, manifest)
        path <- normalizePath(path)
        .invalidate_dataset_cache(path)
        return(invisible(manifest))
    }
    path <- .as_dataset_path(path)
    on_disk <- .manifest_read(path)$generation
    if (!identical(as.integer(manifest$generation), on_disk))
        stop("The manifest of '", path, "' is at generation ", on_disk,
             " but 'manifest' was read at generation ", manifest$generation,
             ": another writer has committed since. Re-read it and re-apply ",
             "the change.", call. = FALSE)
    if (isTRUE(bump))
        manifest <- .manifest_bump_generation(manifest)
    .manifest_write(path, manifest)
    .invalidate_dataset_cache(path)
    invisible(manifest)
}

#' @rdname mzstack-manifest
#'
#' @export
manifestRuns <- function(path) {
    .manifest_runs(.dataset_manifest(.as_dataset_path(path)))
}

#' @rdname mzstack-manifest
#'
#' @export
invalidateDatasetCache <- function(path) {
    if (!is.character(path))
        stop("'path' must be a character vector.", call. = FALSE)
    .invalidate_dataset_cache(path)
    invisible(NULL)
}
