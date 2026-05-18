#' @include MsBackendParquet-functions.R MsBackendParquet.R

#' @title Create a Parquet dataset from raw MS data or a `Spectra` object
#'
#' @description
#'
#' `createMsBackendParquetDataset()` imports raw MS data from one or more
#' input files (mzML, mzXML or netCDF) into a new on-disk Parquet dataset
#' that can be used by [MsBackendParquet()]. Alternatively, an existing
#' `DataFrame` (such as the output of [Spectra::spectraData()]) can be
#' written to disk directly via the `data` argument.
#'
#' @details
#'
#' The function writes a single Parquet dataset (under
#' `<path>/spectra`) containing the spectra metadata plus the m/z and
#' intensity values as Parquet `list<double>` columns.
#'
#' Spectra metadata that are entirely `NA` (and not part of the Spectra
#' core variables) are dropped before writing to keep the on-disk
#' dataset compact.
#'
#' If `partitioning` is supplied (e.g. `partitioning = "dataOrigin"`),
#' Apache Arrow writes a Hive-partitioned dataset. Partitioning
#' typically enables Arrow's partition pruning when filtering and can
#' substantially speed up access to large datasets.
#'
#' @param path `character(1)` with the path to the dataset directory.
#'     The directory must not already contain a `MsBackendParquet`
#'     dataset (use a fresh path for new datasets).
#'
#' @param x `character` with the names of the raw MS data files to
#'     import. Ignored if `data` is supplied.
#'
#' @param data Optional `DataFrame` with the full spectra data to be
#'     written. Must include the `mz` and `intensity` peaks variables.
#'     When supplied, `x` and `backend` are ignored.
#'
#' @param backend MS backend used to read the raw input files. Defaults
#'     to `MsBackendMzR()`.
#'
#' @param chunksize `integer(1)` defining the number of input files
#'     processed per iteration.
#'
#' @param partitioning `character` with the names of the spectra
#'     variables to use as Hive partitioning columns. Use an empty
#'     vector (the default) to write a single un-partitioned set of
#'     files.
#'
#' @param compression `character(1)` with the Parquet compression codec
#'     to use. Defaults to `"snappy"`. See
#'     [arrow::write_parquet()] for supported values.
#'
#' @param BPPARAM Parallel processing setup used for reading the raw
#'     input files (only honoured when `engine = "spectra"`). Defaults
#'     to [BiocParallel::SerialParam()].
#'
#' @param engine `character(1)` selecting the import engine. The
#'     default `"spectra"` reads each chunk of files through a
#'     [Spectra::Spectra()] object (high throughput, supports
#'     `BPPARAM`). `"mzr"` streams spectra directly from mzML / mzXML
#'     / netCDF through [mzR::openMSfile()] into Apache Arrow's
#'     incremental Parquet writer, bounding memory to roughly
#'     `batch_size` spectra of peaks at a time. The streaming engine
#'     requires only raw input files (no `data` argument) and ignores
#'     `chunksize` / `BPPARAM`. `mzR` must be installed.
#'
#' @param batch_size `integer(1)` number of spectra per row group /
#'     write call when `engine = "mzr"`. Larger values amortise write
#'     overhead and compress better at the cost of higher transient
#'     memory; smaller values keep memory tighter. Ignored for
#'     `engine = "spectra"`.
#'
#' @return Invisibly returns `path` (with any tilde expanded).
#'
#' @md
#'
#' @export
#'
#' @importFrom Spectra MsBackendMzR Spectra spectraVariables peaksData
#' @importFrom Spectra spectraData coreSpectraVariables
#' @importFrom BiocParallel SerialParam bpparam
#' @importFrom MsCoreUtils vapply1l
createMsBackendParquetDataset <- function(
    path = character(),
    x = character(),
    data,
    backend = Spectra::MsBackendMzR(),
    chunksize = 10L,
    partitioning = character(),
    compression = .DEFAULT_COMPRESSION,
    BPPARAM = BiocParallel::SerialParam(),
    engine = c("spectra", "mzr"),
    batch_size = 1000L) {

    engine <- match.arg(engine)
    if (!length(path) || !nzchar(path))
        stop("'path' is required.", call. = FALSE)
    path <- normalizePath(path, mustWork = FALSE)
    if (.is_parquet_dataset(path))
        stop("A MsBackendParquet dataset already exists at '", path, "'.",
             call. = FALSE)
    if (!dir.exists(path)) dir.create(path, recursive = TRUE)

    if (!missing(data)) {
        if (engine == "mzr")
            stop("engine = \"mzr\" does not support the 'data' argument: ",
                 "it streams from raw input files. Use engine = \"spectra\" ",
                 "to write an in-memory DataFrame.", call. = FALSE)
        .write_from_spectra_data(path, data, partitioning = partitioning,
                                 compression = compression)
        .write_meta(path, partitioning = partitioning)
        return(invisible(path))
    }

    if (!length(x)) {
        stop("Either 'x' (raw MS file paths) or 'data' must be provided.",
             call. = FALSE)
    }

    if (engine == "mzr") {
        .stream_mzml_to_parquet(
            path = path, files = x,
            batch_size = as.integer(batch_size),
            partitioning = partitioning,
            compression = compression)
        .write_meta(path, partitioning = partitioning)
        return(invisible(path))
    }

    chunksize <- as.integer(chunksize)
    idxs <- seq_along(x)
    chunks <- split(idxs, ceiling(idxs / chunksize))
    next_id <- 0L
    message("Importing data ...")
    for (i in seq_along(chunks)) {
        sps <- Spectra::Spectra(source = backend, x[chunks[[i]]],
                                BPPARAM = BPPARAM)
        next_id <- .insert_from_spectra(
            path, sps, index = next_id,
            partitioning = partitioning,
            compression = compression)
        rm(sps); gc(verbose = FALSE)
    }
    .write_meta(path, partitioning = partitioning)
    invisible(path)
}

#' @title Convert mzML (or related) files into a Parquet dataset
#'
#' @description
#'
#' `mzMLToParquet()` is a thin convenience wrapper around
#' [createMsBackendParquetDataset()] tailored to the most common
#' use-case: importing one or more mass spectrometry data files
#' (mzML, mzXML, netCDF) from disk into a new Apache Parquet dataset
#' that can be read back by [MsBackendParquet()].
#'
#' Compared to calling `createMsBackendParquetDataset()` directly, this
#' function performs basic input validation (existence and recognised
#' file extension), optionally tears down a stale destination
#' directory, prints a short summary of what was written and returns
#' an initialised `MsBackendParquet` instance ready to be wrapped in a
#' [Spectra::Spectra()] object.
#'
#' @param files `character` with the paths to the input mzML, mzXML or
#'     netCDF files. Files must exist on disk and have a recognised
#'     extension (`.mzML`, `.mzXML`, `.cdf`, `.nc`; case-insensitive).
#'
#' @param path `character(1)` with the destination directory for the
#'     Parquet dataset. The directory must not already contain a
#'     `MsBackendParquet` dataset, unless `overwrite = TRUE`.
#'
#' @param partitioning `character` with the spectra variables to use
#'     as Hive partitioning columns (e.g. `"dataOrigin"`). Empty
#'     (default) writes a single un-partitioned set of files.
#'
#' @param compression `character(1)` Parquet compression codec, passed
#'     to [arrow::write_parquet()] / [arrow::write_dataset()]. Defaults
#'     to `"snappy"`.
#'
#' @param chunksize `integer(1)` number of input files processed per
#'     iteration. Higher values trade memory for throughput.
#'
#' @param overwrite `logical(1)` whether to remove an existing dataset
#'     at `path` before writing. Defaults to `FALSE`.
#'
#' @param BPPARAM Parallel processing setup forwarded to the
#'     underlying [Spectra::Spectra()] / `MsBackendMzR` reads (only
#'     honoured when `engine = "spectra"`).
#'
#' @param engine `character(1)` import engine, passed through to
#'     [createMsBackendParquetDataset()]. `"spectra"` (default) goes
#'     through [Spectra::Spectra()] and `MsBackendMzR()`; `"mzr"`
#'     streams batches of spectra directly from mzML via
#'     [mzR::openMSfile()] for bounded-memory imports of large
#'     files. `mzR` must be installed for `"mzr"`.
#'
#' @param batch_size `integer(1)` number of spectra per write batch
#'     when `engine = "mzr"`. Ignored otherwise.
#'
#' @param verbose `logical(1)` whether to print a short summary of the
#'     resulting dataset.
#'
#' @return Invisibly returns an initialised [MsBackendParquet()]
#'     instance pointing at the new dataset.
#'
#' @seealso [createMsBackendParquetDataset()] for the full API,
#'     [MsBackendParquet()] for the backend class.
#'
#' @md
#'
#' @export
#'
#' @importFrom tools file_ext
#'
#' @examples
#' \dontrun{
#' files <- c("sample-1.mzML", "sample-2.mzML")
#' be <- mzMLToParquet(files, path = tempfile(),
#'                     partitioning = "dataOrigin")
#' library(Spectra)
#' sps <- Spectra(be)
#' }
mzMLToParquet <- function(
    files,
    path,
    partitioning = character(),
    compression = .DEFAULT_COMPRESSION,
    chunksize = 10L,
    overwrite = FALSE,
    BPPARAM = BiocParallel::SerialParam(),
    engine = c("spectra", "mzr"),
    batch_size = 1000L,
    verbose = TRUE
) {
    engine <- match.arg(engine)
    if (missing(files) || !length(files)) {
        stop("'files' must be a non-empty character vector of input ",
             "MS data file paths.", call. = FALSE)
    }
    if (missing(path) || !length(path) || !nzchar(path)) {
        stop("'path' must be a non-empty character(1) directory path.",
             call. = FALSE)
    }
    path <- normalizePath(path, mustWork = FALSE)

    files <- as.character(files)
    .check_ms_files(files)

    if (.is_parquet_dataset(path)) {
        if (!isTRUE(overwrite)) {
            stop("A MsBackendParquet dataset already exists at '", path,
                 "'. Pass 'overwrite = TRUE' to replace it.",
                 call. = FALSE)
        }
        if (verbose) {
            message("Removing existing dataset at '", path, "' ...")
        }
        unlink(path, recursive = TRUE, force = TRUE)
    }

    if (verbose) {
        message("Importing ", length(files), " file(s) into '", path,
                "' using engine = \"", engine, "\" ...")
    }
    createMsBackendParquetDataset(
        path = path,
        x = files,
        backend = Spectra::MsBackendMzR(),
        chunksize = as.integer(chunksize),
        partitioning = partitioning,
        compression = compression,
        BPPARAM = BPPARAM,
        engine = engine,
        batch_size = as.integer(batch_size))

    be <- backendInitialize(MsBackendParquet(), path = path)
    if (verbose) {
        message("Wrote ", length(be), " spectrum/spectra to '", path,
                "'.")
        if (length(partitioning))
            message("Partitioned by: ",
                    paste(partitioning, collapse = ", "), ".")
    }
    invisible(be)
}

## ----- writers ------------------------------------------------------------

#' Write the (full) data from a `DataFrame` to a new dataset at `path`.
#'
#' @noRd
.write_from_spectra_data <- function(
    path,
    data,
    partitioning = character(),
    compression = .DEFAULT_COMPRESSION
) {
    if (!all(c("mz", "intensity") %in% colnames(data))) {
        stop("'data' must contain 'mz' and 'intensity' columns.",
             call. = FALSE)
    }
    n <- nrow(data)
    mzs <- as.list(data$mz)
    ints <- as.list(data$intensity)
    data$mz <- NULL
    data$intensity <- NULL
    if ("spectrum_id_" %in% colnames(data)) {
        warning("Overwriting existing 'spectrum_id_' column.",
                call. = FALSE)
        data$spectrum_id_ <- NULL
    }
    data <- as.data.frame(data)
    core_vars <- names(Spectra::coreSpectraVariables())
    data <- .drop_all_na_columns(data, keep = setdiff(colnames(data),
                                                     core_vars))
    if (!"msLevel" %in% colnames(data)) data$msLevel <- NA_integer_
    if (!"rtime" %in% colnames(data)) data$rtime <- NA_real_
    if (!"precursorMz" %in% colnames(data)) data$precursorMz <- NA_real_
    if (!"dataOrigin" %in% colnames(data))
        data$dataOrigin <- "<MsBackendParquet>"
    if (!"dataStorage" %in% colnames(data))
        data$dataStorage <- path
    data$spectrum_id_ <- seq_len(n)
    peaks <- lapply(seq_len(n), function(i) {
        m <- if (is.null(mzs[[i]])) numeric() else as.numeric(mzs[[i]])
        ii <- if (is.null(ints[[i]])) numeric() else as.numeric(ints[[i]])
        if (!length(m) && !length(ii)) {
            matrix(NA_real_, nrow = 0L, ncol = 2L,
                   dimnames = list(NULL, c("mz", "intensity")))
        } else {
            cbind(mz = m, intensity = ii)
        }
    })
    .write_spectra_chunk(path, data, peaks,
                         partitioning = partitioning,
                         compression = compression)
}

#' Insert one chunk of a `Spectra` object into the dataset, returning
#' the last used `spectrum_id_`.
#'
#' @noRd
.insert_from_spectra <- function(path, sps, index = 0L,
                                 partitioning = character(),
                                 compression = .DEFAULT_COMPRESSION) {
    sv <- Spectra::spectraVariables(sps)
    sv <- setdiff(sv, c("mz", "intensity"))
    spd <- as.data.frame(Spectra::spectraData(sps, columns = sv))
    if (nrow(spd)) {
        spd$spectrum_id_ <- seq.int(index + 1L, index + nrow(spd))
        if (!"dataStorage" %in% colnames(spd)) {
            spd$dataStorage <- path
        }
    }
    peaks <- Spectra::peaksData(sps, columns = c("mz", "intensity"))
    .write_spectra_chunk(path, spd, peaks,
                         partitioning = partitioning,
                         compression = compression,
                         append = TRUE)
    if (nrow(spd)) spd$spectrum_id_[nrow(spd)] else index
}

#' Similar to `.insert_from_spectra()` but driven by a chunk factor and
#' a parent `Spectra` object. Used by `setBackend()`.
#'
#' @noRd
.set_backend_insert_data <- function(object, f = NULL, path,
                                     partitioning = character(),
                                     compression = .DEFAULT_COMPRESSION,
                                     ...) {
    if (is.null(f) || !length(f))
        f <- rep(1L, length(object))
    if (!is.factor(f))
        f <- factor(f, levels = unique(f))
    if (length(f) != length(object))
        stop("'length(f)' has to match 'length(object)'.", call. = FALSE)
    if (.is_parquet_dataset(path))
        stop("Destination '", path, "' already contains a dataset.",
             call. = FALSE)
    if (!dir.exists(path)) dir.create(path, recursive = TRUE)
    next_id <- 0L
    for (l in levels(f)) {
        sub <- Spectra::Spectra(object@backend[f == l])
        next_id <- .insert_from_spectra(
            path, sub, index = next_id,
            partitioning = partitioning, compression = compression)
        rm(sub); gc(verbose = FALSE)
    }
    .write_meta(path, partitioning = partitioning)
    invisible(path)
}

## ----- utilities ----------------------------------------------------------

#' Drop columns that are entirely `NA`, except those listed in `keep`.
#'
#' @noRd
.drop_all_na_columns <- function(x, keep = character()) {
    if (!nrow(x)) return(x)
    is_all_na <- MsCoreUtils::vapply1l(x, function(z) {
        all <- all(is.na(z))
        if (length(all) > 1L) FALSE else all
    })
    is_all_na <- is_all_na & !colnames(x) %in% keep
    if (any(is_all_na)) x[, !is_all_na, drop = FALSE] else x
}

## Recognised input extensions (case-insensitive). `mzR`/`MsBackendMzR`
## supports mzML, mzXML and netCDF.
.MS_INPUT_EXTENSIONS <- c("mzml", "mzxml", "cdf", "nc")

#' Validate that all paths exist and have a supported MS file extension.
#'
#' @noRd
.check_ms_files <- function(files) {
    missing_files <- files[!file.exists(files)]
    if (length(missing_files))
        stop("The following input file(s) do not exist: ",
             paste0("'", missing_files, "'", collapse = ", "),
             call. = FALSE)
    ext <- tolower(tools::file_ext(files))
    bad <- ext[!ext %in% .MS_INPUT_EXTENSIONS]
    if (length(bad))
        stop("Unsupported file extension(s): ",
             paste0("'.", unique(bad), "'", collapse = ", "),
             ". Expected one of: ",
             paste0(".", .MS_INPUT_EXTENSIONS, collapse = ", "),
             ".", call. = FALSE)
    invisible(TRUE)
}

## ----- streaming mzR engine -----------------------------------------------

## Mapping from mzR `header()` column names to Spectra canonical
## spectra variable names. Columns not listed here are passed through
## as-is when they survive the all-NA filter; columns named on the
## right that don't appear in the header are filled with NA later.
##
## `isolationWindowLowerOffset` / `isolationWindowUpperOffset` are
## handled separately: we convert them into
## `isolationWindowLowerMz` / `isolationWindowUpperMz` using the
## target m/z, matching the convention used by `MsBackendMzR`.
.MZR_HEADER_RENAME <- c(
    seqNum = "acquisitionNum",
    acquisitionNum = "acquisitionNum",
    msLevel = "msLevel",
    polarity = "polarity",
    retentionTime = "rtime",
    precursorScanNum = "precScanNum",
    precursorMZ = "precursorMz",
    precursorCharge = "precursorCharge",
    precursorIntensity = "precursorIntensity",
    collisionEnergy = "collisionEnergy",
    ionisationEnergy = "ionisationEnergy",
    lowMZ = "lowMz",
    highMZ = "highMz",
    basePeakMZ = "basePeakMz",
    basePeakIntensity = "basePeakIntensity",
    totIonCurrent = "totIonCurrent",
    isolationWindowTargetMZ = "isolationWindowTargetMz",
    spectrumId = "spectrumId",
    centroided = "centroided",
    injectionTime = "injectionTime",
    scanWindowLowerLimit = "scanWindowLowerLimit",
    scanWindowUpperLimit = "scanWindowUpperLimit")

#' Convert an `mzR::header()` data frame into a Spectra-canonical
#' spectra-variable data frame. Renames known columns, derives
#' `isolationWindowLowerMz` / `isolationWindowUpperMz` from the offsets
#' (when both target and offsets are present), drops mzR-only
#' bookkeeping columns that have no Spectra equivalent and are
#' uniformly NA.
#'
#' @noRd
.mzr_header_to_spectra_df <- function(hdr) {
    hdr <- as.data.frame(hdr, stringsAsFactors = FALSE)
    cn <- colnames(hdr)
    new_cn <- cn
    matched <- cn %in% names(.MZR_HEADER_RENAME)
    new_cn[matched] <- .MZR_HEADER_RENAME[cn[matched]]
    colnames(hdr) <- new_cn

    has_target <- "isolationWindowTargetMz" %in% colnames(hdr)
    has_lower <- "isolationWindowLowerOffset" %in% colnames(hdr)
    has_upper <- "isolationWindowUpperOffset" %in% colnames(hdr)
    if (has_target && has_lower) {
        hdr$isolationWindowLowerMz <-
            hdr$isolationWindowTargetMz - hdr$isolationWindowLowerOffset
        hdr$isolationWindowLowerOffset <- NULL
    }
    if (has_target && has_upper) {
        hdr$isolationWindowUpperMz <-
            hdr$isolationWindowTargetMz + hdr$isolationWindowUpperOffset
        hdr$isolationWindowUpperOffset <- NULL
    }

    if ("centroided" %in% colnames(hdr) && !is.logical(hdr$centroided))
        hdr$centroided <- as.logical(hdr$centroided)
    if ("peaksCount" %in% colnames(hdr)) hdr$peaksCount <- NULL

    .drop_all_na_columns(
        hdr,
        keep = c("msLevel", "rtime", "precursorMz", "dataOrigin",
                 "dataStorage", "acquisitionNum"))
}

#' Stream-import a set of mzML / mzXML / netCDF files into a Parquet
#' dataset at `path`, going directly through `mzR::openMSfile()` and
#' Arrow's incremental Parquet writer. Memory peak per file is bounded
#' by `batch_size * mean(peaks per spectrum)`.
#'
#' @param path destination dataset directory.
#'
#' @param files character vector of mzML / mzXML / netCDF files.
#'
#' @param batch_size integer(1) spectra per write batch / row group.
#'
#' @param partitioning character() Hive partitioning columns.
#'
#' @param compression Parquet compression codec.
#'
#' @param starting_id integer(1) first `spectrum_id_` to assign.
#'
#' @return invisibly returns the last assigned `spectrum_id_`.
#'
#' @noRd
.stream_mzml_to_parquet <- function(path, files,
                                    batch_size = 1000L,
                                    partitioning = character(),
                                    compression = .DEFAULT_COMPRESSION,
                                    starting_id = 0L) {
    if (!requireNamespace("mzR", quietly = TRUE))
        stop("Package 'mzR' is required for engine = \"mzr\". Install ",
             "it with `BiocManager::install(\"mzR\")`.", call. = FALSE)
    sp <- .spectra_path(path)
    if (!dir.exists(sp)) dir.create(sp, recursive = TRUE)
    batch_size <- max(1L, as.integer(batch_size))
    next_id <- as.integer(starting_id)
    for (f in files) {
        next_id <- .stream_one_file(
            path = path, file = f, batch_size = batch_size,
            partitioning = partitioning, compression = compression,
            starting_id = next_id)
    }
    invisible(next_id)
}

#' Stream one mzML file into the dataset.
#'
#' @noRd
.stream_one_file <- function(path, file, batch_size,
                             partitioning, compression, starting_id) {
    file_abs <- normalizePath(file, mustWork = TRUE)
    ms <- mzR::openMSfile(file_abs)
    on.exit(try(mzR::close(ms), silent = TRUE), add = TRUE)
    hdr <- mzR::header(ms)
    n <- nrow(hdr)
    if (!n) return(invisible(starting_id))
    sd <- .mzr_header_to_spectra_df(hdr)
    sd$dataOrigin <- file_abs
    sd$dataStorage <- path
    sd$spectrum_id_ <- seq.int(starting_id + 1L, starting_id + n)

    file_token <- paste0(format(Sys.time(), "%H%M%S"), "-",
                         paste(sample(c(letters, 0:9), 6, TRUE),
                               collapse = ""))
    writer <- NULL
    schema <- NULL
    sink <- NULL
    on.exit({
        if (!is.null(writer)) try(writer$Close(), silent = TRUE)
        if (!is.null(sink)) try(sink$close(), silent = TRUE)
    }, add = TRUE)

    batches <- split(seq_len(n), ceiling(seq_len(n) / batch_size))
    for (b in seq_along(batches)) {
        idx <- batches[[b]]
        pks <- .read_peaks_batch(ms, idx)
        batch_df <- sd[idx, , drop = FALSE]
        batch_df$mz <- lapply(pks, function(m) {
            if (is.null(m) || !length(m)) numeric() else as.numeric(m[, 1L])
        })
        batch_df$intensity <- lapply(pks, function(m) {
            if (is.null(m) || !length(m)) numeric() else as.numeric(m[, 2L])
        })
        if (length(partitioning)) {
            .write_batch_dataset(
                batch_df, path = path, partitioning = partitioning,
                compression = compression,
                basename = paste0("part-", file_token, "-",
                                  formatC(b, width = 5, flag = "0"),
                                  "-{i}.parquet"))
        } else {
            tbl <- arrow::as_arrow_table(batch_df)
            if (is.null(writer)) {
                schema <- tbl$schema
                sink <- arrow::FileOutputStream$create(
                    file.path(.spectra_path(path),
                              paste0("part-", file_token, ".parquet")))
                writer <- arrow::ParquetFileWriter$create(
                    schema = schema, sink = sink,
                    properties = arrow::ParquetWriterProperties$create(
                        compression = compression))
            } else {
                tbl <- tbl$cast(schema)
            }
            writer$WriteTable(tbl)
        }
    }
    if (!is.null(writer)) {
        writer$Close()
        writer <- NULL
        sink$close()
        sink <- NULL
    }
    starting_id + n
}

#' Read peaks for a vector of spectrum indices from an open mzR file.
#' Handles older mzR versions that only accept a scalar `i` by falling
#' back to a per-spectrum loop.
#'
#' @noRd
.read_peaks_batch <- function(ms, idx) {
    res <- tryCatch(mzR::peaks(ms, idx), error = function(e) NULL)
    if (is.null(res))
        res <- lapply(idx, function(k) mzR::peaks(ms, k))
    if (is.matrix(res)) list(res) else res
}

#' Write a single in-memory batch as a (chunk of) Hive-partitioned
#' Parquet dataset. Each call uses a distinct `basename_template`, so
#' previously written batches are not overwritten.
#'
#' @noRd
.write_batch_dataset <- function(batch_df, path, partitioning,
                                 compression, basename) {
    tbl <- arrow::as_arrow_table(batch_df)
    arrow::write_dataset(
        tbl, path = .spectra_path(path), format = "parquet",
        partitioning = partitioning,
        compression = compression,
        basename_template = basename,
        existing_data_behavior = "overwrite_or_ignore")
}
