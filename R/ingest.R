# Ingesting mzPeak archives into a queryable dataset.
#
# Ingest builds a *derived index*: one flat row per spectrum, gathered from
# the archive's metadata table and its side tables (scans, precursors,
# selected ions). It never reads a byte of signal data.
#
# The archives themselves are not modified, and by default not even moved;
# the manifest records where each one lives. That is what keeps the dataset
# and the standard interchangeable: exporting a run back to mzPeak is a
# directory copy, because the run never stopped being an mzPeak archive.

# Row group size for the index: the number of spectra whose metadata is
# stored as one block. Parquet records per-block minimum and maximum values,
# and a query can skip blocks that cannot match, so smaller blocks prune
# range filters (retention time) more finely at the cost of more bookkeeping.
.INDEX_ROW_GROUP_SIZE <- 4096L

# Index columns sourced from the spectrum metadata table. Names on the left
# are what the index calls them; the character vector on the right lists the
# paths to look for, in order of preference.
.INDEX_METADATA_COLS <- list(
    spectrum_index = "index",
    id = "id",
    ms_level = "ms_level",
    time = "time",
    scan_polarity = "scan_polarity",
    spectrum_representation = "spectrum_representation",
    spectrum_type = "spectrum_type",
    lowest_observed_mz = "lowest_observed_mz",
    highest_observed_mz = "highest_observed_mz",
    base_peak_mz = "base_peak_mz",
    base_peak_intensity = "base_peak_intensity",
    total_ion_current = "total_ion_current",
    number_of_data_points = "number_of_data_points",
    number_of_peaks = "number_of_peaks")

.INDEX_SCAN_COLS <- list(
    scan_start_time = "scan_start_time",
    filter_string = "filter_string",
    ion_injection_time = "ion_injection_time",
    instrument_configuration_id = "instrument_configuration_id",
    scan_window_lower_limit = c("scan_windows.scan_window_lower_limit",
                                "scan_window_lower_limit"),
    scan_window_upper_limit = c("scan_windows.scan_window_upper_limit",
                                "scan_window_upper_limit"))

.INDEX_SELECTED_ION_COLS <- list(
    selected_ion_mz = "selected_ion_mz",
    charge_state = "charge_state",
    # The spec calls this `peak_intensity` (MS:1000042); older archives
    # wrote a bare `intensity`.
    peak_intensity = c("peak_intensity", "intensity"))

.INDEX_PRECURSOR_COLS <- list(
    precursor_index = "precursor_index",
    isolation_window_target = c("isolation_window.isolation_window_target",
                                "isolation_window.target_mz",
                                "isolation_window_target"),
    isolation_window_lower_offset = c(
        "isolation_window.isolation_window_lower_offset",
        "isolation_window_lower_offset"),
    isolation_window_upper_offset = c(
        "isolation_window.isolation_window_upper_offset",
        "isolation_window_upper_offset"),
    collision_energy = c("activation.collision_energy", "collision_energy"))

#' Projection list for a facet, as `expr AS "name"` fragments.
#'
#' Columns the archive does not have are simply omitted; the view layer
#' fills them in as typed `NULL`s later, so a missing facet never becomes a
#' missing spectra variable.
#'
#' @noRd
.facet_select <- function(leaves, cols) {
    out <- character()
    for (nm in names(cols)) {
        e <- .mzpeak_col_expr(leaves, cols[[nm]])
        if (!is.null(e))
            out <- c(out, paste0(e, " AS ", .quote_ident(nm)))
    }
    out
}

#' A side-table subquery reduced to one row per spectrum.
#'
#' Keeps the first row per `source_index` and records how many there were.
#' `QUALIFY` filters on a window function without a second pass.
#'
#' @param file the side table, or `NA` when the archive has none.
#'
#' @param order_by candidate paths to order by within a spectrum, so "first"
#'     means the same thing on every read.
#'
#' @return `character(1)` SQL, or `NA_character_`.
#'
#' @noRd
.facet_subquery <- function(file, cols, count_name, order_by = character()) {
    if (is.na(file))
        return(NA_character_)
    leaves <- .mzpeak_leaf_paths(file)
    src <- .mzpeak_col_expr(leaves, "source_index")
    if (is.null(src))
        return(NA_character_)
    sel <- .facet_select(leaves, cols)
    ord <- .mzpeak_col_expr(leaves, order_by)
    if (is.null(ord)) ord <- src
    con <- .duckdb_con()
    paste0(
        "SELECT ", src, " AS ", .quote_ident("source_index"),
        if (length(sel)) paste0(", ", paste(sel, collapse = ", ")) else "",
        ", count(*) OVER (PARTITION BY ", src, ") AS ",
        .quote_ident(count_name),
        " FROM read_parquet(", DBI::dbQuoteString(con, file), ")",
        " QUALIFY row_number() OVER (PARTITION BY ", src,
        " ORDER BY ", ord, ") = 1")
}

#' Build the flat per-spectrum index for one archive.
#'
#' Runs entirely inside DuckDB: archive Parquet in, index Parquet out, with
#' no intermediate R data frame. Memory stays flat no matter how large the
#' run is.
#'
#' @return the number of spectra written.
#'
#' @noRd
.build_run_index <- function(v, run_id, uid_base, dest_dir) {
    con <- .duckdb_con()
    if (!dir.exists(dest_dir)) {
        dir.create(dest_dir, recursive = TRUE)
    }
    dest <- file.path(dest_dir, "part-0.parquet")

    leaves <- .mzpeak_leaf_paths(v$metadata)
    idx_expr <- .mzpeak_col_expr(leaves, "index")
    if (is.null(idx_expr))
        stop("Spectrum metadata of '", v$dir, "' has no 'index' column.",
             call. = FALSE)

    sel <- c(
        # 32-bit on purpose: the whole backend addresses spectra with R
        # integer vectors. That caps a dataset at ~2.1e9 spectra, which is
        # far beyond anything practical here but is a real ceiling.
        paste0("CAST(", uid_base, " + ", idx_expr, " AS INTEGER) AS ",
               .quote_ident("spectrum_id_")),
        # `run_id` is deliberately NOT written into the file: the index is
        # laid out in `run_id=<id>` directories, and DuckDB derives the
        # column from the directory name. That lets it skip whole runs
        # without opening their files, which matters once a dataset holds
        # thousands of them.
        paste0(DBI::dbQuoteString(con, v$dir), " AS ",
               .quote_ident("data_origin")),
        .facet_select(leaves, .INDEX_METADATA_COLS))

    facets <- list(
        list(alias = "s", sql = .facet_subquery(
            v$scans, .INDEX_SCAN_COLS, "n_scans", "scan_index"),
            cols = names(.INDEX_SCAN_COLS), count = "n_scans"),
        list(alias = "si", sql = .facet_subquery(
            v$selected_ions, .INDEX_SELECTED_ION_COLS, "n_selected_ions"),
            cols = names(.INDEX_SELECTED_ION_COLS), count = "n_selected_ions"),
        list(alias = "p", sql = .facet_subquery(
            v$precursors, .INDEX_PRECURSOR_COLS, "n_precursors"),
            cols = names(.INDEX_PRECURSOR_COLS), count = "n_precursors"))

    joins <- character()
    for (f in facets) {
        if (is.na(f$sql))
            next
        # Which of the facet's columns actually materialised.
        have <- DBI::dbGetQuery(con, paste0("DESCRIBE ", f$sql))$column_name
        for (nm in intersect(c(f$cols, f$count), have))
            sel <- c(sel, paste0(f$alias, ".", .quote_ident(nm)))
        joins <- c(joins, paste0(
            " LEFT JOIN (", f$sql, ") ", f$alias,
            " ON ", f$alias, ".", .quote_ident("source_index"), " = ",
            idx_expr))
    }

    sql <- paste0(
        "COPY (SELECT ", paste(sel, collapse = ", "),
        " FROM read_parquet(", DBI::dbQuoteString(con, v$metadata), ")",
        paste(joins, collapse = ""),
        " ORDER BY ", idx_expr, ") TO ", DBI::dbQuoteString(con, dest),
        " (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE ",
        .INDEX_ROW_GROUP_SIZE, ")")
    DBI::dbExecute(con, sql)

    as.integer(DBI::dbGetQuery(con, paste0(
        "SELECT count(*) AS n FROM read_parquet(",
        DBI::dbQuoteString(con, dest), ")"))$n)
}

#' Resolve the id a run will be known by.
#'
#' The archive's own `run.id` is preferred, because it is the identity the
#' data was published with. A directory name is the fallback.
#'
#' @noRd
.resolve_run_id <- function(v, dir) {
    id <- .mzpeak_run_id(v$index)
    if (is.na(id)) {
        id <- sub("\\.mzpeak$", "", basename(dir))
    }
    # `run_id` becomes a directory name in the index, so keep it to
    # characters that survive every file system.
    gsub("[^A-Za-z0-9._-]+", "_", id)
}

#' Merge an archive's controlled-vocabulary column mappings into the dataset.
#'
#' Not used for querying. It is the record of what each column *means* --
#' `ms_level` is `MS:1000511`, `total_ion_current` is `MS:1000285` in
#' detector counts -- without which the index would be just as
#' convention-bound as the format it came from.
#'
#' @noRd
.update_column_map <- function(path, run_id, v) {
    fl <- file.path(.index_path(path), "column_map.json")
    cur <- if (file.exists(fl))
        jsonlite::fromJSON(fl, simplifyDataFrame = TRUE) else NULL
    new <- .mzpeak_column_mapping(v$index)
    if (nrow(new))
        new$run_id <- run_id
    all <- if (is.null(cur) || !NROW(cur)) new else rbind(cur, new)
    if (!NROW(all))
        return(invisible(NULL))
    # One row per (path, accession); the run column keeps the first sighting.
    all <- all[!duplicated(all[, c("path", "accession")]), , drop = FALSE]
    writeLines(jsonlite::toJSON(all, auto_unbox = TRUE, pretty = TRUE,
                                na = "null"), fl)
    invisible(fl)
}

#' Ingest one archive into an existing dataset.
#'
#' @noRd
.ingest_archive <- function(path, m, archive, link = "reference",
                            verbose = TRUE) {
    archive <- normalizePath(archive, mustWork = TRUE)

    if (!dir.exists(archive)) {
        dest <- file.path(path, "runs", basename(archive))
        if (verbose)
            message("  unpacking '", basename(archive), "' ...")
        archive <- .mzpeak_unpack(archive, dest)
    } else if (identical(link, "copy")) {
        dest <- file.path(path, "runs", basename(archive))
        dir.create(dest, recursive = TRUE, showWarnings = FALSE)
        file.copy(list.files(archive, full.names = TRUE), dest,
                  recursive = TRUE)
        archive <- normalizePath(dest)
    }

    v <- .mzpeak_validate(archive)
    run_id <- .resolve_run_id(v, archive)
    uid_base <- .manifest_next_uid_base(m)
    n <- .build_run_index(
        v, run_id, uid_base,
        file.path(.index_spectra_path(path), paste0("run_id=", run_id)))

    m <- .manifest_add_run(
        m, run_id = run_id, kind = "mzpeak", path = archive,
        n_spectra = n, layout = v$layout,
        profile = if (is.na(v$profile)) NA_character_ else basename(v$profile),
        centroid = if (is.na(v$centroid)) NA_character_
                   else basename(v$centroid))
    .update_column_map(path, run_id, v)
    if (verbose) {
        message("  ", run_id, ": ", n, " spectra")
    }
    m
}

#' @title Build a queryable dataset from mzPeak archives
#'
#' @description
#'
#' `createMzPeakDataset()` registers one or more
#' [HUPO-PSI mzPeak](https://github.com/HUPO-PSI/mzPeak) archives as a single
#' queryable dataset and `addMzPeakArchives()` adds more to an existing one.
#'
#' The archives are **not modified**, and by default not copied: only a small
#' derived index of the spectrum metadata is written, next to a manifest
#' recording where each archive lives. Peak data continues to be read from
#' the archives themselves.
#'
#' @details
#'
#' Ingest reads each archive's `mzpeak_index.json`, its spectrum metadata
#' table and its scan / precursor / selected-ion side tables. It does not
#' read signal data, so the cost is proportional to the number of spectra
#' rather than to the size of the archives.
#'
#' Side tables are flattened: a spectrum with several scans contributes the
#' first, and the number that were present is recorded in `n_scans` (and
#' likewise `n_selected_ions`, `n_precursors`). The full detail remains in
#' the archive.
#'
#' Archives using mzPeak's *chunked* signal layout are rejected; only the
#' *point* layout is supported.
#'
#' @param archives `character` with paths to mzPeak archives. Each may be an
#'     unpacked directory or a `.mzpeak` ZIP file. ZIPs are unpacked into
#'     `<path>/runs`.
#'
#' @param path `character(1)` with the dataset directory. For
#'     `createMzPeakDataset()` it must not already hold a dataset.
#'
#' @param link `character(1)`; `"reference"` (the default) records where each
#'     archive is and leaves it there, `"copy"` copies it under `<path>/runs`
#'     so the dataset is self-contained.
#'
#' @param verbose `logical(1)` whether to report progress.
#'
#' @return `path`, invisibly.
#'
#' @seealso [MsBackendParquet()] to open the resulting dataset.
#'
#' @md
#'
#' @export
#'
#' @examples
#' \dontrun{
#' path <- tempfile()
#' createMzPeakDataset(c("run1.mzpeak", "run2.mzpeak"), path = path)
#' library(Spectra)
#' sps <- Spectra(backendInitialize(MsBackendParquet(), path = path))
#' }
createMzPeakDataset <- function(archives, path,
                                link = c("reference", "copy"),
                                verbose = TRUE) {
    link <- match.arg(link)
    if (missing(archives) || !length(archives))
        stop("'archives' must be a non-empty character vector of mzPeak ",
             "archive paths.", call. = FALSE)
    if (missing(path) || length(path) != 1L || !nzchar(path))
        stop("'path' must be a non-empty character(1) directory path.",
             call. = FALSE)
    path <- normalizePath(path, mustWork = FALSE)
    if (.is_mzstack_dataset(path))
        stop("An mzStack dataset already exists at '", path,
             "'. Use addMzPeakArchives() to add to it.", call. = FALSE)
    dir.create(.index_spectra_path(path), recursive = TRUE,
               showWarnings = FALSE)
    .manifest_write(path, .manifest_new())
    addMzPeakArchives(path, archives, link = link, verbose = verbose)
}

#' @rdname createMzPeakDataset
#'
#' @export
addMzPeakArchives <- function(path, archives,
                              link = c("reference", "copy"),
                              verbose = TRUE) {
    link <- match.arg(link)
    path <- normalizePath(path, mustWork = TRUE)
    m <- .manifest_read(path)
    if (verbose) {
        message("Ingesting ", length(archives), " archive(s) into '",
                path, "' ...")
    }
    for (a in archives) {
        m <- .ingest_archive(path, m, a, link = link, verbose = verbose)
    }
    m <- .manifest_bump_generation(m)
    .manifest_write(path, m)
    .invalidate_dataset_cache(path)
    if (verbose) {
        message("Dataset holds ", .manifest_n_spectra(m), " spectra in ",
                length(m$runs), " run(s).")
    }
    invisible(path)
}
