# Reading HUPO-PSI mzPeak archives.
#
# An mzPeak archive holds ONE mass spectrometry run. It is either a plain
# directory or an uncompressed ZIP containing:
#
#   mzpeak_index.json          lists every other member and says what it holds
#   spectra_metadata.parquet   one row per spectrum
#   spectra_metadata_*.parquet side tables (scans, precursors, selected ions)
#                              referencing the spectrum row by `source_index`
#   spectra_data.parquet       profile signal, ONE ROW PER DATA POINT
#   spectra_peaks.parquet      centroid signal, same shape
#
# Members are resolved through `mzpeak_index.json` by what they *contain*
# (`entity_type` + `data_kind`), never by file name: the specification is
# explicit that a reader must not depend on member names other than the index
# itself.
#
# Everything in this file is specification-facing. Nothing here knows about
# `Spectra`, and nothing outside this file parses the index. That containment
# is deliberate -- mzPeak is a draft standard and still moving.

# The one member name a reader is allowed to assume.
.MZPEAK_INDEX_FILE <- "mzpeak_index.json"

# Signal layouts. `point` stores one row per data point and is what this
# package reads. `chunk` packs a spectrum into delta- or Numpress-encoded
# blocks, which needs a decoder we do not have yet.
.MZPEAK_LAYOUTS <- c("point", "chunk")

#' @noRd
.mzpeak_index_path <- function(dir) {
    file.path(dir, .MZPEAK_INDEX_FILE)
}

#' Is `dir` an unpacked mzPeak archive?
#'
#' Cheap enough to call in a loop: one `file.exists()`.
#'
#' @noRd
.mzpeak_is_archive <- function(dir) {
    length(dir) == 1L && dir.exists(dir) &&
        file.exists(.mzpeak_index_path(dir))
}

#' Parse an archive's `mzpeak_index.json`.
#'
#' @param dir `character(1)` path to the unpacked archive directory.
#'
#' @return `list` with elements `files` (a `data.frame` with one row per
#'     member) and `metadata` (the archive-level metadata object, carrying
#'     `version`, `cv_list`, `run`, `file_description` and friends).
#'
#' @noRd
.mzpeak_read_index <- function(dir) {
    fl <- .mzpeak_index_path(dir)
    if (!file.exists(fl))
        stop("'", dir, "' is not an mzPeak archive: no ",
             .MZPEAK_INDEX_FILE, ".", call. = FALSE)
    idx <- tryCatch(
        jsonlite::fromJSON(fl, simplifyVector = TRUE,
                           simplifyDataFrame = TRUE),
        error = function(e)
            stop("Could not parse '", fl, "': ", conditionMessage(e),
                 call. = FALSE))
    if (!is.list(idx) || is.null(idx$files))
        stop("'", fl, "' has no 'files' member.", call. = FALSE)
    files <- idx$files
    if (!is.data.frame(files))
        files <- as.data.frame(files, stringsAsFactors = FALSE)
    if (!all(c("name", "entity_type", "data_kind") %in% colnames(files)))
        stop("'", fl, "' entries must have 'name', 'entity_type' and ",
             "'data_kind'.", call. = FALSE)
    idx$files <- files
    idx
}

#' Normalise an `entity_type` / `data_kind` token.
#'
#' The specification writes these inconsistently -- the prose tables use
#' `data arrays` and `wavelength spectrum` while the JSON examples use
#' `data_arrays` and `wavelength_spectrum`. Both mean the same thing, so
#' compare on a canonical form rather than on the literal string.
#'
#' @noRd
.mzpeak_token <- function(x) {
    gsub("[^a-z0-9]+", "_", tolower(as.character(x)))
}

#' Resolve archive members by what they contain.
#'
#' @param idx parsed index, as returned by `.mzpeak_read_index()`.
#'
#' @param entity_type `character(1)`, e.g. `"spectrum"`.
#'
#' @param data_kind `character(1)`, e.g. `"metadata"` or `"data_arrays"`.
#'
#' @return `character` with the matching member names (relative to the
#'     archive root); empty when the archive has no such member, which is
#'     legal -- an archive may omit chromatograms, centroids, or any facet.
#'
#' @noRd
.mzpeak_member <- function(idx, entity_type, data_kind) {
    f <- idx$files
    keep <- .mzpeak_token(f$entity_type) == .mzpeak_token(entity_type) &
        .mzpeak_token(f$data_kind) == .mzpeak_token(data_kind)
    keep[is.na(keep)] <- FALSE
    as.character(f$name[keep])
}

#' Absolute path of a single archive member, or `NA_character_`.
#'
#' Errors when the index names a member that is not on disk: a conformant
#' archive must reference only present files, and silently ignoring the
#' mismatch would surface later as a confusing SQL error.
#'
#' @noRd
.mzpeak_member_path <- function(dir, idx, entity_type, data_kind) {
    nm <- .mzpeak_member(idx, entity_type, data_kind)
    if (!length(nm))
        return(NA_character_)
    p <- file.path(dir, nm[1L])
    if (!file.exists(p))
        stop("'", .MZPEAK_INDEX_FILE, "' of '", dir, "' lists '", nm[1L],
             "' but the file does not exist.", call. = FALSE)
    p
}

#' Column names and types of a Parquet file.
#'
#' `DESCRIBE` asks DuckDB for the schema only; it never reads a data page,
#' so this is cheap even on a multi-gigabyte signal file.
#'
#' @return `data.frame` with `column_name` and `column_type`.
#'
#' @noRd
.mzpeak_columns <- function(file) {
    con <- .duckdb_con()
    res <- DBI::dbGetQuery(con, paste0(
        "DESCRIBE SELECT * FROM read_parquet(",
        DBI::dbQuoteString(con, file), ")"))
    res[, c("column_name", "column_type"), drop = FALSE]
}

#' Value of one Parquet footer metadata key, or `NA_character_`.
#'
#' Parquet files carry a small map of string keys to string values at the
#' end of the file. mzPeak uses it for the *array index*: a JSON description
#' of what each signal column actually is (m/z or intensity, its unit, its
#' data type, and how it was encoded).
#'
#' Two details, both of which cost time to discover:
#'
#' - DuckDB hands keys and values back as BLOBs. `decode()` turns a BLOB into
#'   text; casting it to VARCHAR does *not* -- it produces an escaped
#'   rendering (`\x22` for a quote) that looks plausible and parses as
#'   nothing.
#' - Only the requested key is decoded. Footer values are not required to be
#'   text (Arrow stores its own schema there as base64, and a writer may add
#'   anything), so decoding the whole map risks failing on a blob nobody
#'   asked for.
#'
#' @noRd
.mzpeak_kv_value <- function(file, key) {
    con <- .duckdb_con()
    res <- tryCatch(
        DBI::dbGetQuery(con, paste0(
            "SELECT decode(value) AS v FROM parquet_kv_metadata(",
            DBI::dbQuoteString(con, file), ") WHERE decode(key) = ",
            DBI::dbQuoteString(con, key))),
        error = function(e) NULL)
    if (is.null(res) || !nrow(res))
        return(NA_character_)
    as.character(res$v[1L])
}

#' Signal layout of a data/peaks file: `"point"`, `"chunk"` or `NA`.
#'
#' Read from the array index in the Parquet footer, whose `prefix` names the
#' layout. Falls back to sniffing the schema when the footer is absent or
#' unparseable, because a top-level `point` / `chunk` group is itself a
#' reliable signal (the specification keeps that node precisely so the layout
#' is visible from the schema).
#'
#' @noRd
.mzpeak_layout <- function(file, entity_type = "spectrum") {
    key <- paste0(.mzpeak_token(entity_type), "_array_index")
    raw <- .mzpeak_kv_value(file, key)
    if (!is.na(raw)) {
        ai <- tryCatch(jsonlite::fromJSON(raw, simplifyVector = TRUE),
                       error = function(e) NULL)
        pre <- ai$prefix
        if (length(pre) == 1L && !is.na(pre) && nzchar(pre)) {
            pre <- .mzpeak_token(pre)
            if (pre %in% .MZPEAK_LAYOUTS)
                return(pre)
        }
    }
    cols <- .mzpeak_token(.mzpeak_columns(file)$column_name)
    if (any(cols == "chunk") || any(grepl("_chunk_", cols, fixed = TRUE)))
        return("chunk")
    if (any(cols == "point") || any(cols == "mz"))
        return("point")
    NA_character_
}

# Controlled-vocabulary terms naming the two arrays every spectrum has.
.MS_MZ_ARRAY <- "MS:1000514"
.MS_INTENSITY_ARRAY <- "MS:1000515"

#' The array index of a signal file, as a `data.frame`.
#'
#' This is mzPeak's description of what each signal column *is* -- which one
#' holds m/z, which holds intensity, in what unit, encoded how. A conformant
#' reader is required to resolve arrays through it rather than by guessing
#' from column names, which is what this function exists for.
#'
#' @return `data.frame` with at least `path` and `array_type`, or a zero-row
#'     frame when the file carries no array index.
#'
#' @noRd
.mzpeak_array_index <- function(file, entity_type = "spectrum") {
    empty <- data.frame(path = character(), array_type = character(),
                        array_name = character(), buffer_format = character(),
                        buffer_priority = character(), unit = character(),
                        stringsAsFactors = FALSE)
    key <- paste0(.mzpeak_token(entity_type), "_array_index")
    raw <- .mzpeak_kv_value(file, key)
    if (is.na(raw))
        return(empty)
    ai <- tryCatch(jsonlite::fromJSON(raw, simplifyVector = TRUE,
                                      simplifyDataFrame = TRUE),
                   error = function(e) NULL)
    e <- ai$entries
    if (is.null(e) || !NROW(e))
        return(empty)
    if (!is.data.frame(e))
        e <- as.data.frame(e, stringsAsFactors = FALSE)
    col <- function(nm) if (nm %in% colnames(e)) as.character(e[[nm]])
                        else rep(NA_character_, nrow(e))
    data.frame(path = col("path"), array_type = col("array_type"),
               array_name = col("array_name"),
               buffer_format = col("buffer_format"),
               buffer_priority = col("buffer_priority"),
               unit = col("unit"), stringsAsFactors = FALSE)
}

# Tokens Parquet inserts to express repetition. They are part of the
# physical path but not of the logical one, and mzPeak's own `path`
# convention omits them ("delimited at nesting levels by '.', omitting
# [list, item|element] tokens").
.PARQUET_STRUCTURAL <- c("list", "element", "item")

# How DuckDB joins the tokens of a nested column path in `path_in_schema`.
.PATH_SEP <- ",\\s*"

#' Turn a Parquet leaf path into a DuckDB expression.
#'
#' `path` arrives as the token vector DuckDB reports, e.g.
#' `c("scan_windows", "list", "element", "scan_window_lower_limit")`.
#' A run of structural tokens becomes `[1]` -- we take the first entry of a
#' repeated group, which is the flattening this index deliberately performs.
#'
#' `struct_extract()` is used rather than dotted syntax because a top-level
#' group named `point` would otherwise be ambiguous with a table alias.
#'
#' @noRd
.path_expr <- function(path) {
    expr <- NULL
    i <- 1L
    n <- length(path)
    while (i <= n) {
        if (path[i] %in% .PARQUET_STRUCTURAL) {
            while (i <= n && path[i] %in% .PARQUET_STRUCTURAL) i <- i + 1L
            expr <- paste0(expr, "[1]")
            next
        }
        expr <- if (is.null(expr)) .quote_ident(path[i])
                else paste0("struct_extract(", expr, ", '", path[i], "')")
        i <- i + 1L
    }
    expr
}

#' Every leaf column of a Parquet file, logical path and access expression.
#'
#' This is how the ingest step finds a column without assuming whether the
#' writer nested it. `isolation_window_target` may sit at the top level or
#' inside an `isolation_window` group; both appear here, and the caller
#' matches on the logical path.
#'
#' @return `data.frame` with `path` (dotted, structural tokens removed) and
#'     `expr` (SQL).
#'
#' @noRd
.mzpeak_leaf_paths <- function(file) {
    con <- .duckdb_con()
    # DuckDB reports a nested column's path as its tokens joined with ", "
    # -- `parquet_metadata.path_in_schema` is a VARCHAR, not a list, so the
    # separator is all we get. That is unambiguous here because mzPeak
    # column names are identifier-safe: the specification requires
    # non-identifier characters to be replaced with `_`, so no name can
    # contain a comma.
    res <- tryCatch(
        DBI::dbGetQuery(con, paste0(
            "SELECT DISTINCT path_in_schema AS p FROM parquet_metadata(",
            DBI::dbQuoteString(con, file), ")")),
        error = function(e) NULL)
    if (is.null(res) || !nrow(res))
        return(data.frame(path = character(), expr = character(),
                          stringsAsFactors = FALSE))
    paths <- strsplit(as.character(res$p), .PATH_SEP)
    data.frame(
        path = vapply(paths, function(p)
            paste(setdiff(p, .PARQUET_STRUCTURAL), collapse = "."),
            character(1)),
        expr = vapply(paths, .path_expr, character(1)),
        stringsAsFactors = FALSE)
}

#' SQL for the first leaf column matching any of `paths`, else `NULL`.
#'
#' Candidates are tried in order, so a caller can prefer the nested,
#' specification-shaped location and fall back to a flattened one.
#'
#' @noRd
.mzpeak_col_expr <- function(leaves, paths) {
    for (p in paths) {
        hit <- which(leaves$path == p)
        if (length(hit))
            return(leaves$expr[hit[1L]])
    }
    # Tolerate a writer that nested a column somewhere we did not predict:
    # match on the final path component alone.
    tail_of <- sub("^.*\\.", "", leaves$path)
    for (p in paths) {
        hit <- which(tail_of == sub("^.*\\.", "", p))
        if (length(hit))
            return(leaves$expr[hit[1L]])
    }
    NULL
}

#' Locate the index, m/z and intensity columns of a signal file.
#'
#' Resolution order follows the specification: the m/z and intensity columns
#' come from the array index by CV term, preferring the entry marked
#' `primary`. The entity index column is not in the array index -- the
#' specification fixes its name (`spectrum_index` for spectra) and requires
#' it to be the first column of the layout group.
#'
#' Falls back to plain column names for files that carry no array index,
#' which the specification permits readers to reject but which is cheap to
#' tolerate.
#'
#' @return `list(index=, mz=, intensity=)` of SQL expressions.
#'
#' @noRd
.mzpeak_signal_columns <- function(file, entity_type = "spectrum") {
    leaves <- .mzpeak_leaf_paths(file)
    ai <- .mzpeak_array_index(file, entity_type)

    by_term <- function(term) {
        hit <- which(ai$array_type == term)
        if (!length(hit))
            return(NULL)
        # Several arrays may share a type (different units or precisions);
        # the writer marks the everyday one `primary`.
        prim <- hit[!is.na(ai$buffer_priority[hit]) &
                    ai$buffer_priority[hit] == "primary"]
        .mzpeak_col_expr(leaves, ai$path[if (length(prim)) prim[1L]
                                         else hit[1L]])
    }

    # The layout group, if the writer used one. The specification keeps that
    # node precisely so the layout is visible from the schema.
    prefix <- NA_character_
    top <- unique(sub("\\..*$", "", leaves$path))
    if (any(top %in% .MZPEAK_LAYOUTS))
        prefix <- top[top %in% .MZPEAK_LAYOUTS][1L]
    qualify <- function(nm)
        if (is.na(prefix)) nm else c(paste0(prefix, ".", nm), nm)

    idx_name <- paste0(.mzpeak_token(entity_type), "_index")
    list(index = .mzpeak_col_expr(leaves, qualify(idx_name)),
         # The array index is authoritative; names are the fallback for
         # files that carry none.
         mz = by_term(.MS_MZ_ARRAY) %||% .mzpeak_col_expr(leaves,
                                                          qualify("mz")),
         intensity = by_term(.MS_INTENSITY_ARRAY) %||%
             .mzpeak_col_expr(leaves, qualify("intensity")))
}

#' Validate that an archive is one this package can ingest.
#'
#' Returns a small `list` describing the archive, or stops with a message
#' that names the archive and the specific problem.
#'
#' @noRd
.mzpeak_validate <- function(dir) {
    idx <- .mzpeak_read_index(dir)
    meta <- .mzpeak_member_path(dir, idx, "spectrum", "metadata")
    if (is.na(meta))
        stop("Archive '", dir, "' has no spectrum metadata table.",
             call. = FALSE)
    profile <- .mzpeak_member_path(dir, idx, "spectrum", "data_arrays")
    centroid <- .mzpeak_member_path(dir, idx, "spectrum", "peaks")
    if (is.na(profile) && is.na(centroid))
        stop("Archive '", dir, "' has neither spectrum signal data nor ",
             "peak data.", call. = FALSE)

    layout <- NA_character_
    for (sig in c(profile, centroid)) {
        if (is.na(sig)) next
        l <- .mzpeak_layout(sig)
        if (identical(l, "chunk"))
            stop("Archive '", dir, "' stores signal in the chunked layout, ",
                 "which MsBackendParquet cannot read yet. Only the point ",
                 "layout is supported.", call. = FALSE)
        if (is.na(layout)) layout <- l
    }
    if (is.na(layout))
        stop("Could not determine the signal layout of archive '", dir, "'.",
             call. = FALSE)

    list(dir = dir,
         index = idx,
         metadata = meta,
         scans = .mzpeak_member_path(dir, idx, "spectrum", "scans"),
         precursors = .mzpeak_member_path(dir, idx, "spectrum", "precursors"),
         selected_ions = .mzpeak_member_path(dir, idx, "spectrum",
                                             "selected_ions"),
         profile = profile,
         centroid = centroid,
         layout = layout)
}

#' The archive's own run identifier, or `NA_character_`.
#'
#' @noRd
.mzpeak_run_id <- function(idx) {
    id <- idx$metadata$run$id
    if (length(id) != 1L || is.na(id) || !nzchar(id)) {
        return(NA_character_)
    }
    as.character(id)
}

#' Union of an archive's column mappings.
#'
#' Each entry ties a Parquet column to a controlled-vocabulary term, e.g.
#' `ms_level` to `MS:1000511`. We keep these so a later reader (or an export
#' back to mzPeak) can recover what a column means without guessing from its
#' name.
#'
#' @return `data.frame` with `file`, `path`, `name`, `accession`, `unit`.
#'
#' @noRd
.mzpeak_column_mapping <- function(idx) {
    f <- idx$files
    cm <- f$column_mapping
    empty <- data.frame(file = character(), path = character(),
                        name = character(), accession = character(),
                        unit = character(), stringsAsFactors = FALSE)
    if (is.null(cm))
        return(empty)
    out <- lapply(seq_len(nrow(f)), function(i) {
        m <- cm[[i]]
        if (is.null(m) || !NROW(m))
            return(NULL)
        if (!is.data.frame(m))
            m <- as.data.frame(m, stringsAsFactors = FALSE)
        col <- function(nm) if (nm %in% colnames(m))
            as.character(m[[nm]]) else rep(NA_character_, nrow(m))
        data.frame(file = as.character(f$name[i]),
                   path = col("path"), name = col("name"),
                   accession = col("accession"), unit = col("unit"),
                   stringsAsFactors = FALSE)
    })
    out <- do.call(rbind, out)
    if (is.null(out)) empty else out
}

#' Unpack a `.mzpeak` ZIP into `dest`, returning the archive directory.
#'
#' mzPeak ZIPs store their members uncompressed, so this is a copy rather
#' than a decompression. Archives that are already directories are never
#' touched -- see `.ingest_archive()`.
#'
#' @noRd
.mzpeak_unpack <- function(zipfile, dest) {
    if (!dir.exists(dest))
        dir.create(dest, recursive = TRUE)
    utils::unzip(zipfile, exdir = dest)
    # A ZIP may wrap its members in a single top-level directory, or store
    # them at the root. Accept both.
    if (file.exists(.mzpeak_index_path(dest)))
        return(dest)
    subs <- list.dirs(dest, recursive = FALSE)
    hit <- subs[vapply(subs, .mzpeak_is_archive, logical(1))]
    if (length(hit) == 1L)
        return(hit)
    stop("No ", .MZPEAK_INDEX_FILE, " found after unpacking '", zipfile,
         "'.", call. = FALSE)
}
