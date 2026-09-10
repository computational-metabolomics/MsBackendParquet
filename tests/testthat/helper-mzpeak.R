# Synthetic mzPeak archives for testing.
#
# There is a public corpus of real archives (linked from the specification's
# Tools page), but CI cannot enumerate it and it is far too large for a test
# suite. So the primary fixture is generated here: a small, spec-shaped
# archive we can bend into whatever awkward shape a test needs -- a missing
# facet, a spectrum with two scans, an archive with extra promoted columns,
# or the chunked layout we are supposed to reject.
#
# The shapes follow docs/schemas/spectra.md of the specification:
#   * signal columns live under a top-level `point` group;
#   * the metadata table's key is `index`, side tables reference it through
#     `source_index`;
#   * `time` is in MINUTES;
#   * `spectrum_representation` is a CURIE, not a flag.

#' Build a synthetic mzPeak archive under `dir`.
#'
#' @param dir directory to create the archive in.
#'
#' @param n number of spectra. Odd positions are MS1, even are MS2.
#'
#' @param run_id value written to `metadata.run.id`; `NA` omits the run
#'     object entirely, so the id has to be taken from the directory name.
#'
#' @param centroid when `TRUE` the signal goes to `spectra_peaks.parquet`
#'     (data kind `peaks`) instead of `spectra_data.parquet`.
#'
#' @param facets which side tables to write.
#'
#' @param scans_per_spectrum spectra get this many scan rows; > 1 exercises
#'     the flattening path.
#'
#' @param extra_cols named `list` of extra columns for the metadata table,
#'     standing in for parameters a writer chose to promote.
#'
#' @param layout `"point"` or `"chunk"`; `"chunk"` writes a file the reader
#'     is expected to refuse.
#'
#' @param array_index whether to write the array index into the Parquet
#'     footer. `FALSE` exercises the name-based fallback.
#'
#' @return `dir`, invisibly.
#'
#' @noRd
.make_mzpeak_archive <- function(dir, n = 6L, run_id = "R1",
                                 centroid = FALSE,
                                 facets = c("scans", "precursors",
                                            "selected_ions"),
                                 scans_per_spectrum = 1L,
                                 extra_cols = NULL,
                                 layout = c("point", "chunk"),
                                 array_index = TRUE,
                                 npeaks = 4L,
                                 mz_offset = 0) {
    layout <- match.arg(layout)
    if (!dir.exists(dir))
        dir.create(dir, recursive = TRUE)
    idx <- seq_len(n) - 1L                      # mzPeak indices are 0-based
    ms_level <- ifelse(idx %% 2L == 0L, 1L, 2L)
    is_ms2 <- ms_level == 2L

    ## ---- metadata table -------------------------------------------------
    meta <- data.frame(
        index = idx,
        id = sprintf("controllerType=0 controllerNumber=1 scan=%d", idx + 1L),
        ms_level = ms_level,
        time = (idx + 1L) / 60,                 # MINUTES
        scan_polarity = rep(1L, n),
        spectrum_representation = rep(
            if (centroid) "MS:1000127" else "MS:1000128", n),
        spectrum_type = ifelse(is_ms2, "MS:1000580", "MS:1000579"),
        lowest_observed_mz = 100 + idx,
        highest_observed_mz = 1000 + idx,
        base_peak_mz = 500 + idx,
        base_peak_intensity = 1000 + idx * 10,
        total_ion_current = 5000 + idx * 100,
        stringsAsFactors = FALSE)
    meta[[if (centroid) "number_of_peaks" else "number_of_data_points"]] <-
        rep(as.integer(npeaks), n)
    for (nm in names(extra_cols))
        meta[[nm]] <- extra_cols[[nm]]
    arrow::write_parquet(meta, file.path(dir, "spectra_metadata.parquet"))

    files <- list(list(name = "spectra_metadata.parquet",
                       entity_type = "spectrum", data_kind = "metadata",
                       column_mapping = data.frame(
                           name = c("ms level", "total ion current"),
                           path = c("ms_level", "total_ion_current"),
                           accession = c("MS:1000511", "MS:1000285"),
                           unit = c(NA, "MS:1000131"),
                           stringsAsFactors = FALSE)))

    ## ---- scans ----------------------------------------------------------
    if ("scans" %in% facets) {
        src <- rep(idx, each = scans_per_spectrum)
        k <- seq_along(src) - 1L
        scans <- data.frame(
            source_index = src,
            scan_index = k,
            scan_start_time = (src + 1L) / 60,
            filter_string = sprintf("FTMS + p ESI Full ms%s",
                                    ifelse(ms_level[src + 1L] == 2L, "2", "")),
            ion_injection_time = 10 + k,
            instrument_configuration_id = rep(0L, length(src)),
            stringsAsFactors = FALSE)
        # Nested list<struct>, exactly as the specification shapes it.
        scans$scan_windows <- lapply(seq_along(src), function(i)
            data.frame(scan_window_lower_limit = 200,
                       scan_window_upper_limit = 2000))
        arrow::write_parquet(
            scans, file.path(dir, "spectra_metadata_scans.parquet"))
        files <- c(files, list(list(
            name = "spectra_metadata_scans.parquet",
            entity_type = "spectrum", data_kind = "scans")))
    }

    ## ---- precursors and selected ions (MS2 only) ------------------------
    if ("precursors" %in% facets && any(is_ms2)) {
        p <- data.frame(source_index = idx[is_ms2],
                        precursor_index = idx[is_ms2] - 1L,
                        stringsAsFactors = FALSE)
        p$isolation_window <- data.frame(
            isolation_window_target = 400 + idx[is_ms2],
            isolation_window_lower_offset = rep(0.5, sum(is_ms2)),
            isolation_window_upper_offset = rep(0.75, sum(is_ms2)))
        p$activation <- data.frame(
            collision_energy = 25 + idx[is_ms2])
        arrow::write_parquet(
            p, file.path(dir, "spectra_metadata_precursors.parquet"))
        files <- c(files, list(list(
            name = "spectra_metadata_precursors.parquet",
            entity_type = "spectrum", data_kind = "precursors")))
    }
    if ("selected_ions" %in% facets && any(is_ms2)) {
        si <- data.frame(source_index = idx[is_ms2],
                         precursor_index = idx[is_ms2] - 1L,
                         selected_ion_mz = 400 + idx[is_ms2],
                         charge_state = rep(2L, sum(is_ms2)),
                         peak_intensity = 900 + idx[is_ms2],
                         stringsAsFactors = FALSE)
        arrow::write_parquet(
            si, file.path(dir, "spectra_metadata_selected_ions.parquet"))
        files <- c(files, list(list(
            name = "spectra_metadata_selected_ions.parquet",
            entity_type = "spectrum", data_kind = "selected_ions")))
    }

    ## ---- signal ---------------------------------------------------------
    sig_name <- if (centroid) "spectra_peaks.parquet" else
                    "spectra_data.parquet"
    sig_kind <- if (centroid) "peaks" else "data_arrays"
    si_col <- rep(idx, each = npeaks)
    # `mz_offset` lets a test give each run distinct values, so a query that
    # reads the wrong archive cannot pass by coincidence.
    mz <- 100 * (si_col + 1L) + seq_len(npeaks) + mz_offset
    inten <- 10 * (si_col + 1L) + seq_len(npeaks)

    if (layout == "point") {
        tbl <- arrow::arrow_table(point = arrow::StructArray$create(
            spectrum_index = si_col, mz = as.numeric(mz),
            intensity = as.numeric(inten)))
        if (array_index)
            tbl$metadata <- list(spectrum_array_index = jsonlite::toJSON(
                list(prefix = "point", entries = data.frame(
                    context = c("spectrum", "spectrum"),
                    path = c("point.mz", "point.intensity"),
                    data_type = c("MS:1000523", "MS:1000521"),
                    array_type = c("MS:1000514", "MS:1000515"),
                    array_name = c("m/z array", "intensity array"),
                    unit = c("MS:1000040", "MS:1000131"),
                    buffer_format = c("point", "point"),
                    buffer_priority = c("primary", "primary"),
                    stringsAsFactors = FALSE)),
                auto_unbox = TRUE))
    } else {
        tbl <- arrow::arrow_table(chunk = arrow::StructArray$create(
            spectrum_index = idx,
            mz_chunk_start = as.numeric(100 * (idx + 1L)),
            mz_chunk_end = as.numeric(100 * (idx + 1L) + npeaks),
            chunk_encoding = rep("MS:1003089", n)))
        if (array_index)
            tbl$metadata <- list(spectrum_array_index = jsonlite::toJSON(
                list(prefix = "chunk", entries = data.frame(
                    path = "chunk.mz_chunk_values",
                    array_type = "MS:1000514",
                    buffer_format = "chunk_values",
                    stringsAsFactors = FALSE)), auto_unbox = TRUE))
    }
    arrow::write_parquet(tbl, file.path(dir, sig_name))
    files <- c(files, list(list(name = sig_name, entity_type = "spectrum",
                                data_kind = sig_kind)))

    ## ---- index file -----------------------------------------------------
    metadata <- list(
        version = "0.9.0",
        cv_list = data.frame(
            id = c("MS", "UO"),
            full_name = c("PSI-MS", "Units of measurement ontology"),
            uri = c("http://purl.obolibrary.org/obo/ms/psi-ms.obo",
                    "http://purl.obolibrary.org/obo/uo.obo"),
            version = c("4.1.249", "2026-01-16"), stringsAsFactors = FALSE))
    if (!is.na(run_id))
        metadata$run <- list(id = run_id,
                             default_data_processing_id = "dp0",
                             default_instrument_id = 0L,
                             default_source_file_id = "sf0")
    writeLines(jsonlite::toJSON(list(files = files, metadata = metadata),
                                auto_unbox = TRUE, pretty = TRUE),
               file.path(dir, "mzpeak_index.json"))
    invisible(dir)
}

#' Two archives whose metadata tables have different optional columns.
#'
#' Conformant writers promote different parameters to columns, so two valid
#' archives routinely disagree on schema. This is the case that breaks naive
#' implementations.
#'
#' @noRd
.make_drifted_archives <- function(root) {
    a <- file.path(root, "A")
    b <- file.path(root, "B")
    .make_mzpeak_archive(a, n = 4L, run_id = "A",
                         extra_cols = list(opt_vendor_flag = rep(TRUE, 4L)))
    .make_mzpeak_archive(b, n = 4L, run_id = "B",
                         extra_cols = list(opt_other_thing = 1:4))
    c(a, b)
}
