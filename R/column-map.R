# Translating between mzPeak column names and `Spectra` spectra variables.
#
# Both dataset kinds keep mzPeak's own column names on disk:
#
#   * an mzPeak-backed dataset's derived index stores `ms_level`, `time`,
#     `selected_ion_mz`, ... because that is the vocabulary of the archives
#     it indexes;
#   * a natively converted dataset's `spectra/` files are *written* in the
#     same vocabulary by `.spectra_df_to_mzpeak()` (see below), so the two
#     kinds are byte-for-byte comparable and exporting either one back to an
#     archive is mechanical rather than a reconstruction.
#
# `Spectra` names (`msLevel`, `rtime`, `precursorMz`, ...) are produced by a
# single SQL view over whichever files a dataset has. Putting the translation
# in one view has a practical payoff beyond tidiness: every unit and encoding
# difference between the two models lives here and nowhere else. In particular
#
#   mzPeak `time` is in MINUTES (the specification says MUST);
#   `Spectra` `rtime` is in SECONDS.
#
# That factor of 60 is the single most likely interoperability bug in this
# whole exercise, and it is one line in each direction below.

# Mapping from a Spectra variable to the SQL that produces it (the read
# side).
#
#   spectra  - the name the R user sees
#   expr     - SQL over the on-disk (mzPeak-named) columns
#   requires - columns the expression needs; when any is absent from a
#              dataset, the view emits a typed NULL instead
#   type     - SQL type used for that NULL, so the column still has the type
#              `Spectra` expects
.MZPEAK_TO_SPECTRA <- local({
    r <- function(spectra, expr, requires, type)
        data.frame(spectra = spectra, expr = expr,
                   requires = I(list(requires)), type = type,
                   stringsAsFactors = FALSE)
    do.call(rbind, list(
        r("msLevel", 'CAST("ms_level" AS INTEGER)', "ms_level", "INTEGER"),
        # Minutes -> seconds. See the note above.
        r("rtime", '"time" * 60.0', "time", "DOUBLE"),
        # mzPeak: 1 positive, -1 negative. Spectra follows mzR: 1 positive,
        # 0 negative, NA unknown.
        r("polarity",
          'CASE "scan_polarity" WHEN 1 THEN 1 WHEN -1 THEN 0 END',
          "scan_polarity", "INTEGER"),
        # A CURIE, not a flag: MS:1000127 centroid, MS:1000128 profile.
        r("centroided",
          paste0('CASE "spectrum_representation" ',
                 "WHEN 'MS:1000127' THEN TRUE ",
                 "WHEN 'MS:1000128' THEN FALSE END"),
          "spectrum_representation", "BOOLEAN"),
        r("spectrumId", 'CAST("id" AS VARCHAR)', "id", "VARCHAR"),
        # mzPeak addresses spectra by their 0-based `index`; there is no
        # separate instrument scan number in the model, so an mzPeak-backed
        # dataset derives `acquisitionNum` from that index. A natively
        # converted dataset keeps the real scan number in `acquisition_num_`
        # (mzPeak has no column for it), so prefer that when present.
        r("acquisitionNum", 'CAST("acquisition_num_" AS INTEGER)',
          "acquisition_num_", "INTEGER"),
        r("acquisitionNum", 'CAST("spectrum_index" AS INTEGER)',
          "spectrum_index", "INTEGER"),
        # `spectrum_index` is the dataset's own unique 0-based key, so it is
        # only the right answer for `scanIndex` when the run is a single
        # source file -- which an mzPeak archive always is. A natively
        # converted dataset can span several files, so it keeps each
        # spectrum's position within its file in `scan_index`.
        r("scanIndex", 'CAST("scan_index" AS INTEGER)',
          "scan_index", "INTEGER"),
        r("scanIndex", 'CAST("spectrum_index" AS INTEGER)',
          "spectrum_index", "INTEGER"),
        r("precScanNum", 'CAST("precursor_index" AS INTEGER)',
          "precursor_index", "INTEGER"),
        r("precursorMz", '"selected_ion_mz"', "selected_ion_mz", "DOUBLE"),
        r("precursorCharge", 'CAST("charge_state" AS INTEGER)',
          "charge_state", "INTEGER"),
        # mzPeak's selected-ion intensity is `peak_intensity` (MS:1000042).
        r("precursorIntensity", '"peak_intensity"',
          "peak_intensity", "DOUBLE"),
        r("collisionEnergy", '"collision_energy"', "collision_energy",
          "DOUBLE"),
        # mzPeak stores the window as target plus offsets; Spectra wants
        # absolute bounds.
        r("isolationWindowTargetMz", '"isolation_window_target"',
          "isolation_window_target", "DOUBLE"),
        r("isolationWindowLowerMz",
          '"isolation_window_target" - "isolation_window_lower_offset"',
          c("isolation_window_target", "isolation_window_lower_offset"),
          "DOUBLE"),
        r("isolationWindowUpperMz",
          '"isolation_window_target" + "isolation_window_upper_offset"',
          c("isolation_window_target", "isolation_window_upper_offset"),
          "DOUBLE"),
        # Not core Spectra variables, but every other backend surfaces them
        # and downstream code (including SpectraQL) looks for them.
        r("totIonCurrent", '"total_ion_current"', "total_ion_current",
          "DOUBLE"),
        r("basePeakMz", '"base_peak_mz"', "base_peak_mz", "DOUBLE"),
        r("basePeakIntensity", '"base_peak_intensity"',
          "base_peak_intensity", "DOUBLE"),
        r("lowMz", '"lowest_observed_mz"', "lowest_observed_mz", "DOUBLE"),
        r("highMz", '"highest_observed_mz"', "highest_observed_mz", "DOUBLE"),
        # Whichever representation this spectrum actually stores. An archive
        # holding only profile data has no `number_of_peaks` column at all,
        # so the three candidates are tried in order and the first whose
        # inputs exist wins.
        r("peaksCount",
          'CAST(COALESCE("number_of_peaks", "number_of_data_points") AS INTEGER)',
          c("number_of_peaks", "number_of_data_points"), "INTEGER"),
        r("peaksCount", 'CAST("number_of_peaks" AS INTEGER)',
          "number_of_peaks", "INTEGER"),
        r("peaksCount", 'CAST("number_of_data_points" AS INTEGER)',
          "number_of_data_points", "INTEGER"),
        r("injectionTime", '"ion_injection_time"', "ion_injection_time",
          "DOUBLE"),
        r("filterString", 'CAST("filter_string" AS VARCHAR)',
          "filter_string", "VARCHAR"),
        r("scanWindowLowerLimit", '"scan_window_lower_limit"',
          "scan_window_lower_limit", "DOUBLE"),
        r("scanWindowUpperLimit", '"scan_window_upper_limit"',
          "scan_window_upper_limit", "DOUBLE"),
        r("instrumentConfigurationId",
          'CAST("instrument_configuration_id" AS INTEGER)',
          "instrument_configuration_id", "INTEGER"),
        r("spectrumType", 'CAST("spectrum_type" AS VARCHAR)',
          "spectrum_type", "VARCHAR")))
})

# Encoding a `Spectra`-named metadata `data.frame` into mzPeak's on-disk
# vocabulary -- the inverse of the read view, for the columns a natively
# converted dataset can carry.
#
#   spectra - the incoming Spectra variable
#   mzpeak  - the on-disk column name
#   encode  - NULL for a pure rename, or a function(x) returning the encoded
#             column
#
# The isolation window (absolute bounds -> target + offsets) needs two input
# columns at once and is handled directly in `.spectra_df_to_mzpeak()`.
.SPECTRA_TO_MZPEAK <- local({
    # TRUE centroid MS:1000127, FALSE profile MS:1000128, NA -> NA.
    curie_representation <- function(x) {
        out <- rep(NA_character_, length(x))
        out[which(x)] <- "MS:1000127"
        out[which(!x)] <- "MS:1000128"
        out
    }
    # Spectra 1 positive / 0 negative -> mzPeak 1 / -1, NA -> NA.
    scan_polarity <- function(x) {
        out <- rep(NA_integer_, length(x))
        out[which(x == 1L)] <- 1L
        out[which(x == 0L)] <- -1L
        out
    }
    m <- function(spectra, mzpeak, encode = NULL)
        list(spectra = spectra, mzpeak = mzpeak, encode = encode)
    list(
        m("msLevel", "ms_level"),
        m("rtime", "time", function(x) x / 60),
        m("polarity", "scan_polarity", scan_polarity),
        m("centroided", "spectrum_representation", curie_representation),
        m("spectrumId", "id"),
        m("scanIndex", "scan_index"),
        m("acquisitionNum", "acquisition_num_"),
        m("precScanNum", "precursor_index"),
        m("precursorMz", "selected_ion_mz"),
        m("precursorCharge", "charge_state"),
        m("precursorIntensity", "peak_intensity"),
        m("collisionEnergy", "collision_energy"),
        m("isolationWindowTargetMz", "isolation_window_target"),
        m("totIonCurrent", "total_ion_current"),
        m("basePeakMz", "base_peak_mz"),
        m("basePeakIntensity", "base_peak_intensity"),
        m("lowMz", "lowest_observed_mz"),
        m("highMz", "highest_observed_mz"),
        m("peaksCount", "number_of_data_points"),
        m("injectionTime", "ion_injection_time"),
        m("filterString", "filter_string"),
        m("scanWindowLowerLimit", "scan_window_lower_limit"),
        m("scanWindowUpperLimit", "scan_window_upper_limit"),
        m("instrumentConfigurationId", "instrument_configuration_id"),
        m("spectrumType", "spectrum_type"),
        m("dataOrigin", "data_origin"))
})

#' Encode a `Spectra`-named metadata `data.frame` into mzPeak's on-disk
#' column vocabulary and value encodings.
#'
#' Renames and re-encodes every mapped column, turns the absolute
#' isolation-window bounds back into target + offsets, drops `dataStorage`
#' (the read view supplies it as the dataset path), and sets `spectrum_index`
#' to the dataset's own 0-based key (`spectrum_id_ - 1`). The incoming
#' `scanIndex`, which is a spectrum's position within its *source file* and
#' so repeats across files, is kept separately as `scan_index`. `mz`,
#' `intensity`, `spectrum_id_` and any column the map does not mention (a
#' user-defined spectra variable) pass through untouched.
#'
#' @param df `data.frame` of spectra metadata with `Spectra` column names,
#'     already carrying `spectrum_id_`.
#'
#' @return `data.frame` with mzPeak column names.
#'
#' @noRd
.spectra_df_to_mzpeak <- function(df) {
    df <- as.data.frame(df, stringsAsFactors = FALSE)

    # Isolation window: Spectra carries absolute lower/upper m/z, mzPeak the
    # target plus offsets. Do it while the target still has its Spectra name.
    tgt <- df[["isolationWindowTargetMz"]]
    if (!is.null(tgt)) {
        if (!is.null(df[["isolationWindowLowerMz"]]))
            df[["isolation_window_lower_offset"]] <-
                tgt - df[["isolationWindowLowerMz"]]
        if (!is.null(df[["isolationWindowUpperMz"]]))
            df[["isolation_window_upper_offset"]] <-
                df[["isolationWindowUpperMz"]] - tgt
    }
    # Absolute bounds cannot be represented without the target; drop them
    # either way so they never collide with the view's own output column.
    df[["isolationWindowLowerMz"]] <- NULL
    df[["isolationWindowUpperMz"]] <- NULL

    # The read view injects `dataStorage` as the dataset path.
    df[["dataStorage"]] <- NULL

    for (e in .SPECTRA_TO_MZPEAK) {
        v <- df[[e$spectra]]
        if (is.null(v)) next
        if (!is.null(e$encode)) v <- e$encode(v)
        df[[e$spectra]] <- NULL
        df[[e$mzpeak]] <- v
    }

    # mzPeak's `spectrum_index` MUST be the run's unique, monotonic 0-based
    # key. A native mzStack dataset is one run, so that is exactly
    # `spectrum_id_ - 1` -- never a per-file `scanIndex`, which may repeat.
    if (!is.null(df[["spectrum_id_"]]))
        df[["spectrum_index"]] <- as.integer(df[["spectrum_id_"]]) - 1L

    df
}

#' Translate `Spectra` spectra-variable names to their mzPeak on-disk column
#' names, for callers that pass column names rather than data (Hive
#' partitioning keys). Names the map does not mention are returned unchanged,
#' so the function is idempotent.
#'
#' @noRd
.encode_var_names <- function(x) {
    if (!length(x)) return(x)
    to <- vapply(.SPECTRA_TO_MZPEAK, `[[`, character(1), "mzpeak")
    from <- vapply(.SPECTRA_TO_MZPEAK, `[[`, character(1), "spectra")
    hit <- match(x, from)
    ifelse(is.na(hit), x, to[hit])
}

# Columns carried through the view untouched. `spectrum_id_` is the key the
# whole backend addresses rows by; the rest are provenance, and the `n_*`
# counts are how a caller learns that a spectrum had more than one scan or
# selected ion and that the flattened index therefore shows only the first.
.INDEX_PASSTHROUGH <- c("spectrum_id_", "run_id", "spectrum_index",
                        "n_scans", "n_selected_ions", "n_precursors")

# mzPeak source columns consumed by a read expression above; the generic
# pass-through must not re-expose them (we do not want both `time` and
# `rtime`). `data_origin` becomes `dataOrigin` and is handled explicitly.
.MZPEAK_CONSUMED <- unique(c(unlist(.MZPEAK_TO_SPECTRA$requires),
                            "data_origin"))

#' SQL for the view that presents a dataset's files using `Spectra` names.
#'
#' @param available `character` with the column names present on disk.
#'
#' @param source `character(1)` SQL expression naming the relation to read.
#'
#' @param dataStorage `character(1)` dataset path, reported as the
#'     `dataStorage` spectra variable.
#'
#' @return `character(1)` a complete `SELECT`.
#'
#' @noRd
.view_select_sql <- function(available, source, dataStorage) {
    con <- .duckdb_con()
    sel <- character()
    emitted <- character()

    for (nm in intersect(.INDEX_PASSTHROUGH, available)) {
        sel <- c(sel, .quote_ident(nm))
        emitted <- c(emitted, nm)
    }

    # A variable may have several candidate expressions; the first whose
    # inputs are all present wins, and a variable with none still appears,
    # as a typed NULL.
    for (nm in unique(.MZPEAK_TO_SPECTRA$spectra)) {
        cand <- .MZPEAK_TO_SPECTRA[.MZPEAK_TO_SPECTRA$spectra == nm, ]
        ok <- which(vapply(cand$requires, function(need)
            all(need %in% available), logical(1)))
        expr <- if (length(ok)) cand$expr[ok[1L]]
                else paste0("CAST(NULL AS ", cand$type[1L], ")")
        sel <- c(sel, paste0(expr, " AS ", .quote_ident(nm)))
        emitted <- c(emitted, nm)
    }

    # `dataOrigin` identifies the run a spectrum came from and `dataStorage`
    # the dataset holding it -- the same contract as every other backend.
    origin <- if ("data_origin" %in% available) '"data_origin"'
              else DBI::dbQuoteString(con, NA_character_)
    sel <- c(sel, paste0(origin, " AS ", .quote_ident("dataOrigin")),
             paste0(DBI::dbQuoteString(con, dataStorage), " AS ",
                    .quote_ident("dataStorage")))
    emitted <- c(emitted, "dataOrigin", "dataStorage")

    # Anything the dataset carries that the map neither consumes as a source
    # nor already emits passes through unchanged: a native dataset's peak
    # list columns (`mz`, `intensity`), package extension columns
    # (`acquisition_num_`), and any user-defined spectra variable.
    for (nm in setdiff(available, c(.MZPEAK_CONSUMED, emitted)))
        sel <- c(sel, .quote_ident(nm))

    paste0("SELECT ", paste(sel, collapse = ", "), " FROM ", source)
}

#' Spectra variables the view exposes, given the columns available on disk.
#'
#' Columns whose source is missing are still exposed, as all-`NA` of the
#' right type: a `Spectra` object is easier to work with when its core
#' variables are always present.
#'
#' @noRd
.view_variables <- function(available) {
    emitted <- c(intersect(.INDEX_PASSTHROUGH, available),
                 unique(.MZPEAK_TO_SPECTRA$spectra), "dataOrigin",
                 "dataStorage")
    c(emitted, setdiff(available, c(.MZPEAK_CONSUMED, emitted)))
}
