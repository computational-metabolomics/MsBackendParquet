## A natively converted dataset stores mzPeak's column vocabulary and value
## encodings on disk, exactly like an mzPeak-backed one, and the translating
## view hands `Spectra` its own names and units back. These tests pin the
## on-disk shape and the round trip.

.rich_native_backend <- function() {
    sd <- S4Vectors::DataFrame(
        msLevel = c(1L, 2L, 2L),
        rtime = c(60, 120, 180),                 # seconds
        polarity = c(1L, 1L, 0L),                # Spectra: 1 pos, 0 neg
        centroided = c(FALSE, TRUE, TRUE),
        precursorMz = c(NA_real_, 500.25, 700.5),
        precursorCharge = c(NA_integer_, 2L, 3L),
        precursorIntensity = c(NA_real_, 1e5, 2e5),
        collisionEnergy = c(NA_real_, 27, 30),
        isolationWindowTargetMz = c(NA_real_, 500.5, 700.5),
        isolationWindowLowerMz = c(NA_real_, 500.0, 700.0),
        isolationWindowUpperMz = c(NA_real_, 501.5, 701.0),
        totIonCurrent = c(1000, 2000, 3000),
        peaksCount = c(2L, 2L, 2L),
        dataOrigin = c("a.mzML", "a.mzML", "b.mzML"),
        flag = c("x", "y", "z"))
    sd$mz <- IRanges::NumericList(c(100, 110), c(200, 210), c(300, 310),
                                  compress = FALSE)
    sd$intensity <- IRanges::NumericList(c(1, 2), c(3, 4), c(5, 6),
                                         compress = FALSE)
    path <- tempfile()
    be <- backendInitialize(MsBackendParquet(), path = path, data = sd)
    list(be = be, path = path, sd = sd)
}

.raw_column_names <- function(path) {
    names(arrow::open_dataset(.spectra_path(path)))
}

## Scalar (non-peak) columns of a single-file native dataset, id-ordered.
.read_raw_spectra <- function(path) {
    fl <- list.files(.spectra_path(path), pattern = "\\.parquet$",
                     recursive = TRUE, full.names = TRUE)
    stopifnot(length(fl) == 1L)
    raw <- as.data.frame(arrow::read_parquet(fl))
    raw$mz <- NULL
    raw$intensity <- NULL
    raw[order(raw$spectrum_id_), , drop = FALSE]
}

test_that("native files carry mzPeak column names, not Spectra names", {
    d <- .rich_native_backend()
    nm <- .raw_column_names(d$path)

    expect_true(all(c("ms_level", "time", "scan_polarity",
                      "spectrum_representation", "selected_ion_mz",
                      "charge_state", "peak_intensity", "collision_energy",
                      "isolation_window_target", "isolation_window_lower_offset",
                      "isolation_window_upper_offset", "total_ion_current",
                      "data_origin", "spectrum_index", "number_of_data_points",
                      "spectrum_id_", "flag", "mz", "intensity") %in% nm),
                info = paste(sort(nm), collapse = ", "))
    expect_false(any(c("msLevel", "rtime", "polarity", "centroided",
                       "precursorMz", "isolationWindowLowerMz",
                       "isolationWindowUpperMz", "dataOrigin", "dataStorage") %in%
                     nm))
})

test_that("native files store mzPeak value encodings on disk", {
    d <- .rich_native_backend()
    raw <- .read_raw_spectra(d$path)

    expect_equal(raw$time, c(60, 120, 180) / 60)            # minutes
    expect_equal(raw$scan_polarity, c(1L, 1L, -1L))         # +/- 1
    expect_equal(raw$spectrum_representation,
                 c("MS:1000128", "MS:1000127", "MS:1000127"))
    expect_equal(raw$spectrum_index, 0:2)                   # 0-based
    expect_equal(raw$number_of_data_points, c(2L, 2L, 2L))
    ## target + offsets, not absolute bounds
    expect_equal(raw$isolation_window_lower_offset, c(NA, 0.5, 0.5))
    expect_equal(raw$isolation_window_upper_offset, c(NA, 1.0, 0.5))
})

test_that("the view hands Spectra its own names and units back", {
    d <- .rich_native_backend()
    be <- d$be

    got <- spectraData(be, c("msLevel", "rtime", "polarity", "centroided",
                             "precursorMz", "precursorCharge",
                             "precursorIntensity", "collisionEnergy",
                             "isolationWindowTargetMz", "isolationWindowLowerMz",
                             "isolationWindowUpperMz", "totIonCurrent",
                             "peaksCount", "dataOrigin", "flag"))
    expect_equal(as.integer(got$msLevel), c(1L, 2L, 2L))
    expect_equal(got$rtime, c(60, 120, 180))
    expect_equal(as.integer(got$polarity), c(1L, 1L, 0L))
    expect_equal(got$centroided, c(FALSE, TRUE, TRUE))
    expect_equal(got$precursorMz, c(NA, 500.25, 700.5))
    expect_equal(as.integer(got$precursorCharge), c(NA, 2L, 3L))
    expect_equal(got$precursorIntensity, c(NA, 1e5, 2e5))
    expect_equal(got$collisionEnergy, c(NA, 27, 30))
    expect_equal(got$isolationWindowLowerMz, c(NA, 500.0, 700.0))
    expect_equal(got$isolationWindowUpperMz, c(NA, 501.5, 701.0))
    expect_equal(got$totIonCurrent, c(1000, 2000, 3000))
    expect_equal(as.integer(got$peaksCount), c(2L, 2L, 2L))
    expect_equal(got$dataOrigin, c("a.mzML", "a.mzML", "b.mzML"))
    expect_equal(got$flag, c("x", "y", "z"))
})

test_that("dataStorage is the dataset path, supplied by the view", {
    d <- .rich_native_backend()
    expect_equal(unique(spectraData(d$be, "dataStorage")$dataStorage),
                 normalizePath(.path(d$be), mustWork = FALSE))
})

test_that("peaks survive the standardised metadata schema", {
    d <- .rich_native_backend()
    pks <- peaksData(d$be)
    expect_equal(pks[[1L]][, "mz"], c(100, 110))
    expect_equal(pks[[3L]][, "intensity"], c(5, 6))
})

test_that("filters still push down through the translating native view", {
    d <- .rich_native_backend()
    be <- d$be
    expect_equal(length(filterMsLevel(be, 2L)), 2L)
    expect_equal(length(filterRt(be, c(90, 200))), 2L)
    expect_equal(length(filterDataOrigin(be, "b.mzML")), 1L)
    expect_equal(length(filterPrecursorMzRange(be, c(500, 600))), 1L)
})

test_that("spectrum_index is the dataset key; per-file scanIndex is kept", {
    ## A dataset converted from two source files: Spectra's `scanIndex`
    ## restarts at 0 per file, but mzPeak's `spectrum_index` must be the
    ## run's unique 0-based key.
    sd <- S4Vectors::DataFrame(
        msLevel = rep(1L, 6),
        rtime = c(10, 20, 30, 10, 20, 30),
        scanIndex = c(0L, 1L, 2L, 0L, 1L, 2L),
        acquisitionNum = c(11L, 12L, 13L, 11L, 12L, 13L),
        dataOrigin = rep(c("A.mzML", "B.mzML"), each = 3))
    sd$mz <- IRanges::NumericList(as.list(1:6), compress = FALSE)
    sd$intensity <- IRanges::NumericList(as.list(1:6), compress = FALSE)
    path <- tempfile()
    be <- backendInitialize(MsBackendParquet(), path = path, data = sd)

    raw <- .read_raw_spectra(path)
    expect_equal(raw$spectrum_index, 0:5)                   # unique, monotonic
    expect_equal(raw$scan_index, c(0L, 1L, 2L, 0L, 1L, 2L)) # per source file

    got <- spectraData(be, c("scanIndex", "acquisitionNum", "dataOrigin"))
    expect_equal(as.integer(got$scanIndex), c(0L, 1L, 2L, 0L, 1L, 2L))
    expect_equal(as.integer(got$acquisitionNum), c(11L, 12L, 13L, 11L, 12L, 13L))
})

test_that("a native dataset partitioned on a Spectra key writes mzPeak dirs", {
    d0 <- .rich_native_backend()
    path <- tempfile()
    be <- backendInitialize(MsBackendParquet(), path = path, data = d0$sd,
                            partitioning = "dataOrigin")
    ## Hive directory uses the mzPeak column name.
    expect_true(any(grepl("data_origin=",
                          list.dirs(.spectra_path(path), recursive = TRUE))))
    expect_equal(length(filterDataOrigin(be, "a.mzML")), 2L)
})
