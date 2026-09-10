## Reading mzPeak archives: validation, ingest, and querying the dataset
## that results. Fixtures come from helper-mzpeak.R.

.two_run_dataset <- function(root, ...) {
    a <- file.path(root, "QC01")
    b <- file.path(root, "QC02")
    .make_mzpeak_archive(a, n = 6L, run_id = "QC01", mz_offset = 0, ...)
    .make_mzpeak_archive(b, n = 4L, run_id = "QC02", mz_offset = 10000, ...)
    ds <- file.path(root, "ds")
    createMzPeakDataset(c(a, b), path = ds, verbose = FALSE)
    list(ds = ds, a = a, b = b)
}

test_that("an archive is resolved through its index, not by file name", {
    d <- file.path(tempfile(), "run")
    .make_mzpeak_archive(d, n = 3L, run_id = "R")
    v <- .mzpeak_validate(d)

    expect_identical(basename(v$metadata), "spectra_metadata.parquet")
    expect_identical(basename(v$profile), "spectra_data.parquet")
    expect_true(is.na(v$centroid))
    expect_identical(v$layout, "point")
    expect_identical(.mzpeak_run_id(v$index), "R")

    ## The array index in the Parquet footer says which column is which.
    ai <- .mzpeak_array_index(v$profile)
    expect_setequal(ai$array_type, c("MS:1000514", "MS:1000515"))
    cols <- .mzpeak_signal_columns(v$profile)
    expect_match(cols$mz, "mz")
    expect_match(cols$index, "spectrum_index")
})

test_that("signal columns resolve without an array index too", {
    d <- file.path(tempfile(), "run")
    .make_mzpeak_archive(d, n = 2L, run_id = "R", array_index = FALSE)
    v <- .mzpeak_validate(d)
    expect_identical(v$layout, "point")
    cols <- .mzpeak_signal_columns(v$profile)
    expect_match(cols$mz, "mz")
})

test_that("the chunked layout is refused with a message naming the archive", {
    d <- file.path(tempfile(), "chunky")
    .make_mzpeak_archive(d, n = 2L, run_id = "C", layout = "chunk")
    expect_error(.mzpeak_validate(d), "chunked layout")
    expect_error(.mzpeak_validate(d), "chunky")
})

test_that("a directory that is not an archive is refused", {
    d <- tempfile()
    dir.create(d)
    expect_error(.mzpeak_read_index(d), "not an mzPeak archive")
})

test_that("nested facet columns are found wherever the writer put them", {
    d <- file.path(tempfile(), "run")
    .make_mzpeak_archive(d, n = 4L, run_id = "R")
    leaves <- .mzpeak_leaf_paths(
        file.path(d, "spectra_metadata_precursors.parquet"))
    ## Written nested, exactly as the specification shapes them.
    expect_true("isolation_window.isolation_window_target" %in% leaves$path)
    expect_true("activation.collision_energy" %in% leaves$path)
    expect_match(
        .mzpeak_col_expr(leaves, "activation.collision_energy"),
        "struct_extract")
    ## A list<struct> path takes its first element.
    sl <- .mzpeak_leaf_paths(
        file.path(d, "spectra_metadata_scans.parquet"))
    expect_match(
        .mzpeak_col_expr(sl, "scan_windows.scan_window_lower_limit"),
        "[1]", fixed = TRUE)
})

test_that("ids are allocated per run and never renumber an existing one", {
    m <- .manifest_new()
    m <- .manifest_add_run(m, "A", "mzpeak", "/tmp/a", 6L, "point")
    m <- .manifest_add_run(m, "B", "mzpeak", "/tmp/b", 4L, "point")
    r <- .manifest_runs(m)
    expect_identical(r$uid_base, c(1L, 7L))
    expect_identical(.manifest_n_spectra(m), 10L)

    ## Adding a third run leaves the first two alone.
    m2 <- .manifest_add_run(m, "C", "mzpeak", "/tmp/c", 2L, "point")
    expect_identical(.manifest_runs(m2)$uid_base[1:2], c(1L, 7L))

    expect_error(.manifest_add_run(m, "A", "mzpeak", "/tmp/x", 1L, "point"),
                 "already in this dataset")

    ## Ids map back onto the runs and the archive-local indices.
    sp <- .manifest_split_ids(m, c(1L, 3L, 7L, 10L))
    expect_length(sp, 2L)
    expect_identical(sp[[1L]]$local, c(0L, 2L))
    expect_identical(sp[[2L]]$local, c(0L, 3L))
    expect_error(.manifest_split_ids(m, 99L), "outside the dataset")
})

test_that("the manifest round-trips through disk", {
    p <- tempfile()
    dir.create(p)
    m <- .manifest_add_run(.manifest_new(), "A", "mzpeak", "/tmp/a", 3L,
                           "point", "spectra_data.parquet")
    .manifest_write(p, m)
    back <- .manifest_read(p)
    expect_equal(.manifest_runs(back), .manifest_runs(m))
    expect_identical(back$generation, m$generation)
})

test_that("ingest builds a queryable dataset over untouched archives", {
    root <- tempfile()
    dir.create(root)
    d <- .two_run_dataset(root)

    be <- backendInitialize(MsBackendParquet(), path = d$ds)
    expect_s4_class(be, "MsBackendParquet")
    expect_identical(length(be), 10L)
    expect_identical(.dataset_kind(d$ds), "mzpeak")

    sd <- spectraData(be, c("msLevel", "rtime", "dataOrigin"))
    ## Six spectra from QC01 then four from QC02, MS levels alternating.
    expect_identical(as.integer(sd$msLevel),
                     c(1L, 2L, 1L, 2L, 1L, 2L, 1L, 2L, 1L, 2L))
    expect_equal(sd$rtime, c(1:6, 1:4))
    expect_identical(sum(basename(sd$dataOrigin) == "QC01"), 6L)
    expect_identical(sum(basename(sd$dataOrigin) == "QC02"), 4L)
})

test_that("mzPeak minutes become Spectra seconds", {
    root <- tempfile()
    dir.create(root)
    d <- file.path(root, "run")
    ## The fixture writes `time` = (index + 1) / 60 MINUTES.
    .make_mzpeak_archive(d, n = 3L, run_id = "R")
    ds <- file.path(root, "ds")
    createMzPeakDataset(d, path = ds, verbose = FALSE)

    be <- backendInitialize(MsBackendParquet(), path = ds)
    expect_equal(spectraData(be, "rtime")[, 1L], c(1, 2, 3))
})

test_that("archives whose schemas differ still form one dataset", {
    root <- tempfile()
    dir.create(root)
    archives <- .make_drifted_archives(root)
    ds <- file.path(root, "ds")
    createMzPeakDataset(archives, path = ds, verbose = FALSE)

    be <- backendInitialize(MsBackendParquet(), path = ds)
    expect_identical(length(be), 8L)
    ## Both runs are queryable through the same columns even though only one
    ## archive carried each optional column.
    sd <- spectraData(be, c("msLevel", "rtime"))
    expect_identical(nrow(sd), 8L)
    expect_false(anyNA(sd$rtime))
})

test_that("peaks are read from the archives and matched to the right run", {
    root <- tempfile()
    dir.create(root)
    d <- .two_run_dataset(root)
    be <- backendInitialize(MsBackendParquet(), path = d$ds)

    pks <- peaksData(be)
    expect_length(pks, 10L)
    ## Run QC02's peaks carry a +10000 offset, so reading the wrong archive
    ## cannot pass by coincidence.
    expect_equal(pks[[1L]][, "mz"], 100 + 1:4)
    expect_equal(pks[[7L]][, "mz"], 10000 + 100 + 1:4)
    expect_equal(pks[[1L]][, "intensity"], 10 + 1:4)

    expect_equal(as.numeric(mz(be)[[7L]]), 10000 + 100 + 1:4)
    expect_equal(as.numeric(intensity(be)[[7L]]), 10 + 1:4)
    expect_identical(peaksVariables(be), c("mz", "intensity"))
})

test_that("a scattered subset spanning runs comes back in the asked order", {
    root <- tempfile()
    dir.create(root)
    d <- .two_run_dataset(root)
    be <- backendInitialize(MsBackendParquet(), path = d$ds)

    idx <- c(9L, 2L, 7L, 6L)
    sub <- be[idx]
    expect_identical(sub@spectraIds, idx)
    expect_equal(vapply(peaksData(sub), function(m) m[1L, "mz"], numeric(1)),
                 c(10000 + 300 + 1, 200 + 1, 10000 + 100 + 1, 600 + 1))
    expect_equal(spectraData(sub, "rtime")[, 1L], c(3, 2, 1, 6))
})

test_that("filters work across runs", {
    root <- tempfile()
    dir.create(root)
    d <- .two_run_dataset(root)
    be <- backendInitialize(MsBackendParquet(), path = d$ds)

    expect_identical(uniqueMsLevels(be), c(1L, 2L))
    expect_identical(length(filterMsLevel(be, 2L)), 5L)
    ## rtimes are 1..6 (QC01) and 1..4 (QC02).
    expect_identical(length(filterRt(be, c(2.5, 4.5))), 4L)
    ## `dataOrigin` is the archive's canonical path, so compare against a
    ## normalised one rather than whatever the caller happened to type.
    f <- filterDataOrigin(be, normalizePath(d$b))
    expect_identical(length(f), 4L)
    ## Precursor m/z is set on MS2 spectra only, at 400 + index: 401/403/405
    ## in QC01 and 401/403 in QC02, so [400, 402] selects one from each run.
    expect_identical(length(filterPrecursorMzRange(be, c(400, 402))), 2L)
})

test_that("ingest leaves every archive byte-identical", {
    root <- tempfile()
    dir.create(root)
    a <- file.path(root, "QC01")
    .make_mzpeak_archive(a, n = 5L, run_id = "QC01")
    before <- tools::md5sum(list.files(a, recursive = TRUE, full.names = TRUE))

    ds <- file.path(root, "ds")
    createMzPeakDataset(a, path = ds, verbose = FALSE)
    be <- backendInitialize(MsBackendParquet(), path = ds)
    invisible(peaksData(be))
    invisible(spectraData(be))

    after <- tools::md5sum(list.files(a, recursive = TRUE, full.names = TRUE))
    expect_identical(before, after)
})

test_that("the derived index can be deleted and rebuilt", {
    root <- tempfile()
    dir.create(root)
    d <- .two_run_dataset(root)
    be <- backendInitialize(MsBackendParquet(), path = d$ds)
    ref_sd <- spectraData(be, c("msLevel", "rtime", "precursorMz"))
    ref_pk <- peaksData(be)

    unlink(d$ds, recursive = TRUE)
    .invalidate_dataset_cache(d$ds)
    createMzPeakDataset(c(d$a, d$b), path = d$ds, verbose = FALSE)

    be2 <- backendInitialize(MsBackendParquet(), path = d$ds)
    expect_equal(spectraData(be2, c("msLevel", "rtime", "precursorMz")),
                 ref_sd)
    expect_equal(peaksData(be2), ref_pk)
})

test_that("a missing facet leaves its variables NA rather than absent", {
    root <- tempfile()
    dir.create(root)
    d <- file.path(root, "run")
    .make_mzpeak_archive(d, n = 4L, run_id = "R", facets = "scans")
    ds <- file.path(root, "ds")
    createMzPeakDataset(d, path = ds, verbose = FALSE)

    be <- backendInitialize(MsBackendParquet(), path = ds)
    expect_true(all(c("precursorMz", "collisionEnergy") %in%
                    spectraVariables(be)))
    expect_true(all(is.na(spectraData(be, "precursorMz")[, 1L])))
    expect_false(anyNA(spectraData(be, "rtime")[, 1L]))
})

test_that("flattening records how much detail was left in the archive", {
    root <- tempfile()
    dir.create(root)
    d <- file.path(root, "run")
    .make_mzpeak_archive(d, n = 4L, run_id = "R", scans_per_spectrum = 3L)
    ds <- file.path(root, "ds")
    createMzPeakDataset(d, path = ds, verbose = FALSE)

    be <- backendInitialize(MsBackendParquet(), path = ds)
    expect_true("n_scans" %in% spectraVariables(be))
    expect_identical(unique(as.integer(spectraData(be, "n_scans")[, 1L])), 3L)
})

test_that("representation selects between profile and centroid signal", {
    root <- tempfile()
    dir.create(root)
    d <- file.path(root, "run")
    .make_mzpeak_archive(d, n = 4L, run_id = "R", centroid = TRUE)
    ds <- file.path(root, "ds")
    createMzPeakDataset(d, path = ds, verbose = FALSE)

    be <- backendInitialize(MsBackendParquet(), path = ds,
                            representation = "centroid")
    expect_length(peaksData(be), 4L)
    expect_true(all(spectraData(be, "centroided")[, 1L]))

    ## The archive holds no profile data, so asking for it must fail loudly
    ## rather than silently return centroids.
    bp <- backendInitialize(MsBackendParquet(), path = ds,
                            representation = "profile")
    expect_error(peaksData(bp), "no profile data")
})

test_that("CV terms from the archives are recorded for the dataset", {
    root <- tempfile()
    dir.create(root)
    d <- .two_run_dataset(root)
    cm <- jsonlite::fromJSON(file.path(d$ds, "index", "column_map.json"))
    expect_true("ms_level" %in% cm$path)
    expect_identical(cm$accession[cm$path == "ms_level"][1L], "MS:1000511")
})

test_that("adding archives extends an existing dataset", {
    root <- tempfile()
    dir.create(root)
    a <- file.path(root, "A")
    b <- file.path(root, "B")
    .make_mzpeak_archive(a, n = 3L, run_id = "A")
    .make_mzpeak_archive(b, n = 2L, run_id = "B", mz_offset = 10000)
    ds <- file.path(root, "ds")
    createMzPeakDataset(a, path = ds, verbose = FALSE)
    expect_identical(length(backendInitialize(MsBackendParquet(),
                                              path = ds)), 3L)

    addMzPeakArchives(ds, b, verbose = FALSE)
    be <- backendInitialize(MsBackendParquet(), path = ds)
    expect_identical(length(be), 5L)
    ## The originally-ingested spectra keep their ids and their data.
    expect_equal(peaksData(be)[[1L]][, "mz"], 100 + 1:4)
    expect_equal(peaksData(be)[[4L]][, "mz"], 10000 + 100 + 1:4)

    expect_error(createMzPeakDataset(a, path = ds, verbose = FALSE),
                 "already exists")
})
