## The mzStack manifest, and the distinction it carries.
##
## Both dataset kinds keep their manifest at `<dataset>/mzStack.json`, so the
## file name says nothing about what the dataset holds. The kind is recorded
## *in* the manifest and everything routes on that. These tests pin that down,
## because getting it wrong sends a natively converted dataset into the mzPeak
## read path looking for archives that do not exist.

.native_dataset <- function(root, partitioning = character()) {
    sd <- S4Vectors::DataFrame(
        msLevel = c(1L, 1L, 2L), rtime = c(1, 2, 3),
        dataOrigin = c("file-a", "file-a", "file-b"))
    sd$mz <- IRanges::NumericList(c(100, 110), c(101, 111), c(102, 112),
                                  compress = FALSE)
    sd$intensity <- IRanges::NumericList(c(10, 20), c(11, 21), c(12, 22),
                                         compress = FALSE)
    path <- file.path(root, "native")
    backendInitialize(MsBackendParquet(), path = path, data = sd,
                      partitioning = partitioning)
}

test_that("a converted dataset is an mzStack dataset of kind native", {
    root <- tempfile()
    dir.create(root)
    be <- .native_dataset(root)
    path <- .path(be)

    expect_true(file.exists(file.path(path, "mzStack.json")))
    ## The sentinel that earlier versions wrote and never read is gone.
    expect_false(file.exists(file.path(path, "MsBackendParquet.json")))
    expect_true(.is_mzstack_dataset(path))
    expect_identical(.dataset_kind(path), "native")

    m <- .manifest_read(path)
    expect_identical(m$format, "mzStack")
    expect_identical(.semver_major(m$version), .semver_major(.MZSTACK_VERSION))
    expect_length(m$runs, 1L)
    expect_identical(.manifest_runs(m)$layout, "list")
    expect_identical(.manifest_n_spectra(m), 3L)
})

test_that("an mzPeak-backed dataset is an mzStack dataset of kind mzpeak", {
    root <- tempfile()
    dir.create(root)
    a <- file.path(root, "QC01")
    .make_mzpeak_archive(a, n = 4L, run_id = "QC01")
    ds <- file.path(root, "ds")
    createMzPeakDataset(a, path = ds, verbose = FALSE)

    expect_true(file.exists(file.path(ds, "mzStack.json")))
    expect_true(.is_mzstack_dataset(ds))
    expect_identical(.dataset_kind(ds), "mzpeak")
})

test_that("each kind reads its peaks from the right place", {
    root <- tempfile()
    dir.create(root)

    ## Native: peaks are list columns beside the metadata. If the kind were
    ## mis-detected this would go looking for mzPeak archives and fail.
    be <- .native_dataset(root)
    expect_equal(peaksData(be)[[1L]][, "mz"], c(100, 110))
    expect_equal(as.numeric(mz(be)[[3L]]), c(102, 112))

    ## mzPeak: peaks come from the archive, one row per data point.
    a <- file.path(root, "QC01")
    .make_mzpeak_archive(a, n = 4L, run_id = "QC01")
    ds <- file.path(root, "ds")
    createMzPeakDataset(a, path = ds, verbose = FALSE)
    be2 <- backendInitialize(MsBackendParquet(), path = ds)
    expect_equal(peaksData(be2)[[1L]][, "mz"], 100 + 1:4)
})

test_that("a dataset without a manifest is refused, not half-opened", {
    root <- tempfile()
    dir.create(root)
    be <- .native_dataset(root)

    ## A directory holding only `spectra/`, as written before mzStack naming.
    legacy <- file.path(root, "legacy")
    dir.create(file.path(legacy, "spectra"), recursive = TRUE)
    file.copy(list.files(.spectra_path(.path(be)), full.names = TRUE,
                         recursive = TRUE),
              file.path(legacy, "spectra"))

    expect_false(.is_mzstack_dataset(legacy))
    expect_error(backendInitialize(MsBackendParquet(), path = legacy),
                 "not an mzStack dataset")
    expect_error(backendInitialize(MsBackendParquet(), path = legacy),
                 "mzStack.json", fixed = TRUE)
    expect_error(backendInitialize(MsBackendParquet(), path = legacy),
                 "re-created")
})

test_that("a manifest of a foreign format or major version is rejected", {
    root <- tempfile()
    dir.create(root)

    other <- file.path(root, "other")
    dir.create(other)
    writeLines(jsonlite::toJSON(
        list(format = "somethingElse", version = "0.1.0", generation = 1L,
             runs = list()), auto_unbox = TRUE),
        file.path(other, "mzStack.json"))
    expect_error(.manifest_read(other), "declares format")

    future <- file.path(root, "future")
    dir.create(future)
    writeLines(jsonlite::toJSON(
        list(format = "mzStack", version = "99.0.0", generation = 1L,
             runs = list()), auto_unbox = TRUE),
        file.path(future, "mzStack.json"))
    expect_error(.manifest_read(future), "mzStack version 99.0.0")
})

test_that("Hive partitioning survives a manifest round trip", {
    root <- tempfile()
    dir.create(root)
    be <- .native_dataset(root, partitioning = "dataOrigin")
    m <- .manifest_read(.path(be))
    ## Recorded under the on-disk (mzPeak) column name.
    expect_identical(.manifest_partitioning(m, "native"), "data_origin")
    ## and the dataset still reads back correctly
    expect_identical(length(be), 3L)
    expect_identical(length(filterDataOrigin(be, "file-a")), 2L)
})

test_that("no partitioning recorded means no partitioning key", {
    root <- tempfile()
    dir.create(root)
    be <- .native_dataset(root)
    expect_identical(.manifest_partitioning(.manifest_read(.path(be)),
                                            "native"), character())
})

test_that("projections are refused on a natively converted dataset", {
    root <- tempfile()
    dir.create(root)
    be <- .native_dataset(root)
    expect_error(buildProjection(.path(be), verbose = FALSE),
                 "built from mzPeak archives")
})

test_that("a mixed-kind manifest is rejected rather than guessed at", {
    root <- tempfile()
    dir.create(root)
    ds <- file.path(root, "mixed")
    dir.create(ds)
    m <- .manifest_add_run(.manifest_new(), "A", "mzpeak", "/tmp/a", 2L,
                           "point")
    m <- .manifest_add_run(m, "B", "native", "/tmp/b", 2L, "list")
    .manifest_write(ds, m)
    expect_error(.dataset_kind(ds), "mixes run kinds")
})
