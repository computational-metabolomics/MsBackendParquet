## Projections are caches: building one must change how fast a query runs,
## never what it returns.

.proj_dataset <- function(root) {
    a <- file.path(root, "P1")
    b <- file.path(root, "P2")
    .make_mzpeak_archive(a, n = 6L, run_id = "P1", mz_offset = 0)
    .make_mzpeak_archive(b, n = 4L, run_id = "P2", mz_offset = 10000)
    ds <- file.path(root, "ds")
    createMzPeakDataset(c(a, b), path = ds, verbose = FALSE)
    ds
}

test_that("filterContainsMz gives the same answer with and without a projection", {
    root <- tempfile()
    dir.create(root)
    ds <- .proj_dataset(root)
    be <- backendInitialize(MsBackendParquet(), path = ds)

    ## Peaks of spectrum k (0-based index i) are 100*(i+1) + 1:4, plus the
    ## run's offset. So 201 belongs to the second spectrum of P1 only.
    queries <- list(
        list(mz = 201, tol = 0.5),
        list(mz = c(201, 10101), tol = 0.5),
        list(mz = 99999, tol = 0.5),          # matches nothing
        list(mz = c(101, 301, 501), tol = 0.5))

    for (q in queries) {
        without <- filterContainsMz(be, q$mz, tolerance = q$tol, ppm = 0)
        buildProjection(ds, runs = NULL, verbose = FALSE)
        be2 <- backendInitialize(MsBackendParquet(), path = ds)
        with <- filterContainsMz(be2, q$mz, tolerance = q$tol, ppm = 0)
        expect_identical(with@spectraIds, without@spectraIds,
                         info = paste("mz =", paste(q$mz, collapse = ",")))
        dropProjection(ds, verbose = FALSE)
    }
})

test_that("filterContainsMz selects the expected spectra", {
    root <- tempfile()
    dir.create(root)
    ds <- .proj_dataset(root)
    be <- backendInitialize(MsBackendParquet(), path = ds)

    ## 201 is in P1's second spectrum (id 2) only.
    expect_identical(filterContainsMz(be, 201, tolerance = 0.5,
                                      ppm = 0)@spectraIds, 2L)
    ## 10101 is in P2's first spectrum, which is dataset id 7.
    expect_identical(filterContainsMz(be, 10101, tolerance = 0.5,
                                      ppm = 0)@spectraIds, 7L)
    ## Nothing matches.
    expect_identical(length(filterContainsMz(be, 99999, tolerance = 0.5,
                                             ppm = 0)), 0L)
    ## An empty query is a no-op.
    expect_identical(length(filterContainsMz(be, numeric())), length(be))
})

test_that("filterContainsMz composes with the metadata filters", {
    root <- tempfile()
    dir.create(root)
    ds <- .proj_dataset(root)
    be <- backendInitialize(MsBackendParquet(), path = ds)

    ## Restrict to MS1 first, then by peak: id 2 is MS2, so it drops out.
    ms1 <- filterMsLevel(be, 1L)
    expect_identical(length(filterContainsMz(ms1, 201, tolerance = 0.5,
                                             ppm = 0)), 0L)
    ## id 1 is MS1 and holds 101.
    expect_identical(filterContainsMz(ms1, 101, tolerance = 0.5,
                                      ppm = 0)@spectraIds, 1L)

    ## And peaks still read correctly afterwards.
    sub <- filterContainsMz(be, 10301, tolerance = 0.5, ppm = 0)
    expect_identical(sub@spectraIds, 9L)
    expect_equal(peaksData(sub)[[1L]][, "mz"], 10000 + 300 + 1:4)
})

test_that("a projection is recorded, used, and can be dropped", {
    root <- tempfile()
    dir.create(root)
    ds <- .proj_dataset(root)

    m <- .manifest_read(ds)
    expect_false(.manifest_has_projection(m, "mzsorted", c("P1", "P2")))

    buildProjection(ds, runs = "P1", verbose = FALSE)
    m <- .manifest_read(ds)
    expect_identical(.manifest_has_projection(m, "mzsorted"), "P1")
    expect_false(.manifest_has_projection(m, "mzsorted", c("P1", "P2")))
    expect_true(file.exists(file.path(ds, "index", "projections", "mzsorted",
                                      "run_id=P1", "part-0.parquet")))

    buildProjection(ds, verbose = FALSE)
    m <- .manifest_read(ds)
    expect_true(.manifest_has_projection(m, "mzsorted", c("P1", "P2")))

    dropProjection(ds, verbose = FALSE)
    m <- .manifest_read(ds)
    expect_length(.manifest_has_projection(m, "mzsorted"), 0L)
    expect_false(dir.exists(file.path(ds, "index", "projections",
                                      "mzsorted")))
})

test_that("adding a run invalidates only that run's projection", {
    root <- tempfile()
    dir.create(root)
    ds <- .proj_dataset(root)
    buildProjection(ds, verbose = FALSE)

    c3 <- file.path(root, "P3")
    .make_mzpeak_archive(c3, n = 2L, run_id = "P3", mz_offset = 20000)
    addMzPeakArchives(ds, c3, verbose = FALSE)

    m <- .manifest_read(ds)
    ## The existing runs keep their projections; the new one has none.
    expect_setequal(.manifest_has_projection(m, "mzsorted"), c("P1", "P2"))
    expect_false(.manifest_has_projection(m, "mzsorted",
                                          c("P1", "P2", "P3")))

    ## So a query spanning all three falls back to the archives -- and is
    ## still correct.
    be <- backendInitialize(MsBackendParquet(), path = ds)
    expect_identical(filterContainsMz(be, 20101, tolerance = 0.5,
                                      ppm = 0)@spectraIds, 11L)
})

test_that("filterContainsMz refuses a native dataset", {
    be <- .make_test_backend()
    on.exit(unlink(.path(be), recursive = TRUE), add = TRUE)
    expect_error(filterContainsMz(be, 100), "mzPeak archives")
})
