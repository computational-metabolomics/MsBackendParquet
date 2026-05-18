test_that("empty constructor yields a valid empty backend", {
    be <- MsBackendParquet()
    expect_s4_class(be, "MsBackendParquet")
    expect_equal(length(be), 0L)
    expect_identical(spectraNames(be), character())
})

test_that("backendInitialize writes and reads a dataset", {
    be <- .make_test_backend()
    expect_equal(length(be), 3L)
    expect_true(all(c("mz", "intensity", "msLevel", "rtime") %in%
                    spectraVariables(be)))
    sd <- spectraData(be, c("msLevel", "rtime"))
    expect_equal(as.integer(sd$msLevel), c(1L, 1L, 2L))
    expect_equal(sd$rtime, c(1.0, 2.0, 3.0))
})

test_that("peaksData returns matrices in spectra order", {
    be <- .make_test_backend()
    pks <- peaksData(be)
    expect_length(pks, 3L)
    expect_equal(pks[[1L]][, "mz"], c(100, 110))
    expect_equal(pks[[3L]][, "intensity"], c(12, 22))
})

test_that("mz() and intensity() return NumericList of correct length", {
    be <- .make_test_backend()
    expect_s4_class(mz(be), "NumericList")
    expect_equal(lengths(mz(be)), c(2L, 2L, 2L))
    expect_equal(as.numeric(intensity(be)[[2L]]), c(11, 21))
})

test_that("setters for mz / intensity / spectraNames are blocked", {
    be <- .make_test_backend()
    expect_error(mz(be) <- list(numeric()), "read-only")
    expect_error(intensity(be) <- list(numeric()), "read-only")
    expect_error(spectraNames(be) <- "x", "not supported")
})

test_that("subsetting preserves order and supports reset()", {
    be <- .make_test_backend()
    sub <- be[c(3L, 1L)]
    expect_equal(length(sub), 2L)
    expect_equal(spectraData(sub, "msLevel")[[1L]], c(2L, 1L))
    rst <- reset(sub)
    expect_equal(length(rst), 3L)
    expect_equal(spectraNames(rst), spectraNames(be))
})

test_that("filterMsLevel uses Arrow pushdown", {
    be <- .make_test_backend()
    f1 <- filterMsLevel(be, 1L)
    expect_equal(length(f1), 2L)
    expect_true(all(spectraData(f1, "msLevel")[[1L]] == 1L))
})

test_that("filterRt restricts on rtime", {
    be <- .make_test_backend()
    f <- filterRt(be, c(1.5, 2.5))
    expect_equal(length(f), 1L)
    expect_equal(spectraData(f, "rtime")[[1L]], 2.0)
})

test_that("filterDataOrigin keeps requested origins", {
    be <- .make_test_backend()
    f <- filterDataOrigin(be, "file-b")
    expect_equal(length(f), 1L)
    expect_equal(spectraData(f, "dataOrigin")[[1L]], "file-b")
})

test_that("uniqueMsLevels enumerates levels from the dataset", {
    be <- .make_test_backend()
    expect_equal(uniqueMsLevels(be), c(1L, 2L))
})

test_that("tic(initial = TRUE) returns the stored totIonCurrent", {
    be <- .make_test_backend()
    expect_equal(tic(be, initial = TRUE), c(30, 32, 34))
})

test_that("locally cached variables shadow the on-disk dataset", {
    be <- .make_test_backend()
    be$flag <- c("a", "b", "c")
    expect_true("flag" %in% spectraVariables(be))
    expect_equal(spectraData(be, "flag")[[1L]], c("a", "b", "c"))
})

test_that("dataset can be partitioned by dataOrigin", {
    be <- .make_test_backend(partitioning = "dataOrigin")
    expect_equal(length(be), 3L)
    f <- filterDataOrigin(be, "file-a")
    expect_equal(length(f), 2L)
})

test_that("backendInitialize refuses to overwrite an existing dataset", {
    be <- .make_test_backend()
    expect_error(
        backendInitialize(MsBackendParquet(), path = .path(be),
                          data = .make_test_data()),
        "already exists")
})

test_that("supportsSetBackend is TRUE and backendBpparam echoes BPPARAM", {
    be <- .make_test_backend()
    expect_true(supportsSetBackend(be))
    bp <- BiocParallel::SerialParam()
    expect_identical(Spectra::backendBpparam(be, bp), bp)
})
