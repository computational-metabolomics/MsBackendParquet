test_that("mzMLToParquet rejects empty input", {
    expect_error(mzMLToParquet(character(), tempfile()),
                 "non-empty character vector")
    expect_error(mzMLToParquet("foo.mzML", path = ""),
                 "non-empty character\\(1\\)")
})

test_that("mzMLToParquet rejects missing files", {
    missing_path <- file.path(tempdir(), "definitely-missing.mzML")
    expect_error(mzMLToParquet(missing_path, tempfile()),
                 "do not exist")
})

test_that("mzMLToParquet rejects unsupported extensions", {
    bad <- tempfile(fileext = ".txt")
    file.create(bad)
    on.exit(unlink(bad))
    expect_error(mzMLToParquet(bad, tempfile()),
                 "Unsupported file extension")
})

test_that("mzMLToParquet refuses to overwrite without flag", {
    be <- .make_test_backend()
    dummy <- tempfile(fileext = ".mzML")
    file.create(dummy)
    on.exit(unlink(dummy))
    expect_error(mzMLToParquet(dummy, path = .path(be)),
                 "already exists")
})

test_that("engine argument is validated", {
    dummy <- tempfile(fileext = ".mzML")
    file.create(dummy)
    on.exit(unlink(dummy))
    expect_error(mzMLToParquet(dummy, tempfile(), engine = "bogus"),
                 "should be one of")
})

test_that("engine = 'mzr' rejects the data argument", {
    sd <- .make_test_data()
    expect_error(
        createMsBackendParquetDataset(path = tempfile(), data = sd,
                                      engine = "mzr"),
        "does not support the 'data' argument")
})

test_that("engine = 'mzr' streams an mzML file when mzR is available", {
    testthat::skip_if_not_installed("mzR")
    testthat::skip_if_not_installed("arrow")
    testthat::skip_if_not_installed("MsDataHub")
    f <- tryCatch(
        MsDataHub::X20171016_POOL_POS_3_105.134.mzML(),
        error = function(e) NULL)
    skip_if(is.null(f), "MsDataHub sample mzML unavailable")

    path <- tempfile()
    be <- mzMLToParquet(f, path = path, engine = "mzr",
                        batch_size = 50L, verbose = FALSE)
    expect_s4_class(be, "MsBackendParquet")
    expect_gt(length(be), 0L)
    expect_true(all(c("mz", "intensity", "msLevel", "rtime") %in%
                    spectraVariables(be)))

    ms <- mzR::openMSfile(f)
    on.exit(try(mzR::close(ms), silent = TRUE))
    ref <- mzR::peaks(ms, 1L)
    got <- peaksData(be[1])[[1L]]
    expect_equal(as.numeric(got[, "mz"]), as.numeric(ref[, 1L]))
    expect_equal(as.numeric(got[, "intensity"]),
                 as.numeric(ref[, 2L]))
})
