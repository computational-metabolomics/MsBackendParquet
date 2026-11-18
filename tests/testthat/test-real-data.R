s <- Spectra(MsDataHub::X20171016_POOL_POS_1_105.134.mzML())
b_mzr <- s@backend

test_that("data matches refrence after setBackend", {
    d <- tempfile()
    b_prq <- setBackend(s, MsBackendParquet(), path = d)@backend
    ## all original spectra variables have to be present
    expect_true(all(spectraVariables(b_mzr) %in% spectraVariables(b_prq)))
    
    common <- intersect(spectraVariables(b_mzr), spectraVariables(b_prq))
    common <- setdiff(common, c("dataStorage"))
    a <- spectraData(b_mzr, columns = common)
    b <- spectraData(b_prq, columns = common)

    expect_equal(a, b)
    unlink(d, recursive = TRUE)
    
    ## Spectra with arbitrary additional spectra variables
    s$NEW_VAR <- "A"
    s$num_val <- rnorm(length(s))
    d <- tempfile()
    b_prq <- setBackend(s, MsBackendParquet(), path = d)@backend
    expect_true(all(spectraVariables(s@backend) %in% spectraVariables(b_prq)))
    
    common <- intersect(spectraVariables(s@backend), spectraVariables(b_prq))
    common <- setdiff(common, c("dataStorage"))
    a <- spectraData(s@backend, columns = common)
    b <- spectraData(b_prq, columns = common)

    expect_equal(a, b)
    unlink(d, recursive = TRUE)
})

test_that("data matches reference when creating from the same source", {
    b_prq <- mzMLToParquet(MsDataHub::X20171016_POOL_POS_1_105.134.mzML(),
                           path = tempfile())
    expect_true(all(spectraVariables(b_mzr) %in% spectraVariables(b_prq)))

    common <- intersect(spectraVariables(b_mzr), spectraVariables(b_prq))
    common <- setdiff(common, c("dataStorage"))
    a <- spectraData(b_mzr, columns = common)
    b <- spectraData(b_prq, columns = common)
    expect_equal(a, b)

    a <- peaksData(b_mzr)
    b <- peaksData(b_prq)
    expect_equal(a, b)
})

test_that("both engines store mzR's base peak and m/z range as mzPeak columns", {
    f <- MsDataHub::X20171016_POOL_POS_1_105.134.mzML()
    vars <- c("basePeakMZ", "lowMZ", "highMZ")
    for (engine in c("spectra", "mzr")) {
        path <- tempfile()
        b_prq <- mzMLToParquet(f, path = path, engine = engine)
        disk <- names(arrow::open_dataset(.spectra_path(path)))
        expect_true(all(c("base_peak_mz", "lowest_observed_mz",
                          "highest_observed_mz") %in% disk), info = engine)
        expect_false(any(tolower(disk) %in% tolower(vars)), info = engine)
        expect_false(any(grepl("_1$", spectraVariables(b_prq))),
                     info = engine)
        expect_equal(spectraData(b_prq, columns = vars),
                     spectraData(b_mzr, columns = vars), info = engine)
        unlink(path, recursive = TRUE)
    }
})
