## Peak-annotation variables: list columns stored beside `mz` and
## `intensity` in a native run, one element per peak (mzStack-4 §6), and
## explicit run ids for data written from a DataFrame.

.annotated_data <- function() {
    sd <- S4Vectors::DataFrame(msLevel = c(2L, 2L, 2L),
                               rtime = c(1, 2, 3),
                               dataOrigin = c("a", "a", "b"))
    sd$mz <- IRanges::NumericList(c(100, 200), c(150), numeric(),
                                  compress = FALSE)
    sd$intensity <- IRanges::NumericList(c(1, 2), c(3), numeric(),
                                         compress = FALSE)
    sd$sn <- IRanges::NumericList(c(5.5, NA), c(Inf), numeric(),
                                  compress = FALSE)
    sd$contributor_count <- list(c(3L, 1L), NULL, integer())
    sd
}

test_that("peak-annotation variables round-trip as peak variables", {
    path <- tempfile()
    createMsBackendParquetDataset(path, data = .annotated_data())
    be <- backendInitialize(MsBackendParquet(), path = path)
    expect_identical(peaksVariables(be),
                     c("mz", "intensity", "sn", "contributor_count"))
    expect_false(any(c("sn", "contributor_count") %in% be@.dataset_vars))
    pd <- peaksData(be, columns = c("mz", "intensity", "sn",
                                    "contributor_count"))
    expect_identical(pd[[1]][, "sn"], c(5.5, NA))
    expect_identical(unname(pd[[2]][, "sn"]), Inf)
    expect_identical(pd[[1]][, "contributor_count"], c(3, 1))
    ## A spectrum without the variable gets NA, aligned with its peaks.
    expect_identical(unname(pd[[2]][, "contributor_count"]), NA_real_)
    expect_identical(dim(pd[[3]]), c(0L, 4L))
    ## The default and the fast path are unchanged.
    expect_identical(peaksData(be), peaksData(be, c("mz", "intensity")))
    expect_identical(colnames(peaksData(be)[[1]]), c("mz", "intensity"))
    sps <- Spectra::Spectra(be)
    expect_identical(Spectra::peaksData(sps, columns = c("mz", "sn"))[[1]][
        , "sn"], c(5.5, NA))
})

test_that("peak-annotation variables must align with the peaks", {
    sd <- .annotated_data()
    sd$sn <- IRanges::NumericList(c(1), c(1), numeric(), compress = FALSE)
    expect_error(createMsBackendParquetDataset(tempfile(), data = sd),
                 "as many values as 'mz'")
})

test_that("annotation lists are stored exactly as mz is", {
    ## Arrow and pyarrow both write the Parquet-compliant child name
    ## `element`; what matters is that every list column is alike.
    path <- tempfile()
    createMsBackendParquetDataset(path, data = .annotated_data())
    fl <- list.files(file.path(path, "spectra"), pattern = "parquet$",
                     recursive = TRUE, full.names = TRUE)[1]
    sch <- arrow::read_parquet(fl, as_data_frame = FALSE)$schema
    expect_identical(sch$sn$type$value_field$name,
                     sch$mz$type$value_field$name)
    expect_identical(sch$sn$type$value_type$ToString(), "double")
    expect_identical(sch$contributor_count$type$value_type$ToString(),
                     "int32")
})

test_that("an explicit run_id column names the runs", {
    sd <- .annotated_data()
    sd$run_id <- c("av_intra_A", "av_intra_A", "av_all")
    path <- tempfile()
    createMsBackendParquetDataset(path, data = sd)
    runs <- manifestRuns(path)
    expect_identical(runs$run_id, c("av_intra_A", "av_all"))
    expect_identical(runs$n_spectra, c(2L, 1L))
    expect_identical(runs$source, c("a", "b"))
    sps <- Spectra::Spectra(backendInitialize(MsBackendParquet(),
                                              path = path))
    expect_identical(sps$run_id, sd$run_id)
    expect_identical(sps$dataOrigin, c("a", "a", "b"))
})

test_that("explicit run ids must be valid and contiguous", {
    sd <- .annotated_data()
    sd$run_id <- c("x", "y", "x")
    expect_error(createMsBackendParquetDataset(tempfile(), data = sd),
                 "contiguous")
    sd$run_id <- c("x", "x", "bad id")
    expect_error(createMsBackendParquetDataset(tempfile(), data = sd),
                 "must match")
})

test_that("spectraData() serves annotation variables as peak columns", {
    path <- tempfile()
    createMsBackendParquetDataset(path, data = .annotated_data())
    be <- backendInitialize(MsBackendParquet(), path = path)
    d <- spectraData(be, c("msLevel", "sn"))
    expect_identical(as.list(d$sn)[[1]], c(5.5, NA))
    ## Spectra's own setBackend() moves mz and intensity; it must not fail
    ## on a backend that carries more.
    sps <- Spectra::Spectra(be)
    mem <- Spectra::setBackend(sps, Spectra::MsBackendMemory())
    expect_identical(Spectra::peaksData(mem)[[1]][, "mz"], c(100, 200))
})
