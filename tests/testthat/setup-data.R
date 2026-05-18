## Shared test fixtures: build a tiny in-memory DataFrame holding three
## spectra and materialise it as a MsBackendParquet dataset in a fresh
## temporary directory. Used by every test_that block below.
.make_test_data <- function() {
    sd <- S4Vectors::DataFrame(
        msLevel = c(1L, 1L, 2L),
        rtime = c(1.0, 2.0, 3.0),
        precursorMz = c(NA_real_, NA_real_, 110.5),
        dataOrigin = c("file-a", "file-a", "file-b"),
        totIonCurrent = c(30, 32, 34))
    sd$mz <- IRanges::NumericList(c(100, 110), c(101, 111),
                                  c(102, 112), compress = FALSE)
    sd$intensity <- IRanges::NumericList(c(10, 20), c(11, 21),
                                         c(12, 22), compress = FALSE)
    sd
}

.make_test_backend <- function(partitioning = character()) {
    sd <- .make_test_data()
    path <- tempfile()
    backendInitialize(MsBackendParquet(), path = path, data = sd,
                      partitioning = partitioning)
}
