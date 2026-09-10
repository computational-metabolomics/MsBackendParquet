# Shared test fixtures: build a tiny in-memory DataFrame holding three
# spectra and materialise it as an mzStack dataset in a fresh
# temporary directory.
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

# A backend large enough to cross `.MAX_INLINE_IDS` (1024), so tests can
# reach the registered-id-table join that an inline `IN (...)` list is not
# allowed to grow into. Every spectrum gets a distinct peak count and
# distinct values so a mis-ordered or mis-joined result cannot pass.
.make_large_test_backend <- function(n = 3000L) {
    sd <- S4Vectors::DataFrame(
        msLevel = rep(c(1L, 2L), length.out = n),
        rtime = seq_len(n) * 0.5,
        precursorMz = ifelse(seq_len(n) %% 2L == 0L, 100 + seq_len(n) / 10,
                             NA_real_),
        dataOrigin = rep(c("file-a", "file-b"), each = ceiling(n / 2))[seq_len(n)],
        totIonCurrent = as.numeric(seq_len(n)))
    npk <- (seq_len(n) %% 3L) + 1L
    sd$mz <- IRanges::NumericList(
        lapply(seq_len(n), function(i) i * 100 + seq_len(npk[i])),
        compress = FALSE)
    sd$intensity <- IRanges::NumericList(
        lapply(seq_len(n), function(i) i * 1000 + seq_len(npk[i])),
        compress = FALSE)
    backendInitialize(MsBackendParquet(), path = tempfile(), data = sd)
}
