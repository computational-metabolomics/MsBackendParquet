test_that("C_pack_peaks builds the canonical two-column matrices", {
    mz <- list(c(100, 110, 120), c(200, 210), numeric())
    it <- list(c(1, 2, 3), c(4, 5), numeric())
    res <- .Call(C_pack_peaks, mz, it)

    expect_length(res, 3L)
    expect_true(all(vapply(res, is.matrix, logical(1))))
    expect_identical(dim(res[[1L]]), c(3L, 2L))
    expect_identical(colnames(res[[1L]]), c("mz", "intensity"))
    expect_equal(res[[1L]][, "mz"], c(100, 110, 120))
    expect_equal(res[[2L]][, "intensity"], c(4, 5))
    expect_identical(dim(res[[3L]]), c(0L, 2L))
    # Matches what the R implementation produced.
    expect_equal(res[[1L]], cbind(mz = mz[[1L]], intensity = it[[1L]]))
})

test_that("C_pack_peaks handles NULL elements and NULL columns", {
    # DuckDB yields NULL for a spectrum with no peaks.
    res <- .Call(C_pack_peaks, list(NULL, c(1, 2)), list(NULL, c(3, 4)))
    expect_identical(dim(res[[1L]]), c(0L, 2L))
    expect_equal(res[[2L]][, "mz"], c(1, 2))

    # A whole column absent: the other sets the row count, the missing one
    # fills with NA.
    only_mz <- .Call(C_pack_peaks, list(c(1, 2, 3)), NULL)
    expect_identical(dim(only_mz[[1L]]), c(3L, 2L))
    expect_equal(only_mz[[1L]][, "mz"], c(1, 2, 3))
    expect_true(all(is.na(only_mz[[1L]][, "intensity"])))

    only_int <- .Call(C_pack_peaks, NULL, list(c(7, 8)))
    expect_equal(only_int[[1L]][, "intensity"], c(7, 8))
    expect_true(all(is.na(only_int[[1L]][, "mz"])))
})

test_that("C_pack_peaks coerces integer input and rejects bad input", {
    res <- .Call(C_pack_peaks, list(1:3), list(4:6))
    expect_type(res[[1L]], "double")
    expect_equal(res[[1L]][, "mz"], c(1, 2, 3))

    expect_error(.Call(C_pack_peaks, NULL, NULL), "at least one")
    expect_error(.Call(C_pack_peaks, 1:3, list(1)), "must be a list")
    expect_error(.Call(C_pack_peaks, list(1, 2), list(1)),
                 "same length")
})

test_that("shared dimnames survive modification of one matrix", {
    # The dimnames object is built once and shared across every matrix, which
    # is only safe if R's copy-on-modify kicks in when a caller edits one.
    res <- .Call(C_pack_peaks, list(c(1, 2), c(3, 4)), list(c(5, 6), c(7, 8)))
    colnames(res[[1L]]) <- c("a", "b")
    expect_identical(colnames(res[[1L]]), c("a", "b"))
    expect_identical(colnames(res[[2L]]), c("mz", "intensity"))
})

test_that("C_concat_peaks and C_peak_lengths agree with the R equivalents", {
    peaks <- list(cbind(mz = c(1, 2, 3), intensity = c(4, 5, 6)),
                  cbind(mz = numeric(), intensity = numeric()),
                  cbind(mz = c(7, 8), intensity = c(9, 10)))
    expect_equal(.Call(C_concat_peaks, peaks, 0L), c(1, 2, 3, 7, 8))
    expect_equal(.Call(C_concat_peaks, peaks, 1L), c(4, 5, 6, 9, 10))
    expect_identical(.Call(C_peak_lengths, peaks), c(3L, 0L, 2L))

    expect_equal(.Call(C_concat_peaks, peaks, 0L),
                 unlist(lapply(peaks, function(m) m[, "mz"]),
                        use.names = FALSE))
    expect_error(.Call(C_concat_peaks, peaks, 5L), "only 2 column")
    expect_error(.Call(C_concat_peaks, list(1:3), 0L), "not a matrix")
})

test_that("mz() and intensity() return compressed lists with the right values", {
    be <- .make_large_test_backend(n = 50L)
    on.exit(unlink(.path(be), recursive = TRUE), add = TRUE)

    m <- mz(be)
    expect_s4_class(m, "NumericList")
    expect_equal(lengths(m), (seq_len(50L) %% 3L) + 1L)
    expect_equal(as.numeric(m[[7L]]), 700 + seq_len((7L %% 3L) + 1L))

    # The peak accessors must agree with peaksData(), which takes the other
    # branch through the packer. `unname` on the extracted column is needed
    # because R's drop rules give `m[, "mz"]` a name when the matrix has
    # exactly one row, and this fixture has single-peak spectra.
    pks <- peaksData(be)
    col <- function(nm) unname(lapply(pks, function(p) unname(p[, nm])))
    expect_equal(lapply(as.list(m), as.numeric), col("mz"))
    expect_equal(lapply(as.list(intensity(be)), as.numeric), col("intensity"))
})

test_that("peaksData respects a single-column selection", {
    be <- .make_large_test_backend(n = 20L)
    on.exit(unlink(.path(be), recursive = TRUE), add = TRUE)

    only_mz <- peaksData(be, columns = "mz")
    expect_identical(colnames(only_mz[[1L]]), "mz")
    expect_equal(only_mz[[3L]][, "mz"], peaksData(be)[[3L]][, "mz"])

    only_int <- peaksData(be, columns = "intensity")
    expect_identical(colnames(only_int[[1L]]), "intensity")
    expect_equal(only_int[[3L]][, "intensity"],
                 peaksData(be)[[3L]][, "intensity"])

    expect_error(peaksData(be, columns = "nope"), "Unsupported peaks variable")
})
