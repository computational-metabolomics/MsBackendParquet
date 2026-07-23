# The in-memory index and the SQL path must be interchangeable: whichever one
# answers, the result has to be identical. `MsBackendParquet.cacheMetadata`
# lets each test run the same assertions both ways.

with_cache <- function(enabled, code) {
    old <- options(MsBackendParquet.cacheMetadata = if (enabled) "auto" else "off")
    on.exit(options(old), add = TRUE)
    .meta_cache_clear()
    force(code)
}

test_that("cached and SQL filter paths give identical results", {
    be <- .make_large_test_backend()
    on.exit(unlink(.path(be), recursive = TRUE), add = TRUE)

    ops <- list(
        filterRt = function(b) filterRt(b, c(100, 400)),
        filterRt_msLevel = function(b) filterRt(b, c(100, 400), msLevel. = 1L),
        filterRt_open = function(b) filterRt(b, c(-Inf, 250)),
        filterMsLevel = function(b) filterMsLevel(b, 2L),
        filterMsLevel_both = function(b) filterMsLevel(b, c(1L, 2L)),
        filterDataOrigin = function(b) filterDataOrigin(b, "file-b"),
        filterDataOrigin_rev = function(b)
            filterDataOrigin(b, c("file-b", "file-a")),
        filterPrecursorMzRange = function(b)
            filterPrecursorMzRange(b, c(150, 200)),
        filterPrecursorMzValues = function(b)
            filterPrecursorMzValues(b, c(151.0, 175.5), ppm = 50),
        chained = function(b)
            filterPrecursorMzRange(filterMsLevel(filterRt(b, c(100, 800)), 2L),
                                   c(140, 260)))

    for (nm in names(ops)) {
        cached <- with_cache(TRUE, ops[[nm]](be))
        sql <- with_cache(FALSE, ops[[nm]](be))
        expect_identical(cached@spectraIds, sql@spectraIds,
                         info = paste0(nm, ": ids differ"))
        expect_equal(spectraData(cached, c("msLevel", "rtime", "precursorMz")),
                     spectraData(sql, c("msLevel", "rtime", "precursorMz")),
                     info = paste0(nm, ": spectraData differs"))
        # The peaks fetch must agree too: a cache-driven filter still records
        # a SQL predicate, and that predicate drives the peaks query.
        expect_equal(peaksData(cached), peaksData(sql),
                     info = paste0(nm, ": peaksData differs"))
    }
})

test_that("cached and SQL spectraData agree, including all columns", {
    be <- .make_large_test_backend(n = 200L)
    on.exit(unlink(.path(be), recursive = TRUE), add = TRUE)
    sub <- be[c(150L, 2L, 99L, 1L)]

    for (b in list(be, sub)) {
        expect_equal(with_cache(TRUE, spectraData(b)),
                     with_cache(FALSE, spectraData(b)))
        expect_equal(with_cache(TRUE, spectraData(b, "rtime")),
                     with_cache(FALSE, spectraData(b, "rtime")))
        expect_equal(with_cache(TRUE, spectraData(b, "spectrum_id_")),
                     with_cache(FALSE, spectraData(b, "spectrum_id_")))
        expect_equal(with_cache(TRUE, uniqueMsLevels(b)),
                     with_cache(FALSE, uniqueMsLevels(b)))
    }
})

test_that("NA metadata is filtered with R semantics, not SQL's", {
    # precursorMz is NA for every odd spectrum in the fixture. A range filter
    # must drop those; a `filterRt(msLevel. = )` must keep non-matching MS
    # levels even where the level itself is unknown.
    be <- .make_large_test_backend(n = 100L)
    on.exit(unlink(.path(be), recursive = TRUE), add = TRUE)

    for (enabled in c(TRUE, FALSE)) {
        res <- with_cache(enabled, filterPrecursorMzRange(be, c(0, 1e6)))
        expect_true(all(!is.na(spectraData(res, "precursorMz")[, 1L])))
        expect_equal(length(res), sum(!is.na(spectraData(be, "precursorMz")[, 1L])))
    }
})

test_that("the cache is dropped when the dataset is rewritten", {
    be <- .make_test_backend()
    path <- .path(be)
    on.exit(unlink(path, recursive = TRUE), add = TRUE)
    expect_equal(spectraData(be, "rtime")[, 1L], c(1, 2, 3))
    expect_true(!is.null(.meta_state[[path]]))

    .invalidate_dataset_cache(path)
    expect_null(.meta_state[[path]])
    # and it rebuilds transparently
    expect_equal(spectraData(be, "rtime")[, 1L], c(1, 2, 3))
})

test_that("exceeding the cache budget falls back to SQL, not to an error", {
    be <- .make_large_test_backend(n = 200L)
    on.exit(unlink(.path(be), recursive = TRUE), add = TRUE)
    old <- options(MsBackendParquet.cacheMaxCells = 1)
    on.exit(options(old), add = TRUE)
    .meta_cache_clear()

    expect_null(.meta_values(be, "rtime"))
    expect_equal(length(filterRt(be, c(10, 20))),
                 sum(spectraData(be, "rtime")[, 1L] >= 10 &
                     spectraData(be, "rtime")[, 1L] <= 20))
})
