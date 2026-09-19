## Per-run sample annotation lives in `index/samples.parquet` and is expanded
## in R from the manifest's id blocks. It is never a column of the DuckDB
## view, so these tests pin both halves: that it reads back as an ordinary
## spectra variable, and that it never leaks into the per-spectrum paths.

## Note the assignment target must be a name: `runData(.path(be)) <- x` is a
## nested replacement and R would look for `.path<-`.
.annotated <- function() {
    be <- .make_test_backend()
    runData(be) <- data.frame(
        run_id = c("file-a", "file-b"),
        subject = c("S1", "S2"),
        timepoint = c(0L, 6L),
        ratio = c(1.5, 2.5),
        stringsAsFactors = FALSE)
    be
}

test_that("run annotation round trips with its types", {
    be <- .annotated()
    sd <- runData(be)
    expect_identical(sd$run_id, c("file-a", "file-b"))
    expect_identical(sd$timepoint, c(0L, 6L))     # integer, not "0"
    expect_identical(sd$ratio, c(1.5, 2.5))
    expect_identical(sd$subject, c("S1", "S2"))
    expect_setequal(runVariables(be), c("subject", "timepoint", "ratio"))
    ## Readable from the path as well as from an open backend.
    expect_identical(runData(.path(be)), sd)
})

test_that("a dataset with no annotation is not a special case", {
    be <- .make_test_backend()
    expect_identical(nrow(runData(be)), 0L)
    expect_identical(runVariables(be), character())
    expect_error(filterSampleData(be, timepoint == 6), "no run annotation")
})

test_that("annotation columns become ordinary spectra variables", {
    be <- .annotated()
    expect_true(all(c("subject", "timepoint", "ratio") %in%
                    spectraVariables(be)))

    ## The mixed request is the interesting one: `.fetch_spectra_data()` is
    ## all-or-nothing about the metadata cache, so a sample column asked for
    ## alongside a real one must be split off before it gets there.
    got <- spectraData(be, c("rtime", "timepoint", "msLevel", "subject"))
    expect_equal(got$rtime, c(1, 2, 3))
    expect_equal(as.integer(got$msLevel), c(1L, 1L, 2L))
    expect_equal(got$timepoint, c(0L, 0L, 6L))
    expect_equal(got$subject, c("S1", "S1", "S2"))
    ## and on its own, and through `$`
    expect_equal(spectraData(be, "ratio")$ratio, c(1.5, 1.5, 2.5))
    expect_equal(be$timepoint, c(0L, 0L, 6L))
})

test_that("annotation follows subsetting and reordering", {
    be <- .annotated()
    sub <- be[c(3L, 1L)]
    expect_equal(sub$timepoint, c(6L, 0L))
    expect_equal(sub$subject, c("S2", "S1"))
    expect_identical(nrow(spectraData(be[integer()], "timepoint")), 0L)
})

test_that("runs without an annotation row yield NA", {
    be <- .make_test_backend()
    runData(be) <- data.frame(run_id = "file-a", timepoint = 3L)
    expect_equal(be$timepoint, c(3L, 3L, NA_integer_))
})

test_that("an unknown run_id is refused", {
    be <- .make_test_backend()
    expect_error(
        runData(be) <- data.frame(run_id = "nope", timepoint = 1),
        "No such run")
    expect_error(
        runData(be) <- data.frame(timepoint = 1),
        "must have a 'run_id' column")
    expect_error(
        runData(be) <- data.frame(run_id = c("file-a", "file-a"),
                                  timepoint = 1:2),
        "must be unique")
})

test_that("an annotation column that would shadow a spectra variable is refused", {
    be <- .make_test_backend()
    ## `rtime` is a spectra variable; `time` is the on-disk column the view
    ## consumes to build it, so it would be shadowed rather than reported.
    for (nm in c("rtime", "time", "msLevel", "mz", "id")) {
        df <- data.frame(run_id = "file-a", x = 1)
        names(df)[2] <- nm
        expect_error(runData(be) <- df, "would shadow")
    }
})

test_that("filterSampleData agrees with filtering on the values", {
    be <- .annotated()
    f <- filterSampleData(be, timepoint == 6)
    expect_equal(.ids(f), 3L)
    expect_equal(f$subject, "S2")
    ## Same answer as selecting on the expanded per-spectrum values.
    expect_equal(.ids(f), .ids(be)[be$timepoint == 6])
    expect_equal(peaksData(f), peaksData(be[3L]))

    ## Compound conditions, and the empty result.
    expect_equal(.ids(filterSampleData(be, timepoint == 0 & subject == "S1")),
                 c(1L, 2L))
    expect_equal(length(filterSampleData(be, timepoint > 99)), 0L)
    ## Variables not in the table come from the calling frame.
    cutoff <- 3
    expect_equal(.ids(filterSampleData(be, timepoint > cutoff)), 3L)
    expect_error(filterSampleData(be, timepoint), "one logical value per run")
})

test_that("a run filter carries a range predicate on the sort key", {
    be <- .annotated()
    f <- filterSampleData(be, timepoint == 0)
    ## The point of resolving through the manifest: the predicate is a range
    ## on `spectrum_id_`, which is what the files are ordered by, so a later
    ## peak read prunes row groups instead of matching an id list.
    expect_true(isTRUE(f@.predicate_clean))
    expect_match(f@.pending_predicate, "spectrum_id_.*>=.*1")
    expect_match(f@.pending_predicate, "spectrum_id_.*<=.*2")
    expect_false(grepl("timepoint", f@.pending_predicate))

    ## Adjacent runs merge into one range rather than an OR chain.
    all_runs <- filterSampleData(be, timepoint >= 0)
    expect_false(grepl(" OR ", all_runs@.pending_predicate))
    expect_equal(length(all_runs), 3L)
})

test_that("annotation never enters the per-spectrum metadata cache", {
    .meta_cache_clear()
    be <- .annotated()
    invisible(be$timepoint)
    invisible(filterSampleData(be, timepoint == 6))
    st <- .meta_state[[.path(be)]]
    if (!is.null(st))
        expect_false(any(c("subject", "timepoint", "ratio") %in%
                         names(st$data)))
    ## `dataOrigin` is answered from the manifest too, so it is not warmed.
    expect_false("dataOrigin" %in% .META_DEFAULT_COLUMNS)
})

test_that("filterDataOrigin resolves through runs and agrees with the values", {
    be <- .make_test_backend()
    f <- filterDataOrigin(be, "file-a")
    expect_equal(.ids(f), c(1L, 2L))
    expect_match(f@.pending_predicate, "spectrum_id_")
    expect_equal(length(filterDataOrigin(be, "nope")), 0L)
    ## Several origins still come back in the requested order.
    g <- filterDataOrigin(be, c("file-b", "file-a"))
    expect_equal(as.character(spectraData(g, "dataOrigin")$dataOrigin),
                 c("file-b", "file-a", "file-a"))
})

test_that("filterDataOrigin still works when runs span several origins", {
    ## The interleaved fallback writes one run holding both origins, so the
    ## manifest cannot answer and the per-spectrum column has to.
    sd <- S4Vectors::DataFrame(
        msLevel = rep(1L, 4), rtime = c(1, 2, 3, 4),
        dataOrigin = c("a", "b", "a", "b"))
    sd$mz <- IRanges::NumericList(as.list(1:4), compress = FALSE)
    sd$intensity <- IRanges::NumericList(as.list(1:4), compress = FALSE)
    path <- tempfile()
    suppressMessages(
        backendInitialize(MsBackendParquet(), path = path, data = sd))
    be <- backendInitialize(MsBackendParquet(), path = path)

    expect_identical(nrow(.manifest_runs(.manifest_read(path))), 1L)
    expect_equal(.ids(filterDataOrigin(be, "a")), c(1L, 3L))
    expect_equal(.ids(filterDataOrigin(be, "b")), c(2L, 4L))
})

test_that("annotation works on an mzPeak-backed dataset and survives rewrites", {
    root <- tempfile()
    dir.create(root)
    a <- file.path(root, "P1")
    b <- file.path(root, "P2")
    .make_mzpeak_archive(a, n = 6L, run_id = "P1", mz_offset = 0)
    .make_mzpeak_archive(b, n = 4L, run_id = "P2", mz_offset = 10000)
    ds <- file.path(root, "ds")
    createMzPeakDataset(c(a, b), path = ds, verbose = FALSE)

    runData(ds) <- data.frame(run_id = c("P1", "P2"),
                              timepoint = c(0L, 6L),
                              stringsAsFactors = FALSE)
    be <- backendInitialize(MsBackendParquet(), path = ds)
    expect_equal(be$timepoint, c(rep(0L, 6), rep(6L, 4)))
    f <- filterSampleData(be, timepoint == 6)
    expect_equal(.ids(f), 7:10)
    expect_equal(peaksData(f), peaksData(be[7:10]))

    ## A projection is a cache over the signal; correcting an annotation must
    ## not throw it away.
    buildProjection(ds, verbose = FALSE)
    expect_true(.manifest_has_projection(.manifest_read(ds), "mzsorted",
                                         c("P1", "P2")))
    runData(ds) <- data.frame(run_id = c("P1", "P2"),
                              timepoint = c(0L, 12L),
                              stringsAsFactors = FALSE)
    expect_true(.manifest_has_projection(.manifest_read(ds), "mzsorted",
                                         c("P1", "P2")))
    be <- backendInitialize(MsBackendParquet(), path = ds)
    expect_equal(.ids(filterSampleData(be, timepoint == 12)), 7:10)
})

test_that("rewriting the annotation invalidates what readers hold", {
    be <- .annotated()
    expect_equal(be$timepoint, c(0L, 0L, 6L))
    path <- .path(be)
    runData(be) <- data.frame(run_id = "file-b", timepoint = 99L)
    ## The in-hand backend sees it, and so does one opened afresh: the cached
    ## copy of the table is dropped rather than served again.
    expect_equal(be$timepoint, c(NA_integer_, NA_integer_, 99L))
    fresh <- backendInitialize(MsBackendParquet(), path = path)
    expect_equal(fresh$timepoint, c(NA_integer_, NA_integer_, 99L))
    expect_setequal(runVariables(fresh), "timepoint")
})
