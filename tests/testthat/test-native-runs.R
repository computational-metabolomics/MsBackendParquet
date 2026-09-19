## A native dataset is one run per source file. These tests pin the invariant
## every other part of the package reads off the manifest: ids are allocated in
## contiguous blocks, one per run, tiling 1..N in write order. Nothing errors
## when that breaks -- ids stay a dense `seq_len(N)` either way -- so it has to
## be asserted directly.

## Every run's block, as claimed by the manifest.
.blocks <- function(path) {
    r <- .manifest_runs(.manifest_read(path))
    r[order(r$uid_base), c("run_id", "n_spectra", "uid_base")]
}

## Assert the blocks tile 1..N with no gap, overlap or empty run.
expect_tiles <- function(path, n) {
    r <- .blocks(path)
    expect_true(all(r$n_spectra > 0L))
    expect_identical(r$uid_base, cumsum(c(1L, utils::head(r$n_spectra, -1L))))
    expect_identical(sum(r$n_spectra), as.integer(n))
}

test_that(".origin_blocks cuts on runs of equal values, not on the values", {
    b <- .origin_blocks(c("a", "a", "b"))
    expect_length(b, 2L)
    expect_identical(vapply(b, `[[`, character(1), "run_id"), c("a", "b"))
    expect_identical(vapply(b, `[[`, integer(1), "start"), c(1L, 3L))
    expect_identical(vapply(b, `[[`, integer(1), "end"), c(2L, 3L))

    ## Interleaved: blocking it would need a permutation, so it is refused
    ## outright rather than silently reordered.
    expect_null(.origin_blocks(c("a", "b", "a")))
    expect_null(.origin_blocks(c("a", NA, "b")))
    expect_null(.origin_blocks(c("a", "", "b")))
    expect_null(.origin_blocks(character()))
})

test_that("run ids are sanitised and deduplicated case-insensitively", {
    expect_identical(.sanitise_run_id("QC 01/a=b"), "QC_01_a_b")
    expect_identical(.sanitise_run_id("..hidden"), "hidden")
    expect_identical(.sanitise_run_id(""), "run")
    expect_identical(.native_run_id("/tmp/dir/QC01.mzML"), "QC01")

    ## Same basename in two directories, and a case-only difference: both
    ## would otherwise land in one directory on a case-insensitive file
    ## system, interleaving two runs' parts.
    expect_identical(.unique_run_ids(c("QC01", "QC01")), c("QC01", "QC01-1"))
    expect_identical(.unique_run_ids(c("QC01", "qc01")), c("QC01", "qc01-1"))
    expect_identical(.unique_run_ids("QC01", taken = "qc01"), "QC01-1")
})

test_that(".contiguous_chunks keeps input order, unlike split()", {
    f <- factor(c("y", "y", "x", "x", "y"), levels = c("x", "y"))
    expect_identical(.contiguous_chunks(f), list(1:2, 3:4, 5L))
    expect_identical(.contiguous_chunks(f[3:5], offset = 3L), list(3:4, 5L))
})

test_that("a data= conversion tiles ids across one run per dataOrigin", {
    be <- .make_test_backend()
    expect_identical(be@spectraIds, seq_len(3L))
    expect_tiles(.path(be), 3L)
    expect_identical(.blocks(.path(be))$run_id, c("file-a", "file-b"))
})

test_that("interleaved dataOrigin becomes one run rather than a reorder", {
    sd <- S4Vectors::DataFrame(
        msLevel = rep(1L, 4),
        rtime = c(1, 2, 3, 4),
        dataOrigin = c("a", "b", "a", "b"))
    sd$mz <- IRanges::NumericList(as.list(1:4), compress = FALSE)
    sd$intensity <- IRanges::NumericList(as.list(1:4), compress = FALSE)
    path <- tempfile()
    expect_message(
        backendInitialize(MsBackendParquet(), path = path, data = sd),
        "does not cut the spectra into contiguous blocks")
    be <- backendInitialize(MsBackendParquet(), path = path)

    expect_identical(.blocks(path)$run_id, "native")
    expect_tiles(path, 4L)
    ## The point of the fallback: the caller's order survives.
    expect_equal(rtime(be), c(1, 2, 3, 4))
    expect_equal(as.character(spectraData(be, "dataOrigin")$dataOrigin),
                 c("a", "b", "a", "b"))
})

test_that("a dataset without dataOrigin is a single run", {
    sd <- S4Vectors::DataFrame(msLevel = 1:2, rtime = c(1, 2))
    sd$mz <- IRanges::NumericList(as.list(1:2), compress = FALSE)
    sd$intensity <- IRanges::NumericList(as.list(1:2), compress = FALSE)
    path <- tempfile()
    backendInitialize(MsBackendParquet(), path = path, data = sd)
    expect_identical(.blocks(path)$run_id, "native")
    expect_tiles(path, 2L)
})

test_that("every id maps back to the run whose directory holds it", {
    ## The assertion that catches a permuted write: the manifest says which
    ## run owns an id, the Hive column says which directory the row is
    ## actually in, and the two have to agree for every spectrum.
    be <- .make_test_backend()
    m <- .manifest_read(.path(be))
    claimed <- vapply(.manifest_split_ids(m, .ids(be)), function(p)
        paste(p$run$run_id, paste(p$ids, collapse = ","), sep = ":"),
        character(1))

    got <- spectraData(be, c("spectrum_id_", "run_id"))
    actual <- vapply(split(got$spectrum_id_, got$run_id), function(i)
        paste(sort(i), collapse = ","), character(1))
    expect_identical(
        sort(claimed),
        sort(paste(names(actual), actual, sep = ":")))
})

test_that("setBackend preserves order when f interleaves source files", {
    ## `f` bounds memory, it must not decide write order. Iterating its levels
    ## would write rows 1,3,5,2,4,6 and number them in that order, returning a
    ## silent permutation of the input.
    sd <- S4Vectors::DataFrame(
        msLevel = rep(1L, 6),
        rtime = c(10, 20, 30, 40, 50, 60),
        dataOrigin = rep(c("a.mzML", "b.mzML"), each = 3))
    sd$mz <- IRanges::NumericList(as.list(1:6), compress = FALSE)
    sd$intensity <- IRanges::NumericList(as.list(1:6), compress = FALSE)
    src <- Spectra(backendInitialize(MsBackendParquet(), path = tempfile(),
                                     data = sd))

    dest <- tempfile()
    out <- setBackend(src, MsBackendParquet(), path = dest,
                      f = factor(rep(c("x", "y"), 3)))

    expect_equal(rtime(out), c(10, 20, 30, 40, 50, 60))
    expect_equal(unlist(mz(out)), as.numeric(1:6))
    expect_tiles(dest, 6L)
    expect_identical(.blocks(dest)$run_id, c("a", "b"))
    expect_identical(.blocks(dest)$n_spectra, c(3L, 3L))
})

test_that("a native round trip writes no run_id column beside the run dir", {
    ## `run_id` is a Hive column on read, so it round-trips through
    ## spectraData(); written back as a real column it would collide with the
    ## directory it sits in.
    be <- .make_test_backend()
    expect_true("run_id" %in% spectraVariables(be))

    dest <- tempfile()
    setBackend(Spectra(be), MsBackendParquet(), path = dest)
    fl <- list.files(.spectra_path(dest), pattern = "\\.parquet$",
                     recursive = TRUE, full.names = TRUE)
    for (f in fl)
        expect_false("run_id" %in% names(arrow::read_parquet(f)))
    expect_equal(rtime(backendInitialize(MsBackendParquet(), path = dest)),
                 c(1, 2, 3))
})

test_that("a zero-spectrum run is never recorded", {
    ## Two runs sharing a uid_base would leave findInterval() to pick between
    ## them on tie-breaking alone.
    be <- .make_test_backend()
    path <- .path(be)
    runs <- list(list(run_id = "a", path = .run_dir(path, "file-a"),
                      n_spectra = 2L),
                 list(run_id = "empty", path = .run_dir(path, "empty"),
                      n_spectra = 0L),
                 list(run_id = "b", path = .run_dir(path, "file-b"),
                      n_spectra = 1L))
    .manifest_write_native(path, runs)
    r <- .manifest_runs(.manifest_read(path))
    expect_identical(r$run_id, c("a", "b"))
    expect_identical(r$uid_base, c(1L, 3L))
})

test_that("the mzr streaming engine writes one run per file", {
    ## The streaming writer has its own sink and its own id arithmetic, and it
    ## is the only path where a run boundary needs no inference at all.
    skip_if_not_installed("mzR")
    d <- tempfile()
    dir.create(d)
    src <- system.file("extdata", "demo.mzML", package = "MsBackendParquet")
    fls <- file.path(d, c("sampleA.mzML", "sampleB.mzML"))
    for (f in fls) file.copy(src, f)

    for (part in list(character(), "msLevel")) {
        path <- tempfile()
        be <- suppressWarnings(
            mzMLToParquet(fls, path = path, engine = "mzr", batch_size = 2L,
                          partitioning = part))
        n <- length(be)
        expect_identical(be@spectraIds, seq_len(n))
        expect_tiles(path, n)
        expect_identical(.blocks(path)$run_id, c("sampleA", "sampleB"))
        ## Both copies are the same file, so the two runs must match.
        half <- n / 2L
        expect_equal(rtime(be)[seq_len(half)], rtime(be)[half + seq_len(half)])
        expect_equal(peaksData(be)[[1L]], peaksData(be)[[half + 1L]])
    }
})

test_that("files sharing a basename get distinct run directories", {
    skip_if_not_installed("mzR")
    src <- system.file("extdata", "demo.mzML", package = "MsBackendParquet")
    fls <- vapply(c("one", "two"), function(sub) {
        d <- file.path(tempfile(), sub)
        dir.create(d, recursive = TRUE)
        f <- file.path(d, "QC01.mzML")
        file.copy(src, f)
        f
    }, character(1))

    path <- tempfile()
    suppressWarnings(mzMLToParquet(fls, path = path, engine = "mzr"))
    expect_identical(.blocks(path)$run_id, c("QC01", "QC01-1"))
    expect_tiles(path, .dataset_n_spectra(path))
})

test_that("a manifest that does not account for the dataset is refused", {
    be <- .make_test_backend()
    path <- .path(be)
    expect_error(
        .manifest_write_native(path, list(list(run_id = "a",
                                               path = .run_dir(path, "file-a"),
                                               n_spectra = 2L))),
        "refusing to write an inconsistent manifest")
})
