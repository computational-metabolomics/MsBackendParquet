## The exported manifest API, which layers above this package (the mzStack
## results layer) build on. What they rely on: keys this package does not own
## survive its writes byte-for-byte in meaning, numbers round-trip exactly,
## and a manifest replaced behind the cache's back is noticed.

.results_keys <- function() {
    list(
        uid = "01J8Q2Z4N9K6V3P7R1T5W0X2Y4",
        role = "results",
        sources = list(list(
            key = "study_A", role = "study", resolution = "dataset",
            uid = NULL, path = "../study",
            runs = list(list(run_id = "QC01", uid_base = 1L,
                             n_spectra = 10L, ingested_at = 1L)))),
        provenance = list(list(
            id = "act-0001", generation = 2L,
            outputs = list(runs = list("av_all"), tables = list("feature")),
            parameters = list(ppm = 0.010000000000000002, minfrac = 0.5,
                              tol = 0.1, n = 3L))),
        results = list(
            version = "1.0.0",
            tables = list(feature = list(
                entity = "feature", path = "results/feature", rows = 0L,
                sorted_by = list("exp_mass_to_charge", "feature_id_")))))
}

test_that("readManifest() and manifestRuns() expose the manifest", {
    be <- .make_test_backend()
    m <- readManifest(.path(be))
    expect_identical(m$format, "mzStack")
    expect_identical(m$version, "0.1.0")
    runs <- manifestRuns(be)
    expect_identical(runs, manifestRuns(.path(be)))
    expect_true(all(c("run_id", "uid_base", "n_spectra", "ingested_at") %in%
                    names(runs)))
    expect_identical(runs$run_id, c("file-a", "file-b"))
    expect_error(readManifest(tempfile()), class = "mzstack_format")
})

test_that("keys of other layers survive a write, one-element arrays included", {
    be <- .make_test_backend()
    path <- .path(be)
    m <- readManifest(path)
    m[names(.results_keys())] <- .results_keys()
    writeManifest(path, m)

    back <- readManifest(path)
    expect_identical(back$generation, m$generation + 1L)
    expect_identical(back$sources, .results_keys()$sources)
    expect_identical(back$results, .results_keys()$results)
    expect_identical(back$provenance[[1]]$outputs$runs, list("av_all"))

    raw <- jsonlite::fromJSON(file.path(path, "mzStack.json"),
                              simplifyVector = FALSE)
    expect_type(raw$sources, "list")
    expect_null(names(raw$sources))
    expect_identical(raw$results$tables$feature$sorted_by,
                     list("exp_mass_to_charge", "feature_id_"))

    ## ... and through this package's own writers, which know nothing of them.
    runData(be) <- data.frame(run_id = "file-a", timepoint = 6)
    again <- readManifest(path)
    expect_identical(again$sources, .results_keys()$sources)
    expect_identical(again$results$tables$feature$sorted_by,
                     list("exp_mass_to_charge", "feature_id_"))
})

test_that("numbers in the manifest round-trip exactly", {
    be <- .make_test_backend()
    path <- .path(be)
    m <- readManifest(path)
    vals <- c(0.1 + 0.2, 1 / 3, 0.010000000000000002, 1e-300, 278.093,
              123456789.123456789)
    m$provenance <- list(list(parameters = list(v = I(vals), one = 0.1)))
    writeManifest(path, m)
    back <- readManifest(path)
    expect_identical(unlist(back$provenance[[1]]$parameters$v), vals)
    expect_identical(back$provenance[[1]]$parameters$one, 0.1)
    ## Shortest form: 0.1 is not written as 0.10000000000000001.
    expect_true(any(grepl('"one": 0.1$', readLines(file.path(path,
                                                             "mzStack.json")))))
})

test_that("a one-element partitioning array stays an array", {
    be <- .make_test_backend(partitioning = "msLevel")
    raw <- jsonlite::fromJSON(file.path(.path(be), "mzStack.json"),
                              simplifyVector = FALSE)
    ## On disk the partition column carries its mzPeak name.
    expect_identical(raw$runs[[1]]$signal$partitioning, list("ms_level"))
})

test_that("writeManifest() refuses a manifest read before another commit", {
    be <- .make_test_backend()
    path <- .path(be)
    m1 <- readManifest(path)
    m2 <- readManifest(path)
    writeManifest(path, m1)
    expect_error(writeManifest(path, m2), "another writer")
    expect_error(writeManifest(path, list(format = "x")), "readManifest")
})

test_that("writeManifest(bump = FALSE) keeps the generation", {
    be <- .make_test_backend()
    m <- readManifest(.path(be))
    m$role <- "study"
    out <- writeManifest(.path(be), m, bump = FALSE)
    expect_identical(out$generation, m$generation)
    expect_identical(readManifest(.path(be))$role, "study")
})

test_that("a manifest replaced behind the cache is picked up", {
    be <- .make_test_backend()
    path <- .path(be)
    expect_null(.dataset_manifest(path)$role)
    ## Replace the file without going through this package, as another
    ## process would, and make sure its stamp differs.
    m <- .manifest_read(path)
    m$role <- "study"
    Sys.sleep(0.01)
    .manifest_write(path, m)
    expect_identical(.dataset_manifest(path)$role, "study")
})

test_that("invalidateDatasetCache() drops what is held for a dataset", {
    be <- .make_test_backend()
    path <- .path(be)
    .dataset_manifest(path)
    .dataset_view(path)
    expect_true(exists(path, envir = .manifest_cache, inherits = FALSE))
    expect_true(exists(path, envir = .duckdb_state$views, inherits = FALSE))
    invalidateDatasetCache(path)
    expect_false(exists(path, envir = .manifest_cache, inherits = FALSE))
    expect_false(exists(path, envir = .duckdb_state$views, inherits = FALSE))
    ## Still usable afterwards.
    expect_identical(length(Spectra::Spectra(be)), 3L)
})

test_that("writeManifest() commits the first manifest of a new dataset", {
    path <- file.path(tempfile(), "results")
    m <- newManifest()
    expect_identical(m$generation, 1L)
    expect_length(m$runs, 0L)
    m$role <- "results"
    writeManifest(path, m)
    back <- readManifest(path)
    expect_identical(back$generation, 1L)
    expect_identical(back$role, "results")
    expect_identical(nrow(manifestRuns(path)), 0L)
    ## A second write is an ordinary update.
    expect_identical(writeManifest(path, back)$generation, 2L)
})
