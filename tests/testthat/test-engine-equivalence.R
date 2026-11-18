# Both conversion engines must store the same spectra variables and values
# for a file, equal to what MsBackendMzR reports for it.
expect_engines_agree <- function(file) {
    a <- mzMLToParquet(file, tempfile(), verbose = FALSE)
    b <- mzMLToParquet(file, tempfile(), engine = "mzr", verbose = FALSE)
    ref <- Spectra::Spectra(file, source = Spectra::MsBackendMzR())@backend
    expect_identical(spectraVariables(a), spectraVariables(b))
    expect_true(all(spectraVariables(ref) %in% spectraVariables(a)))
    expect_false(any(grepl("(_1|\\.1)$", spectraVariables(a))))
    sv <- setdiff(spectraVariables(a), c("mz", "intensity", "dataStorage"))
    da <- spectraData(a, columns = sv)
    db <- spectraData(b, columns = sv)
    for (v in sv)
        expect_identical(da[[v]], db[[v]], info = v)
    common <- setdiff(intersect(spectraVariables(ref), sv), "dataStorage")
    rd <- spectraData(ref, columns = common)
    ## mzR reports an unknown polarity as -1; the view reports it as NA.
    rd$polarity[rd$polarity %in% -1L] <- NA_integer_
    expect_equal(da[, common], rd)
    expect_equal(peaksData(a), peaksData(ref))
    expect_equal(peaksData(b), peaksData(ref))
    list(a = a, b = b)
}

test_that("both engines agree with MsBackendMzR on demo.mzML", {
    skip_if_not_installed("mzR")
    expect_engines_agree(system.file("extdata", "demo.mzML",
                                     package = "MsBackendParquet"))
})

test_that("both engines keep scan numbers, positions and MSn parents", {
    skip_if_not_installed("mzR")
    f <- system.file("extdata", "msn.mzML.gz", package = "MsBackendParquet")
    res <- expect_engines_agree(f)
    for (be in res) {
        expect_identical(unique(be$run_id), "msn")
        expect_identical(scanIndex(be), seq_along(be))
        expect_false(identical(acquisitionNum(be), scanIndex(be)))
        msn <- msLevel(be) > 1L
        expect_true(all(precScanNum(be)[msn] %in% acquisitionNum(be)))
        parent <- match(precScanNum(be)[msn], acquisitionNum(be))
        expect_true(all(msLevel(be)[parent] == msLevel(be)[msn] - 1L))
        expect_identical(be$peaksCount, lengths(be))
    }
})

test_that("both engines store each spectrum's first activation", {
    skip_if_not_installed("mzR")
    f <- system.file("extdata", "msn.mzML.gz", package = "MsBackendParquet")
    res <- expect_engines_agree(f)
    be <- res$a
    ## The fixture's filter strings name every stage, the spectrum's own last.
    last <- sub(".*@(hcd|cid)[0-9.]+ \\[.*$", "\\1", be$filterString)
    want <- ifelse(msLevel(be) == 1L, NA_character_,
                   ifelse(last == "hcd", "MS:1000422", "MS:1000133"))
    expect_identical(be$activationType, want)
    expect_identical(res$b$activationType, want)
    expect_setequal(unique(na.omit(want)), c("MS:1000422", "MS:1000133"))

    ## Only the first precursor counts: switch one MS3 spectrum's first
    ## activation to ETD and leave its second stage as it was.
    txt <- readLines(gzfile(f))
    open <- grep("<spectrum ", txt, fixed = TRUE)
    ms3 <- open[which(msLevel(be) == 3L)[1L]]
    act <- grep("accession=\"MS:1000422\"|accession=\"MS:1000133\"", txt)
    act <- act[act > ms3][1:2]
    txt[act[1L]] <- sub("MS:1000(422|133)", "MS:1000598", txt[act[1L]])
    g <- tempfile(fileext = ".mzML")
    writeLines(txt, g)
    for (engine in c("spectra", "mzr")) {
        x <- mzMLToParquet(g, tempfile(), engine = engine, verbose = FALSE)
        k <- which(msLevel(x) == 3L)[1L]
        expect_identical(x$activationType[k], "MS:1000598", info = engine)
        expect_identical(x$activationType[-k], want[-k], info = engine)
    }
})
