# A minimal mzML document: one <spectrum> per element of `spectra`, each a
# list(id, body) where body is the XML inside the spectrum.
.mzml_doc <- function(spectra, head = "", tail = "") {
    sp <- vapply(spectra, function(s)
        sprintf('<spectrum index="0" id="%s" defaultArrayLength="0">%s</spectrum>',
                s$id, s$body), character(1))
    paste0('<?xml version="1.0" encoding="utf-8"?>\n<indexedmzML><mzML>',
           head, '<run><spectrumList count="', length(spectra), '">\n',
           paste(sp, collapse = "\n"), "\n</spectrumList>", tail,
           "</run></mzML></indexedmzML>\n")
}

.precursor <- function(...) {
    terms <- vapply(c(...), function(a)
        sprintf('<cvParam cvRef="MS" accession="%s" name="x" value=""/>', a),
        character(1))
    paste0('<precursor><activation>', paste(terms, collapse = ""),
           '<cvParam cvRef="MS" accession="MS:1000045" name="collision energy" ',
           'value="30"/></activation></precursor>')
}

.write_doc <- function(text, gz = FALSE) {
    f <- tempfile(fileext = if (gz) ".mzML.gz" else ".mzML")
    con <- if (gz) gzfile(f, "w") else file(f, "w")
    writeLines(text, con)
    close(con)
    f
}

test_that("the dissociation methods are PSI-MS terms under MS:1000044", {
    acc <- names(.DISSOCIATION_METHODS)
    expect_true(all(grepl("^MS:[0-9]{7}$", acc)))
    expect_false(anyDuplicated(acc) > 0L)
    expect_false(any(c("MS:1000044", "MS:1002631", "MS:1003182") %in% acc))
    expect_true(all(.SUPPLEMENTAL_DISSOCIATION %in% acc))
})

test_that("an activation records its main dissociation method", {
    pick <- function(...) .pick_dissociation_method(.precursor(...))
    expect_identical(pick("MS:1000422"), "MS:1000422")
    expect_identical(pick(), NA_character_)                 # energy only
    expect_identical(pick("MS:1002679", "MS:1000598"), "MS:1000598")
    expect_identical(pick("MS:1002679"), "MS:1002679")      # supplemental only
    expect_identical(pick("MS:1002631"), NA_character_)     # combined method
    expect_identical(.pick_dissociation_method("<activation/>"),
                     NA_character_)
})

test_that("each spectrum takes its first precursor's activation", {
    doc <- .mzml_doc(list(
        list(id = "scan=1", body = ""),
        list(id = "scan=2", body = paste0("<precursorList>",
                                          .precursor("MS:1000422"),
                                          "</precursorList>")),
        list(id = "scan=3", body = paste0("<precursorList>",
                                          .precursor("MS:1000133"),
                                          .precursor("MS:1000422"),
                                          "</precursorList>")),
        list(id = "scan=4", body = "<precursorList><precursor><activation/></precursor></precursorList>")),
        tail = paste0("<chromatogramList><chromatogram>",
                      .precursor("MS:1000598"), "</chromatogram></chromatogramList>"))
    for (gz in c(FALSE, TRUE)) {
        tab <- .mzml_activation(.write_doc(doc, gz))
        expect_identical(tab$spectrumId, paste0("scan=", 1:4))
        expect_identical(tab$activationType,
                         c(NA, "MS:1000422", "MS:1000133", NA))
    }
})

test_that("the result does not depend on where chunks are cut", {
    body <- function(a) paste0("<precursorList>", .precursor(a),
                               "</precursorList>")
    doc <- .mzml_doc(lapply(seq_len(40), function(i)
        list(id = paste0("controllerType=0 controllerNumber=1 scan=", i),
             body = body(if (i %% 2) "MS:1000422" else "MS:1000133"))))
    f <- .write_doc(doc, gz = TRUE)
    ref <- .mzml_activation(f)
    expect_identical(nrow(ref), 40L)
    for (n in c(7L, 64L, 1000L))
        expect_identical(.mzml_activation(f, chunk_bytes = n), ref)
})

test_that("referenced parameter groups, quoting and entities are handled", {
    head <- paste0('<referenceableParamGroupList count="1">',
                   '<referenceableParamGroup id="hcd">',
                   '<cvParam cvRef="MS" accession="MS:1000422" name="x" value=""/>',
                   '</referenceableParamGroup></referenceableParamGroupList>')
    doc <- .mzml_doc(list(
        list(id = "a&amp;b", body = paste0(
            "<precursorList><precursor><activation>",
            '<referenceableParamGroupRef ref="hcd"/>',
            "</activation></precursor></precursorList>")),
        list(id = "c", body = paste0(
            "<precursorList><precursor><activation>",
            "<cvParam cvRef='MS' accession='MS:1000133' name='x' value=''/>",
            "</activation></precursor></precursorList>"))), head = head)
    tab <- .mzml_activation(.write_doc(gsub("\n", "", doc)))
    expect_identical(tab$spectrumId, c("a&b", "c"))
    expect_identical(tab$activationType, c("MS:1000422", "MS:1000133"))
})

test_that("activation is matched by native id, then by position", {
    tab <- data.frame(spectrumId = c("s1", "s2", "s3"),
                      activationType = c(NA, "MS:1000422", "MS:1000133"))
    expect_identical(.match_activation(tab, c("s3", "s2")),
                     c("MS:1000133", "MS:1000422"))
    expect_identical(.match_activation(tab, c("x", "y"), c(2L, 3L)),
                     c("MS:1000422", "MS:1000133"))
    expect_warning(res <- .match_activation(tab, "x", 9L), "matched")
    expect_null(res)
    expect_null(.match_activation(tab[1, ], "s1"))   # nothing recorded
    expect_null(.match_activation(NULL, "s1"))
})

test_that("mzML files are recognised by name or content", {
    expect_true(.is_mzml_file(.write_doc(.mzml_doc(list()), gz = TRUE)))
    f <- tempfile()
    writeLines(.mzml_doc(list()), f)
    expect_true(.is_mzml_file(f))
    g <- tempfile(fileext = ".mzXML")
    writeLines("<mzXML/>", g)
    expect_false(.is_mzml_file(g))
    expect_null(.source_activation_table(g))
    expect_null(.source_activation_table(NA_character_))
})

test_that("demo.mzML records no activation", {
    f <- system.file("extdata", "demo.mzML", package = "MsBackendParquet")
    tab <- .mzml_activation(f)
    expect_identical(nrow(tab), 3L)
    expect_true(all(is.na(tab$activationType)))
})
