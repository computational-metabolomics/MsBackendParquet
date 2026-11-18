# The dissociation method of each spectrum's first precursor, read from the
# mzML `<activation>` elements. mzR does not report it, so it is read here in
# a second, streaming pass over the file.

# The descendants of MS:1000044 (dissociation method) in PSI-MS 4.2.14.
# Combined methods such as EThcD (MS:1002631) are not among them.
.DISSOCIATION_METHODS <- c(
    "MS:1000133" = "collision-induced dissociation",
    "MS:1000134" = "plasma desorption",
    "MS:1000135" = "post-source decay",
    "MS:1000136" = "surface-induced dissociation",
    "MS:1000242" = "blackbody infrared radiative dissociation",
    "MS:1000250" = "electron capture dissociation",
    "MS:1000262" = "infrared multiphoton dissociation",
    "MS:1000282" = "sustained off-resonance irradiation",
    "MS:1000422" = "beam-type collision-induced dissociation",
    "MS:1000433" = "low-energy collision-induced dissociation",
    "MS:1000435" = "photodissociation",
    "MS:1000598" = "electron transfer dissociation",
    "MS:1000599" = "pulsed q dissociation",
    "MS:1001880" = "in-source collision-induced dissociation",
    "MS:1002000" = "LIFT",
    "MS:1002472" = "trap-type collision-induced dissociation",
    "MS:1002481" = "higher energy beam-type collision-induced dissociation",
    "MS:1002678" = "supplemental beam-type collision-induced dissociation",
    "MS:1002679" = "supplemental collision-induced dissociation",
    "MS:1003246" = "ultraviolet photodissociation",
    "MS:1003247" = "negative electron transfer dissociation",
    "MS:1003294" = "electron activated dissociation")

# Methods that only supplement another one, as in ETD with supplemental
# activation.
.SUPPLEMENTAL_DISSOCIATION <- c("MS:1002678", "MS:1002679")

#' The dissociation method an `<activation>` element records: the first
#' method that is not supplemental, else the first supplemental one, else
#' `NA`. Collision energy and other terms are ignored.
#'
#' @param block `character(1)`, the element's text, with any referenced
#'     parameter groups appended.
#'
#' @noRd
.pick_dissociation_method <- function(block) {
    acc <- regmatches(block, gregexpr("accession=[\"'](MS:[0-9]+)[\"']",
                                      block, perl = TRUE))[[1L]]
    acc <- sub("^accession=[\"'](MS:[0-9]+)[\"']$", "\\1", acc, perl = TRUE)
    acc <- acc[acc %in% names(.DISSOCIATION_METHODS)]
    main <- acc[!acc %in% .SUPPLEMENTAL_DISSOCIATION]
    if (length(main)) return(main[1L])
    if (length(acc)) return(acc[1L])
    NA_character_
}

#' Unescape the predefined XML entities.
#'
#' @noRd
.xml_unescape <- function(x) {
    x <- gsub("&lt;", "<", x, fixed = TRUE)
    x <- gsub("&gt;", ">", x, fixed = TRUE)
    x <- gsub("&quot;", "\"", x, fixed = TRUE)
    x <- gsub("&apos;", "'", x, fixed = TRUE)
    gsub("&amp;", "&", x, fixed = TRUE)
}

#' The value of attribute `name` in a start tag, unescaped, or `NA`.
#'
#' @noRd
.xml_attr <- function(tag, name) {
    m <- regmatches(tag, regexec(
        paste0("(?<![A-Za-z0-9_:.-])", name, "\\s*=\\s*([\"'])(.*?)\\1"),
        tag, perl = TRUE))[[1L]]
    if (length(m) < 3L) return(NA_character_)
    x <- .xml_unescape(m[3L])
    Encoding(x) <- "UTF-8"
    x
}

# Elements the activation pass needs, in document order. Peak data is
# base64 and holds no `<`, so the expression skips it quickly.
.MZML_ACTIVATION_TOKENS <- paste0(
    "(?s)<spectrum\\s[^>]*>|</spectrum>",
    "|<activation(?:\\s[^>]*[^/])?>.*?</activation>|<activation\\s*/>",
    "|<referenceableParamGroup\\s[^>]*>.*?</referenceableParamGroup>",
    "|<chromatogramList[\\s>]|</spectrumList>")

#' Where an element opened by `open` but not closed by `close` starts in
#' `text`, or `NA` if every such element is closed.
#'
#' @noRd
.unclosed_at <- function(text, open, close) {
    o <- gregexpr(open, text, perl = TRUE, useBytes = TRUE)[[1L]]
    if (o[1L] == -1L) return(NA_integer_)
    last <- o[length(o)]
    cl <- gregexpr(close, text, perl = TRUE, useBytes = TRUE)[[1L]]
    if (cl[1L] != -1L && cl[length(cl)] > last) NA_integer_ else last
}

#' The dissociation method of every spectrum's first precursor in an mzML
#' file, plain or gzip-compressed.
#'
#' Reads the file in chunks of `chunk_bytes`, each cut after its last `>` or
#' before an element that is still open, so no element is split. Only the
#' first `<activation>` inside a `<spectrum>` is used: the first
#' `<precursor>` is the spectrum's own. Parsing stops at the end of the
#' spectrum list.
#'
#' @return `data.frame` with `spectrumId` and `activationType` (a PSI-MS
#'     CURIE or `NA`), one row per spectrum in file order.
#'
#' @noRd
.mzml_activation <- function(file, chunk_bytes = 4194304L) {
    con <- gzfile(file, "rb")
    on.exit(close(con))
    ids <- list()
    acts <- list()
    k <- 0L
    groups <- list()
    in_spec <- FALSE
    have_act <- FALSE
    cur_id <- NA_character_
    cur_act <- NA_character_
    carry <- raw()
    emit <- function() {
        k <<- k + 1L
        ids[[k]] <<- cur_id
        acts[[k]] <<- cur_act
        in_spec <<- FALSE
    }
    done <- FALSE
    while (!done) {
        r <- readBin(con, "raw", chunk_bytes)
        eof <- !length(r)
        buf <- c(carry, r)
        if (!length(buf)) break
        if (eof) {
            text <- rawToChar(buf)
            carry <- raw()
        } else {
            gt <- which(buf == as.raw(62L))
            if (!length(gt)) {
                carry <- buf
                next
            }
            cut <- gt[length(gt)]
            text <- rawToChar(buf[seq_len(cut)])
            carry <- buf[-seq_len(cut)]
            split <- c(
                .unclosed_at(text, "<activation(?:\\s[^>]*[^/])?>",
                             "</activation>"),
                .unclosed_at(text, "<referenceableParamGroup\\s",
                             "</referenceableParamGroup>"))
            split <- split[!is.na(split)]
            if (length(split)) {
                split <- min(split)
                bytes <- charToRaw(text)
                carry <- c(bytes[seq.int(split, length(bytes))], carry)
                text <- if (split > 1L) rawToChar(bytes[seq_len(split - 1L)])
                        else ""
            }
        }
        if (!nzchar(text)) {
            if (eof) break
            next
        }
        tok <- regmatches(text, gregexpr(.MZML_ACTIVATION_TOKENS, text,
                                         perl = TRUE, useBytes = TRUE))[[1L]]
        for (t in tok) {
            if (startsWith(t, "<spectrum")) {
                if (in_spec) emit()
                in_spec <- TRUE
                have_act <- FALSE
                cur_id <- .xml_attr(t, "id")
                cur_act <- NA_character_
                if (endsWith(t, "/>")) emit()
            } else if (startsWith(t, "</spectrum>")) {
                if (in_spec) emit()
            } else if (startsWith(t, "<activation")) {
                if (in_spec && !have_act) {
                    have_act <- TRUE
                    refs <- regmatches(t, gregexpr(
                        "<referenceableParamGroupRef\\s[^>]*>", t,
                        perl = TRUE))[[1L]]
                    for (ref in refs) {
                        g <- groups[[.xml_attr(ref, "ref")]]
                        if (!is.null(g)) t <- paste(t, g)
                    }
                    cur_act <- .pick_dissociation_method(t)
                }
            } else if (startsWith(t, "<referenceableParamGroup")) {
                gid <- .xml_attr(t, "id")
                if (!is.na(gid)) groups[[gid]] <- t
            } else {
                done <- TRUE
                break
            }
        }
        if (eof) break
    }
    if (in_spec) emit()
    data.frame(spectrumId = as.character(unlist(ids)),
               activationType = as.character(unlist(acts)),
               stringsAsFactors = FALSE)
}

#' Whether `file` is mzML: by its name, or for a file without an extension,
#' by its first bytes.
#'
#' @noRd
.is_mzml_file <- function(file) {
    ext <- .ms_file_ext(file)
    if (ext %in% c("mzml", "mzml.gz")) return(TRUE)
    if (nzchar(ext)) return(FALSE)
    head <- tryCatch({
        con <- gzfile(file, "rb")
        on.exit(close(con))
        b <- readBin(con, "raw", 4096L)
        rawToChar(b[b != as.raw(0L)])
    }, error = function(e) "")
    grepl("<(indexedmzML|mzML)[\\s>]", head, perl = TRUE)
}

#' The activation table of a source file, or `NULL` when `origin` is not a
#' single readable mzML file. A file that cannot be parsed gives a warning,
#' never an error: the conversion goes on without `activationType`.
#'
#' @noRd
.source_activation_table <- function(origin) {
    if (length(origin) != 1L || is.na(origin) || !file.exists(origin) ||
        !.is_mzml_file(origin))
        return(NULL)
    tryCatch(.mzml_activation(origin), error = function(e) {
        warning("Could not read the activation of '", basename(origin),
                "' (", conditionMessage(e), "); 'activationType' is left ",
                "missing.", call. = FALSE)
        NULL
    })
}

#' The activation of each spectrum, from the source file's table: matched
#' by native id, else by the 1-based position in the file (`scanIndex`).
#'
#' @return `character`, or `NULL` if the file records no activation or the
#'     spectra cannot be matched to it (with a warning).
#'
#' @noRd
.match_activation <- function(tab, spectrum_id = NULL, scan_index = NULL) {
    if (is.null(tab) || !nrow(tab) || all(is.na(tab$activationType)))
        return(NULL)
    if (!is.null(spectrum_id) && !anyDuplicated(tab$spectrumId)) {
        k <- match(as.character(spectrum_id), tab$spectrumId)
        if (!anyNA(k)) return(tab$activationType[k])
    }
    if (!is.null(scan_index)) {
        k <- suppressWarnings(as.integer(scan_index))
        if (!anyNA(k) && all(k >= 1L & k <= nrow(tab)))
            return(tab$activationType[k])
    }
    warning("The spectra could not be matched to their source file's ",
            "activation; 'activationType' is left missing.", call. = FALSE)
    NULL
}

#' Fill `activationType` from the source file's activation table, unless the
#' spectra already report it.
#'
#' @noRd
.with_activation <- function(df, tab) {
    have <- df[["activationType"]]
    if (!is.null(have) && any(!is.na(have)))
        return(df)
    act <- .match_activation(tab, df[["spectrumId"]], df[["scanIndex"]])
    if (!is.null(act) && any(!is.na(act)))
        df$activationType <- act
    df
}
