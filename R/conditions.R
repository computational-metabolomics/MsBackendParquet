#' Typed mzStack error conditions
#'
#' @description
#'
#' The mzStack specification (mzStack-5 §3) requires an implementation to
#' distinguish a fixed set of failure classes. The class names and the
#' mechanism are left to each implementation; the distinctions are not. In R
#' they are condition classes, so a caller can react to one kind of failure
#' without matching message text:
#'
#' ```r
#' tryCatch(Spectra(backendInitialize(MsBackendParquet(), path = p)),
#'          mzstack_format = function(e) NULL)
#' ```
#'
#' Every condition inherits from `mzstack_error`, then `error` and
#' `condition`, so existing `tryCatch(..., error = )` handlers keep working.
#'
#' | Class | Raised when |
#' | --- | --- |
#' | `mzstack_format` | the directory is not readable as mzStack: no manifest, another format, an unsupported major version, mixed run kinds |
#' | `mzstack_archive` | a referenced mzPeak archive cannot be read as its specification requires |
#' | `mzstack_study` | a repository study cannot be ingested |
#' | `mzstack_stale` | a projection, index or reference no longer matches the run it was built from |
#' | `mzstack_unsupported` | a valid request outside what this implementation supports |
#' | `mzstack_semantic` | a well-formed request that is incoherent |
#' | `mzstack_capability` | a request for data the dataset does not contain |
#' | `mzstack_resource` | a request that cannot be completed within a declared bound |
#'
#' A `mzstack_capability` error and an empty result are different outcomes:
#' the first says the question could not be asked, the second that nothing
#' matched.
#'
#' `mzstackError()` is exported so that packages layered on MsBackendParquet
#' raise the same classes.
#'
#' @param class `character(1)`, one of the classes above without the
#'     `mzstack_` prefix.
#'
#' @param ... pieces of the message, pasted together as by [stop()].
#'
#' @param data named `list` of further fields to carry on the condition, for
#'     example the run or source key the error concerns.
#'
#' @param call the call to record on the condition. Defaults to none, which
#'     matches the `call. = FALSE` convention used throughout the package.
#'
#' @return `mzstackError()` does not return; it signals the condition.
#'     `mzstackCondition()` returns it unsignalled.
#'
#' @author Ossama Edbali
#'
#' @name mzstack-conditions
#'
#' @examples
#' res <- tryCatch(mzstackError("capability", "no such table"),
#'                 mzstack_capability = function(e) conditionMessage(e))
#' res
NULL

.MZSTACK_ERROR_CLASSES <- c("format", "archive", "study", "stale",
                            "unsupported", "semantic", "capability",
                            "resource")

#' @rdname mzstack-conditions
#'
#' @export
mzstackCondition <- function(class = .MZSTACK_ERROR_CLASSES, ...,
                             data = list(), call = NULL) {
    class <- match.arg(class)
    if (length(data) && (is.null(names(data)) || any(!nzchar(names(data)))))
        stop("'data' must be a named list.", call. = FALSE)
    structure(
        c(list(message = paste0(..., collapse = ""), call = call), data),
        class = c(paste0("mzstack_", class), "mzstack_error", "error",
                  "condition"))
}

#' @rdname mzstack-conditions
#'
#' @export
mzstackError <- function(class = .MZSTACK_ERROR_CLASSES, ...,
                         data = list(), call = NULL) {
    stop(mzstackCondition(class, ..., data = data, call = call))
}
