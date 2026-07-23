# Lifecycle hooks for the package-level DuckDB connection used by the
# read path (see `.duckdb_con()` in MsBackendParquet-functions.R).

.onUnload <- function(libpath) {
    con <- .duckdb_state$con
    if (!is.null(con)) {
        try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
        .duckdb_state$con <- NULL
    }
}
