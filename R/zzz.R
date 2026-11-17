# Lifecycle hooks for the package-level DuckDB connection used by the
# read path (see `.duckdb_con()` in MsBackendParquet-functions.R).

# Arrow's thread pool is process-wide and sized to every core. It is capped
# to the same count DuckDB uses (see `.n_threads()`) when the package loads.
# It is never raised, so a smaller pool set beforehand stays as it is.
.onLoad <- function(libname, pkgname) {
    n <- .n_threads()
    if (arrow::cpu_count() > n)
        arrow::set_cpu_count(n)
}

.onUnload <- function(libpath) {
    con <- .duckdb_state$con
    if (!is.null(con)) {
        try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
        .duckdb_state$con <- NULL
    }
}
