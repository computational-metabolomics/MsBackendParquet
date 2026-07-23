/* Assembly of peak matrices from the two list columns DuckDB returns.
 *
 * `peaksData()` has to hand back one n x 2 matrix per spectrum. Doing that in
 * R as Map(function(m, i) cbind(mz = m, intensity = i), ...) costs a closure
 * call, a cbind dispatch, two vector copies and a freshly built dimnames pair
 * for every spectrum -- measured at 800 ms for 76,020 spectra, more than the
 * DuckDB query that produced the data.
 *
 * Here each matrix is one allocation plus two memcpy, and the dimnames object
 * is built once and shared by every matrix (R's reference counting copies it
 * if a caller ever modifies one, so sharing is safe).
 */

#include <R.h>
#include <Rinternals.h>
#include <string.h>

/* Copy one list element into `dest`, coercing if DuckDB handed back something
 * other than a double vector, and treating NULL / NA as an empty spectrum.
 * Returns the number of values written. */
static R_xlen_t copy_column(SEXP src, double *dest, R_xlen_t n)
{
    if (src == R_NilValue) return 0;
    if (TYPEOF(src) == REALSXP) {
        memcpy(dest, REAL(src), (size_t) n * sizeof(double));
        return n;
    }
    SEXP coerced = PROTECT(Rf_coerceVector(src, REALSXP));
    memcpy(dest, REAL(coerced), (size_t) n * sizeof(double));
    UNPROTECT(1);
    return n;
}

static R_xlen_t element_length(SEXP x)
{
    return (x == R_NilValue) ? 0 : XLENGTH(x);
}

/* C_pack_peaks(mz, intensity)
 *
 * mz, intensity: lists of numeric vectors, one element per spectrum, as
 *   returned by the DuckDB driver for a list<double> column. Either may be
 *   R_NilValue to request a column of NA.
 *
 * Returns a list of n x 2 double matrices with dimnames
 * list(NULL, c("mz", "intensity")).
 */
SEXP C_pack_peaks(SEXP mz, SEXP intensity)
{
    int has_mz = (mz != R_NilValue);
    int has_int = (intensity != R_NilValue);
    if (!has_mz && !has_int)
        Rf_error("at least one of 'mz' and 'intensity' must be supplied");
    if (has_mz && TYPEOF(mz) != VECSXP)
        Rf_error("'mz' must be a list");
    if (has_int && TYPEOF(intensity) != VECSXP)
        Rf_error("'intensity' must be a list");

    R_xlen_t n = has_mz ? XLENGTH(mz) : XLENGTH(intensity);
    if (has_mz && has_int && XLENGTH(mz) != XLENGTH(intensity))
        Rf_error("'mz' and 'intensity' must have the same length");

    SEXP colnames = PROTECT(Rf_allocVector(STRSXP, 2));
    SET_STRING_ELT(colnames, 0, Rf_mkChar("mz"));
    SET_STRING_ELT(colnames, 1, Rf_mkChar("intensity"));
    SEXP dimnames = PROTECT(Rf_allocVector(VECSXP, 2));
    SET_VECTOR_ELT(dimnames, 0, R_NilValue);
    SET_VECTOR_ELT(dimnames, 1, colnames);

    SEXP out = PROTECT(Rf_allocVector(VECSXP, n));

    for (R_xlen_t i = 0; i < n; i++) {
        SEXP mz_i = has_mz ? VECTOR_ELT(mz, i) : R_NilValue;
        SEXP int_i = has_int ? VECTOR_ELT(intensity, i) : R_NilValue;

        R_xlen_t len_mz = element_length(mz_i);
        R_xlen_t len_int = element_length(int_i);
        /* A spectrum's two arrays are the same length by construction; if one
         * is absent the other sets the row count and the missing column is
         * filled with NA. */
        R_xlen_t nrow = (len_mz > len_int) ? len_mz : len_int;

        SEXP m = PROTECT(Rf_allocMatrix(REALSXP, (int) nrow, 2));
        double *dest = REAL(m);

        if (len_mz > 0) {
            copy_column(mz_i, dest, len_mz);
            for (R_xlen_t k = len_mz; k < nrow; k++) dest[k] = NA_REAL;
        } else {
            for (R_xlen_t k = 0; k < nrow; k++) dest[k] = NA_REAL;
        }
        if (len_int > 0) {
            copy_column(int_i, dest + nrow, len_int);
            for (R_xlen_t k = len_int; k < nrow; k++) dest[nrow + k] = NA_REAL;
        } else {
            for (R_xlen_t k = 0; k < nrow; k++) dest[nrow + k] = NA_REAL;
        }

        Rf_setAttrib(m, R_DimNamesSymbol, dimnames);
        SET_VECTOR_ELT(out, i, m);
        UNPROTECT(1);
    }

    UNPROTECT(3);
    return out;
}

/* C_concat_peaks(peaks, column)
 *
 * peaks: list of n x 2 (or n x k) double matrices, as produced by mzR or held
 *   in memory before a write.
 * column: 0-based column index to extract.
 *
 * Returns a single numeric vector holding that column of every matrix end to
 * end. The writers used two lapply()s building one R vector per spectrum,
 * which is the same allocation storm as the read side had.
 */
SEXP C_concat_peaks(SEXP peaks, SEXP column)
{
    if (TYPEOF(peaks) != VECSXP)
        Rf_error("'peaks' must be a list");
    int col = Rf_asInteger(column);
    if (col == NA_INTEGER || col < 0)
        Rf_error("'column' must be a non-negative integer");

    R_xlen_t n = XLENGTH(peaks);
    R_xlen_t total = 0;
    for (R_xlen_t i = 0; i < n; i++) {
        SEXP m = VECTOR_ELT(peaks, i);
        if (m == R_NilValue) continue;
        SEXP dim = Rf_getAttrib(m, R_DimSymbol);
        if (dim == R_NilValue || LENGTH(dim) != 2)
            Rf_error("element %lld of 'peaks' is not a matrix",
                     (long long) (i + 1));
        total += INTEGER(dim)[0];
    }

    SEXP out = PROTECT(Rf_allocVector(REALSXP, total));
    double *dest = REAL(out);
    R_xlen_t at = 0;
    for (R_xlen_t i = 0; i < n; i++) {
        SEXP m = VECTOR_ELT(peaks, i);
        if (m == R_NilValue) continue;
        SEXP dim = Rf_getAttrib(m, R_DimSymbol);
        int nrow = INTEGER(dim)[0], ncol = INTEGER(dim)[1];
        if (nrow == 0) continue;
        if (col >= ncol)
            Rf_error("element %lld of 'peaks' has only %d column(s)",
                     (long long) (i + 1), ncol);
        if (TYPEOF(m) == REALSXP) {
            memcpy(dest + at, REAL(m) + (R_xlen_t) col * nrow,
                   (size_t) nrow * sizeof(double));
        } else {
            SEXP c = PROTECT(Rf_coerceVector(m, REALSXP));
            memcpy(dest + at, REAL(c) + (R_xlen_t) col * nrow,
                   (size_t) nrow * sizeof(double));
            UNPROTECT(1);
        }
        at += nrow;
    }

    UNPROTECT(1);
    return out;
}

/* C_peak_lengths(peaks): row count of each matrix, so the caller can build
 * the partitioning of a CompressedNumericList without an R-level loop. */
SEXP C_peak_lengths(SEXP peaks)
{
    if (TYPEOF(peaks) != VECSXP)
        Rf_error("'peaks' must be a list");
    R_xlen_t n = XLENGTH(peaks);
    SEXP out = PROTECT(Rf_allocVector(INTSXP, n));
    int *dest = INTEGER(out);
    for (R_xlen_t i = 0; i < n; i++) {
        SEXP m = VECTOR_ELT(peaks, i);
        if (m == R_NilValue) { dest[i] = 0; continue; }
        SEXP dim = Rf_getAttrib(m, R_DimSymbol);
        dest[i] = (dim == R_NilValue || LENGTH(dim) != 2)
            ? 0 : INTEGER(dim)[0];
    }
    UNPROTECT(1);
    return out;
}
