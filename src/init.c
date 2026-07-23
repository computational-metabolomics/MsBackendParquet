#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>
#include <R_ext/Visibility.h>

SEXP C_pack_peaks(SEXP mz, SEXP intensity);
SEXP C_concat_peaks(SEXP peaks, SEXP column);
SEXP C_peak_lengths(SEXP peaks);

static const R_CallMethodDef callMethods[] = {
    {"C_pack_peaks",   (DL_FUNC) &C_pack_peaks,   2},
    {"C_concat_peaks", (DL_FUNC) &C_concat_peaks, 2},
    {"C_peak_lengths", (DL_FUNC) &C_peak_lengths, 1},
    {NULL, NULL, 0}
};

void attribute_visible R_init_MsBackendParquet(DllInfo *dll)
{
    R_registerRoutines(dll, NULL, callMethods, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
    R_forceSymbols(dll, TRUE);
}
