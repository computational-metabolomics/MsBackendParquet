test_that("SQL predicate builders reproduce R's %in% / NA semantics", {
    # In SQL a NULL operand makes IN yield NULL and NOT(NULL) yield NULL, so
    # such rows would be dropped. In R, NA %in% 1 is FALSE and !(NA %in% 1) is
    # TRUE, so the spectrum is kept. The COALESCE wrapper restores that.
    expect_match(.pred_in("msLevel", 1:2), "COALESCE", fixed = TRUE)
    expect_match(.pred_in("msLevel", 1:2), "IN (1, 2)", fixed = TRUE)
    expect_identical(.pred_in("msLevel", integer()), "FALSE")

    expect_match(.pred_in("dataOrigin", "a'b", quote = TRUE),
                 "'a''b'", fixed = TRUE)

    # Infinite bounds are dropped rather than emitted as literals.
    expect_identical(.pred_range("rtime", -Inf, Inf), "TRUE")
    expect_match(.pred_range("rtime", 1, Inf), ">= 1", fixed = TRUE)
    expect_false(grepl("<=", .pred_range("rtime", 1, Inf), fixed = TRUE))

    # Numeric literals must round-trip exactly: truncating an m/z bound
    # would silently change which spectra match. Assert the round-trip rather
    # than a typed digit string -- the value tested is not exactly
    # representable, so its shortest exact rendering is not what was typed.
    for (v in c(278.09312345678901, 1 / 3, 1e-300, 6.02214076e23)) {
        p <- .pred_range("precursorMz", v, Inf)
        lit <- sub(".*>= ", "", sub(")$", "", p))
        expect_identical(as.numeric(lit), v)
    }
    expect_identical(.sql_num(NA_real_), "NULL")

    expect_identical(.pred_and("a"), "a")
    expect_identical(.pred_or("a"), "a")
    expect_identical(.pred_and(), "TRUE")
    expect_identical(.pred_or(), "FALSE")
})

test_that(".ids_where uses BETWEEN for contiguous runs and defers huge sets", {
    expect_null(.ids_where(1:10, full = TRUE))
    expect_identical(.ids_where(integer()), "FALSE")
    expect_identical(.ids_where(5:9), "spectrum_id_ BETWEEN 5 AND 9")
    expect_match(.ids_where(c(1L, 5L, 9L)), "IN (1,5,9)", fixed = TRUE)
    # Past the inline cap the caller must switch to a registered id table
    # rather than build hundreds of KB of SQL text per query.
    expect_true(is.na(.ids_where(as.integer(seq(1L, 1e5L, by = 2L)))))
    # ... but a contiguous run of any size still fits in a BETWEEN.
    expect_identical(.ids_where(1:100000),
                     "spectrum_id_ BETWEEN 1 AND 100000")
})

test_that("scattered subsets larger than the inline cap round-trip", {
    be <- .make_large_test_backend()
    on.exit(unlink(.path(be), recursive = TRUE), add = TRUE)

    # Non-contiguous and larger than .MAX_INLINE_IDS, so this can only be
    # answered through the registered-id-table join.
    idx <- seq(1L, length(be), by = 2L)
    expect_gt(length(idx), .MAX_INLINE_IDS)
    sub <- be[idx]

    expect_equal(length(sub), length(idx))
    expect_equal(spectraData(sub, "totIonCurrent")[, 1L], as.numeric(idx))

    pks <- peaksData(sub)
    expect_length(pks, length(idx))
    expect_equal(pks[[1L]][, "mz"], 100 + seq_len(2L))
    expect_equal(pks[[length(idx)]][, "mz"],
                 tail(idx, 1L) * 100 + seq_len((tail(idx, 1L) %% 3L) + 1L))
})

test_that("a scattered, out-of-order subset comes back in the asked order", {
    be <- .make_large_test_backend()
    on.exit(unlink(.path(be), recursive = TRUE), add = TRUE)

    idx <- c(2000L, 3L, 1500L, 1L, 2999L)
    sub <- be[idx]
    expect_equal(spectraData(sub, "totIonCurrent")[, 1L], as.numeric(idx))
    expect_equal(vapply(peaksData(sub), function(m) m[1L, "mz"], numeric(1)),
                 idx * 100 + 1)
    expect_equal(as.numeric(mz(sub)[[1L]])[1L], 2000 * 100 + 1)
})

test_that("filterRt with msLevel. keeps spectra of other MS levels", {
    be <- .make_large_test_backend()
    on.exit(unlink(.path(be), recursive = TRUE), add = TRUE)

    # MS1 spectra outside the rt window are dropped; every MS2 spectrum is
    # untouched regardless of its rt.
    res <- filterRt(be, c(10, 20), msLevel. = 1L)
    sd <- spectraData(res, c("msLevel", "rtime"))
    ms1 <- sd$msLevel == 1L
    expect_true(all(sd$rtime[ms1] >= 10 & sd$rtime[ms1] <= 20))
    expect_equal(sum(!ms1), sum(msLevel(be) == 2L))
})

test_that("the DuckDB connection is per-process", {
    skip_on_os("windows")
    skip_if_not_installed("BiocParallel")

    be <- .make_test_backend()
    on.exit(unlink(.path(be), recursive = TRUE), add = TRUE)
    parent_con <- .duckdb_con()
    parent_pid <- .duckdb_state$pid
    expect_identical(parent_pid, Sys.getpid())

    # A forked worker inherits the parent's external pointer, for which
    # dbIsValid() still reports TRUE even though the DuckDB instance behind
    # it belongs to the parent. The pid guard has to notice and open its own.
    res <- BiocParallel::bplapply(
        1:2, function(i) {
            pk <- MsBackendParquet::peaksData(be)
            list(pid = Sys.getpid(),
                 state_pid = MsBackendParquet:::.duckdb_state$pid,
                 mz1 = pk[[1L]][, "mz"])
        },
        BPPARAM = BiocParallel::MulticoreParam(2))

    for (r in res) {
        expect_identical(r$state_pid, r$pid)
        expect_equal(r$mz1, c(100, 110))
    }
    # The parent's own connection must have survived the children exiting.
    expect_true(DBI::dbIsValid(parent_con))
    expect_equal(peaksData(be)[[1L]][, "mz"], c(100, 110))
})
