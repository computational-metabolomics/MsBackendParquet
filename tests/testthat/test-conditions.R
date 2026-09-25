test_that("mzstackError() signals a condition of the named class", {
    e <- tryCatch(mzstackError("stale", "run ", "A", " changed"),
                  error = identity)
    expect_s3_class(e, "mzstack_stale")
    expect_s3_class(e, "mzstack_error")
    expect_s3_class(e, "error")
    expect_identical(conditionMessage(e), "run A changed")
    expect_null(conditionCall(e))
})

test_that("every specified class can be raised and caught by name", {
    for (cl in c("format", "archive", "study", "stale", "unsupported",
                 "semantic", "capability", "resource"))
        expect_error(mzstackError(cl, "x"), class = paste0("mzstack_", cl))
})

test_that("an unknown class is refused", {
    expect_error(mzstackError("reference", "x"), "should be one of")
})

test_that("extra fields travel on the condition", {
    e <- mzstackCondition("stale", "stale", data = list(source = "study_A",
                                                        run_id = "QC01"))
    expect_identical(e$source, "study_A")
    expect_identical(e$run_id, "QC01")
    expect_error(mzstackCondition("stale", "x", data = list(1)), "named list")
})
