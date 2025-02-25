test_that("as_timestamptz_vec", {
  expect_equal(as_timestamptz_vec("1991_05_05", "12:12:13", "Pacific/Honolulu"),
               ymd_hms("1991-05-05 12:12:13", tz = "Pacific/Honolulu"))
})

test_that("as_timestamptz_vec tz", {
  expect_equal(with_tz(as_timestamptz_vec("1991_05_05", "12:12:13", "Pacific/Honolulu"), tzone = "UTC"),
               ymd_hms("1991-05-05 22:12:13", tz = "UTC"))
})

test_that("as_timestamptz_vec tz_vec", {
  expect_equal(with_tz(as_timestamptz_vec(c("1991_05_05","1991_05_05"), c("12:12:13", "12:12:13"), c("America/Sao_Paulo", "Pacific/Honolulu")), tzone = "UTC"),
               ymd_hms(c("1991-05-05 15:12:13 UTC", "1991-05-05 22:12:13 UTC")))
})

test_that("as_timestamptz_vec tz_vec tz_out", {
  expect_equal(as_timestamptz_vec(c("1991_05_05","1991_05_05"), c("12:12:13", "12:12:13"), c("America/Sao_Paulo", "Pacific/Honolulu"), tz_out = "UTC"),
               ymd_hms(c("1991-05-05 15:12:13 UTC", "1991-05-05 22:12:13 UTC")))
})

test_that("as_timestamptz_vec tz_vec tz_out", {
  expect_equal(as_timestamptz_vec(c("1991_05_05","1991_05_05"), c("12:12:13", "12:12:13"), c("America/Sao_Paulo", "Pacific/Honolulu"), tz_out = c("UTC", "Pacific/Honolulu")),
               list(with_tz(ymd_hms("1991-05-05 15:12:13"), tzone = "UTC"),
                    with_tz(ymd_hms("1991-05-05 12:12:13") + hours(10), tzone = "Pacific/Honolulu")))
})
