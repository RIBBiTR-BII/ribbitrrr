test_that("survey_timestamps_utc: timed rows as start/end_timestamp_utc, dated rows span the local day", {
  ts = survey_timestamps_utc(as.Date(c("2022-05-10", "2022-05-10", "2022-05-10", NA)),
                             c("21:30:00", "23:30:00", NA, NA),
                             c("23:10:00", "01:15:00", NA, NA),
                             "America/New_York")

  expect_equal(ts$timestamp_precision, c("minute", "minute", "day", NA))
  expect_equal(ts$start_timestamp_utc[1], as.POSIXct("2022-05-11 01:30:00", tz = "UTC"))
  expect_equal(ts$end_timestamp_utc[1], as.POSIXct("2022-05-11 03:10:00", tz = "UTC"))
  # end after midnight rolls to the next day
  expect_equal(ts$end_timestamp_utc[2], as.POSIXct("2022-05-11 05:15:00", tz = "UTC"))
  # date only: local midnight to local midnight (EDT, UTC-4)
  expect_equal(ts$start_timestamp_utc[3], as.POSIXct("2022-05-10 04:00:00", tz = "UTC"))
  expect_equal(ts$end_timestamp_utc[3], as.POSIXct("2022-05-11 04:00:00", tz = "UTC"))
  expect_true(is.na(ts$start_timestamp_utc[4]))
})

test_that("survey_timestamps_utc: day spans follow DST changes", {
  ts = survey_timestamps_utc(as.Date("2024-03-10"), NA, NA, "America/New_York")
  expect_equal(as.numeric(difftime(ts$end_timestamp_utc, ts$start_timestamp_utc, units = "hours")), 23)
})

test_that("survey_timestamps_utc: span covers all rows", {
  ts = survey_timestamps_utc(as.Date(c("2022-05-10", "2022-05-11", NA)), NA, NA, "America/New_York", span = TRUE)
  expect_equal(ts$start_timestamp_utc, rep(as.POSIXct("2022-05-10 04:00:00", tz = "UTC"), 3))
  expect_equal(ts$end_timestamp_utc, rep(as.POSIXct("2022-05-12 04:00:00", tz = "UTC"), 3))
  expect_equal(ts$timestamp_precision, rep("day", 3))
  # nothing known: left NA
  ts = survey_timestamps_utc(as.Date(NA), NA, NA, "America/New_York", span = TRUE)
  expect_true(is.na(ts$start_timestamp_utc) & is.na(ts$timestamp_precision))
})

test_that("match_visit: exact, then same date, then night; ties left unmatched", {
  visits = data.frame(site_id = c("a", "a", "a", "b", "b", "c"),
                      date = as.Date(c("2022-05-11", "2022-06-23", "2022-06-23", "2022-07-01", "2022-07-03", "2022-08-01")),
                      time_of_day = c("night", "day", "night", "night", "night", "day"),
                      visit_id = c("v1", "v2", "v3", "v4", "v5", "v6"))
  data = data.frame(site_id = c("a", "a", "a", "b", "c", "c", "d"),
                    date = as.Date(c("2022-05-10", "2022-06-23", "2022-06-23", "2022-07-02", "2022-08-01", "2022-08-05", "2022-08-01")),
                    time_of_day = c(NA, NA, "day", NA, "day", NA, NA))

  out = suppressWarnings(match_visit(data, visits))

  expect_equal(out$visit_match, c("nearby", "nearby", "exact", "ambiguous", "exact", "none", "none"))
  expect_equal(out$matched_visit_id, c("v1", "v3", "v2", NA, "v6", NA, NA))
  expect_equal(out$visit_date, as.Date(c("2022-05-11", "2022-06-23", "2022-06-23", "2022-07-02", "2022-08-01", "2022-08-05", "2022-08-01")))
  # collection date untouched
  expect_equal(out$date, data$date)
  expect_warning(match_visit(data, visits), "ambiguous")
})

test_that("match_visit: same date preferred over matching time of day on another date", {
  visits = data.frame(site_id = c("a", "a"), date = as.Date(c("2023-02-07", "2023-02-08")), time_of_day = c("night", "day"))
  data = data.frame(site_id = "a", date = as.Date("2023-02-07"), time_of_day = "day")
  out = match_visit(data, visits)
  expect_equal(out$visit_time_of_day, "night")
  expect_equal(out$visit_match, "nearby")
  expect_true(is.na(out$matched_visit_id))
})
