
data <- data.table::fread(
  system.file('testdata/local_time_vec_data.csv', package = 'ribbitrrr'))

test_that("local_time_vec vec-tez", {
  expect_equal(local_time_vec(data$timestamptz, data$tz, drop_tz = FALSE) , data$timestamptz)
})

test_that("local_time_vec vec-tez drop_tz", {
  p = ymd_hms(local_time_vec(data$timestamptz, data$tz, drop_tz = TRUE))
  d = ymd_hms(data$utc)
  for( n in seq_along(p)) {
    expect_equal(p[[n]], d[n])
  }
})

test_that("local_time_vec", {
  expect_equal(local_time_vec(data$timestamptz, "UTC", drop_tz = TRUE) , as.character(data$timestamptz))
})

test_that("local_time_vec drop_tz", {
  p = local_time_vec(data$timestamptz, "UTC", drop_tz = FALSE)
  d = data$utc

  for (n in 1:3) {
    expect_true(p[n] == d[n])
  }

  for (n in 4:24) {
    expect_true(p[n] != d[n])
  }
})
