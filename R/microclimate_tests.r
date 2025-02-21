librarian::shelf(tidyverse, dbplyr, here, janitor, dataPreparation, lubridate, RPostgres, stringr, DBI, parsedate, uuid, hms, RIBBiTR-BII/ribbitrrr, ggplot2, plotly)


dbcon <- hopToDB("ribbitr")

dat = tbl(dbcon, Id("survey_data", "timestamp_test")) %>%
  collect()

dis_dat = dat %>%
  mutate(new_col = local_time_vec(timestamptz, tz, drop_tz = TRUE))
dis_dat$new_col

dis_dat = dat %>%
  mutate(new_col = local_time_vec(timestamptz, tz, drop_tz = FALSE))
dis_dat$new_col

dis_dat = dat %>%
  mutate(new_col = local_time_vec(timestamptz, "UTC", drop_tz = TRUE))
dis_dat$new_col

dis_dat = dat %>%
  mutate(new_col = local_time_vec(timestamptz, "UTC", drop_tz = FALSE))
dis_dat$new_col


this = as_timestamptz_vec("1991_05_05", "12:12:13", "Pacific/Honolulu")
this

this = as_timestamptz_vec("1991_05_05", "12:12:13", "Pacific/Honolulu", tz_out = "UTC")
this

this = as_timestamptz_vec(c("1991_05_05","1991_05_05"), c("12:12:13", "12:12:13"), c("America/Sao_Paulo", "Pacific/Honolulu"))
this

this = as_timestamptz_vec(c("1991_05_05","1991_05_05"), c("12:12:13", "12:12:13"), c("America/Sao_Paulo", "Pacific/Honolulu"), tz_out = "UTC")
this

this = as_timestamptz_vec(c("1991_05_05","1991_05_05"), c("12:12:13", "12:12:13"), c("America/Sao_Paulo", "Pacific/Honolulu"), tz_out = c("UTC", "Pacific/Honolulu"))
this

