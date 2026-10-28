
#' Convert timestamp to local time
#'
#' Takes in timestamp with timezone data and local timezone, outputs in local time.
#' @param timestamptz Column name of timestamp data
#' @param local_tz Column name specifying the local timezone, otherwise a valid timezone (sting) \link[base]{OlsonNames}
#' @param drop_tz do you want to discard timezone info in output? If yes, returns a datetime column. If no, returns a list of datetimes with (possibly) varying timezone attached.
#' @return Returns data frame with additional column specified
#' @examples
#'
#'data_local = data %>%
#'mutate(local_time_data = local_time_db(timestamptz, tz))
#'
#' @importFrom dplyr mutate %>%
#' @importFrom lubridate with_tz
#' @importFrom purrr pmap pmap_vec
#' @export
#'
local_time_vec = function(timestamptz, local_tz, drop_tz = TRUE) {

  if (drop_tz) {
    output = pmap_vec(list({{timestamptz}}, {{local_tz}}), ~format(with_tz(..1, tzone = ..2)), "%Y-%m-%d %H:%M:%S")
  } else {
    if (length(unique({{local_tz}})) > 1) {
      output = pmap(list({{timestamptz}}, {{local_tz}}), ~with_tz(..1, tzone = ..2))
    } else {
      output = pmap_vec(list({{timestamptz}}, {{local_tz}}), ~with_tz(..1, tzone = ..2))
    }
  }

  return(output)
}


#' Vectorized assigning of timezones, for (potentially) multiple timezones
#'
#' Takes in date, time, and timezone vectors, and combines them into a single timestamp with timezone vector.
#' @importFrom lubridate ymd_hms
#' @importFrom purrr pmap_vec
#' @export
as_timestamptz_vec = function(date, time, timezone, tz_out = NA) {
  output = pmap_vec(list(date, time, timezone),
           ~ ymd_hms(paste0(..1, " ", ..2), tz = ..3))

  if (!is.na(first(tz_out))) {
    output = local_time_vec(output, local_tz = tz_out, drop_tz = FALSE)
  }


  return(output)
}

#' Determine first and last times from times without dates, assuming less than 24 hours among group.
#'
#' Takes in times, determines the first and last values assuming 24h time format and a max time range "max_range" < 24h
#' @param times vector of all times to be considered
#' @param max_range max range considered when calculating times, in hours. must be < 24
#' @importFrom lubridate ymd_hms
#' @importFrom purrr pmap_vec
#' @export

find_time_range <- function(times, max_range = 12) {

  if (max_range >= 24) {
    stop("max_range must be < 24 hours")
  }

  # Remove NA values
  times <- times[!is.na(times)]

  if (length(times) == 0) {
    return(list(first_time = NA, last_time = NA))
  }

  # Convert to hours for calculation
  hours <- hour(times) + minute(times)/60 + second(times)/3600

  # Check if times span midnight
  max_diff <- max(hours) - min(hours)

  if (max_diff > max_range) {
    # Times span midnight, adjust hours
    hours_adjusted <- ifelse(hours < max_range, hours + 24, hours)

    # Find min and max in adjusted time
    first_time <- times[which.min(hours_adjusted)]
    last_time <- times[which.max(hours_adjusted)]
  } else {
    # No midnight spanning
    first_time <- times[which.min(hours)]
    last_time <- times[which.max(hours)]
  }

  return(list(
    first_time = first_time,
    last_time = last_time
  ))
}

#' start_timestamp_local
#'
#' converts time, date, timezone to a timestamp
#' @param date date as Date
#' @param start_time as time
#' @param tz timezone from OlsonNames()
#' @importFrom lubridate ymd_hms force_tz
#' @export
start_timestamp_local = function(date, start_time, tz) {
  force_tz(ymd_hms(paste(date, start_time)), tzone = tz)
}

#' start_timestamp_utc
#'
#' converts time, date, and original timezone to a timestamp in UTC
#' @param date date as Date
#' @param start_time as time
#' @param tz timezone from OlsonNames()
#' @importFrom lubridate with_tz
#' @export
start_timestamp_utc = function(date, start_time, tz) {
  stl = start_timestamp_local(date, start_time, tz)
  return(with_tz(stl, tzone = "UTC"))
}

#' end_timestamp_local
#'
#' converts time, date, timezone to a timestamp, assuming the end_time is between 0 and 24 hours after the start time and date.
#' @param date date as Date
#' @param start_time as time
#' @param end_time as time
#' @param tz timezone from OlsonNames()
#' @importFrom lubridate ymd_hms force_tz days
#' @export
end_timestamp_local = function(date, start_time, end_time, tz) {
  start = force_tz(ymd_hms(paste(date, start_time)), tzone = tz)
  end_1 = force_tz(ymd_hms(paste(date, end_time)), tzone = tz)
  dur_1 = as.numeric(difftime(end_1, start, units = "mins"))
  end_date = as.Date(date - days(floor(dur_1 / 1440)))
  end_timestamp = force_tz(ymd_hms(paste(end_date, end_time)), tzone = tz)
  return(end_timestamp)
}

#' end_timestamp_utc
#'
#' converts time, date, and original timezone to a timestamp in UTC, assuming the end_time is between 0 and 24 hours after the start time and date.
#' @param date date as Date
#' @param start_time as time
#' @param tz timezone from OlsonNames()
#' @importFrom lubridate with_tz
#' @export
end_timestamp_utc = function(date, start_time, end_time, tz) {
  etl = end_timestamp_local(date, start_time, end_time, tz)
  return(with_tz(etl, tzone = "UTC"))
}

#' timestamp_of_capture_utc
#'
#' calculates timestamp of capture from date, time_of_capture, and time_zone. If the timestamp of capture is before the start timestamp, it adds 24 hours to the timestamp of capture.
#' @param df dataframe with "date", "time_of_capture", and "time_zone" columns
#' @param tz timezone from OlsonNames(). If NA, uses the `time_zone` column in the dataframe.
#' @importFrom lubridate ymd_hms as_datetime hours
#' @export
timestamp_of_capture_utc = function(df, tz = NA) {

  if ("time_of_capture" %in% colnames(df)) {
    if (is.na(tz)) {
      df_out = df %>%
        mutate(timestamp_of_capture_utc = start_timestamp_utc(date, time_of_capture, time_zone),
               timestamp_of_capture_utc = as_datetime(ifelse(timestamp_of_capture_utc < start_timestamp_utc,
                                                             timestamp_of_capture_utc + hours(24),
                                                             timestamp_of_capture_utc)))
    } else {
      df_out = df %>%
        mutate(timestamp_of_capture_utc = start_timestamp_utc(date, time_of_capture, tz),
               timestamp_of_capture_utc = as_datetime(ifelse(timestamp_of_capture_utc < start_timestamp_utc,
                                                             timestamp_of_capture_utc + hours(24),
                                                             timestamp_of_capture_utc)))
    }
    return(df_out)

  } else {
    warning("time_of_capture column not found in dataframe, skipping 'timestamp_of_capture_utc' calculation.")
    return(df)
  }

}


#' time duration from timestamp
#'
#' calculates time duration from start and end timestamps in minutes, rounded to the nearest whole
#' @param start_timestamp timestamp #1
#' @param end_timestamp timestamp #2
#' @export
duration_minutes = function(start_timestamp, end_timestamp) {
  return(as.integer(round(as.numeric(difftime(end_timestamp, start_timestamp, units = "mins")), digits = 0)))
}

#' Count observers
#'
#' counts number of observers in sting (separated by ","). use purrr::map_chr to apply within groups.
#' @param start_timestamp timestamp #1
#' @param end_timestamp timestamp #2
#' @export
count_observers = function(observers) {
  c_obs = length(unique(na.omit(unlist(str_split(gsub(" ", "", observers), ",")))))
  c_obs[c_obs == 0] = NA
  return(c_obs)
}

#' Clean up survey times
#'
#' takes in dataframe with "date", "start_time", "end_time" columns, as well as a local timezone tz, calculates start_timestamp_utc, end_timestamp_utc, and duration_minutes.
#' @param df dataframe with "date", "start_time", "end_time" columns
#' @param tz valid timezone as sting from OlsonNames()
#' @export
clean_survey_times = function(df, tz) {
  df_out = df %>%
    mutate(start_timestamp_utc = start_timestamp_utc(date, start_time, tz),
           end_timestamp_utc = end_timestamp_utc(date, start_time, end_time, tz),
           duration_minutes = duration_minutes(start_timestamp_utc, end_timestamp_utc))
}


#' survey_timestamps_utc
#'
#' Survey start & end timestamps in UTC, with their precision. Where a start time is known, timestamps are as
#' `start_timestamp_utc()` & `end_timestamp_utc()` (precision "minute"). Where only the date is known, the survey
#' spans that local day: from local midnight to the following local midnight (precision "day").
#' Returns a data frame, so it can be used unnamed in `mutate()` to add all three columns. With `span = TRUE` (e.g. in a
#' `mutate()` grouped by survey) every row gets the span of all rows: earliest start, latest end, and "day" precision if
#' any row is date only.
#' @param date date as Date (local date of the survey)
#' @param start_time local start time (hms or "HH:MM:SS"), NA where unknown
#' @param end_time local end time (hms or "HH:MM:SS"), NA where unknown
#' @param tz timezone from OlsonNames()
#' @param span Logical: return the span of all rows on every row? FALSE by default.
#' @return data frame with columns start_timestamp_utc, end_timestamp_utc & timestamp_precision
#' @examples
#' survey_timestamps_utc(as.Date(c("2022-05-10", "2022-05-10")),
#'                       c("21:30:00", NA), c("23:10:00", NA), "America/New_York")
#' @export
survey_timestamps_utc = function(date, start_time, end_time = NA, tz, span = FALSE) {
  if (length(tz) != 1 || is.na(tz)) {
    stop("tz must be a single time zone from OlsonNames()")
  }

  n = max(length(date), length(start_time), length(end_time))
  date = rep(as.Date(date), length.out = n)
  start_time = rep(start_time, length.out = n)
  end_time = rep(end_time, length.out = n)

  timed = !is.na(date) & !is.na(start_time)
  timed_end = timed & !is.na(end_time)
  dated = !is.na(date) & is.na(start_time)

  start_ts = .POSIXct(rep(NA_real_, n), tz = "UTC")
  end_ts = .POSIXct(rep(NA_real_, n), tz = "UTC")

  if (any(timed)) {
    start_ts[timed] = start_timestamp_utc(date[timed], start_time[timed], tz)
  }
  if (any(timed_end)) {
    end_ts[timed_end] = end_timestamp_utc(date[timed_end], start_time[timed_end], end_time[timed_end], tz)
  }
  # date only: the whole local day
  if (any(dated)) {
    start_ts[dated] = start_timestamp_utc(date[dated], "00:00:00", tz)
    end_ts[dated] = start_timestamp_utc(date[dated] + 1, "00:00:00", tz)
  }

  precision = rep(NA_character_, n)
  precision[timed] = "minute"
  precision[dated] = "day"

  if (span && any(!is.na(start_ts))) {
    start_ts[] = min(start_ts, na.rm = TRUE)
    end_ts[] = if (any(!is.na(end_ts))) max(end_ts, na.rm = TRUE) else NA
    precision[] = if (any(precision %in% "day")) "day" else "minute"
  }

  data.frame(start_timestamp_utc = start_ts,
             end_timestamp_utc = end_ts,
             timestamp_precision = precision)
}
