#' Query microclimate data for a buffer period preceding a given survey
#'
#' From a table of surveys, returns microclimate data from desired timeseries from the available, corresponding sites for a buffer window preceing the survey start.
#' @param survey_data table containing columns survey_id, site_id, date, start_time (or start_timestamp_utc if time_sensitive = TRUE)
#' @param dbcon DBI database connection object (e.g. returned by \link[ribbitrrr]{hopToDB})
#' @param buffer a lubridate time period (e.g. \link[lubridate]{days}) of interest for time series data query preceding each survey
#' @param time_series_pointer a \link[dplyr]{tbl} object for the time series table of interest (e.g. tbl(dbcon, Id("microclimate_data", "ts_temperature") for temperature data)
#' @param time_sensitive Boolean: Do you want to query by time using survey start_timestamp_utc? If FALSE, queried by date only.
#' @param output_timezone What timezone do you want resulting data to be output in (default: NA <=> UTC)? Format as string, options found in \link[base]{OlsonNames}
#' @return coinciding time series data (where available) with an associated survey_id
#' @importFrom dplyr tbl %>% select distinct filter pull mutate left_join collect
#' @importFrom DBI Id
#' @importFrom purrr pmap pmap_df
#' @export
microclimate_presurvey = function(survey_data, dbcon, buffer, time_series_pointer, time_sensitive = FALSE, output_timezone = NA) {

  # microclimate table pointers
  db_sensor = tbl(dbcon, Id("microclimate_data", "sensor"))
  db_logger = tbl(dbcon, Id("microclimate_data", "logger"))

  # check data format
  if (is.data.frame(survey_data)) {
    data_cols = colnames(survey_data)
  } else {
    stop("survey_data format not recognized. Expected data frame.")
  }

  # time/date check
  if (time_sensitive) {
    # check for timestamp
    if ("start_timestamp_utc" %in% data_cols) {
      # check format
      if (!all(class(survey_data$start_timestamp_utc) == c("POSIXct", "POSIXt"))) {
        stop("Unexpected format of 'start_timestamp' column. Expected c('POSIXct', 'POSIXt')")
      }
    } else {
      stop("Required column 'start_timestamp' not found in survey_data")
    }
  } else {
    # check for date
    if ("date" %in% data_cols) {
      # check format
      if (!is.Date(survey_data$date)) {
        stop("Unexpected format of 'date' column. Expected Date.")
      }
    } else {
      stop("Required column 'date' not found in survey_data")
    }
  }

  # check for site_id
  if ("site_id" %in% data_cols) {
    # check format
    if (!is.character(survey_data$site_id)) {
      stop("Unexpected format of 'site_id' column. Expected character.")
    }
  } else {
    stop("Required column 'site_id' not found in survey_data")
  }

  # check for survey_id
  if ("survey_id" %in% data_cols) {
    # check format
    if (!is.character(survey_data$site_id)) {
      stop("Unexpected format of 'site_id' column. Expected character.")
    }
  } else {
    stop("Required column 'site_id' not found in survey_data")
  }

  # parse buffer
  if (is.na(buffer)) {
    buffer = days(0)
  } else {
    if (!class(buffer) == "Period") {
      stop("'buffer' format not recognized. Expected lubridate 'Period'.")
    }
  }

  # build sites_present list
  sites_data = unique(sort(survey_data$site_id))

  sites_mc = db_logger %>%
    select(site_id) %>%
    distinct() %>%
    filter(site_id %in% sites_data) %>%
    pull(site_id)

  sites_present = intersect(sites_data, sites_mc)
  sites_absent = setdiff(sites_data, sites_mc)

  if (length(sites_absent) != 0) {
    warning(paste0("The following ", length(sites_absent), " site_id's found in provided data have no associated microclimate loggers and will be ignored: ", paste(sites_absent, collapse = "\n")))
  }

  if (time_sensitive) {
    survey_start_end = survey_data %>%
      filter(site_id %in% sites_present) %>%
      mutate(start = start_timestamp_utc - buffer,
             end = start_timestamp_utc) %>%
      select(survey_id, site_id, start, end)

    time_var = "start_timestamp"
  } else {
    survey_start_end = survey_data %>%
      filter(site_id %in% sites_present) %>%
      mutate(start = date - buffer,
             end = date) %>%
      select(survey_id, site_id, start, end)

    time_var = "date"
  }

  invalid_start_end = survey_start_end %>%
    filter(is.na(start) | is.na(end)) %>%
    pull(survey_id)

  if (length(invalid_start_end) > 0) {
    warning(paste0("NA values for survey ", time_var, " for the following surveys:\n\t"),
            paste0(invalid_start_end, sep = "\n\t"),
            "\n")
  }

  survey_start_end_valid = survey_start_end %>%
    filter(!(survey_id %in% invalid_start_end))


  if (nrow(survey_start_end_valid) > 0) {
    # build single compound query to fetch all data (less expensive than hundreds of queries)
    message("Pulling data from server... ", appendLF = FALSE)
    # create filter expression for one row
    create_row_filter <- function(site_id, start, end) {
      expr(
        (site_id == !!site_id &
           timestamp_utc >= !!start &
           timestamp_utc <= !!end)
      )
    }

    # create full filter expression
    create_full_filter <- function(filter_df) {
      row_filters <- pmap(filter_df, create_row_filter)
      reduce(row_filters, ~ expr(!!.x | !!.y))
    }

    # Create filter
    full_filter <- create_full_filter(survey_start_end_valid %>%
                                        select(-survey_id))
    # fetch all data
    mc_data = time_series_pointer %>%
      left_join(db_sensor, by = "sensor_id") %>%
      left_join(db_logger, by = "logger_id") %>%
      filter(!!full_filter) %>%
      select(site_id,
             all_of(colnames(time_series_pointer))) %>%
      collect()
    message("done.")

    # map data to each survey
    message("Remapping data to surveys... ", appendLF = FALSE)
    mc_data_mapped = pmap_df(survey_start_end_valid,
                             function(survey_id, site_id, start, end) {
                               mc_data %>%
                                 filter(site_id == site_id,
                                        timestamp_utc >= start,
                                        timestamp_utc <= end) %>%
                                 mutate(survey_id = survey_id) %>%
                                 select(-site_id)
                             })
    message("done.")

  } else {
    stop("No valid surveys for given parameters, query aborted.")
  }

  # join with metadata
  mc_meta = db_sensor %>%
    left_join(db_logger, by = "logger_id") %>%
    collect()

  mc_data_final = mc_data_mapped %>%
    left_join(mc_meta, by = "sensor_id") %>%
    select(survey_id,
           sensor_id,
           everything()) %>%
    mutate(timestamp_utc = force_tz(timestamp_utc,tzone = "UTC"))

  # reset to initial timezone
  if (!is.na(output_timezone) & time_sensitive) {
    mc_data_final = mc_data_final %>%
      mutate(timestamp_tz = with_tz(timestamp_utc, tzone = output_timezone))
  }

  return(mc_data_final)

}
