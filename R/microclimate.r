
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
