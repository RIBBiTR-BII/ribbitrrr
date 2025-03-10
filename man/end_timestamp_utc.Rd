% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/db_time_mgmt.r
\name{end_timestamp_utc}
\alias{end_timestamp_utc}
\title{end_timestamp_utc}
\usage{
end_timestamp_utc(date, start_time, end_time, tz)
}
\arguments{
\item{date}{date as Date}

\item{start_time}{as time}

\item{tz}{timezone from OlsonNames()}
}
\description{
converts time, date, and original timezone to a timestamp in UTC, assuming the end_time is between 0 and 24 hours after the start time and date.
}
