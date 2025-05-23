% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/taxonomy.r
\name{scrape_cites}
\alias{scrape_cites}
\title{Scrape taxa metadata from CITES}
\usage{
scrape_cites(sci_nam, authentication_token, quietly = FALSE)
}
\arguments{
\item{sci_nam}{Scientific name(s) of species of interest}

\item{authentication_token}{token for CITES api}

\item{quietly}{TRUE/FALSE, do you want to print each query?}
}
\value{
Associated metadata (if any)
}
\description{
Searches scientific names for entries and status listing of species in CITES db
}
\examples{
scientific_names <- c("Rana muscosa", "Oedipina gracilis")
cites_token = "abcdefg"
results <- scrape_cites(scientific_names, cites_token)
print(results)
}
