#' Scrape taxa metadata from AmphibiaWeb
#'
#' Searches scientific names for corresponding species in AmphibiaWeb
#' @param sci_nam Scientific name(s) of species of interest
#' @return Associated metadata (if any)
#' @examples
#' scientific_names <- c("Rana muscosa", "Oedipina gracilis")
#' results <- scrape_amphibiaweb(scientific_names)
#' print(results)
#' @importFrom httr GET stop_for_status content
#' @importFrom xml2 read_xml xml_text xml_find_first
#' @importFrom purrr map_df
#' @importFrom dplyr tibble
#' @export

scrape_amphibiaweb <- function(sci_nam) {
  if (!requireNamespace("taxize", quietly = TRUE)) {
    stop("The 'taxize' package is required but not installed. Please install it to use this function.")
  }

  base_url <- "https://amphibiaweb.org/cgi/amphib_ws?where-genus={genus}&where-species={species}&src=eol"

  process_name <- function(name_submitted) {
    cat("\033[38;5;240m", "Processing -- ", name_submitted, "\n")

    # Initialize result tibble with default NA values
    result <- tibble(
      name_submitted = name_submitted,
      scientific_name = NA,
      record_id = NA,
      sort_score = NA,
      matched_name_id = NA,
      amphib_id = NA,
      ordr = NA,
      family = NA,
      subfamily = NA,
      genus = NA,
      species = NA,
      clade = NA,
      common_name = NA,
      url = NA
    )

    # Attempt GNA verification
    tryCatch({
      gna_results <- taxize::gna_verifier(name_submitted, data_sources = 118, capitalize = TRUE)
      if (!is.null(gna_results) && nrow(gna_results) > 0) {
        result$scientific_name <- as.character(gna_results[1, 'matchedName'])
        result$record_id <- gna_results[1, 'recordId']
        result$sort_score <- gna_results[1, 'sortScore']
        result$matched_name_id <- gna_results[1, 'matchedNameID']
      }
    }, error = function(e) {
      # warning(paste("GNA verification failed for", name_submitted, ":", e$message))
    })

    # Attempt AmphibiaWeb scraping
    tryCatch({
      name_parts <- strsplit(ifelse(is.na(result$scientific_name), name_submitted, result$scientific_name), " ")[[1]]
      genus <- name_parts[1]
      species <- if (length(name_parts) > 1) name_parts[2] else ""

      url <- gsub("\\{genus\\}", genus, base_url)
      url <- gsub("\\{species\\}", species, url)
      result$url <- url

      response <- GET(url)
      stop_for_status(response)
      xml_data <- content(response, as = "text", encoding = "UTF-8")
      parsed_xml <- read_xml(xml_data)

      extract_value <- function(xpath) {
        value <- xml_text(xml_find_first(parsed_xml, paste0("//species", xpath)), trim = TRUE)
        if (length(value) == 0 || value == "") NA else value
      }

      result$amphib_id <- extract_value("//amphib_id")
      result$ordr <- extract_value("//ordr")
      result$family <- extract_value("//family")
      result$subfamily <- extract_value("//subfamily")
      result$genus <- extract_value("//genus")
      result$species <- extract_value("//species")
      result$clade <- extract_value("//clade")
      result$common_name <- extract_value("//common_name")
    }, error = function(e) {
      # warning(paste("AmphibiaWeb scraping failed for", name_submitted, ":", e$message))
    })

    return(result)
  }

  results <- map_df(sci_nam, process_name)
  return(results)
}
