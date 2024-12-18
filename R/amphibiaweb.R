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
#' @importFrom taxize gna_verifier
#' @importFrom purrr map_df safely
#' @importFrom dplyr tibble
#' @export

scrape_amphibiaweb <- function(sci_nam) {
  base_url <- "https://amphibiaweb.org/cgi/amphib_ws?where-genus={genus}&where-species={species}&src=eol"

  process_name <- function(name_submitted) {
    cat("\033[38;5;240m", "Processing -- ", name_submitted, "\n")

    safe_gna_verifier <- safely(gna_verifier)
    gna_results <- safe_gna_verifier(name_submitted, data_sources = 118, capitalize = TRUE)

    if (is.null(gna_results$result) || nrow(gna_results$result) == 0) {
      return(tibble(
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
      ))
    }

    name_matched <- as.character(gna_results$result[1, 'matchedName'])
    name_parts <- strsplit(name_matched, " ")[[1]]
    genus <- name_parts[1]
    species <- name_parts[2]

    url <- gsub("\\{genus\\}", genus, base_url)
    url <- gsub("\\{species\\}", species, url)

    tryCatch({
      response <- GET(url)
      stop_for_status(response)
      xml_data <- content(response, as = "text", encoding = "UTF-8")
      parsed_xml <- read_xml(xml_data)

      extract_value <- function(xpath) {
        value <- xml_text(xml_find_first(parsed_xml, paste0("//species", xpath)), trim = TRUE)
        if (length(value) == 0 || value == "") NA else value
      }

      tibble(
        name_submitted = name_submitted,
        scientific_name = name_matched,
        record_id = gna_results$result[1, 'recordId'],
        sort_score = gna_results$result[1, 'sortScore'],
        matched_name_id = gna_results$result[1, 'matchedNameID'],
        amphib_id = extract_value("//amphib_id"),
        ordr = extract_value("//ordr"),
        family = extract_value("//family"),
        subfamily = extract_value("//subfamily"),
        genus = extract_value("//genus"),
        species = extract_value("//species"),
        clade = extract_value("//clade"),
        common_name = extract_value("//common_name"),
        url = url
      )
    }, error = function(e) {
      warning(paste("Error processing", name_submitted, ":", e$message))
      return(tibble(
        name_submitted = name_submitted,
        scientific_name = name_matched,
        record_id = gna_results$result[1, 'recordId'],
        sort_score = gna_results$result[1, 'sortScore'],
        matched_name_id = gna_results$result[1, 'matchedNameID'],
        amphib_id = NA,
        ordr = NA,
        family = NA,
        subfamily = NA,
        genus = NA,
        species = NA,
        clade = NA,
        common_name = NA,
        url = url
      ))
    })
  }

  results <- map_df(sci_nam, process_name)
  return(results)
}
