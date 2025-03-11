#' Scrape taxa metadata from AmphibiaWeb
#'
#' Searches scientific names for corresponding species in AmphibiaWeb
#' @param sci_nam Scientific name(s) of species of interest
#' @param quietly TRUE/FALSE, do you want to print each query?
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

scrape_amphibiaweb <- function(sci_nam, quietly = FALSE) {
  if (!requireNamespace("taxize", quietly = TRUE)) {
    stop("The 'taxize' package is required but not installed. Please install it to use this function.")
  }

  base_url <- "https://amphibiaweb.org/cgi/amphib_ws?where-genus={genus}&where-species={species}&src=eol"

  process_name <- function(name_submitted) {

    if (!quietly) {
      cat("\033[38;5;240m", "Processing -- ", name_submitted, "\n")
    }

    # Initialize result tibble with default NA values
    result <- tibble(
      name_submitted = name_submitted,
      amphib_id = NA,
      order = NA,
      family = NA,
      subfamily = NA,
      genus = NA,
      species = NA,
      clade = NA,
      common_name = NA,
      url = NA
    )

    # Attempt AmphibiaWeb scraping
    tryCatch({
      name_parts <- strsplit(name_submitted, " ")[[1]]
      genus <- name_parts[1]
      species <- if (length(name_parts) > 1) name_parts[2] else ""

      url <- gsub("\\{genus\\}", genus, base_url)
      url <- gsub("\\{species\\}", species, url)

      response <- GET(url)
      stop_for_status(response)
      xml_data <- content(response, as = "text", encoding = "UTF-8")
      parsed_xml <- read_xml(xml_data)

      extract_value <- function(xpath) {
        value <- xml_text(xml_find_first(parsed_xml, paste0("//species", xpath)), trim = TRUE)
        if (length(value) == 0 || value == "") NA else value
      }

      result$amphib_id <- extract_value("//amphib_id")
      result$order <- extract_value("//ordr")
      result$family <- extract_value("//family")
      result$subfamily <- extract_value("//subfamily")
      result$genus <- extract_value("//genus")
      result$species <- extract_value("//species")
      result$clade <- extract_value("//clade")
      result$common_name <- extract_value("//common_name")

      if (!is.na(result$amphib_id)) {
        result$url <- url
      }
    }, error = function(e) {
      # warning(paste("AmphibiaWeb scraping failed for", name_submitted, ":", e$message))
    })

    return(result)
  }

  results <- map_df(sci_nam, process_name)
  return(results)
}

#' Scrape taxa metadata from CITES
#'
#' Searches scientific names for entries and status listing of species in CITES db
#' @param sci_nam Scientific name(s) of species of interest
#' @param authentication_token token for CITES api
#' @param quietly TRUE/FALSE, do you want to print each query?
#' @return Associated metadata (if any)
#' @examples
#' scientific_names <- c("Rana muscosa", "Oedipina gracilis")
#' cites_token = "abcdefg"
#' results <- scrape_cites(scientific_names, cites_token)
#' print(results)
#' @importFrom httr GET add_headers
#' @importFrom jsonlite fromJSON
#' @importFrom purrr map_df
#' @importFrom dplyr tibble
#' @export

scrape_cites <- function(sci_nam, authentication_token, quietly = FALSE) {
  if (is.null(authentication_token)) {
    stop("authentication_token required.")
  }

  base_url <- "https://api.speciesplus.net/api/v1/taxon_concepts.xml?name={taxon}"

  process_name <- function(name_submitted, token = authentication_token) {

    if (!quietly) {
      cat("\033[38;5;240m", "Processing -- ", name_submitted, "\n")
    }

    # Initialize result tibble with default NA values
    result <- tibble(
      name_submitted = name_submitted,
      cites_id = NA,
      cites_appendix = NA
    )

    # Attempt CITES scrape
    tryCatch({
      name_submitted = gsub("_", " ", name_submitted)

      response <- GET(
        url = "https://api.speciesplus.net/api/v1/taxon_concepts",
        query = list(name = name_submitted),
        add_headers("X-Authentication-Token" = token)
      )

      # Parse JSON content
      content <- fromJSON(rawToChar(response$content))

      res = content[["taxon_concepts"]][["cites_listings"]][[1]]

      result$cites_id <- res$id
      result$cites_appendix <- res$appendix

    }, error = function(e) {
      if (!quietly) {
        warning(paste("CITES scraping failed for", name_submitted, ":", e$message))
      }
    })

    return(result)
  }

  results <- map_df(sci_nam, process_name)
  return(results)
}

comment_taxa_dict = list("chiasmocleis_cf._atlantica" = "chiasmocleis_cf._atlantica",
                         'ischnocnema_sp._02_(aff._lactea)' = 'ischnocnema_sp._02_(aff._lactea)',
                         'ololygon_aff._brieni' = 'ololygon_aff._brieni',
                         'ololygon_aff._littoralis' = 'ololygon_aff._littoralis',
                         'ololygon_cf_litoralis' = 'ololygon_cf_litoralis',
                         "pristimantis_sp._potential_new_sp" = "potential new species",
                         'rana_catesbeiana_x_rana_clamitans_(possibly)' = 'Rana_catesbeiana or Rana_clamitans',
                         "tadpole_species_1" = "tadpole species 1",
                         "tad_spp_2" = "tadpole species 2")

clean_taxa_dict <- list(
  'boana_platenera' = 'boana_platanera',
  'bolitoglossa_spp' = 'bolitoglossa',
  'boana_bandeirante' = 'boana_bandeirantes',
  'brachycephalus_sp' = 'brachycephalus',
  'brachycephalus_sp.' = 'brachycephalus',
  'bufo_americanus' = 'anaxyrus_americanus',
  'bufo_sp.' = 'bufo',
  'caecilia_spp' = 'caecilia',
  'chiasmocleis_cf._atlantica' = 'chiasmocleis',
  'cochranella_spp' = 'cochranella',
  'colostethus_panamensis' = 'colostethus_panamansis',
  "controle" = NA_character_,
  'craugastor_spp' = 'craugastor',
  'cycloramphus_sp.' = 'cycloramphus',
  'dendrophryniscus_haddadi' = 'dendropsophus_haddadi',
  'desmog_spp' = 'desmognathus',
  'desmog_spp.' = 'desmognathus',
  'desmoganthus_sp.' = 'desmognathus',
  'desmognathus_so' = 'desmognathus',
  'Desmognathaus_sp.' = 'desmognathus',
  'desmognathus_sp.' = 'desmognathus',
  'desmongnathus_sp' = 'desmognathus',
  'diasporus_spp' = 'diasporus',
  'diasporus_spp.' = 'diasporus',
  'duellmanohyla_spp' = 'duellmanohyla',
  'esparadana_prosoblepon' = 'espadarana_prosoblepon',
  'eurycea_bislaneata' = 'eurycea_bislineata',
  "hadadus_binotatus" = "haddadus_binotatus",
  'hyalinobatrachium_fleishmanni' = 'hyalinobatrachium_fleischmanni',
  'hyalinobatrachium_spp' = 'hyalinobatrachium',
  'hyliola_regilla' = 'pseudacris_regilla',
  "hyllodes_phyllodes" = "hylodes_phyllodes",
  "ischnocnema__henselii" = "ischnocnema_henselii",
  'ischnocnema_sp' = 'ischnocnema',
  'ischnocnema_sp._02_(aff._lactea)' = 'ischnocnema',
  'larval_salamander_sp.' = 'caudata',
  'leptodactylus_marmoratus' = 'adenomera_marmorata',
  'leptodactylus_spp' = 'leptodactylus',
  'lithobates_sylvaticus' = 'rana_sylvatica',
  "na" = NA_character_,
  'ololygon_aff._brieni' = 'ololygon',
  'ololygon_aff._littoralis' = 'ololygon',
  'ololygon_cf_litoralis' = 'ololygon',
  'physalaemus_sp' = 'physalaemus',
  'plethodon_glutinosis' = 'plethodon_glutinosus',
  'pristimantis_sp._potential_new_sp' = 'pristimantis',
  'pristimantis_spp' = 'pristimantis',
  'rana_catesbeiana_x_rana_clamitans_(possibly)' = 'rana',
  'rana_sp' = 'rana',
  'rana_spp' = 'rana',
  'red_backed_salamander' = 'plethodon_cinereus',
  'see_notes'= NA_character_,
  'silverstoneia_spp' = 'silverstoneia',
  'scinax_crospedopilus' = 'scinax_crospedospilus',
  'smilisca_spp' = 'smilisca',
  'tad_spp_2' = 'anura',
  'tadpole_species_1' = 'anura',
  'toad_sp.' = 'bufonidae',
  'unknown' = NA_character_,
  'unknown_species' = NA_character_,
  'uptidactylus_sarajay' = 'leptodactylus_savagei'
)


#' Systematically clean taxa names
#'
#' References a dictionary to remap taxa names, adding comments where needed.
#' @param data data frame with taxa data
#' @param taxon_column column with taxon names
#' @param comment_column optional column, for remapping some taxon details to comments prior to cleaning
#' @return Data frame with cleaned taxa
#' @examples
#' # example data
#' my_data <- tibble(
#'   taxa_names = c("desmog_spp", "rana_muscosa", "pristimantis_sp._potential_new_sp", "ololygon_aff._littoralis"),
#'   comments = c(NA, "very cute", NA, "got away"),
#'   other_data = c(1, 3, 5, 7)
#' )
#' clean_data = my_data %>%
#'   ribbitr_clean_taxa(taxa_names, comments)
#' @importFrom dplyr %>% mutate
#' @importFrom rlang enquo quo_is_null
#' @export
ribbitr_clean_taxa <- function(data, taxon_column, comment_column = NULL) {
  taxon_col_sym <- enquo(taxon_column)

  data_out <- data %>%
    mutate(!!taxon_col_sym := tolower(gsub(" ", "_", !!taxon_col_sym)))

  if (!quo_is_null(enquo(comment_column))) {
    comment_col_sym <- enquo(comment_column)
    data_out <- data_out %>%
      mutate(!!comment_col_sym := ifelse(!!taxon_col_sym %in% names(comment_taxa_dict),
                                         ifelse(is.na(!!comment_col_sym),
                                                as.character(comment_taxa_dict[as.character(!!taxon_col_sym)]),
                                                as.character(paste(!!comment_col_sym,
                                                                   comment_taxa_dict[as.character(!!taxon_col_sym)],
                                                                   sep = "; "))),
                                         !!comment_col_sym))
  }

  data_out <- data_out %>%
    mutate(!!taxon_col_sym := as.character(ifelse(!!taxon_col_sym %in% names(clean_taxa_dict),
                                                  clean_taxa_dict[as.character(!!taxon_col_sym)],
                                                  !!taxon_col_sym)))

  return(data_out)
}
