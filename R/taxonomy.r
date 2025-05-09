#' @keywords internal
require_taxize <- function() {
  if (!requireNamespace("taxize", quietly = TRUE)) {
    stop("Please install taxize to use this feature: `install.packages(taxize)`")
  }
}

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
    result <- dplyr::tibble(
      name_submitted = name_submitted,
      cites_id = NA,
      cites_appendix = NA
    )

    # Attempt CITES scrape
    tryCatch({
      name_submitted = gsub("_", " ", name_submitted)

      response <- httr::GET(
        url = "https://api.speciesplus.net/api/v1/taxon_concepts",
        query = list(name = name_submitted),
        httr::add_headers("X-Authentication-Token" = token)
      )

      # Parse JSON content
      content <- jsonlite::fromJSON(rawToChar(response$content))

      res = content[["taxon_concepts"]][["cites_listings"]][[1]]

      if (!is.null(result$cites_id)){
        result$cites_id <- res$id
      }
      if (!is.null(result$cites_appendix)){
        result$cites_appendix <- res$appendix
      }

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
  comment_taxa_dict = list('aplastodiscus_cf._albosignatus' = 'aplastodiscus_cf._albosignatus',
                           'chiasmocleis_cf._atlantica' = 'chiasmocleis_cf._atlantica',
                           'dendrophryniscus_cf._leucomystax' = 'dendrophryniscus_cf._leucomystax',
                           'fritziana_cf._fissilis' = 'fritziana_cf._fissilis',
                           'ischnocnema_cf._lactea' = 'ischnocnema_cf._lactea',
                           'ischnocnema_2_(aff._lactea)' = 'ischnocnema_2_(aff._lactea)',
                           'ischnocnema_sp._02_(aff._lactea)' = 'ischnocnema_sp._02_(aff._lactea)',
                           'ololygon_aff._brieni' = 'ololygon_aff._brieni',
                           'ololygon_aff._hiemalis' = 'ololygon_aff._hiemalis',
                           'ololygon_aff._littoralis' = 'ololygon_aff._littoralis',
                           'ololygon_cf_litoralis' = 'ololygon_cf_litoralis',
                           'paratelmatobius_nov.' = 'paratelmatobius possibly new species',
                           'pristimantis_sp._potential_new_sp' = 'potential new species',
                           'rana_catesbeiana_x_rana_clamitans_(possibly)' = 'Rana_catesbeiana or Rana_clamitans',
                           'tadpole_species_1' = 'tadpole species 1',
                           'tad_spp_2' = 'tadpole species 2')

  clean_taxa_dict <- list(
    'aplastodiscus_cf._albosignatus' = 'aplastodiscus',
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
    'controle' = NA_character_,
    'craugastor_spp' = 'craugastor',
    'cycloramphus_sp.' = 'cycloramphus',
    'dendrophryniscus_cf._leucomystax' = 'dendrophryniscus',
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
    'fritziana_cf._fissilis' = 'fritziana',
    'hadadus_binotatus' = 'haddadus_binotatus',
    'hyalinobatrachium_fleishmanni' = 'hyalinobatrachium_fleischmanni',
    'hyalinobatrachium_spp' = 'hyalinobatrachium',
    'hyllodes_asper' = 'hylodes_asper',
    'hyliola_regilla' = 'pseudacris_regilla',
    'hyllodes_phyllodes' = 'hylodes_phyllodes',
    'ischnocnema_2_(aff._lactea)' = 'ischnocnema',
    'ischnocnema_cf._lactea' = 'ischnocnema',
    'ischnocnema__henselii' = 'ischnocnema_henselii',
    'ischnocnema_henselli' = 'ischnocnema_henselii',
    'ischnocnema_sp' = 'ischnocnema',
    'ischnocnema_sp._02_(aff._lactea)' = 'ischnocnema',
    'larval_salamander_sp.' = 'caudata',
    'leptodactylus_marmoratus' = 'adenomera_marmorata',
    'leptodactylus_spp' = 'leptodactylus',
    'lithobates_sylvaticus' = 'rana_sylvatica',
    'na' = NA_character_,
    'ololygon_aff._brieni' = 'ololygon',
    'ololygon_aff._hiemalis' = 'ololygon',
    'ololygon_aff._littoralis' = 'ololygon',
    'ololygon_cf_litoralis' = 'ololygon',
    'ololygon_rizibilis' = 'scinax_rizibilis',
    'paratelmatobius_nov.' = 'paratelmatobius',
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


map_rank = function(rank) {
  rmap = c("kingdom" = 1,
           "subkingdom" = 2,
           "infrakingdom" = 3,
           "phylum" = 4,
           "subphylum" = 5,
           "infraphylum" = 6,
           "superclass" = 7,
           "class" = 8,
           "order" = 9,
           "family" = 10,
           "subfamily" = 11,
           "genus" = 12,
           "species" = 13)

  return(as.integer(rmap[rank]))
}


#' @importFrom dplyr %>% mutate filter select bind_cols rename_with any_of
#' @importFrom tidyr pivot_wider
#' @importFrom purrr map_int
ribbitr_taxa_lookup_single = function(taxa, itis = TRUE, ncbi = TRUE, gbif = TRUE, iucn = TRUE, cites = FALSE, cites_token = NA) {
  # intentionally written non-vectorized, to build in time buffers between queries for each database as requested by various databases

  require_taxize()

  safely_gna_verifier = purrr::safely(taxize::gna_verifier)

  if (cites & is.na(cites_token)) {
    stop("cites_token must be provided to query cites via API.")
  }

  taxa_raw = taxa
  taxa = tolower(gsub("_", " ", taxa_raw))

  cat("\033[1;37m", taxa, ": ", sep = "")
  cat("\033[38;5;240m", "AmphibiaWeb", sep = "")
  taxa_aw = scrape_amphibiaweb(taxa, quietly = TRUE)

  if (itis) {
    cat(", ITIS", sep = "")
    taxa_itis = safely_gna_verifier(taxa, data_sources = 3, capitalize = TRUE)


    if (is.null(taxa_itis$error)){
      if (taxa_itis$result$matchType == "PartialExact") {
        taxa_itis$error$message = "PartialExact"
        itis_pos = FALSE
      } else if (!is.na(taxa_itis$result$currentRecordId)){
        itis_pos = TRUE
      } else {
        itis_pos = FALSE
      }
    } else {
      itis_pos = FALSE
    }

    if (itis_pos) {
      cat(", ITIS hierarchy", sep = "")
      rankname_itis = tolower(taxize::itis_taxrank(taxa_itis$result$currentRecordId))
      ranknum_itis = map_rank(rankname_itis)

      hierarchy_itis = taxize::itis_hierarchy(taxa_itis$result$currentRecordId, "full") %>%
        mutate(ranknum = map_int(rankname, ~ map_rank(.x))) %>%
        filter(ranknum >= 8,
               ranknum <= ranknum_itis) %>%
        select(rankname,
               taxonname) %>%
        pivot_wider(names_from = rankname,
                    values_from = taxonname) %>%
        mutate(rankname = rankname_itis)
    } else {
      hierarchy_itis <- data.frame(
        class = NA_character_,
        order = NA_character_,
        family = NA_character_,
        genus = NA_character_,
        species = NA_character_,
        rankname = NA_character_
      )
    }
  }

  if (ncbi) {
    cat(", NCBI", sep = "")
    taxa_ncbi = safely_gna_verifier(taxa, data_sources = 4, capitalize = TRUE)
    if (is.null(taxa_ncbi$error)){
      if (taxa_ncbi$result$matchType == "PartialExact") {
        taxa_ncbi$error$message = "PartialExact"
      }
    }
  }


  if (gbif) {
    cat(", GBIF", sep = "")
    taxa_gbif = safely_gna_verifier(taxa, data_sources = 11, capitalize = TRUE)
    if (is.null(taxa_gbif$error)){
      if (taxa_gbif$result$matchType == "PartialExact") {
        taxa_gbif$error$message = "PartialExact"
      }
    }
  }

  if (iucn) {
    cat(", IUCN", sep = "")
    taxa_iucn = safely_gna_verifier(taxa, data_sources = 163, capitalize = TRUE)
    if (is.null(taxa_iucn$error)){
      if (taxa_iucn$result$matchType == "PartialExact") {
        taxa_iucn$error$message = "PartialExact"
      }
    }
  }

  if (cites) {
    cat(", CITES", sep = "")
    taxa_cites = scrape_cites(taxa, cites_token, quietly = TRUE)
  }

  cat("\n", sep = "")

  taxa_out = taxa_aw %>%
    rename_with(~ paste0("aw_", .)) %>%
    mutate(taxa_search = taxa)

  if (itis & is.null(taxa_itis$error)) {
    taxa_out = taxa_out %>%
      bind_cols(taxa_itis$result %>%
                  rename_with(~ paste0("itis_", .)))


    taxa_out = taxa_out %>%
      bind_cols(hierarchy_itis %>%
                  rename_with(~ paste0("itis_", .)))
  }

  if (ncbi & is.null(taxa_ncbi$error)) {
    taxa_out = taxa_out %>%
      bind_cols(taxa_ncbi$result %>%
                  rename_with(~ paste0("ncbi_", .)))
  }

  if (gbif & is.null(taxa_gbif$error)) {
    taxa_out = taxa_out %>%
      bind_cols(taxa_gbif$result %>%
                  rename_with(~ paste0("gbif_", .)))
  }

  if (iucn & is.null(taxa_iucn$error)) {
    taxa_out = taxa_out %>%
      bind_cols(taxa_iucn$result %>%
                  rename_with(~ paste0("iucn_", .)))
  }

  if (cites) {
    taxa_out = taxa_out %>%
      bind_cols(taxa_cites)
  }

  taxa_out$taxa_raw = taxa_raw

  return(taxa_out)
}


#' Systematically look up taxa names
#'
#' References a number of taxanomic databases to build or update a taxonomy lookup table. Primary reference in Amphibiaweb, others are optional.
#' @param taxa character vector (or sting) of taxa names to be looked up
#' @param itis do you want to query ITIS?
#' @param ncbi do you want to query NCBI?
#' @param gbif do you want to query GBIF?
#' @param iucn do you want to query IUCN?
#' @param cites do you want to query CITES?
#' @param cites_token CITES API token, required to query CITES.
#' @param format how do you want the output formatted: "full" returns everything; "simplified" returns less, "simple" returns even less.
#' @return table of results for submitted taxa
#' @examples
#' # example data
#'
#' my_taxa = c(
#' "rana_muscosa",
#' "atelopus_zeteki",
#' "pristimantis")
#'
#' my_taxa_lookup = ribbitr_taxa_lookup(my_taxa)
#' @importFrom dplyr %>% mutate rename select any_of
#' @importFrom purrr map_df
#' @export
ribbitr_taxa_lookup = function(taxa, itis = TRUE, ncbi = TRUE, gbif = TRUE, iucn = TRUE, cites = FALSE, cites_token = NA, format = "full") {
  if (class(taxa) != "character") {
    stop("taxa must be a sting or character vector.")
  }

  if (length(taxa) == 1) {
    output = ribbitr_taxa_lookup_single(taxa, itis = itis, ncbi = ncbi, gbif = gbif, iucn = iucn, cites = cites, cites_token = cites_token)
  } else {
    output = map_df(taxa, ~ ribbitr_taxa_lookup_single(.x, itis = itis, ncbi = ncbi, gbif = gbif, iucn = iucn, cites = cites, cites_token = cites_token))
  }

  if (format %in% c("simplified", "simple")) {
    output = output %>%
      mutate("amphibiaweb_species" = ifelse(is.na(aw_species), NA, paste(aw_genus, aw_species)),
             "amphibiaweb_class" = ifelse(is.na(aw_species), NA, "Amphibia"),
             "aw_url" = gsub("_ws\\?", "_query?", aw_url)) %>%
      rename(any_of(c("taxon_id" = "taxa_raw",
                      "taxon" = "taxa_search",
                      "amphibiaweb_id" = "aw_amphib_id",
                      "amphibiaweb_order" = "aw_order",
                      "amphibiaweb_family" = "aw_family",
                      "amphibiaweb_subfamily" = "aw_subfamily",
                      "amphibiaweb_genus" = "aw_genus",
                      "amphibiaweb_common" = "aw_common_name",
                      "amphibiaweb_url" = "aw_url",
                      "itis_tsn_matched" = "itis_recordId",
                      "itis_canonical_matched" = "itis_matchedCanonicalSimple",
                      "itis_status_matched" = "itis_taxonomicStatus",
                      "itis_match_type" = "itis_matchType",
                      "itis_tsn_current" = "itis_currentRecordId",
                      "itis_canonical_current" = "itis_currentCanonicalSimple",
                      "itis_rank_current" = "itis_rankname",
                      "ncbi_id_matched" = "ncbi_recordId",
                      "ncbi_canonical_matched" = "ncbi_matchedCanonicalSimple",
                      "ncbi_status_matched" = "ncbi_taxonomicStatus",
                      "ncbi_id_current" = "ncbi_currentRecordId",
                      "ncbi_canonical_current" = "ncbi_currentCanonicalSimple",
                      "gbif_id_matched" = "gbif_recordId",
                      "gbif_canonical_matched" = "gbif_matchedCanonicalSimple",
                      "gbif_status_matched" = "gbif_taxonomicStatus",
                      "gbif_id_current" = "gbif_currentRecordId",
                      "gbif_canonical_current" = "gbif_currentCanonicalSimple",
                      "iucn_tsn_matched" = "iucn_recordId",
                      "iucn_canonical_matched" = "iucn_matchedCanonicalSimple",
                      "iucn_status_matched" = "iucn_taxonomicStatus",
                      "iucn_tsn_current" = "iucn_currentRecordId",
                      "iucn_canonical_current" = "iucn_currentCanonicalSimple"))) %>%
      select(any_of(c("taxon_id",
                      "taxon",
                      "amphibiaweb_id",
                      "amphibiaweb_class",
                      "amphibiaweb_order",
                      "amphibiaweb_family",
                      "amphibiaweb_subfamily",
                      "amphibiaweb_genus",
                      "amphibiaweb_species",
                      "amphibiaweb_common",
                      "amphibiaweb_url",
                      "itis_tsn_matched",
                      "itis_canonical_matched",
                      "itis_status_matched",
                      "itis_match_type",
                      "itis_tsn_current",
                      "itis_canonical_current",
                      "itis_rank_current",
                      "itis_class",
                      "itis_order",
                      "itis_family",
                      "itis_genus",
                      "itis_species",
                      "ncbi_id_matched",
                      "ncbi_canonical_matched",
                      "ncbi_status_matched",
                      "ncbi_id_current",
                      "ncbi_canonical_current",
                      "gbif_id_matched",
                      "gbif_canonical_matched",
                      "gbif_status_matched",
                      "gbif_id_current",
                      "gbif_canonical_current",
                      "iucn_tsn_matched",
                      "iucn_canonical_matched",
                      "iucn_status_matched",
                      "iucn_tsn_current",
                      "iucn_canonical_current",
                      "cites_id",
                      "cites_appendix")))
  }

  if (format == "simple") {
    output = output %>%
      rename(any_of(c("itis_tsn" = "itis_tsn_current",
                      "itis_taxon" = "itis_canonical_current",
                      "itis_rank" = "itis_rank_current",
                      "ncbi_id" = "ncbi_id_matched",
                      "ncbi_taxon" = "ncbi_canonical_matched",
                      "gbif_id" = "gbif_id_current",
                      "gbif_taxon" = "gbif_canonical_current",
                      "iucn_tsn" = "iucn_tsn_current",
                      "iucn_taxon" = "iucn_canonical_current",
                      "amphibiaweb_common_name" = "amphibiaweb_common"))) %>%
      select(any_of(c("taxon_id",
                      "taxon",
                      "amphibiaweb_id",
                      "amphibiaweb_class",
                      "amphibiaweb_order",
                      "amphibiaweb_family",
                      "amphibiaweb_genus",
                      "amphibiaweb_species",
                      "amphibiaweb_common_name",
                      "amphibiaweb_url",
                      "itis_tsn",
                      "itis_rank",
                      "itis_class",
                      "itis_order",
                      "itis_family",
                      "itis_genus",
                      "itis_species",
                      "ncbi_id",
                      "ncbi_taxon",
                      "gbif_id",
                      "gbif_taxon",
                      "iucn_tsn",
                      "iucn_taxon",
                      "cites_id",
                      "cites_appendix")))
  }

  return(output)
}
