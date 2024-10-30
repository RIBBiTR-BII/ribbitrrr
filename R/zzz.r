#' Check package version against GitHub repository
#'
#' @importFrom utils packageVersion installed.packages
#' @importFrom httr GET content stop_for_status
#' @keywords internal
check_package_version <- function() {
  # Get current installed version
  current_version <- packageVersion("ribbitrrr")
  message("Current version: ", current_version)

  # Try to fetch latest version from GitHub
  tryCatch({
    # Read DESCRIPTION file from main branch
    desc_url <- "https://raw.githubusercontent.com/RIBBiTR-BII/ribbitrrr/main/DESCRIPTION"
    response <- httr::GET(desc_url)
    httr::stop_for_status(response)
    desc_content <- httr::content(response, "text", encoding = "UTF-8")
    message("Successfully fetched DESCRIPTION file")

    # Extract version line
    version_line <- grep("^Version:", strsplit(desc_content, "\n")[[1]], value = TRUE)
    latest_version <- gsub("^Version:\\s*", "", version_line)
    message("Latest version: ", latest_version)

    # Compare versions
    if (package_version(latest_version) > current_version) {
      message <- sprintf(
        "\nA newer version of ribbitrrr is available on GitHub (%s > %s).\n%s\n%s\n",
        latest_version, current_version,
        "Consider updating using:",
        "remotes::install_github('RIBBiTR-BII/ribbitrrr')\n"
      )
    } else {
      message <- NULL
    }
    return(message)
  }, error = function(e) {
    warning("Error checking version: ", conditionMessage(e))
    return(NULL)
  })
}

#' @keywords internal
.onLoad <- function(libname, pkgname) {
  message(".onLoad function called")
  # Perform the version check during package loading
  update_message <- check_package_version()
  # Store the message for later use in .onAttach
  options(ribbitrrr_update_message = update_message)
}

#' @keywords internal
.onAttach <- function(libname, pkgname) {
  message(".onAttach function called")
  # Display the update message, if any
  update_message <- getOption("ribbitrrr_update_message")
  if (!is.null(update_message)) {
    packageStartupMessage(update_message)
  }
}
