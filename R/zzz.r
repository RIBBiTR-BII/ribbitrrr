#' Check package version against GitHub repository
#'
#' @importFrom utils packageVersion installed.packages
#' @keywords internal
check_package_version <- function() {
  # Get current installed version
  current_version <- packageVersion("ribbitrrr")

  # Try to fetch latest version from GitHub
  tryCatch({
    # Read DESCRIPTION file from main branch
    desc_url <- "https://raw.githubusercontent.com/RIBBiTR-BII/ribbitrrr/main/DESCRIPTION"
    desc_content <- readLines(desc_url)

    # Extract version line
    version_line <- grep("^Version:", desc_content, value = TRUE)
    latest_version <- gsub("^Version:\\s*", "", version_line)

    # Compare versions
    if (package_version(latest_version) > current_version) {
      message <- sprintf(
        "\nA newer version of ribbitrrr is available on GitHub (%s > %s).\n%s\n%s\n",
        latest_version, current_version,
        "Consider updating using:",
        "remotes::install_github('RIBBiTR-BII/ribbitrrr')"
      )
    } else {
      message <- NULL
    }
    return(message)
  }, error = function(e) {
    # Silently fail if unable to check version
    return(NULL)
  })
}

#' @keywords internal
.onLoad <- function(libname, pkgname) {
  # Perform the version check during package loading
  update_message <- check_package_version()
  # Store the message for later use in .onAttach
  options(ribbitrrr_update_message = update_message)
}

#' @keywords internal
.onAttach <- function(libname, pkgname) {
  # Display the update message, if any
  update_message <- getOption("ribbitrrr_update_message")
  if (!is.null(update_message)) {
    packageStartupMessage(update_message)
  }
}
