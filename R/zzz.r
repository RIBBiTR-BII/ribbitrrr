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
      message(
        "\nA newer version of ribbitrrr is available on GitHub (", 
        latest_version, " > ", current_version, ").\n",
        "Consider updating using:\n",
        "remotes::install_github('RIBBiTR-BII/ribbitrrr')\n"
      )
    }
  }, error = function(e) {
    # Silently fail if unable to check version
    return(NULL)
  })
}

#' @keywords internal
.onAttach <- function(libname, pkgname) {
  check_package_version()
}