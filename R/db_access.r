
#' HopToDB
#'
#' A simple function to easily to the RIBBiTR (or another) remote database. The following database connection credentials must be saved to your local .Renviron file(see \link[DBI]{dbConnect}): dbname - the database name, host - the database host, port - the database port, user - your database username, password - your database password. Variables may be defined with an optional prefix seperated by and underscore (e.g. "ribbitr_dbname") to distinguish multiple connections.
#' @param prefix an optional prefix (string) added to the front of connection credential variables to distinguish between sets of credentials.
#' @param timezone an optional timezone parameter to help convert data from various timezones to your local time. Valid options are found using \link[lubridate]{OlsonNames}.
#' @return database connection object, to be passed to other functions (e.g. \link[DBI]{dbListTables}, \link[dbplyr]{tbl}, etc.).
#' @examples
#' 
#' # open your local .Renviron file
#' usethis::edit_r_environ()
#' 
#' # copy the following to .Renviron, replacing corresponding database credentials
#' 
#' ribbitr.dbname = "[DATABASE_NAME]"
#' ribbitr.host = "[DATABASE_HOST]"
#' ribbitr.port = "[DATABASE_PORT]"
#' ribbitr.user = "[USERNAME]"
#' ribbitr.password = "[PASSWORD]"
#' 
#' # connect to your database with a single line of code
#' dbcon <- HopToDB(prefix = "ribbitr")
#' 
#' @export
HopToDB = function(prefix = NA, timezone = NULL) {
  
  dbname = paste(na.omit(c(prefix, "dbname")), collapse = ".")
  host = paste(na.omit(c(prefix, "host")), collapse = ".")
  port = paste(na.omit(c(prefix, "port")), collapse = ".")
  user = paste(na.omit(c(prefix, "user")), collapse = ".")
  password = paste(na.omit(c(prefix, "password")), collapse = ".")
  
  tryCatch({
    cat("Connecting to database... ")
    dbcon <- dbConnect(dbDriver("Postgres"),
                     dbname = Sys.getenv(dbname),
                     host = Sys.getenv(host),
                     port = Sys.getenv(port),
                     user = Sys.getenv(user),
                     password = Sys.getenv(password),
                     timezone = timezone)
    cat("Success!\n")
  },
  error=function(cond) {
    message("\nUnable to connect: ", cond$message,  "\n")
  })
  return(dbcon)
}

#' Table primary key
#'
#' Identify the primary key column(s) for a given table in the database metadata. A table's primary key caries a unique and not-null constraint, and is used to uniquely identify the rows in the table. There is only one primary key per table, usually (though not necessarily) single column.
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return The name(s) of the columns comprising the primary key for the table provided.
#' @examples 
#' survey_pkey <- tbl_pkey('survey', mdc)
#' @export
tbl_pkey = function(tbl_name, metadata_columns) {
  metadata_columns %>%
    filter(table_name == tbl_name,
           key_type == "PK") %>%
    pull(column_name)
}

#' Table foreign key
#'
#' Identify the foreign key column(s) for a given table in the database metadata. A foreign key points to and mirrors the primary key of another table, establishing relationships between tables. There may be multiple foreign keys in a given table, usually (though not necessarily) single column per key.
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return The name(s) of the columns with foreign key status in the table provided
#' @examples 
#' survey_fkey <- tbl_fkey('survey', mdc)
#' @export
tbl_fkey = function(tbl_name, metadata_columns) {
  metadata_columns %>%
    filter(table_name == tbl_name,
           key_type == "FK") %>%
    pull(column_name)
}

#' Table natural key
#'
#' Identify the natural key column(s) for a given table in the database metadata.  A natural key is comprised of the columns which naturally identify a given row, and which are generally used to generate ID columns used as primary keys. There is only one natural key per table, though it may be comprised of multiple columns (a "composite" key).
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return The name(s) of the columns with natural key status in the table provided
#' @examples 
#' survey_nkey <- tbl_nkey('survey', mdc)
#' @export
tbl_nkey = function(tbl_name, metadata_columns) {
  metadata_columns %>%
    filter(table_name == tbl_name,
           natural_key) %>%
    pull(column_name)
}

#' All table keys
#'
#' Identify primary, foreign, and natural key columns for a given table.
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return The uniqie name(s) of the columns with primary, foreign, or natural key status in the table provided. If a column has multiple statuses, it will show up only once.
#' @examples 
#' survey_keys <- tbl_keys('survey', mdc)
#' 
#' # collect table with all key columns from DB
#' db_survey <- tbl(dbcon, "survey") %>%
#'                 select(all_of(survey_keys)) %>%
#'                 collect()
#' @export
tbl_keys = function(tbl_name, metadata_columns) {
  return(unique(c(
    tbl_pkey(tbl_name, metadata_columns=metadata_columns),
    tbl_nkey(tbl_name, metadata_columns=metadata_columns),
    tbl_fkey(tbl_name, metadata_columns=metadata_columns)
  )))
}

#' Identify reference (linked) tables
#'
#' For each foreign key in tbl_name, pull information on the associated reference table.
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return Returns a link object (list of referenced tables and their attributes)
#' @examples 
#' 
#' capture_link = tbl_link("capture", mdc)
#' 
#' @export
tbl_link = function(tbl_name, metadata_columns) {
  link = list()
  link[["root"]] = tbl_name
  
  parents = list()
  
  tbl_current = tbl_name
  
  fkey_vec = tbl_fkey(tbl_name, mdc)
  
  for (ffkey in fkey_vec) {
    pkey = ffkey
    
    tbl_parent = metadata_columns %>%
      filter(column_name == pkey,
             key_type == "PK") %>%
      select(table_schema, table_name) %>%
      collect()
    
    nkey = tbl_nkey(tbl_parent$table_name, metadata_columns)
    fkey = tbl_fkey(tbl_parent$table_name, metadata_columns)
    
    parents[[tbl_parent$table_name]] = list(schema=tbl_parent$table_schema,
                                            name=tbl_parent$table_name,
                                            pkey=pkey,
                                            nkey=nkey,
                                            fkey=fkey)
  }
  link[["parents"]] = parents
  return(link)
}


tbl_chain = function(tbl_name, metadata_columns, until=NA) {
  chain = list()
  tbl_list = list(tbl_name)
  tbl_remaining = TRUE
  
  # nest in list if not already
  until = list(unlist(until))
  
  while (tbl_remaining) {
    
    # pop tbl_list
    tbl_active = tbl_list[[1]]
    tbl_list[[1]] = NULL
    
    link_active = tbl_link(tbl_active, metadata_columns)
    
    if (length(link_active$parents) > 0) {
      for (ll in link_active$parents) {
        
        if (!(ll$name %in% until)) {
          # add to search list, unless
          tbl_list <- append(tbl_list, ll$name)
        }
        
        chain$parents[[ll$name]] = ll
      }
    }
    
    tbl_remaining = length(tbl_list)
    
  }
  
  return(chain)
}


tbl_join = function(dbcon, link, tbl=NA, join="left", by="pkey", columns=NA) {
  
  if (is.na(tbl)[[1]]) {
    tbl = tbl(dbcon, link$root)
  }
  
  
  for (pp in link$parents) {
    tbl_next = tbl(dbcon, Id(pp$schema, pp$name)) %>%
      select(any_of(na.omit(unique(unlist(c(
        pp$pkey,
        pp$nkey,
        pp$fkey,
        columns
      ))))))
    
    cat("Joining with", pp$name, "...")
    
    if (join == "left") {
      tbl = tbl %>%
        left_join(tbl_next, by = c(pp[[by]]))
    } else if (join == "full") {
      tbl = tbl %>%
        full_join(tbl_next, by = c(pp[[by]]))
    } else if (join == "inner") {
      tbl = tbl %>%
        inner_join(tbl_next, by = c(pp[[by]]))
    } else {
      stop(join, " is not a valid join type. Should it be programmed in?")
    }
    
    cat("done.\n")
    
  }
  
  return(tbl)
}


tbl_chain_join = function(dbcon, chain, tbl=NA, join="left", on="pkey", columns=NA) {
  # fetch table if not provided
  if (is.na(tbl)[[1]]) {
    tbl_running = tbl(dbcon, link$root)
  } else {
    tbl_running = tbl
  }
  
  tbl_running = tbl_join(dbcon, link, tbl=tbl_running, join=join, on=on, columns=columns)
  
  return(tbl_running)
}