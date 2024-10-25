
#' HopToDB
#'
#' A simple function to easily to the RIBBiTR (or another) remote database. The following database connection credentials must be saved to your local .Renviron file(see \link[DBI]{dbConnect}): dbname - the database name, host - the database host, port - the database port, user - your database username, password - your database password. Variables may be defined with an optional prefix seperated by and underscore (e.g. "ribbitr_dbname") to distinguish multiple connections.
#' @param prefix an optional prefix (string) added to the front of connection credential variables to distinguish between sets of credentials.
#' @param timezone an optional timezone parameter to help convert data from various timezones to your local time.
#' @return database connection object, to be passed to other functions (e.g. \link[DBI]{dbListTables}, \link[dplyr]{tbl}, etc.).
#' @examples
#' 
#' ## open your local .Renviron file
#' # usethis::edit_r_environ

#' ## copy the following to .Renviron, replacing corresponding database credentials
#' 
#' # ribbitr.dbname = "[DATABASE_NAME]"
#' # ribbitr.host = "[DATABASE_HOST]"
#' # ribbitr.port = "[DATABASE_PORT]"
#' # ribbitr.user = "[USERNAME]"
#' # ribbitr.password = "[PASSWORD]"
#' 
#' # connect to your database with a single line of code
#' dbcon <- HopToDB(prefix = "ribbitr")
#' @importFrom DBI dbConnect dbDriver dbListTables
#' @importFrom RPostgres Postgres
#' @importFrom stats na.omit
#' @importFrom dplyr tbl
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
#' @importFrom dplyr %>% filter pull
#' @export
tbl_pkey = function(tbl_name, metadata_columns) {
  metadata_columns %>%
    filter(table_name == tbl_name,
           key_type == "PK") %>%
    pull("column_name")
}

#' Table foreign key
#'
#' Identify the foreign key column(s) for a given table in the database metadata. A foreign key points to and mirrors the primary key of another table, establishing relationships between tables. There may be multiple foreign keys in a given table, usually (though not necessarily) single column per key.
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return The name(s) of the columns with foreign key status in the table provided
#' @examples 
#' survey_fkey <- tbl_fkey('survey', mdc)
#' @importFrom dplyr %>% filter pull
#' @export
tbl_fkey = function(tbl_name, metadata_columns) {
  metadata_columns %>%
    filter(table_name == tbl_name,
           key_type == "FK") %>%
    pull("column_name")
}

#' Table natural key
#'
#' Identify the natural key column(s) for a given table in the database metadata.  A natural key is comprised of the columns which naturally identify a given row, and which are generally used to generate ID columns used as primary keys. There is only one natural key per table, though it may be comprised of multiple columns (a "composite" key).
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return The name(s) of the columns with natural key status in the table provided
#' @examples 
#' survey_nkey <- tbl_nkey('survey', mdc)
#' @importFrom dplyr %>% filter pull
#' @export
tbl_nkey = function(tbl_name, metadata_columns) {
  metadata_columns %>%
    filter(table_name == tbl_name,
           natural_key) %>%
    pull("column_name")
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

#' Identify linked reference tables
#'
#' For each foreign key in tbl_name, pull information on the associated reference table. To do this recursively use \link[ribbitrrr]{tbl_chain}
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return Returns a link object listing referenced tables and their attributes
#' @examples 
#' 
#' capture_link = tbl_link("capture", mdc)
#' @importFrom dplyr %>% filter select collect
#' @export
tbl_link = function(tbl_name, metadata_columns) {
  link = list()
  
  tbl_root = tbl_parent = metadata_columns %>%
    filter(table_name == tbl_name,
           key_type == "PK") %>%
    select(table_schema, column_name) %>%
    collect()
  
  # record initial table as "root"
  link$root$schema = tbl_root$table_schema
  link$root$table = tbl_name
  link$root$pkey = tbl_root$column_name
  link$root$nkey = tbl_nkey(tbl_name, metadata_columns)
  link$root$fkey = tbl_fkey(tbl_name, metadata_columns)
    
  parents = list()
  
  tbl_current = tbl_name  # preload for while loop
  
  fkey_vec = list(unlist(link$root$fkey))  # working list for fkeys
  
  for (ffkey in fkey_vec) {
    pkey = ffkey  # name change ~ point of view of referenced table
    
    # filter metadata columns for a column with pkey == fkey (requires unique pkey/fkey column names within database)
    tbl_parent = metadata_columns %>%
      filter(column_name == pkey,
             key_type == "PK") %>%
      select(table_schema, table_name) %>%
      collect()
    
    # identify nkeys
    nkey = tbl_nkey(tbl_parent$table_name, metadata_columns)
    # identify fkeys
    fkey = tbl_fkey(tbl_parent$table_name, metadata_columns)
    #save all to parents list
    parents[[tbl_parent$table_name]] = list(schema=tbl_parent$table_schema,
                                            table=tbl_parent$table_name,
                                            pkey=pkey,
                                            nkey=nkey,
                                            fkey=fkey)
  }
  link[["parents"]] = parents
  return(link)
}

#' Recursively identify linked reference tables
#'
#' For each foreign key in tbl_name, pull information on the associated reference table. Do the same for any foreign keys found in this reference table, and so on. Stops when table network is exausted. Does not search beyond any tables listed in "until". To identify one iteration of reference tables, use \link[ribbitrrr]{tbl_link}.
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @param until Name(s) of table(s) beyond which you do not want to continue link search (string or list of strings)
#' @return Returns a link object (list of all encountered referenced tables and their attributes)
#' @examples 
#' 
#' capture_chain = tbl_chain("capture", mdc, until=c("region"))
#' @export
tbl_chain = function(tbl_name, metadata_columns, until=NA) {
  chain = list()
  tbl_list = list(tbl_name)  # list of tables yet to search
  tbl_remaining = TRUE
  pull_root = TRUE
  
  # nest in list if not already
  until = list(unlist(until))
  
  # recursively execute tbl_link until tbl_list is exhausted
  while (tbl_remaining) {
    
    # pop tbl_list
    tbl_active = tbl_list[[1]]
    tbl_list[[1]] = NULL
    
    link_active = tbl_link(tbl_active, metadata_columns)
    
    if (pull_root) {
      chain$root = link_active$root
    }
    
    # for each reference table found
    if (length(link_active$parents) > 0) {
      for (ll in link_active$parents) {
        
        if (!(ll$table %in% until)) {
          # add to tbl_list, unless tbl is in "until"
          tbl_list <- append(tbl_list, ll$table)
        }
        
        chain$parents[[ll$table]] = ll
      }
    }
    
    # continue while loop?
    tbl_remaining = length(tbl_list)
  }
  
  return(chain)
}

#' Join table with reference tables
#'
#' Recursively join linked database tables following provided link object. Only primary, natural, and foreign key columns are joined by default. Specify additional columns to include in "columns"
#' @param dbcon database connection object from \link[DBI]{dbConnect} or \link[ribbitrrr]{HopToDB}
#' @param link a link object generated from \link[ribbitrrr]{tbl_link} or \link[ribbitrrr]{tbl_chain}
#' @param tbl A lazy table object from \link[dplyr]{tbl} corresponding to the root table in the provided link object (optional). If not provided, link object root table will be pulled in its entirity. Passing your own table allows you to filter or select specific columns prior to joining, thereby avoiding pulling nonessential data. Be sure to minimally include essential columns: fkeys, and nkeys or pkeys depending on "by".
#' @param join Type of join to be executed ("left", "inner", "full", or "right")
#' @param by What columns to perform join on ("pkey" or "nkey")
#' @param columns Additional columns to be included from joined tables (string or list of stings). Primary, natural, and foreign key columns are included by default.
#' @return Returns a single lazy table object of all linked tables joined as specified
#' @examples 
#' # generate link object
#' capture_chain = tbl_chain("capture", mdc, until=c("region"))
#' 
#' # pre-filter root table (optional)
#' tbl_capture = tbl(dbcon, Id("survey_data", "capture")) %>%
#'   select(all_of(tbl_keys("capture", mdc)),
#'        species_capture,
#'        bd_swab_id) %>%
#'   filter(!is.na(bd_swab_id))
#' 
#' # create and filter join table
#' tbl_capture_brazil = tbl_join(dbcon, capture_chain, tbl=db_capture) %>%
#'   filter(region == "brazil")
#' 
#' # collect (pull) data from database
#' capture_brazil = tbl_capture_brazil %>%
#'   collect()
#' 
#' @importFrom DBI Id
#' @importFrom dplyr %>% tbl select any_of left_join full_join inner_join right_join
#' @importFrom stats na.omit
#' @export
tbl_join = function(dbcon, link, tbl=NA, join="left", by="pkey", columns=NA) {
  # load table if not provided
  if (is.na(tbl)[[1]]) {
    tbl = tbl(dbcon, link$root$table) %>%
      select(any_of(unique(unlist(c(
        link$root$pkey,
        link$root$nkey,
        link$root$fkey,
        columns
      )))))
  }
  
  # for each parent in link object
  for (pp in link$parents) {
    # pull for
    tbl_next = tbl(dbcon, Id(pp$schema, pp$table)) %>%
      select(any_of(na.omit(unique(unlist(c(
        pp$pkey,
        pp$nkey,
        pp$fkey,
        columns
      ))))))
    
    cat("Joining with", pp$table, "...")
    
    if (join == "left") {
      tbl = tbl %>%
        left_join(tbl_next, by = c(pp[[by]]))
    } else if (join == "full") {
      tbl = tbl %>%
        full_join(tbl_next, by = c(pp[[by]]))
    } else if (join == "inner") {
      tbl = tbl %>%
        inner_join(tbl_next, by = c(pp[[by]]))
    } else if (join == "right") {
      tbl = tbl %>%
        right_join(tbl_next, by = c(pp[[by]]))
    } else {
      stop(join, " is not a valid join type... YET. Should it be included?")
    }
    
    cat("done.\n")
    
  }
  
  return(tbl)
}
