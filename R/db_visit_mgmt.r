#' Match records to visits
#'
#' Matches records (e.g. eDNA samples) to a visit at the same site: exactly (same date & time of day) where such a
#' visit exists, otherwise to the nearest visit within `day_window` days. Candidates are ranked by date offset, then
#' same time of day as the record, then `prefer_tod`. A tie after ranking is "ambiguous" and is left unmatched.
#' Unmatched records keep their own date & time of day, so they make their own visit as before.
#'
#' The record's own date is left untouched (it remains the collection date): the visit keys are returned in new columns.
#' @param data data frame of records, with site, date & time of day columns (time of day may be NA where unknown)
#' @param visits data frame of candidate visits with columns named as `site_col`, "date" & "time_of_day", and
#'   optionally "visit_id". Typically visits with non-eDNA surveys (see `db_anchor_visits()`) plus this batch's own
#'   non-eDNA visit keys.
#' @param site_col name of the site column, shared by `data` & `visits` ("site_id" by default, or "site")
#' @param date_col name of the date column in `data`
#' @param tod_col name of the time of day column in `data`
#' @param day_window maximum date offset (in days) for a nearby match
#' @param prefer_tod time of day preferred between otherwise equal candidates
#' @return `data` with columns visit_date, visit_time_of_day, visit_match ("exact", "nearby", "ambiguous", "none") &
#'   matched_visit_id (NA if `visits` has no visit_id, or unmatched)
#' @importFrom dplyr %>% select distinct filter mutate group_by ungroup arrange desc left_join inner_join summarise first n if_else
#' @export
match_visit = function(data, visits, site_col = "site_id", date_col = "date", tod_col = "time_of_day",
                       day_window = 1, prefer_tod = "night") {
  if (!all(c(site_col, date_col, tod_col) %in% colnames(data))) {
    stop("data must have columns: ", paste(c(site_col, date_col, tod_col), collapse = ", "))
  }
  if (!all(c(site_col, "date", "time_of_day") %in% colnames(visits))) {
    stop("visits must have columns: ", paste(c(site_col, "date", "time_of_day"), collapse = ", "))
  }
  out_cols = c("visit_date", "visit_time_of_day", "visit_match", "matched_visit_id")
  if (any(out_cols %in% colnames(data))) {
    stop("data already has column(s): ", paste(intersect(out_cols, colnames(data)), collapse = ", "))
  }

  if (!("visit_id" %in% colnames(visits))) {
    visits$visit_id = NA_character_
  }

  keys = data.frame(.site = data[[site_col]],
                    .date = as.Date(data[[date_col]]),
                    .tod = as.character(data[[tod_col]])) %>%
    distinct()

  # one candidate per visit key, keeping a known visit_id
  cand = data.frame(.site = visits[[site_col]],
                    v_date = as.Date(visits$date),
                    v_tod = as.character(visits$time_of_day),
                    v_id = as.character(visits$visit_id)) %>%
    filter(!is.na(.site), !is.na(v_date)) %>%
    arrange(is.na(v_id)) %>%
    distinct(.site, v_date, v_tod, .keep_all = TRUE)

  ranked = keys %>%
    filter(!is.na(.site), !is.na(.date)) %>%
    inner_join(cand, by = ".site", relationship = "many-to-many") %>%
    mutate(offset = abs(as.numeric(v_date - .date)),
           tod_same = (!is.na(v_tod) & !is.na(.tod) & v_tod == .tod) | (is.na(v_tod) & is.na(.tod)),
           tod_pref = v_tod %in% prefer_tod) %>%
    filter(offset <= day_window) %>%
    arrange(.site, .date, .tod, offset, desc(tod_same), desc(tod_pref)) %>%
    group_by(.site, .date, .tod) %>%
    # candidates ranked equal to the best one
    filter(offset == first(offset), tod_same == first(tod_same), tod_pref == first(tod_pref)) %>%
    summarise(n_best = n(),
              visit_date = first(v_date),
              visit_time_of_day = first(v_tod),
              matched_visit_id = first(v_id),
              exact = first(offset) == 0 & first(tod_same),
              .groups = "drop") %>%
    mutate(visit_match = ifelse(n_best > 1, "ambiguous", ifelse(exact, "exact", "nearby")))

  matched = keys %>%
    left_join(ranked %>% select(-n_best, -exact), by = c(".site", ".date", ".tod")) %>%
    mutate(visit_match = ifelse(is.na(visit_match), "none", visit_match),
           unmatched = visit_match %in% c("none", "ambiguous"),
           visit_date = if_else(unmatched, .date, visit_date),
           visit_time_of_day = if_else(unmatched, .tod, visit_time_of_day),
           matched_visit_id = if_else(unmatched, NA_character_, matched_visit_id)) %>%
    select(-unmatched)

  n_ambiguous = sum(matched$visit_match == "ambiguous")
  if (n_ambiguous > 0) {
    warning(n_ambiguous, " site/date/time of day key(s) match more than one visit equally well: left unmatched (visit_match == \"ambiguous\").")
  }

  data_keys = data.frame(.site = data[[site_col]],
                         .date = as.Date(data[[date_col]]),
                         .tod = as.character(data[[tod_col]]))
  out = data_keys %>%
    left_join(matched, by = c(".site", ".date", ".tod"))

  data$visit_date = out$visit_date
  data$visit_time_of_day = out$visit_time_of_day
  data$visit_match = out$visit_match
  data$matched_visit_id = out$matched_visit_id
  data
}


# tables (and columns) with a foreign key to schema.table
fk_children = function(dbcon, schema, table) {
  DBI::dbGetQuery(dbcon, "
    select cn.nspname as table_schema, cl.relname as table_name, a.attname as column_name
    from pg_constraint c
    join pg_class cl on cl.oid = c.conrelid
    join pg_namespace cn on cn.oid = cl.relnamespace
    join pg_class rl on rl.oid = c.confrelid
    join pg_namespace rn on rn.oid = rl.relnamespace
    join pg_attribute a on a.attrelid = c.conrelid and a.attnum = any(c.conkey)
    where c.contype = 'f' and rn.nspname = $1 and rl.relname = $2",
    params = list(schema, table))
}

qualified_name = function(dbcon, schema, table) {
  paste0(DBI::dbQuoteIdentifier(dbcon, schema), ".", DBI::dbQuoteIdentifier(dbcon, table))
}

pg_array = function(x) {
  paste0("{", paste(unique(stats::na.omit(as.character(x))), collapse = ","), "}")
}

# rows per id in every table with a foreign key to schema.table
fk_child_counts = function(dbcon, schema, table, ids) {
  children = fk_children(dbcon, schema, table)
  counts = lapply(seq_len(nrow(children)), function(ii) {
    tbl_name = qualified_name(dbcon, children$table_schema[ii], children$table_name[ii])
    col = DBI::dbQuoteIdentifier(dbcon, children$column_name[ii])
    res = DBI::dbGetQuery(dbcon,
                          paste0("select ", col, "::text as id, count(*)::integer as n from ", tbl_name,
                                 " where ", col, " = any($1::uuid[]) group by ", col),
                          params = list(pg_array(ids)))
    res$child_table = paste0(children$table_schema[ii], ".", children$table_name[ii])
    res
  })
  dplyr::bind_rows(data.frame(id = character(0), n = integer(0), child_table = character(0)), counts)
}


#' Visits to match records to
#'
#' Pulls visits with at least one survey of a detection type other than `exclude_detection_types`: the visits other
#' records (e.g. eDNA) are matched to with `match_visit()`. Visits holding nothing but eDNA are not anchors.
#' @param dbcon Database connection (A valid and active DBI database connection object)
#' @param site_ids site_id(s) to limit to, NULL for all sites
#' @param exclude_detection_types detection types which do not make a visit an anchor
#' @param nonempty Logical: only count surveys with rows in a child table (capture, environmental, ...)? TRUE by default.
#' @param schema schema of the visit, survey & site tables
#' @return data frame with visit_id, site_id, site, date & time_of_day
#' @export
db_anchor_visits = function(dbcon, site_ids = NULL, exclude_detection_types = "edna", nonempty = TRUE, schema = "survey_data") {
  sql = paste0(
    "select distinct v.visit_id::text as visit_id, v.site_id::text as site_id, s.site, v.date, v.time_of_day
     from ", qualified_name(dbcon, schema, "visit"), " v
     join ", qualified_name(dbcon, schema, "site"), " s on s.site_id = v.site_id
     join ", qualified_name(dbcon, schema, "survey"), " sv on sv.visit_id = v.visit_id
     where not (sv.detection_type = any($1::text[]))")
  params = list(pg_array(exclude_detection_types))

  if (!is.null(site_ids)) {
    sql = paste0(sql, " and v.site_id = any($2::uuid[])")
    params = c(params, list(pg_array(site_ids)))
  }

  if (nonempty) {
    children = fk_children(dbcon, schema, "survey")
    if (nrow(children) > 0) {
      exists_sql = vapply(seq_len(nrow(children)), function(ii) {
        paste0("exists (select 1 from ", qualified_name(dbcon, children$table_schema[ii], children$table_name[ii]),
               " c where c.", DBI::dbQuoteIdentifier(dbcon, children$column_name[ii]), " = sv.survey_id)")
      }, character(1))
      sql = paste0(sql, " and (", paste(exists_sql, collapse = " or "), ")")
    }
  }

  DBI::dbGetQuery(dbcon, sql, params = params)
}


#' Merge visits
#'
#' Moves surveys from one visit to another at the same site, then deletes visits left without surveys. Survey ids are
#' kept, so ids derived from them (e.g. edna_sample_id) remain valid. Surveys without timestamps are first given those
#' of their original visit date (`survey_timestamps_utc()`), so the original date is kept on the survey.
#'
#' A moved survey matching a survey already on the target visit (same detection type, subtype, transect & start time)
#' is absorbed into it: its child rows (edna_sample, capture, ...) are re-pointed and it is deleted. The survivor is the
#' target visit's survey where there is one, otherwise the moved survey with the most child rows. The survivor's
#' timestamps are widened to cover the surveys absorbed.
#'
#' The target visit's date is not changed. Where a survey on it now starts on an earlier local date, it is listed in
#' `date_check` for review.
#' @param dbcon Database connection (A valid and active DBI database connection object)
#' @param visit_map data frame with from_visit_id & to_visit_id, and optionally survey_id to move only those surveys
#'   (NA survey_id: all surveys of from_visit_id)
#' @param dry_run Logical: report the changes without writing? TRUE by default.
#' @param carry_comments Logical: append comments of deleted visits to the visit they are merged into? TRUE by default.
#' @param schema schema of the visit, survey, site & region tables
#' @return list of data frames: surveys (one row per moved survey and its action), visits_deleted, visit_comments,
#'   date_check; and executed (logical)
#' @importFrom dplyr %>% select distinct filter mutate group_by ungroup arrange desc left_join inner_join anti_join summarise first n coalesce rename bind_rows
#' @importFrom lubridate ymd_hms with_tz
#' @export
db_merge_visits = function(dbcon, visit_map, dry_run = TRUE, carry_comments = TRUE, schema = "survey_data") {
  if (!all(c("from_visit_id", "to_visit_id") %in% colnames(visit_map))) {
    stop("visit_map must have columns from_visit_id & to_visit_id")
  }
  vm = data.frame(from_visit_id = as.character(visit_map$from_visit_id),
                  to_visit_id = as.character(visit_map$to_visit_id),
                  map_survey_id = if ("survey_id" %in% colnames(visit_map)) as.character(visit_map$survey_id) else NA_character_) %>%
    distinct()

  if (any(is.na(vm$from_visit_id) | is.na(vm$to_visit_id))) stop("NA visit ids in visit_map")
  if (any(vm$from_visit_id == vm$to_visit_id)) stop("from_visit_id equals to_visit_id in visit_map")
  if (length(intersect(vm$from_visit_id, vm$to_visit_id)) > 0) stop("Visits both merged from and into: merge in steps")

  q = function(sql, params = NULL) DBI::dbGetQuery(dbcon, sql, params = params)
  ts_text = function(x) ifelse(is.na(x), NA_character_, format(x, "%Y-%m-%d %H:%M:%S", tz = "UTC"))

  # visits
  visits = q(paste0(
    "select v.visit_id::text as visit_id, v.site_id::text as site_id, v.date, v.time_of_day, v.comments_visit, r.time_zone
     from ", qualified_name(dbcon, schema, "visit"), " v
     join ", qualified_name(dbcon, schema, "site"), " s on s.site_id = v.site_id
     join ", qualified_name(dbcon, schema, "region"), " r on r.region_id = s.region_id
     where v.visit_id = any($1::uuid[])"),
    list(pg_array(c(vm$from_visit_id, vm$to_visit_id))))

  missing_visits = setdiff(c(vm$from_visit_id, vm$to_visit_id), visits$visit_id)
  if (length(missing_visits) > 0) stop("Visits not found: ", paste(missing_visits, collapse = ", "))

  vm = vm %>%
    left_join(visits %>% select(visit_id, from_site = site_id), by = c("from_visit_id" = "visit_id")) %>%
    left_join(visits %>% select(visit_id, to_site = site_id), by = c("to_visit_id" = "visit_id"))
  if (any(vm$from_site != vm$to_site)) stop("visit_map merges visits at different sites")

  # surveys on all visits involved
  survey_cols = q(paste0("select column_name from information_schema.columns where table_schema = $1 and table_name = 'survey'"),
                  list(schema))$column_name
  has_precision = "timestamp_precision" %in% survey_cols
  surveys = q(paste0(
    "select survey_id::text as survey_id, visit_id::text as visit_id, detection_type, detection_subtype, transect,
            start_time::text as start_time, end_time::text as end_time,
            to_char(start_timestamp_utc, 'YYYY-MM-DD HH24:MI:SS') as start_ts,
            to_char(end_timestamp_utc, 'YYYY-MM-DD HH24:MI:SS') as end_ts, ",
    if (has_precision) "timestamp_precision" else "null::text as timestamp_precision", "
     from ", qualified_name(dbcon, schema, "survey"), " where visit_id = any($1::uuid[])"),
    list(pg_array(visits$visit_id))) %>%
    mutate(start_ts = ymd_hms(start_ts, tz = "UTC", quiet = TRUE),
           end_ts = ymd_hms(end_ts, tz = "UTC", quiet = TRUE))

  # surveys to move
  src = surveys %>%
    inner_join(vm %>% select(from_visit_id, to_visit_id, map_survey_id), by = c("visit_id" = "from_visit_id"),
               relationship = "many-to-many") %>%
    filter(is.na(map_survey_id) | map_survey_id == survey_id) %>%
    select(-map_survey_id) %>%
    distinct() %>%
    rename(from_visit_id = visit_id)
  if (any(duplicated(src$survey_id))) stop("Surveys mapped to more than one visit")

  # timestamps from the original visit date, where missing
  src = src %>%
    left_join(visits %>% select(visit_id, from_date = date, time_zone), by = c("from_visit_id" = "visit_id")) %>%
    mutate(backfilled = is.na(start_ts))
  if (any(src$backfilled & is.na(src$time_zone))) stop("Region time_zone missing for visits with surveys to backfill")
  for (tz in unique(src$time_zone[src$backfilled])) {
    idx = which(src$backfilled & src$time_zone == tz)
    ts = survey_timestamps_utc(src$from_date[idx], src$start_time[idx], src$end_time[idx], tz)
    src$start_ts[idx] = ts$start_timestamp_utc
    src$end_ts[idx] = ts$end_timestamp_utc
    src$timestamp_precision[idx] = ts$timestamp_precision
  }

  # child rows per survey
  child_n = fk_child_counts(dbcon, schema, "survey", surveys$survey_id) %>%
    group_by(id) %>%
    summarise(n_children = sum(n), .groups = "drop")

  # surveys sharing keys on the target visit: one survivor, the others are absorbed
  targets = surveys %>%
    filter(visit_id %in% vm$to_visit_id) %>%
    mutate(role = "target", to_visit_id = visit_id, backfilled = FALSE)
  grouped = bind_rows(targets, src %>% mutate(role = "moved")) %>%
    left_join(child_n, by = c("survey_id" = "id")) %>%
    mutate(n_children = coalesce(n_children, 0L)) %>%
    group_by(to_visit_id, detection_type, detection_subtype, transect, start_time) %>%
    arrange(desc(role == "target"), desc(n_children), start_ts, survey_id, .by_group = TRUE) %>%
    mutate(survivor_survey_id = first(survey_id),
           # widen the survivor over the surveys absorbed which hold rows
           group_start = suppressWarnings(min(start_ts[role == "target" | n_children > 0 | survey_id == first(survey_id)], na.rm = TRUE)),
           group_end = suppressWarnings(max(end_ts[role == "target" | n_children > 0 | survey_id == first(survey_id)], na.rm = TRUE)),
           group_precision = ifelse(any(timestamp_precision %in% "day"), "day", first(timestamp_precision)),
           n_group = n()) %>%
    ungroup() %>%
    mutate(group_start = ifelse(is.finite(group_start), group_start, NA),
           group_end = ifelse(is.finite(group_end), group_end, NA),
           group_start = .POSIXct(group_start, tz = "UTC"),
           group_end = .POSIXct(group_end, tz = "UTC"))

  plan = grouped %>%
    filter(role == "moved") %>%
    mutate(action = ifelse(survey_id == survivor_survey_id, "move", "absorb"),
           start_timestamp_utc = ifelse(action == "move" & n_group > 1, group_start, start_ts),
           end_timestamp_utc = ifelse(action == "move" & n_group > 1, group_end, end_ts),
           start_timestamp_utc = .POSIXct(start_timestamp_utc, tz = "UTC"),
           end_timestamp_utc = .POSIXct(end_timestamp_utc, tz = "UTC"),
           timestamp_precision = ifelse(action == "move" & n_group > 1, group_precision, timestamp_precision)) %>%
    select(survey_id, detection_type, from_visit_id, to_visit_id, action, survivor_survey_id, n_children,
           start_timestamp_utc, end_timestamp_utc, timestamp_precision, backfilled)

  # target surveys absorbing others: widen their timestamps
  target_updates = grouped %>%
    filter(role == "target", survey_id == survivor_survey_id, n_group > 1) %>%
    mutate(timestamp_precision = group_precision) %>%
    filter(is.na(start_ts) | start_ts != group_start | coalesce(end_ts != group_end, TRUE)) %>%
    select(survey_id, start_timestamp_utc = group_start, end_timestamp_utc = group_end, timestamp_precision)

  # visits left without surveys
  remaining = surveys %>%
    filter(visit_id %in% vm$from_visit_id) %>%
    anti_join(src, by = "survey_id") %>%
    distinct(visit_id)
  other_children = fk_child_counts(dbcon, schema, "visit", unique(vm$from_visit_id)) %>%
    filter(child_table != paste0(schema, ".survey"))
  visits_deleted = vm %>%
    distinct(from_visit_id, to_visit_id) %>%
    filter(!(from_visit_id %in% remaining$visit_id),
           !(from_visit_id %in% other_children$id)) %>%
    left_join(visits %>% select(visit_id, date, time_of_day, comments_visit), by = c("from_visit_id" = "visit_id"))

  # comments of deleted visits carried to the target
  visit_comments = visits_deleted %>%
    filter(!is.na(comments_visit), carry_comments) %>%
    group_by(to_visit_id) %>%
    summarise(add = paste(unique(comments_visit), collapse = "; "), .groups = "drop") %>%
    left_join(visits %>% select(visit_id, comments_visit), by = c("to_visit_id" = "visit_id")) %>%
    mutate(already = mapply(function(x, y) !is.na(y) && grepl(x, y, fixed = TRUE), add, comments_visit),
           comments_visit_new = ifelse(is.na(comments_visit), add,
                                       ifelse(already, comments_visit, paste(comments_visit, add, sep = "; ")))) %>%
    filter(is.na(comments_visit) | comments_visit_new != comments_visit) %>%
    select(visit_id = to_visit_id, comments_visit = comments_visit_new)

  # target visits with surveys starting on an earlier local date
  date_check = bind_rows(targets %>% filter(!(survey_id %in% target_updates$survey_id)) %>% select(to_visit_id, start_ts),
                         targets %>% select(survey_id, to_visit_id) %>%
                           inner_join(target_updates %>% select(survey_id, start_ts = start_timestamp_utc), by = "survey_id") %>%
                           select(-survey_id),
                         plan %>% filter(action == "move") %>% select(to_visit_id, start_ts = start_timestamp_utc)) %>%
    left_join(visits %>% select(visit_id, date, time_zone), by = c("to_visit_id" = "visit_id")) %>%
    filter(!is.na(start_ts)) %>%
    mutate(start_local_date = as.Date(vapply(seq_along(start_ts), function(ii) format(with_tz(start_ts[ii], time_zone[ii]), "%Y-%m-%d"), character(1)))) %>%
    group_by(visit_id = to_visit_id, date) %>%
    summarise(earliest_survey_date = min(start_local_date), .groups = "drop") %>%
    filter(earliest_survey_date < date)

  out = list(surveys = plan,
             target_surveys_widened = target_updates,
             visits_deleted = visits_deleted %>% select(visit_id = from_visit_id, merged_into = to_visit_id, date, time_of_day, comments_visit),
             visits_kept = vm %>% distinct(from_visit_id) %>% filter(!(from_visit_id %in% visits_deleted$from_visit_id)),
             visit_comments = visit_comments,
             date_check = date_check,
             executed = FALSE)

  message(sum(plan$action == "move"), " survey(s) moved, ", sum(plan$action == "absorb"), " absorbed (",
          sum(plan$n_children[plan$action == "absorb"]), " child rows re-pointed), ",
          sum(plan$backfilled), " given timestamps from their visit date. ",
          nrow(visits_deleted), " visit(s) deleted. ", nrow(date_check), " target visit(s) to review in date_check.")

  if (dry_run) {
    return(out)
  }

  survey_tbl = qualified_name(dbcon, schema, "survey")
  survey_children = fk_children(dbcon, schema, "survey")
  set_precision = if (has_precision) ", timestamp_precision = $4" else ""

  DBI::dbWithTransaction(dbcon, {
    # moved survivors: new visit & timestamps
    moves = plan %>% filter(action == "move")
    for (ii in seq_len(nrow(moves))) {
      params = list(moves$to_visit_id[ii], ts_text(moves$start_timestamp_utc[ii]), ts_text(moves$end_timestamp_utc[ii]))
      if (has_precision) params = c(params, list(moves$timestamp_precision[ii]))
      params = c(params, list(moves$survey_id[ii]))
      DBI::dbExecute(dbcon, paste0("update ", survey_tbl, " set visit_id = $1::uuid, start_timestamp_utc = $2::timestamp, end_timestamp_utc = $3::timestamp",
                                   set_precision, " where survey_id = $", length(params), "::uuid"),
                     params = params)
    }

    # target survivors: widened timestamps
    for (ii in seq_len(nrow(target_updates))) {
      params = list(ts_text(target_updates$start_timestamp_utc[ii]), ts_text(target_updates$end_timestamp_utc[ii]))
      if (has_precision) params = c(params, list(target_updates$timestamp_precision[ii]))
      params = c(params, list(target_updates$survey_id[ii]))
      DBI::dbExecute(dbcon, paste0("update ", survey_tbl, " set start_timestamp_utc = $1::timestamp, end_timestamp_utc = $2::timestamp",
                                   if (has_precision) ", timestamp_precision = $3" else "",
                                   " where survey_id = $", length(params), "::uuid"),
                     params = params)
    }

    # absorbed: re-point child rows to the survivor, then delete
    absorbs = plan %>% filter(action == "absorb")
    for (ii in seq_len(nrow(absorbs))) {
      for (jj in seq_len(nrow(survey_children))) {
        col = DBI::dbQuoteIdentifier(dbcon, survey_children$column_name[jj])
        DBI::dbExecute(dbcon, paste0("update ", qualified_name(dbcon, survey_children$table_schema[jj], survey_children$table_name[jj]),
                                     " set ", col, " = $1::uuid where ", col, " = $2::uuid"),
                       params = list(absorbs$survivor_survey_id[ii], absorbs$survey_id[ii]))
      }
      DBI::dbExecute(dbcon, paste0("delete from ", survey_tbl, " where survey_id = $1::uuid"), params = list(absorbs$survey_id[ii]))
    }

    # visits
    for (ii in seq_len(nrow(visit_comments))) {
      DBI::dbExecute(dbcon, paste0("update ", qualified_name(dbcon, schema, "visit"), " set comments_visit = $1 where visit_id = $2::uuid"),
                     params = list(visit_comments$comments_visit[ii], visit_comments$visit_id[ii]))
    }
    for (ii in seq_len(nrow(visits_deleted))) {
      DBI::dbExecute(dbcon, paste0("delete from ", qualified_name(dbcon, schema, "visit"), " where visit_id = $1::uuid"),
                     params = list(visits_deleted$from_visit_id[ii]))
    }
  })

  out$executed = TRUE
  out
}
