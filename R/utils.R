setwd("D:/ucr_data_storage/clean_data/nibrs")
library(lubridate)
library(stringr)
library(fastDummies)
library(tidyr)
library(janitor)
library(dplyr)
library(splitstackshape)
library(crimeutils)
library(here)
library(parallel)
age_fix <- c("over 98 years old"            = "99",
             "under 24 hours \\(neonate\\)" = "0",
             "1-6 days old"                 = "0",
             "7-364 days old"               = "0")

race_fix <- c("^asian/pacific islander$"                    = "asian",
              "^P$"                                         = "asian",
              "^M$"                                         = "unknown",
              "^native hawaiian or other pacific islander$" = "asian",
              "^american indian/alaskan native$"            = "american indian")
ethnicity_fix <- c("^not of hispanic origin$" = "not_hispanic",
                   "^hispanic origin$"         = "hispanic")


# Keep most common year
keep_most_common_year <- function(data) {
  most_common_year <- sort(table(data$year), decreasing = TRUE)[1]
  most_common_year <- names(most_common_year)
  data <-
    data %>%
    filter(year(date) %in% most_common_year)
  return(data)
}


prep_offense <- function(file) {
  data <- readRDS(file) %>%
    mutate(date = ymd(incident_date),
           date = floor_date(date, unit = "month"),
           year = year(date)) %>%
    select(ori,
           unique_incident_id,
           date,
           year,
           offense = ucr_offense_code,
           type_weapon_force_involved_1,
           type_weapon_force_involved_2,
           type_weapon_force_involved_3) %>%
    mutate_if(is.character, tolower)
  data <- keep_most_common_year(data)
  data$gun_involved <- "no_gun"
  for (i in 1:3) {
    data$temp <- data[, paste0("type_weapon_force_involved_", i)]
    data$temp[is.na(data$temp)] <- "none"
    data$gun_involved[data$temp %in% c("handgun",
                                       "firearm (type not stated)",
                                       "rifle",
                                       "shotgun",
                                       "other firearm")] <- "gun"
    data[, paste0("type_weapon_force_involved_", i)] <- NULL
  }
  data$temp <- NULL

  data$offense <- paste("offense", data$offense)
  data$gun_involved <- paste("offense", data$gun_involved)
  return(data)
}


prep_victim <- function(file) {
  data <- readRDS(file) %>%
    select(ori,
           type_of_victim,
           unique_incident_id,
           ucr_offense_code_1,
           ucr_offense_code_2,
           ucr_offense_code_3,
           ucr_offense_code_4,
           ucr_offense_code_5,
           ucr_offense_code_6,
           ucr_offense_code_7,
           ucr_offense_code_8,
           ucr_offense_code_9,
           ucr_offense_code_10,
           age_of_victim,
           sex_of_victim,
           race_of_victim,
           ethnicity_of_victim,
           type_of_injury_1,
           type_of_injury_2,
           type_of_injury_3,
           type_of_injury_4,
           type_of_injury_5,
           incident_date) %>%
    mutate(date = ymd(incident_date),
           date = floor_date(date, unit = "month"),
           year = year(date)) %>%
    select(-incident_date) %>%
    mutate_if(is.character, tolower)
  data <- keep_most_common_year(data)
  data$victim_injury           <- "unknown injury"
  for (i in 1:5) {
    data$temp <- data[, paste0("type_of_injury_", i)]
    data$temp[is.na(data$temp)] <- "unknown injury"
    data$victim_injury[data$temp %in% "none"] <- "no injury"
    data$victim_injury[data$temp %in% "apparent minor injuries"] <- "minor injury"
    data$victim_injury[data$temp %in% c("severe laceration",
                                        "apparent broken bones",
                                        "other major injury",
                                        "unconsciousness",
                                        "loss of teeth",
                                        "possible internal injury")] <- "serious injury"

    data[, paste0("type_of_injury_", i)] <- NULL
  }
  data$temp <- NULL

  # Loops through each of the 10 UCR offenses and turns people who had multiple
  # crimes into multiple rows. For example, if someone had 3 crimes then would
  # becomes 3 rows in the data
  final <- data.frame()
  ucr_offense_cols <- grep("ucr_offense", names(data), value = TRUE)
  for (i in 1:10) {
    # Keeps all rows where there is a crime - i.e. is not NA
    temp <- data[!is.na(data[, paste0("ucr_offense_code_", i)]), ]
    if (nrow(temp) > 0) {
      temp$offense <- temp[, paste0("ucr_offense_code_", i)]
      # Drops all the UCR columns
      temp <- temp[!names(temp) %in% ucr_offense_cols]

      final <- bind_rows(final, temp)
    }
  }
  data <- final

  data$age_of_victim <- str_replace_all(data$age_of_victim, age_fix)
  data$age_of_victim[data$age_of_victim %in% "unknown"] <- NA
  data$age_of_victim <- as.numeric(data$age_of_victim)

  data$age_category                                <- "victim_unknown_age"
  data$age_category[data$age_of_victim %in% 0:17]  <- "victim_juvenile"
  data$age_category[data$age_of_victim %in% 18:99] <- "victim_adult"

  data$sex_of_victim[data$sex_of_victim %in%
                       c(NA, "unknown")] <- "unknown_sex"
  data$race_of_victim[data$race_of_victim %in%
                        c(NA, "unknown")] <- "unknown_race"
  data$ethnicity_of_victim[data$ethnicity_of_victim %in%
                             c(NA, "unknown")] <- "unknown_ethnicity"
  data$ethnicity_of_victim <- tolower(data$ethnicity_of_victim)
  data$ethnicity_of_victim <- str_replace_all(data$ethnicity_of_victim, ethnicity_fix)

  data$race_of_victim <- tolower(data$race_of_victim)
  data$race_of_victim <- str_replace_all(data$race_of_victim, race_fix)

  data$sex_of_victim       <- paste0("victim_", data$sex_of_victim)
  data$race_of_victim      <- paste0("victim_", data$race_of_victim)
  data$ethnicity_of_victim <- paste0("victim_", data$ethnicity_of_victim)
  data$victim_injury       <- paste0("victim_", data$victim_injury)
  data$offense             <- paste("victim", data$offense)
  return(data)
}


prep_offender <- function(file, offense_data) {
  data <- readRDS(file) %>%
    select(ori,
           unique_incident_id,
           age_of_offender,
           sex_of_offender,
           race_of_offender,
           incident_date) %>%
    mutate(date = ymd(incident_date),
           date = floor_date(date, unit = "month"),
           year = year(date)) %>%
    select(-incident_date) %>%
    mutate_if(is.character, tolower) %>%
    filter(ori %in% offense_data$ori) %>%
    left_join(offense_data, by = c("ori", "unique_incident_id"))
  data <- keep_most_common_year(data)
  data$offense <- gsub("^offense ", "", data$offense)

  data$sex_of_offender[data$sex_of_offender %in% c(NA, "unknown")] <- "unknown_sex"
  data$sex_of_offender <- paste0("offender_", data$sex_of_offender)

  data$race_of_offender <- str_replace_all(data$race_of_offender, race_fix)
  data$race_of_offender[data$race_of_offender %in% c(NA, "unknown")] <- "unknown_race"
  data$race_of_offender <- paste0("offender_", data$race_of_offender)

  data$age_of_offender <- str_replace_all(data$age_of_offender, age_fix)
  data$age_of_offender[data$age_of_offender %in% "unknown"] <- NA
  data$age_of_offender <- as.numeric(data$age_of_offender)
  data$age_category <- "offender_unknown_age"
  data$age_category[data$age_of_offender %in% 0:17]  <- "offender_juvenile"
  data$age_category[data$age_of_offender %in% 18:99] <- "offender_adult"


  data$offense <- paste("offender", data$offense)
  return(data)
}


prep_arrestee <- function(arrestee_file, arrestee_group_b_file) {
  arrestee         <- readRDS(arrestee_file)
  arrestee_group_b <- readRDS(arrestee_group_b_file) %>%
    select(-arrestee_weapon_1,
           -arrestee_weapon_2,
           -automatic_weapon_indicator_1,
           -automatic_weapon_indicator_2)
  data         <- bind_rows(arrestee, arrestee_group_b) %>%
    select(ori,
           arrest_date,
           type_of_arrest,
           offense = ucr_arrest_offense_code,
           age_of_arrestee,
           sex_of_arrestee,
           race_of_arrestee,
           ethnicity_of_arrestee) %>%
    mutate(date = ymd(arrest_date),
           date = floor_date(date, unit = "month"),
           year = year(date)) %>%
    select(-arrest_date) %>%
    mutate_if(is.character, tolower)
  data <- keep_most_common_year(data)

  data$type_of_arrest <- gsub(" \\(.*", "", data$type_of_arrest)

  data$age_of_arrestee <- str_replace_all(data$age_of_arrestee, age_fix)
  data$age_of_arrestee[data$age_of_arrestee %in% "unknown"] <- NA
  data$age_of_arrestee <- as.numeric(data$age_of_arrestee)
  data$age_category <- "arrestee_unknown_age"
  data$age_category[data$age_of_arrestee %in% 0:17]  <- "arrestee_juvenile"
  data$age_category[data$age_of_arrestee %in% 18:99] <- "arrestee_adult"

  data$ethnicity_of_arrestee <- tolower(data$ethnicity_of_arrestee)
  data$ethnicity_of_arrestee <- str_replace_all(data$ethnicity_of_arrestee, ethnicity_fix)

  data$race_of_arrestee <- str_replace_all(data$race_of_arrestee, race_fix)
  data$sex_of_arrestee[data$sex_of_arrestee %in% c(NA, "unknown")]             <- "unknown_sex"
  data$race_of_arrestee[data$race_of_arrestee %in% c(NA, "unknown")]           <- "unknown_race"
  data$ethnicity_of_arrestee[data$ethnicity_of_arrestee %in% c(NA, "unknown")] <- "unknown_ethnicity"

  data$sex_of_arrestee       <- paste0("arrestee_", data$sex_of_arrestee)
  data$race_of_arrestee      <- paste0("arrestee_", data$race_of_arrestee)
  data$ethnicity_of_arrestee <- paste0("arrestee_", data$ethnicity_of_arrestee)
  data$offense               <- paste("arrestee", data$offense)
  data$type_of_arrest        <- paste("arrestee", data$type_of_arrest)
  return(data)
}


aggregate_data <- function(data, variables = NULL, time_unit, victim_type = FALSE) {

  # Drop NIBRS offenses without names - EXTREMELY rare. These are likely
  # new subtypes of crimes.
  if(any(grepl("30A|61A|90K", data$offense))) {
    data <- data[-grep("30A|61A|90K", data$offense, ignore.case = TRUE), ]
  }

  weapon_crimes <- c("aggravated assault",
                     "extortion/blackmail",
                     "fondling (incident liberties/child molest)",
                     "human trafficking - commercial sex acts",
                     "human trafficking - involuntary servitude",
                     "justifiable homicide",
                     "kidnapping/abduction",
                     "murder/nonnegligent manslaughter",
                     "negligent manslaughter",
                     "rape",
                     "robbery",
                     "sexual assault with an object",
                     "sodomy",
                     "weapon law violations")
  injury_crimes <- c("aggravated assault",
                     "extortion/blackmail",
                     "fondling (incident liberties/child molest)",
                     "human trafficking - commercial sex acts",
                     "human trafficking - involuntary servitude",
                     "kidnapping/abduction",
                     "rape",
                     "robbery",
                     "sexual assault with an object",
                     "simple assault",
                     "sodomy")
  person_victim_types <- c("individual",
                           "law enforcement officer")

  data$time_unit <- data[, time_unit]
  cols_temp <- c("ori",
                 "time_unit",
                 "offense")
  main_agg <- data %>%
    count_(cols_temp) %>%
    pivot_wider(names_from  = offense,
                values_from = n) %>%
    rename_all(make_clean_names)
  main_agg[is.na(main_agg)] <- 0
  main_agg <- dummy_rows(main_agg, select_columns = c("ori", "time_unit"),
                         dummy_value = NA)

  if (!is.null(variables)) {
    for (variable in variables) {
      cols_temp <- c("ori",
                     "time_unit",
                     "offense",
                     variable)
      temp_agg <- data %>%
        count_(cols_temp)

      # Not all crimes can have weapons involved.
      # Subsets to only ones that can
      if (variable %in% "gun_involved") {
        temp_agg <-
          data %>%
          count_(cols_temp) %>%
          filter(offense %in% paste("offense", weapon_crimes))
      }
      # Only person victims ("individual" or "law enforcement officer"
      # can have demographics)
      if (victim_type) {
        temp_agg <-
          data %>%
          filter(type_of_victim %in% person_victim_types) %>%
          count_(cols_temp)
      }
      # Not all crimes can have victim injury involved.
      # Subsets to only ones that can
      if (variable %in% "victim_injury") {
        temp_agg <-
          temp_agg %>%
          filter(offense %in% paste("victim", injury_crimes))
      }


      temp_agg$offense_variable <- paste(temp_agg[, variable], temp_agg$offense)
      temp_agg[, variable] <- NULL
      temp_agg <-
        temp_agg %>%
        select(-offense) %>%
        pivot_wider(names_from  = offense_variable,
                    values_from = n) %>%
        rename_all(make_clean_names)
      temp_agg[is.na(temp_agg)] <- 0
      temp_agg <- dummy_rows(temp_agg, select_columns = c("ori", "time_unit"),
                             dummy_value = NA)

      main_agg <-
        main_agg %>%
        left_join(temp_agg, by = c("ori", "time_unit"))

    }
  }

  names(main_agg) <- gsub("_offense_", "_", names(main_agg))
  names(main_agg) <- gsub("_victim_", "_", names(main_agg))
  names(main_agg) <- gsub("_arrestee_", "_", names(main_agg))
  names(main_agg) <- gsub("_offender_", "_", names(main_agg))

  return(main_agg)
}



make_largest_agency_json <- function(data) {
  largest_agency <- data %>%
    filter(year %in% max(data$year)) %>%
    dplyr::group_by(state) %>%
    dplyr::top_n(1, population) %>%
    dplyr::select(state, agency)
  largest_agency <- jsonlite::toJSON(largest_agency, pretty = TRUE)
  write(largest_agency, "largest_agency_choices.json")
}

make_state_agency_choices <- function(data) {
  data <- data.table::as.data.table(data)
  for (selected_state in unique(data$state)) {
    temp   <- data[state %in% selected_state]
    agency <- unique(temp$agency)

    agency <- jsonlite::toJSON(agency, pretty = FALSE)
    write(agency, paste0(selected_state, "_agency_choices.json"))
  }
}


make_agency_csvs <- function(data,
                             type = "year") {

  data <-
    data %>%
    dplyr::group_split(ORI)
  parallel::mclapply(data,
                     make_csv_test,
                     type = type)

}

make_all_na <- function(col) {
  col <- NA
}

make_csv_test <- function(temp, type) {

  temp   <- dummy_rows_missing_years(temp, type = type)

  state  <- unique(temp$state)
  agency <- unique(temp$agency)
  state  <- gsub(" ", "_", state)
  agency <- gsub(" |:", "_", agency)
  agency <- gsub("/", "_", agency)
  agency <- gsub("_+", "_", agency)
  agency <- gsub("\\(|\\)", "", agency)

  data.table::fwrite(temp, file = paste0(state, "_", agency, ".csv"))
}

dummy_rows_missing_years <- function(data, type) {

  if (type == "year") {
    missing_years <- min(data$year):max(data$year)
  } else {
    missing_years <- seq.Date(lubridate::ymd(min(data$year)),
                              lubridate::ymd(max(data$year)),
                              by = "month")
    missing_years <- as.character(missing_years)
  }

  missing_years <- missing_years[!missing_years %in% data$year]

  if (length(missing_years) > 0) {

    temp <- data
    temp <- temp[1, ]
    temp <- splitstackshape::expandRows(temp,
                                        count = length(missing_years),
                                        count.is.col = FALSE)
    temp$year <- missing_years
    temp <-
      temp %>%
      dplyr::mutate_at(vars(-one_of("year", "agency", "state", "ORI")),
                       make_all_na)

    data <-
      data %>%
      dplyr::bind_rows(temp) %>%
      dplyr::arrange(desc(year)) %>%
      dplyr::mutate(year = as.character(year))
  }
  return(data)
}


