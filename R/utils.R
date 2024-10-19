packages <- c(
  "lubridate",
  "stringr",
  "fastDummies",
  "tidyr",
  "janitor",
  "dplyr",
  "splitstackshape",
  "crimeutils",
  "here",
  "parallel",
  "readr",
  "priceR"
)

library(groundhog)
groundhog.library(packages, "2024-10-01")

setwd("F:/ucr_data_storage/clean_data/nibrs")


age_fix <- c(
  "over 98 years old" = "99",
  "under 24 hours \\(neonate\\)" = "0",
  "1 to 6 days old \\(newborn\\)" = "0",
  "between 6 days and 1 year old \\(baby\\)" = "0",
  "7-364 days old" = "0"
)

race_fix <- c(
  "^asian/pacific islander$" = "asian",
  "^P$" = "asian",
  "^M$" = "unknown",
  "^p$" = "asian",
  "^m$" = "unknown",
  "^native hawaiian or other pacific islander$" = "asian",
  "^american indian\\/alaskan native$" = "american indian",
  "^american indian\\/alaska native$" = "american indian"
)
ethnicity_fix <- c(
  "^not hispanic or latino$" = "not_hispanic",
  "^hispanic or latino$" = "hispanic"
)

theft_crimes <- c(
  "larceny/theft offenses - all other larceny",
  "larceny/theft offenses - pocket-picking",
  "larceny/theft offenses - purse-snatching",
  "larceny/theft offenses - shoplifting",
  "larceny/theft offenses - theft from building",
  "larceny/theft offenses - theft from coin-operated machine or device",
  "larceny/theft offenses - theft from motor vehicle",
  "larceny/theft offenses - theft of motor vehicle parts/accessories"
)



relationship_unknown <- "relationship unknown"
relationship_stranger <- "victim was stranger"
relationship_other <- c(
  "victim was acquaintance",
  "victim was babysittee (the baby)",
  "victim was cohabitant",
  "victim was employee",
  "victim was employer",
  "victim was friend",
  "victim was neighbor",
  "victim was offender",
  "victim was otherwise known"
)

relationship_intimate_partner <- c(
  "victim was boyfriend/girlfriend",
  "victim was common-law spouse",
  "victim was ex-relationship (ex-boyfriend/ex-girlfriend)",
  "victim was ex-spouse",
  "victim was spouse",
  "victim was in a homosexual relationship with the offender"
)


relationship_other_family <- c(
  "victim was child",
  "victim was child of boyfriend/girlfriend",
  "victim was foster child",
  "victim was foster parent",
  "victim was grandchild",
  "victim was grandparent",
  "victim was in-law",
  "victim was other family member",
  "victim was parent",
  "victim was sibling (brother or sister)",
  "victim was step-child",
  "victim was step-parent",
  "victim was step-sibling (stepbrother or stepsister)"
)

location_other_unknown <- c(
  "abandoned/condemned structure",
  "air/bus/train terminal",
  "amusement park",
  "arena/stadium/fairgrounds/coliseum",
  "auto dealership new/used",
  "bank/savings and loan",
  "church/synagogue/temple/mosque",
  "commercial/office building",
  "community center",
  "construction site",
  "convenience store",
  "cyberspace",
  "department/discount store",
  "drug store/doctors office/hospital",
  "farm facility",
  "gambling facility/casino/race track",
  "government/public building",
  "grocery/supermarket",
  "hotel/motel/etc.",
  "industrial site",
  "jail/prison/penitentiary/corrections facility",
  "liquor store",
  "military installation",
  "other/unknown",
  "rental storage facility",
  "restaurant",
  "service/gas station",
  "shelter - mission/homeless",
  "shopping mall",
  "specialty store (tv, fur, etc.)",
  "tribal lands"
)


location_bar_club <- "bar/nightclub"
location_outside <- c(
  "atm separate from bank",
  "camp/campground",
  "dock/wharf/freight/modal terminal",
  "field/woods",
  "highway/road/alley/street/sidewalk",
  "lake/waterway/beach",
  "park/playground",
  "parking lot/drop lot/parking garage",
  "rest area"
)

location_home <- "residence/home"

location_school <- c(
  "school - college/university",
  "school - elementary/secondary",
  "school/college",
  "daycare facility"
)

subtype_buy_possess_consume <- c(
  "buying/receiving",
  "possessing/concealing",
  "using/consuming"
)
subtype_sell_create_assist <- c(
  "cultivating/manufacturing/publishing (i.e., production of any type)",
  "distributing/selling",
  "transporting/transmitting/importing",
  "operating/promoting/assisting"
)



combine_agg_data <- function(type, batch_data, states_to_keep = NULL) {
  setwd("~/crimedatatool_helper_nibrs/data/")
  files <- list.files(pattern = paste0("temp_agg_", type))
  if (type %in% c("year", "month")) {
    files <- files[-grep("property", files)]
  }
  print(files)
  final <- data.frame()
  for (file in files) {
    temp <- readRDS(file)
    if (!is.null(states_to_keep)) {
      temp <- temp %>% filter(toupper(substr(ori, 1, 2)) %in% states_to_keep)
    }


    final <- bind_rows(final, temp)
    rm(temp)
    gc()
    Sys.sleep(0.2)
    message(file)
  }

  final$ori <- toupper(final$ori)
  final <- final %>% rename(
    year = time_unit,
    ORI = ori
  )

  if (grepl("month", files[1])) {
    final <-
      final %>%
      filter(paste(ORI, year(year)) %in% paste(batch_header$ORI, batch_header$year))
  } else {
    final <-
      final %>%
      filter(paste(ORI, year) %in% paste(batch_header$ORI, batch_header$year))
  }


  names(final) <- gsub(
    "^victim_refused_to_cooperate", "offense_victim_refused_to_cooperate",
    names(final)
  )
  names(final) <- gsub(
    "^prosecution_declined", "offense_prosecution_declined",
    names(final)
  )
  names(final) <- gsub(
    "^death_of_suspect", "offense_death_of_suspect",
    names(final)
  )
  names(final) <- gsub(
    "^extradition_denied", "offense_extradition_denied",
    names(final)
  )
  names(final) <- gsub(
    "^juvenile_no_custody", "offense_juvenile_no_custody",
    names(final)
  )
  names(final) <- gsub(
    "^cleared_by_arrest", "offense_cleared_by_arrest",
    names(final)
  )

  final[is.na(final)] <- 0
  return(final)
}







prep_admin <- function(file) {
  data <- readRDS(file) %>%
    select(
      unique_incident_id,
      total_arrestee_segments,
      cleared_exceptionally
    ) %>%
    mutate_if(is.character, tolower) %>%
    distinct(unique_incident_id)

  data$cleared <- "not cleared"
  data$cleared[data$cleared_exceptionally %in% "victim refused to cooperate"] <- "victim refused to cooperate"
  data$cleared[data$cleared_exceptionally %in% "prosecution declined (for other than lack of probable cause)"] <- "prosecution declined"
  data$cleared[data$cleared_exceptionally %in% "death of offender"] <- "death of suspect"
  data$cleared[data$cleared_exceptionally %in% "extradition denied"] <- "extradition denied"
  data$cleared[data$cleared_exceptionally %in% "juvenile/no custody"] <- "juvenile/no custody"

  data$cleared[data$total_arrestee_segments > 0] <- "cleared by arrest"
  data$total_arrestee_segments <- NULL
  data$cleared_exceptionally <- NULL
  return(data)
}

prep_offense <- function(file, batch_header) {
  data <- readRDS(file) %>%
    mutate(
      date = ymd(incident_date),
      date = floor_date(date, unit = "month"),
      year = year(date)
    ) %>%
    select(ori,
      unique_incident_id,
      date,
      year,
      offense = ucr_offense_code,
      type_weapon_force_involved_1,
      type_weapon_force_involved_2,
      type_weapon_force_involved_3,
      type_criminal_activity_1,
      type_criminal_activity_2,
      type_criminal_activity_3,
      location_type
    ) %>%
    left_join(batch_header %>%
      select(
        ori = ORI,
        year,
        number_of_months_reported
      ), by = join_by(ori, year)) %>%
    filter(number_of_months_reported %in% 12) %>%
    select(-number_of_months_reported)

  firearms_not_handgun <- c(
    "firearm (type not stated)",
    "other firearm",
    "rifle",
    "shotgun"
  )

  # Top code gun
  data$gun_involved <- "no_gun"
  data$gun_involved[data$type_weapon_force_involved_1 %in% firearms_not_handgun |
    data$type_weapon_force_involved_2 %in% firearms_not_handgun |
    data$type_weapon_force_involved_3 %in% firearms_not_handgun] <- "other/unknown gun"
  data$gun_involved[data$type_weapon_force_involved_1 %in% "handgun" |
    data$type_weapon_force_involved_2 %in% "handgun" |
    data$type_weapon_force_involved_3 %in% "handgun"] <- "handgun"

  # Top code drug subtype
  data$subtype <- NA
  data$subtype[data$type_criminal_activity_1 %in% "subtype_buy_possess_consume" |
    data$type_criminal_activity_2 %in% "subtype_buy_possess_consume" |
    data$type_criminal_activity_3 %in% "subtype_buy_possess_consume"] <- "buy_possess_consume"
  data$subtype[data$type_criminal_activity_1 %in% "subtype_sell_create_assist" |
    data$type_criminal_activity_2 %in% "subtype_sell_create_assist" |
    data$type_criminal_activity_3 %in% "subtype_sell_create_assist"] <- "sell_create_assist"

  # Now top-code animal abuse
  neglect_subtype <- "simple/gross neglect (unintentionally, intentionally, or knowingly failing to provide food, water, shelter, veterinary care, hoarding, etc.)"
  torture_subtype <- "intentional abuse and torture (tormenting, mutilating, poisoning, or abandonment)"
  animal_fighting_subtype <- "organized abuse (dog fighting and cock fighting)"
  bestiality_subtype <- "animal sexual abuse (bestiality)"
  data$subtype[data$type_criminal_activity_1 %in% "neglect_subtype" |
    data$type_criminal_activity_2 %in% "neglect_subtype" |
    data$type_criminal_activity_3 %in% "neglect_subtype"] <- "neglect"
  data$subtype[data$type_criminal_activity_1 %in% "torture_subtype" |
    data$type_criminal_activity_2 %in% "torture_subtype" |
    data$type_criminal_activity_3 %in% "torture_subtype"] <- "abuse/torture"
  data$subtype[data$type_criminal_activity_1 %in% "animal_fighting_subtype" |
    data$type_criminal_activity_2 %in% "animal_fighting_subtype" |
    data$type_criminal_activity_3 %in% "animal_fighting_subtype"] <- "animal fighting"
  data$subtype[data$type_criminal_activity_1 %in% "bestiality_subtype" |
    data$type_criminal_activity_2 %in% "bestiality_subtype" |
    data$type_criminal_activity_3 %in% "bestiality_subtype"] <- "bestiality"


  data$location <- NA
  data$location[data$location_type %in% location_other_unknown] <- "other/unknown location"
  data$location[is.na(data$location_type)] <- "other/unknown location"
  data$location[data$location_type %in% location_bar_club] <- "bar/nightclub"
  data$location[data$location_type %in% location_home] <- "home"
  data$location[data$location_type %in% location_school] <- "school"
  data$location[data$location_type %in% location_outside] <- "outside"

  data <-
    data %>%
    select(
      -type_criminal_activity_1,
      -type_criminal_activity_2,
      -type_criminal_activity_3,
      -type_weapon_force_involved_1,
      -type_weapon_force_involved_2,
      -type_weapon_force_involved_3,
      -location_type
    )

  data <- group_theft_offenses(data)
  data$offense <- paste("offense", data$offense)
  data$subtype <- paste("offense", data$subtype)
  data$gun_involved <- paste("offense", data$gun_involved)
  data$location <- paste("offense", data$location)
  return(data)
}


prep_victim <- function(file, batch_header) {
  data <- readRDS(file) %>%
    select(
      ori,
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
      relation_of_vict_to_offender1,
      incident_date
    ) %>%
    mutate(
      date = ymd(incident_date),
      date = floor_date(date, unit = "month"),
      year = year(date),
      age_of_victim = str_replace_all(age_of_victim, age_fix),
      ethnicity_of_victim = str_replace_all(ethnicity_of_victim, ethnicity_fix),
      race_of_victim = str_replace_all(race_of_victim, race_fix)
    ) %>%
    select(-incident_date) %>%
    left_join(batch_header %>%
      select(
        ori = ORI,
        year,
        number_of_months_reported
      ), by = join_by(ori, year)) %>%
    filter(number_of_months_reported %in% 12) %>%
    select(-number_of_months_reported)

  # Top code injury
  serious_injury <- c(
    "severe laceration",
    "apparent broken bones",
    "other major injury",
    "unconsciousness",
    "loss of teeth",
    "possible internal injury"
  )
  data$victim_injury <- "unknown injury"
  data$victim_injury[data$type_of_injury_1 %in% "none" |
    data$type_of_injury_2 %in% "none" |
    data$type_of_injury_3 %in% "none" |
    data$type_of_injury_4 %in% "none" |
    data$type_of_injury_5 %in% "none"] <- "no injury"
  data$victim_injury[data$type_of_injury_1 %in% "apparent minor injuries" |
    data$type_of_injury_2 %in% "apparent minor injuries" |
    data$type_of_injury_3 %in% "apparent minor injuries" |
    data$type_of_injury_4 %in% "apparent minor injuries" |
    data$type_of_injury_5 %in% "apparent minor injuries"] <- "minor injury"
  data$victim_injury[data$type_of_injury_1 %in% serious_injury |
    data$type_of_injury_2 %in% serious_injury |
    data$type_of_injury_3 %in% serious_injury |
    data$type_of_injury_4 %in% serious_injury |
    data$type_of_injury_5 %in% serious_injury] <- "serious injury"


  # Loops through each of the 10 UCR offenses and turns people who had multiple
  # crimes into multiple rows. For example, if someone had 3 crimes then would
  # becomes 3 rows in the data
  final <- data.frame()
  ucr_offense_cols <- grep("ucr_offense", names(data), value = TRUE)
  for (i in 2:10) {
    # Keeps all rows where there is a crime - i.e. is not NA
    temp <- data[!is.na(data[, paste0("ucr_offense_code_", i)]), ]
    if (nrow(temp) > 0) {
      temp$offense <- temp[, paste0("ucr_offense_code_", i)]
      # Drops all the UCR columns
      temp <- temp[!names(temp) %in% ucr_offense_cols]

      final <- bind_rows(final, temp)
    }
  }
  data <-
    data %>%
    bind_rows(final)

  data <-
    data %>%
    select(
      -ucr_offense_code_1,
      -ucr_offense_code_2,
      -ucr_offense_code_3,
      -ucr_offense_code_4,
      -ucr_offense_code_5,
      -ucr_offense_code_6,
      -ucr_offense_code_7,
      -ucr_offense_code_8,
      -ucr_offense_code_9,
      -ucr_offense_code_10
    )

  data$relationship <- NA
  data$relationship[data$relation_of_vict_to_offender1 %in% c(NA, relationship_unknown)] <- "unknown relationship"
  data$relationship[data$relation_of_vict_to_offender1 %in% relationship_stranger] <- "stranger"
  data$relationship[data$relation_of_vict_to_offender1 %in% relationship_other] <- "other relationship"
  data$relationship[data$relation_of_vict_to_offender1 %in% relationship_intimate_partner] <- "intimate partner"
  data$relationship[data$relation_of_vict_to_offender1 %in% relationship_other_family] <- "other family"

  data$age_of_victim[data$age_of_victim %in% "unknown"] <- NA
  data$age_of_victim <- as.numeric(data$age_of_victim)
  data$age_category <- "victim_unknown_age"
  data$age_category[data$age_of_victim %in% 0:17] <- "victim_juvenile"
  data$age_category[data$age_of_victim %in% 18:99] <- "victim_adult"

  data$sex_of_victim[data$sex_of_victim %in%
    c(NA, "unknown")] <- "unknown_sex"
  data$race_of_victim[data$race_of_victim %in%
    c(NA, "unknown")] <- "unknown_race"
  data$ethnicity_of_victim[data$ethnicity_of_victim %in%
    c(NA, "unknown")] <- "unknown_ethnicity"


  data <- group_theft_offenses(data)
  data$sex_of_victim <- paste0("victim_", data$sex_of_victim)
  data$race_of_victim <- paste0("victim_", data$race_of_victim)
  data$ethnicity_of_victim <- paste0("victim_", data$ethnicity_of_victim)
  data$relationship <- paste0("victim_", data$relationship)
  data$victim_injury <- paste0("victim_", data$victim_injury)
  data$offense <- paste("victim", data$offense)
  return(data)
}


prep_offender <- function(file, offense_data, batch_header) {
  data <- readRDS(file) %>%
    select(
      ori,
      unique_incident_id,
      age_of_offender,
      sex_of_offender,
      race_of_offender,
      incident_date
    ) %>%
    mutate(
      date = ymd(incident_date),
      date = floor_date(date, unit = "month"),
      year = year(date)
    ) %>%
    select(-incident_date) %>%
    filter(ori %in% offense_data$ori) %>%
    left_join(offense_data, by = c("ori", "unique_incident_id"), relationship = "many-to-many") %>%
    mutate(offense = gsub("^ offense ", "", offense)) %>%
    left_join(batch_header %>%
      select(
        ori = ORI,
        year,
        number_of_months_reported
      ), by = join_by(ori, year)) %>%
    filter(number_of_months_reported %in% 12) %>%
    select(-number_of_months_reported)

  data$sex_of_offender[data$sex_of_offender %in% c(NA, "unknown")] <- "unknown_sex"
  data$sex_of_offender <- paste0("offender_", data$sex_of_offender)

  data$race_of_offender <- str_replace_all(data$race_of_offender, race_fix)
  data$race_of_offender[data$race_of_offender %in% c(NA, "unknown")] <- "unknown_race"
  data$race_of_offender <- paste0("offender_", data$race_of_offender)

  data$age_of_offender <- str_replace_all(data$age_of_offender, age_fix)
  data$age_of_offender[data$age_of_offender %in% "unknown"] <- NA
  data$age_of_offender <- as.numeric(data$age_of_offender)
  data$age_category <- "offender_unknown_age"
  data$age_category[data$age_of_offender %in% 0:17] <- "offender_juvenile"
  data$age_category[data$age_of_offender %in% 18:99] <- "offender_adult"

  data <- group_theft_offenses(data)
  data$offense <- paste("offender", data$offense)
  return(data)
}


prep_arrestee <- function(arrestee_file, arrestee_group_b_file, batch_header) {
  arrestee <- readRDS(arrestee_file)
  arrestee_group_b <- readRDS(arrestee_group_b_file) %>%
    select(
      -arrestee_weapon_1,
      -arrestee_weapon_2,
      -automatic_weapon_indicator_1,
      -automatic_weapon_indicator_2
    )
  data <- bind_rows(arrestee, arrestee_group_b) %>%
    select(ori,
      arrest_date,
      type_of_arrest,
      offense = ucr_arrest_offense_code,
      age_of_arrestee,
      sex_of_arrestee,
      race_of_arrestee,
      ethnicity_of_arrestee
    ) %>%
    mutate(
      date = ymd(arrest_date),
      date = floor_date(date, unit = "month"),
      year = year(date)
    ) %>%
    select(-arrest_date) %>%
    mutate(
      type_of_arrest = gsub(" \\(.*", "", type_of_arrest),
      age_of_arrestee = str_replace_all(age_of_arrestee, age_fix),
      ethnicity_of_arrestee = str_replace_all(ethnicity_of_arrestee, ethnicity_fix),
      race_of_arrestee = str_replace_all(race_of_arrestee, race_fix)
    ) %>%
    left_join(batch_header %>%
      select(
        ori = ORI,
        year,
        number_of_months_reported
      ), by = join_by(ori, year)) %>%
    filter(number_of_months_reported %in% 12) %>%
    select(-number_of_months_reported)

  data$age_of_arrestee[data$age_of_arrestee %in% "unknown"] <- NA
  data$age_of_arrestee <- as.numeric(data$age_of_arrestee)
  data$age_category <- "arrestee_unknown_age"
  data$age_category[data$age_of_arrestee %in% 0:17] <- "arrestee_juvenile"
  data$age_category[data$age_of_arrestee %in% 18:99] <- "arrestee_adult"


  data$sex_of_arrestee[data$sex_of_arrestee %in% c(NA, "unknown")] <- "unknown_sex"
  data$race_of_arrestee[data$race_of_arrestee %in% c(NA, "unknown")] <- "unknown_race"
  data$ethnicity_of_arrestee[data$ethnicity_of_arrestee %in% c(NA, "unknown")] <- "unknown_ethnicity"

  data <- group_theft_offenses(data)
  data$sex_of_arrestee <- paste0("arrestee_", data$sex_of_arrestee)
  data$race_of_arrestee <- paste0("arrestee_", data$race_of_arrestee)
  data$ethnicity_of_arrestee <- paste0("arrestee_", data$ethnicity_of_arrestee)
  data$offense <- paste("arrestee", data$offense)
  data$type_of_arrest <- paste("arrestee", data$type_of_arrest)
  return(data)
}


aggregate_data <- function(data, variables = NULL, time_unit, victim_type = FALSE) {
  weapon_crimes <- c(
    "assault offenses - aggravated assault",
    "assault offenses - simple assault",
    "extortion/blackmail",
    "human trafficking - commercial sex acts",
    "human trafficking - involuntary servitude",
    "justifiable homicide - not a crime",
    "kidnapping/abduction",
    "murder/nonnegligent manslaughter",
    "negligent manslaughter",
    "robbery",
    "sex offenses - fondling (indecent liberties/child molest)",
    "sex offenses - rape",
    "sex offenses - sexual assault with an object",
    "sex offenses - sodomy",
    "weapon law violations - explosives",
    "weapon law violations - weapon law violations"
  )
  injury_crimes <- c(
    "assault offenses - aggravated assault",
    "assault offenses - intimidation",
    "assault offenses - simple assault",
    "destruction/damage/vandalism of property",
    "extortion/blackmail",
    "human trafficking - commercial sex acts",
    "human trafficking - involuntary servitude",
    "kidnapping/abduction",
    "motor vehicle theft",
    "murder/nonnegligent manslaughter",
    "negligent manslaughter",
    "robbery",
    "sex offenses - fondling (indecent liberties/child molest)",
    "sex offenses - incest",
    "sex offenses - rape",
    "sex offenses - sexual assault with an object",
    "sex offenses - sodomy",
    "sex offenses - statutory rape"
  )
  person_victim_types <- c(
    "individual",
    "law enforcement officer"
  )
  crimes_with_subtypes <- c(
    "animal cruelty",
    "assault offenses - aggravated assault",
    "assault offenses - intimidation",
    "assault offenses - simple assault",
    "commerce violations - federal liquor offenses",
    "commerce violations - federal tobacco offenses",
    "commerce violations - wildlife trafficking",
    "counterfeiting/forgery",
    "drug/narcotic offenses - drug equipment violations",
    "drug/narcotic offenses - drug/narcotic violations",
    "gambling offenses - gambling equipment violations",
    "kidnapping/abduction",
    "murder/nonnegligent manslaughter",
    "negligent manslaughter",
    "pornography/obscene material",
    "robbery",
    "sex offenses - fondling (indecent liberties/child molest)",
    "sex offenses - rape",
    "sex offenses - sexual assault with an object",
    "sex offenses - sodomy",
    "stolen property offenses (receiving, selling, etc.)",
    "weapon law violations - explosives",
    "weapon law violations - weapon law violations"
  )

  data$time_unit <- data[, time_unit]
  cols_temp <- c(
    "ori",
    "time_unit",
    "offense"
  )
  main_agg <- data %>%
    count_(cols_temp) %>%
    pivot_wider(
      names_from = offense,
      values_from = n
    ) %>%
    rename_all(make_clean_names)
  main_agg[is.na(main_agg)] <- 0
  main_agg <- dummy_rows(main_agg,
    select_columns = c("ori", "time_unit"),
    dummy_value = NA
  )

  if (!is.null(variables)) {
    for (variable in variables) {
      cols_temp <- c(
        "ori",
        "time_unit",
        "offense",
        variable
      )
      temp_agg <- data %>%
        count_(cols_temp)

      # Not all crimes can have subtypes.
      # Subsets to only ones that can
      if (variable %in% "subtype") {
        temp_agg <-
          data %>%
          count_(cols_temp) %>%
          filter(offense %in% paste("offense", crimes_with_subtypes))
      }
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
        pivot_wider(
          names_from = offense_variable,
          values_from = n
        ) %>%
        rename_all(make_clean_names)
      temp_agg[is.na(temp_agg)] <- 0
      temp_agg <- dummy_rows(temp_agg,
        select_columns = c("ori", "time_unit"),
        dummy_value = NA
      )

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
    temp <- data[state %in% selected_state]
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
    type = type
  )
}

make_all_na <- function(col) {
  col <- NA
}

make_csv_test <- function(temp, type) {
  temp <- dummy_rows_missing_years(temp, type = type)

  state <- unique(temp$state)
  agency <- unique(temp$agency)
  state <- gsub(" ", "_", state)
  agency <- gsub(" |:", "_", agency)
  agency <- gsub("/", "_", agency)
  agency <- gsub("_+", "_", agency)
  agency <- gsub("\\(|\\)", "", agency)

  data.table::fwrite(temp, file = paste0(state, "_", agency, ".csv"))
}

dummy_rows_missing_years <- function(data, type) {
  data$year <- as.character(data$year)
  if (type == "year") {
    missing_years <- min(data$year):max(data$year)
  } else {
    missing_years <- seq.Date(lubridate::ymd(min(data$year)),
      lubridate::ymd(max(data$year)),
      by = "month"
    )
    missing_years <- as.character(missing_years)
  }

  missing_years <- missing_years[!missing_years %in% data$year]

  if (length(missing_years) > 0) {
    temp <- data
    temp <- temp[1, ]
    temp <- splitstackshape::expandRows(temp,
      count = length(missing_years),
      count.is.col = FALSE
    )
    temp$year <- missing_years
    temp <-
      temp %>%
      dplyr::mutate_at(
        vars(-one_of("year", "agency", "state", "ORI")),
        make_all_na
      )
    temp$year <- as.character(temp$year)

    data <-
      data %>%
      dplyr::bind_rows(temp) %>%
      dplyr::arrange(desc(year)) %>%
      dplyr::mutate(year = as.character(year))
  }
  return(data)
}



group_theft_offenses <- function(data) {
  data$offense[data$offense %in% theft_crimes] <- "theft"
  return(data)
}
