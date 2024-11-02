source("~/crimedatatool_helper_nibrs/R/utils.R")




get_agg_data <- function(years) {
  batch_header <- readRDS("F:/ucr_data_storage/clean_data/combined_years/nibrs/nibrs_batch_header_1991_2023.rds") %>%
    select(
      number_of_months_reported,
      ORI = ori,
      year,
      state,
      population
    )


  setwd("F:/ucr_data_storage/clean_data/nibrs")
  admin_files <- list.files(pattern = "admin.*rds$")
  offense_files <- list.files(pattern = "offense.*rds$")
  victim_files <- list.files(pattern = "victim.*rds$")
  offender_files <- list.files(pattern = "offender.*rds$")
  arrestee_files <- list.files(pattern = "nibrs_arrestee.*rds$")
  arrestee_group_b_files <- list.files(pattern = "group_b.*rds$")

  final_yearly <- vector("list", length = length(years))
  final_monthly <- vector("list", length = length(years))
  for (i in 1:length(years)) {
    year_temp <- years[i]
    offense <- prep_offense(
      offense_files[grep(year_temp, offense_files)],
      batch_header
    )
    admin <- prep_admin(admin_files[grep(year_temp, admin_files)])
    offense <-
      offense %>%
      left_join(admin, by = join_by(unique_incident_id))

    offense_small <- offense %>%
      select(
        ori,
        unique_incident_id,
        offense
      )
    offense_agg_year <- suppressWarnings(aggregate_data(offense,
      variables = c(
        "gun_involved",
        "cleared",
        "location",
        "subtype"
      ),
      time_unit = "year"
    ))
    offense_agg_month <- suppressWarnings(aggregate_data(offense,
      variables = c(
        "gun_involved",
        "cleared",
        "location",
        "subtype"
      ),
      time_unit = "date"
    ))
    rm(offense)
    gc()

    offender <- prep_offender(
      offender_files[grep(year_temp, offender_files)],
      offense_small,
      batch_header
    )
    offender_agg_year <- suppressWarnings(aggregate_data(offender,
      variables = c(
        "sex_of_offender",
        "race_of_offender",
        "age_category"
      ),
      time_unit = "year"
    ))
    offender_agg_month <- suppressWarnings(aggregate_data(offender,
      variables = c(
        "sex_of_offender",
        "race_of_offender",
        "age_category"
      ),
      time_unit = "date"
    ))
    rm(offender)
    gc()

    arrestee <- prep_arrestee(
      arrestee_files[grep(year_temp, arrestee_files)],
      arrestee_group_b_files[grep(year_temp, arrestee_group_b_files)],
      batch_header
    ) %>%
      filter(ori %in% offense_small$ori)
    arrestee_agg_year <- suppressWarnings(aggregate_data(arrestee,
      variables = c(
        "sex_of_arrestee",
        "race_of_arrestee",
        "ethnicity_of_arrestee",
        "age_category",
        "type_of_arrest"
      ),
      time_unit = "year"
    ))
    arrestee_agg_month <- suppressWarnings(aggregate_data(arrestee,
      variables = c(
        "sex_of_arrestee",
        "race_of_arrestee",
        "ethnicity_of_arrestee",
        "age_category",
        "type_of_arrest"
      ),
      time_unit = "date"
    ))
    rm(arrestee)
    gc()

    victim <- prep_victim(
      victim_files[grep(year_temp, victim_files)],
      batch_header
    ) %>%
      filter(ori %in% offense_small$ori)
    victim_agg_year <- suppressWarnings(aggregate_data(victim,
      variables = c(
        "sex_of_victim",
        "race_of_victim",
        "ethnicity_of_victim",
        "age_category",
        "victim_injury",
        "relationship"
      ),
      time_unit = "year",
      victim_type = TRUE
    ))
    victim_agg_month <- suppressWarnings(aggregate_data(victim,
      variables = c(
        "sex_of_victim",
        "race_of_victim",
        "ethnicity_of_victim",
        "age_category",
        "victim_injury",
        "relationship"
      ),
      time_unit = "date",
      victim_type = TRUE
    ))

    rm(victim)
    gc()
    rm(offense_small)
    gc()


    temp_agg_year <- offense_agg_year %>%
      left_join(offender_agg_year, by = c("ori", "time_unit")) %>%
      left_join(arrestee_agg_year, by = c("ori", "time_unit")) %>%
      left_join(victim_agg_year, by = c("ori", "time_unit")) %>%
      filter(time_unit %in% year_temp)


    saveRDS(
      temp_agg_year,
      paste0("~/crimedatatool_helper_nibrs/data/agg_data/temp_agg_year_", year_temp, ".rds")
    )

    temp_agg_month <- offense_agg_month %>%
      left_join(offender_agg_month, by = c("ori", "time_unit")) %>%
      left_join(arrestee_agg_month, by = c("ori", "time_unit")) %>%
      left_join(victim_agg_month, by = c("ori", "time_unit")) %>%
      mutate(year = year(time_unit)) %>%
      filter(year %in% year_temp) %>%
      select(-year)
    saveRDS(
      temp_agg_month,
      paste0("~/crimedatatool_helper_nibrs/data/agg_data/temp_agg_month_", year_temp, ".rds")
    )

    rm(temp_agg_year, temp_agg_month)
    message(year_temp)
    gc()
    Sys.sleep(1)
    gc()
  }
}


add_missing_columns <- function(data) {
  # final_agg_year <- combine_agg_data(type = "year", batch_header)
  # Adds in any missing columns.
  column_values <- grep("counterfeiting_forgery", names(data), value = TRUE)
  column_values <- gsub("counterfeiting_forgery", "", column_values)
  column_values2 <- grep("extortion_blackmail", names(data), value = TRUE)
  column_values2 <- gsub("extortion_blackmail", "", column_values2)
  column_values <- unique(c(
    column_values,
    column_values2
  ))
  column_values <- column_values[!column_values %in% "offense_na_"]
  injury_columns <- c(
    "victim_no_injury_",
    "victim_serious_injury_",
    "victim_unknown_injury_",
    "victim_minor_injury_"
  )
  victim_columns <- c(
    "victim_",
    "victim_female_",
    "victim_male_",
    "victim_unknown_sex_",
    "victim_asian_",
    "victim_black_",
    "victim_unknown_race_",
    "victim_white_",
    "victim_american_indian_",
    "victim_unknown_ethnicity_",
    "victim_not_hispanic_",
    "victim_hispanic_",
    "victim_adult_",
    "victim_juvenile_",
    "victim_unknown_age_",
    "victim_unknown_relationship_",
    "victim_other_family_",
    "victim_intimate_partner_",
    "victim_stranger_",
    "victim_other_relationship_"
  )
  gun_columns <- c(
    "offense_handgun_",
    "offense_no_gun_",
    "offense_other_unknown_gun_"
  )
  subtype_columns <- c(
    "offense_buy_possess_consume_",
    "offense_sell_create_assist_"
  )
  column_values <- column_values[!column_values %in% c(
    victim_columns,
    injury_columns,
    gun_columns,
    subtype_columns
  )]


  # Identify offenses with various subtypes/outcomes
  offense_data <- readRDS("F:/ucr_data_storage/clean_data/nibrs/nibrs_offense_segment_2023.rds") %>%
    bind_rows(readRDS("F:/ucr_data_storage/clean_data/nibrs/nibrs_offense_segment_2022.rds")) %>%
    bind_rows(readRDS("F:/ucr_data_storage/clean_data/nibrs/nibrs_offense_segment_2021.rds"))
  victim_data <- readRDS("F:/ucr_data_storage/clean_data/nibrs/nibrs_victim_segment_2023.rds")


  injury_offenses <- c(
    "assault_offenses_aggravated_assault",
    "assault_offenses_simple_assault",
    "extortion_blackmail",
    "human_trafficking_commercial_sex_acts",
    "human_trafficking_involuntary_servitude",
    "kidnapping_abduction",
    "robbery",
    "sex_offenses_fondling_indecent_liberties_child_molest",
    "sex_offenses_rape",
    "sex_offenses_sexual_assault_with_an_object",
    "sex_offenses_sodomy"
  )

  subtype_offenses <- make_clean_names(sort(unique(offense_data$ucr_offense_code[offense_data$type_criminal_activity_1 %in%
    c(c(
      subtype_buy_possess_consume,
      subtype_sell_create_assist
    ))])))

  guns <- c(
    "firearm (type not stated)",
    "handgun",
    "other firearm",
    "rifle",
    "shotgun"
  )

  gun_offenses <- make_clean_names(sort(unique(offense_data$ucr_offense_code[offense_data$type_weapon_force_involved_1 %in% guns])))
  victim_offenses <- make_clean_names(sort(unique(victim_data$ucr_offense_code_1[!is.na(victim_data$race_of_victim)])))

  offense_values <- make_clean_names(sort(unique(offense_data$ucr_offense_code)))
  offense_values[offense_values %in% make_clean_names(theft_crimes)] <- "theft"
  offense_values <- c(
    offense_values,
    make_clean_names(c(
      "commerce violations - import violations",
      "commerce violations - export Violations",
      "commerce violations - federal liquor offenses",
      "commerce violations - federal tobacco offenses",
      "commerce violations - wildlife trafficking",
      "immigration violations - illegal entry into the united states",
      "immigration violations - false citizenship",
      "immigration violations - smuggling aliens",
      "immigration violations - re-entry into the united states after deportation",
      "fugitive offenses - harboring escapee - concealing from arrest",
      "fugitive offenses - flight to avoid prosecution",
      "fugitive offenses - flight to avoid deportation",
      "sex offenses - failure to register as a sex offender",
      "treason",
      "weapon law violations - violation of national firearm act of 1934",
      "weapon law violations - weapons of mass destruction",
      "weapon law violations - explosives",
      "bond default - failure to appear",
      "federal resource violations",
      "perjury",
      "fraud offenses - money laundering",
      "espionage"
    ))
  )


  rm(offense_data, victim_data)
  offense_values <- unique(offense_values)
  offense_values
  for (offense in offense_values) {
    all_columns <- column_values
    if (offense %in% gun_offenses) {
      all_columns <- c(all_columns, gun_columns)
    }
    if (offense %in% injury_offenses) {
      all_columns <- c(all_columns, injury_columns)
    }
    if (offense %in% subtype_offenses) {
      all_columns <- c(all_columns, subtype_columns)
    }
    if (offense %in% victim_offenses) {
      all_columns <- c(all_columns, victim_columns)
    }

    for (column in all_columns) {
      col_value <- paste0(column, offense)
      if (!col_value %in% names(data)) {
        data[, (col_value) := 0]
        message(paste0("Adding ", col_value))
        gc()
      }
    }
  }
  gc()
  return(data)
}

get_agg_data(1991:2023)

make_nibrs_data <- function(time = "year") {
  gc()
  yearly_files <- list.files("~/crimedatatool_helper_nibrs/data/agg_data/", pattern = "year", full.names = TRUE)
  if (time == "year") {

    yearly_data <- vector("list", length = length(yearly_files))
    for (i in 1:length(yearly_files)) {
      yearly_data[[i]] <- readRDS(yearly_files[i])
      message(i)
    }
    yearly_data <- data.table::rbindlist(yearly_data, fill = TRUE)
    gc()

    yearly_data <- add_missing_columns(yearly_data)
    gc()

    save_as_csv_for_site(yearly_data, type = "year")
    rm(yearly_data)
    gc()
    Sys.sleep(1)
    gc()
  } else {
    monthly_files <- list.files("~/crimedatatool_helper_nibrs/data/agg_data/", pattern = "month", full.names = TRUE)
    monthly_data <- vector("list", length = length(monthly_files))
    for (i in 1:length(yearly_files)) {
      monthly_data[[i]] <- readRDS(monthly_files[i])
      message(i)
    }
    monthly_data <- data.table::rbindlist(monthly_data, fill = TRUE)
    gc()

    monthly_data <- add_missing_columns(monthly_data)
    gc()

    save_as_csv_for_site(monthly_data, type = "month")
    rm(monthly_data)
    gc()
    Sys.sleep(1)
    gc()
  }
}
make_nibrs_data("year")
make_nibrs_data("month")
