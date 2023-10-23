source("E:/Dropbox/R_project/crimedatatool_helper_nibrs/R/utils.R")
admin_files <- list.files(pattern = "admin.*rds$")
offense_files <- list.files(pattern = "offense.*rds$")
victim_files <- list.files(pattern = "victim.*rds$")
offender_files <- list.files(pattern = "offender.*rds$")
arrestee_files <- list.files(pattern = "nibrs_arrestee.*rds$")
arrestee_group_b_files <- list.files(pattern = "group_b.*rds$")

setwd("F:/ucr_data_storage/clean_data/nibrs")
batch_header <- get_batch_header()
sort(unique(batch_header$state))

get_agg_data <- function(years) {
  for (year_temp in years) {
    offense <- prep_offense(offense_files[grep(year_temp, offense_files)])
    admin <- prep_admin(admin_files[grep(year_temp, admin_files)])
    offense <-
      offense %>%
      left_join(admin)

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
      offense_small
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
      arrestee_group_b_files[grep(year_temp, arrestee_group_b_files)]
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

    victim <- prep_victim(victim_files[grep(year_temp, victim_files)]) %>%
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
      left_join(victim_agg_year, by = c("ori", "time_unit"))


    saveRDS(
      temp_agg_year,
      paste0("E:/Dropbox/R_project/crimedatatool_helper_nibrs/data/temp_agg_year_", year_temp, ".rds")
    )

    temp_agg_month <- offense_agg_month %>%
      left_join(offender_agg_month, by = c("ori", "time_unit")) %>%
      left_join(arrestee_agg_month, by = c("ori", "time_unit")) %>%
      left_join(victim_agg_month, by = c("ori", "time_unit"))
    saveRDS(
      temp_agg_month,
      paste0("E:/Dropbox/R_project/crimedatatool_helper_nibrs/data/temp_agg_month_", year_temp, ".rds")
    )

    message(year_temp)
  }
}
setwd("F:/ucr_data_storage/clean_data/nibrs")
get_agg_data(1991:2022)
#
# add_missing_columns <- function(data) {
#   for (col in c(
#     "arrestee_unknown_age_gambling_offenses_sports_tampering",
#     "arrestee_unknown_age_human_trafficking_commercial_sex_acts",
#     "arrestee_unknown_age_human_trafficking_involuntary_servitude",
#     "arrestee_unknown_age_fraud_offenses_hacking_computer_invasion",
#     "arrestee_unknown_age_animal_cruelty",
#     "arrestee_american_indian_gambling_offenses_sports_tampering",
#     "arrestee_unknown_race_gambling_offenses_sports_tampering",
#     "arrestee_american_indian_fraud_offenses_hacking_computer_invasion",
#     "arrestee_white_gambling_offenses_sports_tampering",
#     "offender_american_indian_gambling_offenses_sports_tampering",
#     "victim_unknown_sex_sex_offenses_rape",
#     "victim_unknown_sex_sex_offenses_statutory_rape",
#     "offense_victim_refused_to_cooperate_gambling_offenses_sports_tampering",
#     "offense_death_of_suspect_gambling_offenses_sports_tampering",
#     "offense_death_of_suspect_prostitution_offenses_purchasing_prostitution",
#     "offense_death_of_suspect_human_trafficking_involuntary_servitude",
#     "offense_extradition_denied_gambling_offenses_sports_tampering",
#     "offense_juvenile_no_custody_gambling_equipment_violations",
#     "offense_juvenile_no_custody_gambling_offenses_sports_tampering",
#     "offense_juvenile_no_custody_prostitution_offenses_purchasing_prostitution",
#     "offense_juvenile_no_custody_human_trafficking_involuntary_servitude"
#   )) {
#     if (!all(grepl(col, names(data)))) {
#       data[, col] <- 0
#     }
#   }
#
#   data$victim_unknown_relationship_animal_cruelty <- NULL
#   return(data)
# }




final_agg_year <- combine_agg_data(type = "year", batch_header)
# final_agg_year <- add_missing_columns(final_agg_year)
# Adds in any missing columns.
column_values <- grep("counterfeiting_forgery", names(final_agg_year), value = TRUE)
column_values <- gsub("counterfeiting_forgery", "", column_values)
column_values2 <- grep("extortion_blackmail", names(final_agg_year), value = TRUE)
column_values2 <- gsub("extortion_blackmail", "", column_values2)
column_values <- unique(c(column_values,
                          column_values2))
column_values <- column_values[!column_values %in% "offense_na_"]
injury_columns <- c("victim_no_injury_",
                    "victim_serious_injury_",
                    "victim_unknown_injury_",
                    "victim_minor_injury_")
victim_columns <- c("victim_",
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
                    "victim_other_relationship_")
gun_columns <- c("offense_handgun_",
                 "offense_no_gun_",
                 "offense_other_unknown_gun_")
subtype_columns <- c("offense_buy_possess_consume_",
                     "offense_sell_create_assist_")
column_values <- column_values[!column_values %in% c(victim_columns,
                                                     injury_columns,
                                                     gun_columns,
                                                     subtype_columns)]
injury_offenses <- c(
  "assault_offenses_aggravated_assault",
  "extortion_blackmail",
  "sex_offenses_fondling_incident_liberties_child_molest",
  "human_trafficking_commercial_sex_acts",
  "human_trafficking_involuntary_servitude",
  "kidnapping_abduction",
  "sex_offenses_rape",
  "robbery",
  "sexual_assault_with_an_object",
  "simple_assault",
  "sex_offenses_sodomy"
)
subtype_offenses <- c(
  "animal_cruelty",
  "counterfeiting_forgery",
  "drug_narcotic_offenses_drug_equipment_violations",
  "drug_narcotic_offenses_drug_narcotic_violations",
  "weapon_law_violations_explosives" ,
  "gambling_offenses_gambling_equipment_violations",
  "fugitive_offenses_harboring_escappee_concealing_from_arrest" ,
  "stolen_property_offenses_receiving_selling_etc",
  "weapon_law_violations_weapon_law_violations",
  "weapon_law_violations_violation_of_national_firearm_act_of_1934" ,
  "weapon_law_violations_weapons_of_mass_destruction"
)
gun_offenses <- c(
  "assault_offenses_aggravated_assault",
  "extortion_blackmail",
  "weapon_law_violations_explosives",
  "sex_offenses_fondling_incident_liberties_child_molest",
  "human_trafficking_commercial_sex_acts",
  "human_trafficking_involuntary_servitude",
  "justifiable_homicide_not_a_crime",
  "kidnapping_abduction",
  "murder_nonnegligent_manslaughter",
  "negligent_manslaughter",
  "sex_offenses_rape",
  "robbery",
  "sexual_assault_with_an_object",
  "weapon_law_violations_weapon_law_violations",
  "sex_offenses_sodomy",
  "weapon_law_violations_violation_of_national_firearm_act_of_1934",
  "weapon_law_violations_weapons_of_mass_destruction"
)
victim_offenses <- c(
  "assault_offenses_aggravated_assault",
  "arson",
  "bribery",
  "burglary_breaking_and_entering",
  "counterfeiting_forgery",
  "fraud_offenses_credit_card_atm_fraud",
  "destruction_damage_vandalism_of_property",
  "embezzlement",
  "extortion_blackmail",
  "fraud_offenses_false_pretenses_swindle_confidence_game",
  "failure_to_appear" ,
  "federal_resource_violations" ,
  "fugitive_offenses_flight_to_avoid_deportation" ,
  "fugitive_offenses_flight_to_avoid_prosecution" ,
  "sex_offenses_fondling_incident_liberties_child_molest",
  "fraud_offenses_hacking_computer_invasion",
  "human_trafficking_commercial_sex_acts",
  "human_trafficking_involuntary_servitude",
  "fraud_offenses_identity_theft",
  "fraud_offenses_impersonation",
  "sex_offenses_incest",
  "assault_offenses_intimidation",
  "justifiable_homicide_not_a_crime",
  "kidnapping_abduction",
  "motor_vehicle_theft",
  "fraud_offenses_money_laundering" ,
  "murder_nonnegligent_manslaughter",
  "negligent_manslaughter",
  "pocket_picking",
  "sex_offenses_rape",
  "robbery",
  "sexual_assault_with_an_object",
  "simple_assault",
  "immigration_violations_smuggling_aliens" ,
  "sex_offenses_sodomy",
  "statutory_rape",
  "stolen_property_offenses_receiving_selling_etc",
  "theft" ,
  "fraud_offenses_welfare_fraud",
  "wire_fraud"
)

offense_values <- c("commerce violations - import violations",
                    "commerce violations - export Violations",
                    "commerce violations - federal liquor offenses",
                    "commerce violations - federal tobacco offenses",
                    "commerce violations - wildlife trafficking",
                    "immigration violations - illegal entry into the united states",
                    "immigration violations - false citizenship",
                    "immigration violations - smuggling aliens",
                    "immigration violations - re-entry into the united states",
                    "fugitive offenses - harboring escappee/concealing from arrest",
                    "fugitive offenses - flight to avoid prosecution",
                    "fugitive offenses - flight to avoid deportation",
                    "sex offenses - failure to register as a sex offender",
                    "treason",
                    "weapon law violations - violation of national firearm act of 1934",
                    "weapon law violations - weapons of mass destruction",
                    "weapon law violations - explosives",
                    "failure to appear",
                    "federal resource violations",
                    "perjury",
                    "espionage",
                    "animal_cruelty",
                    "arson",
                    "assault_offenses_aggravated_assault",
                    "assault_offenses_intimidation",
                    "assault_offenses_simple_assault",
                    "bribery",
                    "burglary_breaking_and_entering",
                    "counterfeiting_forgery",
                    "destruction_damage_vandalism_of_property",
                    "drug_narcotic_offenses_drug_equipment_violations",
                    "drug_narcotic_offenses_drug_narcotic_violations",
                    "embezzlement",
                    "extortion_blackmail",
                    "fraud_offenses_credit_card_atm_fraud",
                    "fraud_offenses_false_pretenses_swindle_confidence_game",
                    "fraud_offenses_hacking_computer_invasion",
                    "fraud_offenses_identity_theft",
                    "fraud_offenses_impersonation",
                    "fraud_offenses_money_laundering",
                    "fraud_offenses_welfare_fraud",
                    "fraud_offenses_wire_fraud",
                    "gambling_offenses_betting_wagering",
                    "gambling_offenses_gambling_equipment_violations",
                    "gambling_offenses_operating_promoting_assisting_gambling",
                    "gambling_offenses_sports_tampering",
                    "human_trafficking_commercial_sex_acts",
                    "human_trafficking_involuntary_servitude",
                    "justifiable_homicide_not_a_crime",
                    "kidnapping_abduction",
                    "larceny_theft_offenses_all_other_larceny",
                    "larceny_theft_offenses_pocket_picking",
                    "larceny_theft_offenses_purse_snatching",
                    "larceny_theft_offenses_shoplifting",
                    "larceny_theft_offenses_theft_from_building",
                    "larceny_theft_offenses_theft_from_coin_operated_machine_or_device",
                    "larceny_theft_offenses_theft_from_motor_vehicle",
                    "larceny_theft_offenses_theft_of_motor_vehicle_parts_accessories",
                    "motor_vehicle_theft",
                    "murder_nonnegligent_manslaughter",
                    "negligent_manslaughter",
                    "pornography_obscene_material",
                    "prostitution_offenses_assisting_or_promoting_prostitution",
                    "prostitution_offenses_prostitution",
                    "prostitution_offenses_purchasing_prostitution",
                    "robbery",
                    "sex_offenses_fondling_incident_liberties_child_molest",
                    "sex_offenses_incest",
                    "sex_offenses_rape",
                    "sex_offenses_sexual_assault_with_an_object",
                    "sex_offenses_sodomy",
                    "sex_offenses_statutory_rape",
                    "stolen_property_offenses_receiving_selling_etc",
                    "weapon_law_violations_weapon_law_violations")
offense_values <- make_clean_names(offense_values)
offense_values <- sort(offense_values)
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
    if (!col_value %in% names(final_agg_year)) {
      final_agg_year[, col_value] <- 0
      message(paste0("Adding ", col_value))
    }
  }
}

gc()
final_agg_year <-
  final_agg_year %>%
  filter(ORI %in% batch_header$ORI) %>%
  left_join(batch_header) %>%
  select(
    ORI,
    year,
    agency,
    state,
    population,
    everything()
  ) %>%
  filter(!is.na(agency))
gc()
setwd("E:/Dropbox/R_project/crimedatatool_helper_nibrs/data/nibrs")
make_largest_agency_json(final_agg_year)
make_state_agency_choices(final_agg_year)
files <- list.files(pattern = "agency_choices")
files
file.copy(files, "E:/Dropbox/R_project/crimedatatool_helper_nibrs/data/nibrs_monthly/", overwrite = TRUE)
make_agency_csvs(final_agg_year)
state_abb <- unique(substr(unique(final_agg_year$ORI), 1, 2))
state_abb <- sort(state_abb)
state_abb


gc()

state_abb_first_half <- state_abb[1:25]
state_abb_second_half <- state_abb[26:length(state_abb)]
state_abb_groups <- list(
  state_abb_first_half,
  state_abb_second_half
)
state_abb_groups
for (i in 1:2) {
  states_temp <- state_abb_groups[[i]]
  final_agg_month <- combine_agg_data(type = "month", batch_header, states_temp)
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
      if (!col_value %in% names(final_agg_month)) {
        final_agg_month[, col_value] <- 0
        message(paste0("Adding ", col_value))
      }
    }
  }
 # final_agg_month <- add_missing_columns(final_agg_month)
  final_agg_month$state_abb <- substr(final_agg_month$ORI, 1, 2)
  state_abb <- unique(substr(unique(final_agg_year$ORI), 1, 2))
  state_abb <- sort(state_abb)
  state_abb
  setwd("E:/Dropbox/R_project/crimedatatool_helper_nibrs/data/nibrs_monthly")
  for (state_abb_temp in state_abb) {
    temp <- final_agg_month %>% filter(state_abb %in% state_abb_temp)
    final_agg_month <- final_agg_month %>% filter(!state_abb %in% state_abb_temp)
    gc()

    temp$state_abb <- NULL
    temp <-
      temp %>%
      mutate(
        date = year,
        year = year(year)
      ) %>%
      filter(ORI %in% batch_header$ORI) %>%
      left_join(batch_header, by = c("ORI", "year")) %>%
      filter(!is.na(agency)) %>%
      select(-year) %>%
      rename(year = date) %>%
      select(
        ORI,
        year,
        agency,
        state,
        population,
        everything()
      )
    gc()
    temp$year <- as.character(temp$year)
    make_agency_csvs(temp, type = "month")
  }
  rm(final_agg_month)
  gc()
}

