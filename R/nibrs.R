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

get_agg_data(1991:2022)
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
    offense_agg_year <- aggregate_data(offense,
      variables = c(
        "gun_involved",
        "cleared",
        "location",
        "subtype"
      ),
      time_unit = "year"
    )
    offense_agg_month <- aggregate_data(offense,
      variables = c(
        "gun_involved",
        "cleared",
        "location",
        "subtype"
      ),
      time_unit = "date"
    )
    rm(offense)
    gc()

    offender <- prep_offender(
      offender_files[grep(year_temp, offender_files)],
      offense_small
    )
    offender_agg_year <- aggregate_data(offender,
      variables = c(
        "sex_of_offender",
        "race_of_offender",
        "age_category"
      ),
      time_unit = "year"
    )
    offender_agg_month <- aggregate_data(offender,
      variables = c(
        "sex_of_offender",
        "race_of_offender",
        "age_category"
      ),
      time_unit = "date"
    )
    rm(offender)
    gc()

    arrestee <- prep_arrestee(
      arrestee_files[grep(year_temp, arrestee_files)],
      arrestee_group_b_files[grep(year_temp, arrestee_group_b_files)]
    ) %>%
      filter(ori %in% offense_small$ori)
    arrestee_agg_year <- aggregate_data(arrestee,
      variables = c(
        "sex_of_arrestee",
        "race_of_arrestee",
        "ethnicity_of_arrestee",
        "age_category",
        "type_of_arrest"
      ),
      time_unit = "year"
    )
    arrestee_agg_month <- aggregate_data(arrestee,
      variables = c(
        "sex_of_arrestee",
        "race_of_arrestee",
        "ethnicity_of_arrestee",
        "age_category",
        "type_of_arrest"
      ),
      time_unit = "date"
    )
    rm(arrestee)
    gc()

    victim <- prep_victim(victim_files[grep(year_temp, victim_files)]) %>%
      filter(ori %in% offense_small$ori)
    victim_agg_year <- aggregate_data(victim,
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
    )
    victim_agg_month <- aggregate_data(victim,
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
    )

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
      paste0("E:Dropbox/R_project/crimedatatool_helper_nibrs/data/temp_agg_month_", year_temp, ".rds")
    )

    message(year_temp)
  }
}


add_missing_columns <- function(data) {
  for (col in c(
    "arrestee_unknown_age_sports_tampering",
    "arrestee_unknown_age_human_trafficking_commercial_sex_acts",
    "arrestee_unknown_age_human_trafficking_involuntary_servitude",
    "arrestee_unknown_agehacking_computer_invasion",
    "arrestee_unknown_age_animal_cruelty",
    "arrestee_american_indian_sports_tampering",
    "arrestee_unknown_race_sports_tampering",
    "arrestee_american_indian_hacking_computer_invasion",
    "arrestee_white_sports_tampering",
    "offender_american_indian_sports_tampering",
    "victim_unknown_sex_rape",
    "victim_unknown_sex_statutory_rape",
    "offense_victim_refused_to_cooperate_sports_tampering",
    "offense_death_of_suspect_sports_tampering",
    "offense_death_of_suspect_purchasing_prostitution",
    "offense_death_of_suspect_human_trafficking_involuntary_servitude",
    "offense_extradition_denied_sports_tampering",
    "offense_juvenile_no_custody_gambling_equipment_violations",
    "offense_juvenile_no_custody_sports_tampering",
    "offense_juvenile_no_custody_purchasing_prostitution",
    "offense_juvenile_no_custody_human_trafficking_involuntary_servitude"
  )) {
    if (!all(grepl(col, names(data)))) {
      data[, col] <- 0
    }
  }

  data$victim_unknown_relationship_animal_cruelty <- NULL
  return(data)
}




final_agg_year <- combine_agg_data(type = "year", batch_header)
final_agg_year <- add_missing_columns(final_agg_year)
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

rm(final_agg_year)
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
  final_agg_month <- add_missing_columns(final_agg_month)
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
