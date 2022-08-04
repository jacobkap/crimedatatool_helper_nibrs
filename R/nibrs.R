source("C:/Users/jkkap/Dropbox/R_project/crimedatatool_helper_nibrs/R/utils.R", echo=TRUE)

offense_files  <- list.files(pattern = "offense.*rds$")
victim_files   <- list.files(pattern = "victim.*rds$")
offender_files <- list.files(pattern = "offender.*rds$")
arrestee_files <- list.files(pattern = "nibrs_arrestee.*rds$")
arrestee_group_b_files <- list.files(pattern = "group_b.*rds$")
ucr <- readRDS("D:/ucr_data_storage/clean_data/offenses_known/offenses_known_yearly_1960_2020.rds") %>%
  select(ORI = ori9,
         agency = crosswalk_agency_name,
         year,
         state,
         population) %>%
  filter(year %in% 1991:2020,
         !is.na(ORI),
         !is.na(state)) %>%
  mutate(state  =  capitalize_words(state),
         agency = capitalize_words(agency))
ucr_population_only <-
  ucr %>%
  select(ORI,
         year,
         population)
ucr_name_only <-
  ucr %>%
  select(ORI,
         agency,
         state) %>%
  distinct(ORI, .keep_all = TRUE)


#get_agg_data(1991:2020)
get_agg_data <- function(years) {
  for (year_temp in years) {
    offense <- prep_offense(offense_files[grep(year_temp, offense_files)])
    offense_small <- offense %>%
      select(ori,
             unique_incident_id,
             offense)
    offense_agg_year <- aggregate_data(offense,
                                       variables = "gun_involved",
                                       time_unit = "year")
    offense_agg_month <- aggregate_data(offense,
                                        variables = "gun_involved",
                                        time_unit = "date")
    rm(offense); gc()

    offender <- prep_offender(offender_files[grep(year_temp, offender_files)],
                              offense_small)
    offender_agg_year <- aggregate_data(offender,
                                        variables = c("sex_of_offender",
                                                      "race_of_offender",
                                                      "age_category"),
                                        time_unit = "year")
    offender_agg_month <- aggregate_data(offender,
                                         variables = c("sex_of_offender",
                                                       "race_of_offender",
                                                       "age_category"),
                                         time_unit = "date")
    rm(offender); gc()

    arrestee <- prep_arrestee(arrestee_files[grep(year_temp, arrestee_files)],
                              arrestee_group_b_files[grep(year_temp, arrestee_group_b_files)]) %>%
      filter(ori %in% offense_small$ori)
    arrestee_agg_year <- aggregate_data(arrestee,
                                        variables = c("sex_of_arrestee",
                                                      "race_of_arrestee",
                                                      "ethnicity_of_arrestee",
                                                      "age_category",
                                                      "type_of_arrest"),
                                        time_unit = "year")
    arrestee_agg_month <- aggregate_data(arrestee,
                                         variables = c("sex_of_arrestee",
                                                       "race_of_arrestee",
                                                       "ethnicity_of_arrestee",
                                                       "age_category",
                                                       "type_of_arrest"),
                                         time_unit = "date")
    rm(arrestee); gc()

    victim <- prep_victim(victim_files[grep(year_temp, victim_files)]) %>%
      filter(ori %in% offense_small$ori)
    victim_agg_year <- aggregate_data(victim,
                                      variables = c("sex_of_victim",
                                                    "race_of_victim",
                                                    "ethnicity_of_victim",
                                                    "age_category",
                                                    "victim_injury"),
                                      time_unit = "year",
                                      victim_type = TRUE)
    victim_agg_month <- aggregate_data(victim,
                                       variables = c("sex_of_victim",
                                                     "race_of_victim",
                                                     "ethnicity_of_victim",
                                                     "age_category",
                                                     "victim_injury"),
                                       time_unit = "date",
                                       victim_type = TRUE)

    rm(victim); gc()
    rm(offense_small); gc()


    temp_agg_year <- offense_agg_year %>%
      left_join(offender_agg_year,                  by = c("ori", "time_unit")) %>%
      left_join(arrestee_agg_year,                  by = c("ori", "time_unit")) %>%
      left_join(victim_agg_year,                    by = c("ori", "time_unit"))


    saveRDS(temp_agg_year,
            paste0("C:/Users/jkkap/Dropbox/R_project/crimedatatool_helper_nibrs/data/temp_agg_year_", year_temp, ".rds"))

    temp_agg_month <- offense_agg_month %>%
      left_join(offender_agg_month,                  by = c("ori", "time_unit")) %>%
      left_join(arrestee_agg_month,                  by = c("ori", "time_unit")) %>%
      left_join(victim_agg_month,                    by = c("ori", "time_unit"))
    saveRDS(temp_agg_month,
            paste0("C:/Users/jkkap/Dropbox/R_project/crimedatatool_helper_nibrs/data/temp_agg_month_", year_temp, ".rds"))

    message(year_temp)
  }
}

combine_agg_data <- function(type) {
  setwd("C:/Users/jkkap/Dropbox/R_project/crimedatatool_helper_nibrs/data/")
  files <- list.files(pattern = paste0("temp_agg_", type))

  final <- data.frame()
  for (file in files) {
    temp <- readRDS(file)
    temp[is.na(temp)] <- 0
    final <- bind_rows(final, temp)
    rm(temp); gc()
    message(file)
  }

  final <- final[, -grep("30a|61a|90k", names(final))]
  final$ori <- toupper(final$ori)
  final <- final %>% rename(year = time_unit,
                            ORI  = ori)
  return(final)
}

add_missing_columns <- function(data) {
  for (col in c("arrestee_age_unknown_sports_tampering",
                "arrestee_age_unknown_human_trafficking_commercial_sex_acts",
                "arrestee_age_unknown_human_trafficking_involuntary_servitude",
                "arrestee_age_unknown_hacking_computer_invasion",
                "arrestee_age_unknown_animal_cruelty",
                "arrestee_american_indian_sports_tampering",
                "arrestee_unknown_race_sports_tampering",
                "arrestee_american_indian_hacking_computer_invasion",
                "arrestee_white_sports_tampering",
                "offender_american_indian_sports_tampering",
                "victim_unknown_sex_rape",
                "victim_unknown_sex_statutory_rape")) {
    if (!all(grepl(col, names(data)))) {
      data[, col] <- 0
    }
  }
  return(data)
}



final_agg_year  <- combine_agg_data(type = "year")
final_agg_year  <- add_missing_columns(final_agg_year); gc()
final_agg_year  <-
  final_agg_year %>%
  filter(ORI %in% ucr$ORI) %>%
  left_join(ucr_population_only) %>%
  left_join(ucr_name_only) %>%
  select(ORI,
         year,
         agency,
         state,
         population,
         everything())
setwd("nibrs")
make_largest_agency_json(final_agg_year)
make_state_agency_choices(final_agg_year)
files <- list.files(pattern = "agency_choices")
files
file.copy(files, "C:/Users/jkkap/Dropbox/R_project/crimedatatool_helper_nibrs/data/nibrs_monthly/", overwrite = TRUE)
names(final_agg_year) <- gsub("age_unknown", "unknown_age", names(final_agg_year))
final_agg_year <- final_agg_year[, -grep("demographics_total", names(final_agg_year))]
grep("victim.*sex.*rape", names(final_agg_year), value = TRUE)
make_agency_csvs(final_agg_year)
rm(final_agg_year); gc()



final_agg_month <- combine_agg_data(type = "month")
final_agg_month <- add_missing_columns(final_agg_month)
names(final_agg_month) <- gsub("age_unknown", "unknown_age", names(final_agg_month))
final_agg_month <- final_agg_month[, -grep("demographics_total", names(final_agg_month))]
final_agg_month$state_abb <- substr(final_agg_month$ORI, 1, 2)
state_abb <- unique(substr(unique(final_agg_month$ORI), 1, 2))
state_abb <- sort(state_abb)
state_abb
setwd("C:/Users/jkkap/Dropbox/R_project/crimedatatool_helper_nibrs/data/nibrs_monthly")
for (state_abb_temp in state_abb) {
  temp <- final_agg_month %>% filter(state_abb %in% state_abb_temp)
  final_agg_month <- final_agg_month %>% filter(!state_abb %in% state_abb_temp)
  gc()

  temp$state_abb <- NULL
  temp  <-
    temp %>%
    filter(ORI %in% ucr$ORI) %>%
    mutate(date = year,
           year = year(year)) %>%
    left_join(ucr_population_only, by = c("ORI", "year")) %>%
    left_join(ucr_name_only, by = "ORI") %>%
    select(-year) %>%
    rename(year = date) %>%
    select(ORI,
           year,
           agency,
           state,
           population,
           everything())
  gc()
  temp$year <- as.character(temp$year)
  make_agency_csvs(temp, type = "month")
}


