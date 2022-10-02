source("C:/Users/jkkap/Dropbox/R_project/crimedatatool_helper_nibrs/R/utils.R")

batch_header_files <- list.files(pattern = "batch.*rds")
batch_header_files <- batch_header_files[-grep("zip", batch_header_files)]
batch_header <- data.frame()
for (file in batch_header_files) {
  temp <- readRDS(file) %>%
    select(ORI = ori,
           year,
           state,
           population,
           number_of_months_reported) %>%
    filter(number_of_months_reported %in% 12) %>%
    select(-number_of_months_reported)
  message(file)
  batch_header <- bind_rows(batch_header, temp)
}
ucr <- readRDS("D:/ucr_data_storage/clean_data/offenses_known/offenses_known_yearly_1960_2020.rds") %>%
  select(ORI = ori9,
         agency = crosswalk_agency_name) %>%
  distinct(ORI, .keep_all = TRUE)

batch_header <- batch_header %>%
  left_join(ucr) %>%
  filter(!is.na(agency)) %>%
  mutate(agency = capitalize_words(agency)) %>%
  filter(!is.na(agency),
         !is.na(state),
         !is.na(ORI))
batch_header$state <- gsub(" V2", "", batch_header$state)
rm(ucr); gc()
sort(unique(batch_header$state))
head(batch_header)

property_files <- list.files(pattern = "nibrs_property.*rds$")
property_files


final_data_agg_yearly  <- data.frame()
final_data_agg_monthly <- data.frame()
for (i in 1:length(property_files)) {

  data <- readRDS(property_files[i]) %>%
    select(ori,
           date = incident_date,
           type_of_property_loss,
           property_description) %>%
    filter(!type_of_property_loss %in% c("unknown", "none")) %>%
    mutate(date  = ymd(date),
           year  = year(date),
           month = floor_date(date, unit = "month"))
  data$type_of_property_loss[data$type_of_property_loss %in% "stolen/etc. (includes bribed, defrauded, embezzled, extorted, ransomed, robbed, etc.)"] <- "stolen"
  data$type_of_property_loss[data$type_of_property_loss %in% "destroyed/damaged/vandalized"] <- "destroyed"
  data$type_of_property_loss[data$type_of_property_loss %in% "counterfeited/forged"] <- "counterfeited"
  data_agg_yearly  <- get_property_agg(data, "year")
  data_agg_monthly <- get_property_agg(data, "month")


  final_data_agg_yearly  <- bind_rows(final_data_agg_yearly, data_agg_yearly)
  final_data_agg_monthly <- bind_rows(final_data_agg_monthly, data_agg_monthly)

  final_data_agg_yearly[is.na(final_data_agg_yearly)]   <- 0
  final_data_agg_monthly[is.na(final_data_agg_monthly)] <- 0
  message(property_files[i])
}
final_data_agg_monthly <-
  final_data_agg_monthly %>%
  filter(paste(ori, year(year)) %in% paste(batch_header$ORI, batch_header$year)) %>%
  mutate(month = year,
         year = year(year)) %>%
  rename(ORI = ori) %>%
  left_join(batch_header) %>%
  select(ORI,
         year,
         month,
         agency,
         state,
         population,
         everything()) %>%
  select(-year) %>%
  rename(year = month)

final_data_agg_yearly <-
  final_data_agg_yearly %>%
  filter(paste(ori, year) %in% paste(batch_header$ORI, batch_header$year)) %>%
  rename(ORI = ori) %>%
  left_join(batch_header) %>%
  select(ORI,
         year,
         agency,
         state,
         population,
         everything())

setwd("C:/Users/jkkap/Dropbox/R_project/crimedatatool_helper_nibrs/data/nibrs_property")
make_agency_csvs(final_data_agg_yearly)
make_largest_agency_json(final_data_agg_yearly)
make_state_agency_choices(final_data_agg_yearly)
files <- list.files(pattern = "agency_choices")
files
file.copy(files, "C:/Users/jkkap/Dropbox/R_project/crimedatatool_helper_nibrs/data/nibrs_property_monthly/", overwrite = TRUE)
setwd("C:/Users/jkkap/Dropbox/R_project/crimedatatool_helper_nibrs/data/nibrs_property_monthly")
make_agency_csvs(final_data_agg_monthly, type = "month")

get_property_agg <- function(data, time_unit) {
  main_agg <- data.frame()
  for (property_type in unique(data$type_of_property_loss)) {
    data$time_unit <- data[, time_unit]

    temp_agg <-
      data %>%
      filter(type_of_property_loss %in% property_type) %>%
      group_by(ori, time_unit) %>%
      count(property_description) %>%
      pivot_wider(names_from  = property_description,
                  values_from = n)

    names(temp_agg)[3:ncol(temp_agg)] <- paste0("property_", property_type, "_", names(temp_agg)[3:ncol(temp_agg)])

    if (nrow(main_agg) == 0) {
      main_agg <- temp_agg
    } else {
      main_agg <-
        main_agg %>%
        full_join(temp_agg, by = c("ori", "time_unit"))
    }

  }
  names(main_agg) <- gsub("^time_unit$", "year", names(main_agg))
  if (time_unit %in% "month") {
    main_year <- names(sort(table(year(main_agg$year)), decreasing = TRUE)[1])
    main_agg  <- main_agg %>% filter(year(year) %in% main_year)
  } else {
    main_year <- names(sort(table(main_agg$year), decreasing = TRUE)[1])
    main_agg <- main_agg %>% filter(year %in% main_year)
  }


  main_agg <-
    main_agg %>%
    rename_all(make_clean_names)

  main_agg <- dummy_rows(main_agg, select_columns = c("ori", "year"),
                         dummy_value = NA)
  main_agg[is.na(main_agg)] <- 0
  return(main_agg)
}


