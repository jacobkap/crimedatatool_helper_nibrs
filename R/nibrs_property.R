source("~/crimedatatool_helper_nibrs/R/utils.R")
library(blscrapeR)
# https://www.usinflationcalculator.com/
# inflation <- inflation_adjust(2022) %>%
#   select(
#     year,
#     pct_increase
#   )
# inflation$multiplier <- inflation$pct_increase / 100
# inflation$multiplier <- abs(inflation$multiplier)
# inflation$multiplier <- 1 - inflation$multiplier
# inflation$multiplier <- 1 / inflation$multiplier
# inflation$year <- as.numeric(inflation$year)
# inflation$pct_increase <- NULL
# 999999999 999999998
library(quantmod)
# https://stackoverflow.com/questions/12590180/inflation-adjusted-prices-package
inflation <- getSymbols("CPIAUCSL", src='FRED')
avg.cpi <- apply.yearly(CPIAUCSL, mean)
cf <- avg.cpi/as.numeric(avg.cpi['2022']) #using 2022 as the base year
cf <- as.data.frame(cf)
cf$year <- rownames(cf)
rownames(cf) <- 1:nrow(cf)
cf <-
  cf %>%
  rename(inflation_adjuster = CPIAUCSL)
cf$year <- year(cf$year)
inflation <- cf
inflation$multiplier <- inflation$inflation_adjuster

batch_header <- readRDS("F:/ucr_data_storage/clean_data/combined_years/nibrs/nibrs_batch_header_1991_2023.rds") %>%
  filter(number_of_months_reported %in% 12) %>%
  select(ORI = ori,
         year,
         state,
         population)
batch_header$agency <- gsub(":", "-", batch_header$agency)
sort(unique(batch_header$state))
head(batch_header)

property_files <- list.files(pattern = "nibrs_property.*rds$")
property_files


for (i in 1:length(property_files)) {
  year_temp <- parse_number(property_files[i])

  data <- readRDS(property_files[i]) %>%
    select(ori,
      date = incident_date,
      type_of_property_loss,
      value = value_of_property,
      property_description,
      suspected_drug_type_1,
      suspected_drug_type_2,
      suspected_drug_type_3
    ) %>%
    filter(!type_of_property_loss %in% c("unknown", "none")) %>%
    mutate(
      date = ymd(date),
      year = year(date),
      month = floor_date(date, unit = "month")
    ) %>%
    left_join(inflation, by = "year")
  data$value[data$value %in% "unknown"] <- NA
  data$value <- parse_number(data$value)

  # Adjusted to 2022 cumulative inflation
  data$value <- data$value * data$multiplier

  data$type_of_property_loss[data$type_of_property_loss %in% "stolen/etc. (includes bribed, defrauded, embezzled, extorted, ransomed, robbed, etc.)"] <- "stolen"
  data$type_of_property_loss[data$type_of_property_loss %in% "destroyed/damaged/vandalized"] <- "destroyed"
  data$type_of_property_loss[data$type_of_property_loss %in% "counterfeited/forged"] <- "counterfeited"

  data_agg_yearly <- get_property_agg(data, "year")
  data_agg_monthly <- get_property_agg(data, "month")
  rm(data)
  gc()

  saveRDS(
    data_agg_yearly,
    paste0("~/crimedatatool_helper_nibrs/data/temp_agg_year_nibrs_property_", year_temp, ".rds")
  )
  saveRDS(
    data_agg_monthly,
    paste0("~/crimedatatool_helper_nibrs/data/temp_agg_month_nibrs_property_", year_temp, ".rds")
  )


  # final_data_agg_yearly  <- bind_rows(final_data_agg_yearly, data_agg_yearly)
  # final_data_agg_monthly <- bind_rows(final_data_agg_monthly, data_agg_monthly)

  rm(data_agg_yearly)
  rm(data_agg_monthly)
  gc()
  Sys.sleep(3)


  # final_data_agg_yearly[is.na(final_data_agg_yearly)]   <- 0
  # final_data_agg_monthly[is.na(final_data_agg_monthly)] <- 0
  message(property_files[i])
}

final_data_agg_yearly <- combine_agg_data(type = "year_nibrs_property_", batch_header)
final_data_agg_monthly <- combine_agg_data(type = "month_nibrs_property_", batch_header)
gc()
Sys.sleep(3)

final_data_agg_monthly <-
  final_data_agg_monthly %>%
  mutate(
    month = year,
    year = year(year)
  ) %>%
  left_join(batch_header) %>%
  select(-year) %>%
  mutate(year = month) %>%
  select(
    ORI,
    year,
    month,
    agency,
    state,
    population,
    everything()
  )


final_data_agg_yearly <-
  final_data_agg_yearly %>%
  left_join(batch_header) %>%
  select(
    ORI,
    year,
    agency,
    state,
    population,
    everything()
  )

names(final_data_agg_yearly) <- gsub("property_", "", names(final_data_agg_yearly))
names(final_data_agg_yearly)
names(final_data_agg_monthly) <- gsub("property_", "", names(final_data_agg_monthly))
gc()
Sys.sleep(5)

setwd("~/crimedatatool_helper_nibrs/data/nibrs_property")
make_agency_csvs(final_data_agg_yearly)
make_largest_agency_json(final_data_agg_yearly)
make_state_agency_choices(final_data_agg_yearly)
files <- list.files(pattern = "agency_choices")
files
file.copy(files, "~/crimedatatool_helper_nibrs/data/nibrs_property_monthly/", overwrite = TRUE)
setwd("~/crimedatatool_helper_nibrs/data/nibrs_property_monthly")
make_agency_csvs(final_data_agg_monthly, type = "month")

get_property_agg <- function(data, time_unit) {
  main_agg <- data.frame()
  for (property_type in unique(data$type_of_property_loss)) {
    data$time_unit <- data[, time_unit]

    options(dplyr.summarise.inform = FALSE)
    temp_agg_value <-
      data %>%
      filter(type_of_property_loss %in% property_type) %>%
      group_by(ori, time_unit, property_description) %>%
      summarize(
        mean_value = mean(value, na.rm = TRUE),
        median_value = median(value, na.rm = TRUE)
      ) %>%
      mutate(
        mean_value = round(mean_value),
        median_value = round(median_value)
      ) %>%
      pivot_wider(
        names_from = property_description,
        values_from = c(mean_value, median_value)
      )

    temp_agg <-
      data %>%
      filter(type_of_property_loss %in% property_type) %>%
      group_by(ori, time_unit) %>%
      count(property_description) %>%
      pivot_wider(
        names_from = property_description,
        values_from = n
      )

    temp_agg <-
      temp_agg %>%
      left_join(temp_agg_value, by = c("ori", "time_unit"))

    names(temp_agg)[3:ncol(temp_agg)] <- paste0("property_", property_type, "_", names(temp_agg)[3:ncol(temp_agg)])




    if (nrow(main_agg) == 0) {
      main_agg <- temp_agg
    } else {
      main_agg <-
        main_agg %>%
        full_join(temp_agg, by = c("ori", "time_unit"))
    }
  }
  # Get drugs
  data_drugs1 <-
    data %>%
    select(ori,
      time_unit,
      drug = suspected_drug_type_1
    )
  data_drugs2 <-
    data %>%
    select(ori,
      time_unit,
      drug = suspected_drug_type_2
    )
  data_drugs3 <-
    data %>%
    select(ori,
      time_unit,
      drug = suspected_drug_type_3
    )

  data_drugs <-
    data_drugs1 %>%
    bind_rows(data_drugs2) %>%
    bind_rows(data_drugs3) %>%
    filter(drug != "over 3 drug types")
  data_drugs$drug <- gsub(":.*| \\(.*", "", data_drugs$drug)

  drugs_agg <-
    data_drugs %>%
    filter(!is.na(drug)) %>%
    group_by(ori, time_unit) %>%
    count(drug) %>%
    pivot_wider(
      names_from = drug,
      values_from = n
    ) %>%
    rename_all(make_clean_names)
  names(drugs_agg)[3:ncol(drugs_agg)] <- paste0("drugs_", names(drugs_agg)[3:ncol(drugs_agg)])

  main_agg <-
    main_agg %>%
    left_join(drugs_agg, by = c("ori", "time_unit"))

  # names(main_agg) <- gsub("^time_unit$", "year", names(main_agg))
  if (time_unit %in% "month") {
    main_year <- names(sort(table(year(main_agg$time_unit)), decreasing = TRUE)[1])
    main_agg <- main_agg %>% filter(year(time_unit) %in% main_year)
  } else {
    main_year <- names(sort(table(main_agg$time_unit), decreasing = TRUE)[1])
    main_agg <- main_agg %>% filter(time_unit %in% main_year)
  }


  main_agg <-
    main_agg %>%
    rename_all(make_clean_names)

  main_agg <- dummy_rows(main_agg,
    select_columns = c("ori", "time_unit"),
    dummy_value = NA
  )
  main_agg[is.na(main_agg)] <- 0
  return(main_agg)
}
