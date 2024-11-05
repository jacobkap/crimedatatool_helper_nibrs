get_property_years <- function(years) {
  inflation_adjust <- data.frame(year = 1991:2023, price = 1)
  inflation_adjust$in_current_dollars <- adjust_for_inflation(inflation_adjust$price,
    inflation_adjust$year,
    "US",
    to_date = 2023
  )
  batch_header <- readRDS("F:/ucr_data_storage/clean_data/combined_years/nibrs/nibrs_batch_header_1991_2023.rds") %>%
    select(
      ori,
      number_of_months_reported,
      year
    )

  for (i in 1:length(years)) {
    setwd("F:/ucr_data_storage/clean_data/nibrs")
    property_files <- list.files(pattern = "nibrs_property.*rds$", full.names = TRUE)
    property_files
    year_temp <- years[i]
    file <- property_files[grep(year_temp, property_files)]

    data <- readRDS(file) %>%
      select(ori,
        unique_incident_id,
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
      left_join(inflation_adjust, by = "year") %>%
      left_join(batch_header, by = join_by(ori, year)) %>%
      filter(number_of_months_reported %in% 12) %>%
      select(-number_of_months_reported)
    data$value[data$value %in% "unknown"] <- NA
    data$value <- parse_number(data$value)

    # Adjusted to 2022 cumulative inflation
    data$value <- data$value * data$in_current_dollars
    data$in_current_dollars <- NULL

    data$type_of_property_loss[data$type_of_property_loss %in% "stolen (includes bribed, defrauded, embezzled, extorted, ransomed, robbed, etc.)"] <- "stolen"
    data$type_of_property_loss[data$type_of_property_loss %in% "destroyed/damaged/vandalized"] <- "destroyed"
    data$type_of_property_loss[data$type_of_property_loss %in% "counterfeited/forged"] <- "counterfeited"

    data_agg_yearly <- get_property_agg(data, time_unit = "year")
    data_agg_monthly <- get_property_agg(data, time_unit = "month")
    rm(data)
    gc()

    data_agg_yearly$unique_incident_id <- NULL
    saveRDS(
      data_agg_yearly,
      paste0("~/crimedatatool_helper_nibrs/data/agg_data_property/temp_agg_year_nibrs_property_", year_temp, ".rds")
    )
    saveRDS(
      data_agg_monthly,
      paste0("~/crimedatatool_helper_nibrs/data/agg_data_property/temp_agg_month_nibrs_property_", year_temp, ".rds")
    )


    rm(data_agg_yearly, data_agg_monthly)
    gc()
    Sys.sleep(1)
    gc()

    message(years[i])
  }
}


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
      unique_incident_id,
      time_unit,
      drug = suspected_drug_type_1
    )
  data_drugs2 <-
    data %>%
    select(ori,
      unique_incident_id,
      time_unit,
      drug = suspected_drug_type_2
    )
  data_drugs3 <-
    data %>%
    select(ori,
      unique_incident_id,
      time_unit,
      drug = suspected_drug_type_3
    )

  data_drugs <-
    data_drugs1 %>%
    bind_rows(data_drugs2) %>%
    bind_rows(data_drugs3)
  data_drugs$drug[data_drugs$drug %in% "over 3 drug types"] <- "unknown type drug"

  data_drugs <-
    data_drugs %>%
    distinct(ori,
      unique_incident_id,
      drug,
      .keep_all = TRUE
    )
  data_drugs$unique_incident_id <- NULL
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

  if (time_unit %in% "month") {
    main_year <- names(sort(table(year(main_agg$time_unit)), decreasing = TRUE)[1])
    main_agg <- main_agg %>% filter(year(time_unit) %in% main_year)
  } else {
    main_year <- names(sort(table(main_agg$time_unit), decreasing = TRUE)[1])
    main_agg <- main_agg %>% filter(time_unit %in% main_year)
  }


  gc()
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


source("~/crimedatatool_helper_nibrs/R/utils.R")
#get_property_years(1991:2023)

make_nibrs_property_data <- function(time = "year") {
  yearly_files <- list.files("~/crimedatatool_helper_nibrs/data/agg_data_property/", pattern = "year", full.names = TRUE)
  monthly_files <- list.files("~/crimedatatool_helper_nibrs/data/agg_data_property/", pattern = "month", full.names = TRUE)
  yearly_files
  monthly_files
  if (time %in% "year") {
    yearly_data <- vector("list", length = length(yearly_files))
    for (i in 1:length(yearly_files)) {
      yearly_data[[i]] <- readRDS(yearly_files[i])
      message(i)
    }

    yearly_data <- data.table::rbindlist(yearly_data, fill = TRUE)
    gc()
    names(yearly_data) <- gsub("property_", "", names(yearly_data))
    save_as_csv_for_site(yearly_data, type = "year", property = TRUE)
    rm(yearly_data)
    gc()
    Sys.sleep(1)
    gc()
  } else {
    monthly_data <- vector("list", length = length(monthly_files))
    for (i in 1:length(yearly_files)) {
      monthly_data[[i]] <- readRDS(monthly_files[i])
      message(i)
    }

    monthly_data <- data.table::rbindlist(monthly_data, fill = TRUE)
    gc()
    names(monthly_data) <- gsub("property_", "", names(monthly_data))
    save_as_csv_for_site(monthly_data, type = "month", property = TRUE)
    rm(monthly_data)
    gc()
    Sys.sleep(1)
    gc()
  }
}
make_nibrs_property_data("year")
make_nibrs_property_data("month")
