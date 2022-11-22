# checks for data collection

library(checksupporteR)
library(tidyverse)
library(lubridate)
library(glue)
library(cluster)

# read data ---------------------------------------------------------------

df_tool_data <- readxl::read_excel(path = "inputs/RMS_Uganda_2022_Data.xlsx") %>% 
  mutate(i.check.uuid = `_uuid`,
         i.check.start_date = as_date(start),
         i.check.enumerator_id = as.character(enumerator_id),
         i.check.settlement = settlement,
         i.check.household_id = household_id) %>% 
  filter(i.check.start_date > as_date("2022-11-23"))

hh_roster_data <- readxl::read_excel(path = dataset_location, sheet = "hh_roster")
df_repeat_hh_roster_data <- df_tool_data %>% 
  inner_join(hh_roster_data, by = c("_uuid" = "_submission__uuid"))


df_survey <- readxl::read_excel(path = "inputs/RMS_PILOT_v3.xlsx", sheet = "survey")
df_choices <- readxl::read_excel(path = "inputs/RMS_PILOT_v3.xlsx", sheet = "choices")

df_sample_data <- read_csv("inputs/RMS_sampling_hhids.csv") %>% 
  janitor::clean_names() %>% 
  rename(unique_hhid_number = id)

# output holder -----------------------------------------------------------

logic_output <- list()

# data not meeting minimum requirements -----------------------------------

# testing_data
df_testing_data <- df_tool_data %>% 
  filter(i.check.start_date < as_date("2022-11-23") | str_detect(string = household_id, pattern = fixed('test', ignore_case = TRUE))) %>% 
  mutate(i.check.type = "remove_survey",
         i.check.name = "",
         i.check.current_value = "",
         i.check.value = "",
         i.check.issue_id = "logic_c_testing_data",
         i.check.issue = "testing_data",
         i.check.other_text = "",
         i.check.checked_by = "",
         i.check.checked_date = as_date(today()),
         i.check.comment = "", 
         i.check.reviewed = "1",
         i.check.adjust_log = "",
         i.check.so_sm_choices = "") %>% 
  dplyr::select(starts_with("i.check.")) %>% 
  rename_with(~str_replace(string = .x, pattern = "i.check.", replacement = ""))

add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_testing_data")


# time checks -------------------------------------------------------------

# Time interval for the survey
min_time_of_survey <- 20
max_time_of_survey <- 120


df_survey_time <- check_survey_time(input_tool_data = df_tool_data, 
                                    input_min_time = min_time_of_survey,
                                    input_max_time = max_time_of_survey)

add_checks_data_to_list(input_list_name = "logic_output",input_df_name = "df_survey_time")

# check the time between surveys
min_time_btn_surveys <- 5

df_time_btn_surveys <- check_time_interval_btn_surveys(input_tool_data = df_tool_data, 
                                                       input_min_time = min_time_btn_surveys)

add_checks_data_to_list(input_list_name = "logic_output",input_df_name = "df_time_btn_surveys")


# outlier checks ----------------------------------------------------------

df_c_outliers <- checksupporteR::check_outliers_cleaninginspector(input_tool_data = df_tool_data)

add_checks_data_to_list(input_list_name = "logic_output",input_df_name = "df_c_outliers")


# checks on hh_ids ----------------------------------------------------------

sample_hhid_nos <- df_sample_data %>% 
  pull(unique_hhid_number) %>% 
  unique()

# duplicate hh_ids
df_c_duplicate_hhid_nos <- check_duplicate_hhid_numbers(input_tool_data = df_tool_data,
                                                        input_sample_hhid_nos_list = sample_hhid_nos)

add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_c_duplicate_hhid_nos")

# hh_id does not exist in sample
df_c_hhid_not_in_sample <- check_hhid_number_not_in_samples(input_tool_data = df_tool_data, 
                                                            input_sample_hhid_nos_list = sample_hhid_nos)

add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_c_hhid_not_in_sample")


# others checks -----------------------------------------------------------

df_others_data <- extract_other_specify_data(input_tool_data = df_tool_data, 
                                             input_survey = df_survey, 
                                             input_choices = df_choices)

add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_others_data")


# logical checks for different responses ----------------------------------

# Respondent reports 'crop production on own land' as a livelihood, but reports to not have arable land. i.e. 
#(selected(${hh_primary_livelihood}, "crop_production_on_own_land") OR selected(${other_livelihoods_hh_engaged_in}, 
#"crop_production_on_own_land")) AND farming_land_availability = 'no'

# If respondent reports "Ebola is not real, there are no symptoms", but reports that "There is an increased chance of getting Ebola 
# at the Ebola treatment centres ", check

df_ebola_symptoms <- df_tool_data %>% 
  filter(recm_no_centre_reason == "there_is_an_increased_chance_of_getting_ebola_at_the_ebola_treatment_centres_", (recm_no_centre_reason %in% c("there_is_an_increased_chance_of_getting_ebola_at_the_ebola_treatment_centres_") |
                                               str_detect(string = other_livelihoods_hh_engaged_in, pattern = "there_is_an_increased_chance_of_getting_ebola_at_the_ebola_treatment_centres_"))) %>% 
  mutate(i.check.type = "change_response",
         i.check.name = "there_is_an_increased_chance_of_getting_ebola_at_the_ebola_treatment_centres_",
         i.check.current_value = there_is_an_increased_chance_of_getting_ebola_at_the_ebola_treatment_centres_,
         i.check.value = "",
         i.check.issue_id = "logic_c_symptoms",
         i.check.issue = glue("there_is_an_increased_chance_of_getting_ebola_at_the_ebola_treatment_centres_: {there_is_an_increased_chance_of_getting_ebola_at_the_ebola_treatment_centres_}, but recm_no_centre_reason: {recm_no_centre_reason} and other_livelihoods_hh_engaged_in: {other_livelihoods_hh_engaged_in}"),
         i.check.other_text = "",
         i.check.checked_by = "",
         i.check.checked_date = as_date(today()),
         i.check.comment = "", 
         i.check.reviewed = "",
         i.check.adjust_log = "",
         i.check.so_sm_choices = "") %>% 
  dplyr::select(starts_with("i.check.")) %>% 
  rename_with(~str_replace(string = .x, pattern = "i.check.", replacement = ""))

add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_hh_livelihood_crop_production_on_own_land_1")











# combine and output checks -----------------------------------------------

# combine checks
df_combined_checks <- bind_rows(logic_output)

# output the combined checks
write_csv(x = df_combined_checks, file = paste0("outputs/", butteR::date_file_prefix(), "_combined_checks_RMS.csv"), na = "")


# similarity and silhouette analysis --------------------------------------

# silhouette analysis

# NOTE: the column for "col_admin" is kept in the data
omit_cols_sil <- c("start", "end", "today", "duration", "duration_minutes", 
                   "consent_one", "consent_two",  "consent","hoh", "hoh_equivalent",
                   "deviceid", "audit", "audit_URL", "instance_name", "end_survey","settlement_name",
                   "demo_check", "hh_roster_note","edu_note","cami_note", "lcsi_note","fcs_note", "mdd_note", "end_note", "geopoint", "_geopoint_latitude", "_geopoint_altitude", "_geopoint_precision", "_id" ,"_submission_time","_validation_status","_notes","_status","_submitted_by","_tags","_index","Too short", "pmi_issues",
                   "i.check.enumerator_id")

data_similartiy_sil <- df_tool_data %>% 
  select(- any_of(omit_cols_sil))

df_sil_data <- calculateEnumeratorSimilarity(data = data_similartiy_sil,
                                             input_df_survey = df_survey, 
                                             col_enum = "enumerator_id",
                                             col_admin = "settlement") %>% 
  mutate(si2= abs(si))

df_sil_data[order(df_sil_data$`si2`, decreasing = TRUE),!colnames(df_sil_data)%in%"si2"] %>%  
  openxlsx::write.xlsx(paste0("outputs/", butteR::date_file_prefix(), "_silhouette_analysis_RMS.xlsx"))


# similarity analysis

data_similartiy <- df_tool_data %>% 
  select(- any_of(c(omit_cols_sil, "settlement")))

df_sim_data <- calculateDifferences(data = data_similartiy, 
                                    input_df_survey = df_survey) %>% 
  openxlsx::write.xlsx(paste0("outputs/", butteR::date_file_prefix(), 
                              "_most_similar_analysis_RMS.xlsx"))



