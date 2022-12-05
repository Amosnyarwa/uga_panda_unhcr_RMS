library(checksupporteR)
library(tidyverse)
library(lubridate)
library(glue)
library(cluster)

source("R/support_functions.R")


# read data ---------------------------------------------------------------

dataset_location <- "inputs/RMS_data.xlsx"

df_tool_data <- readxl::read_excel(path = dataset_location) %>% 
  mutate(i.check.uuid = `_uuid`,
         i.check.start_date = as_date(start),
         i.check.enumerator_id = as.character(enumerator_id),
         i.check.district_name = case_when(settlement %in% c("rhino_camp") ~ "madi_okollo",
                                           settlement %in% c("bidibidi") ~ "yumbe",
                                           settlement %in% c("imvepi") ~ "terego",
                                           settlement %in% c("palabek") ~ "lamwo",
                                           settlement %in% c("kyangwali") ~ "kikube",
                                           settlement %in% c("lobule") ~ "koboko",
                                           settlement %in% c("nakivale") ~ "isingiro",
                                           settlement %in% c("oruchinga") ~ "isingiro",
                                           settlement %in% c("palorinya") ~ "obongi",
                                           settlement %in% c("rwamwanja") ~ "kamwenge",
                                           settlement %in% c("kyaka_ii") ~ "kyegegwa",
                                           settlement %in% c("any_adjumani_settlements") ~ "adjumani",
                                           TRUE ~ settlement),
         district_name = i.check.district_name,
         i.check.settlement = settlement,
         i.check.point_number = household_id,
         point_number = i.check.point_number) %>% 
  filter(i.check.start_date > as_date("2022-11-22") & end_result == 1)

hh_roster_data <- readxl::read_excel(path = dataset_location, sheet = "S1")
df_repeat_hh_roster_data <- df_tool_data %>% 
  inner_join(hh_roster_data, by = c("_uuid" = "_submission__uuid"))

df_survey <- readxl::read_excel(path = "inputs/RMS_tool.xlsx", sheet = "survey")
df_choices <- readxl::read_excel(path = "inputs/RMS_tool.xlsx", sheet = "choices")

# Read sample data
df_sample_data <- read_csv("inputs/pa_rms_sampling_hhids.csv")

# output holder -----------------------------------------------------------

logic_output <- list()


# data not meeting minimum requirements -----------------------------------

# # testing_data
# df_testing_data <- df_tool_data %>% 
#   filter(i.check.start_date < as_date("2022-11-15") | str_detect(string = household_id, pattern = fixed('test', ignore_case = TRUE))) %>% 
#   mutate(i.check.type = "remove_survey",
#          i.check.name = "",
#          i.check.label = "",
#          i.check.current_value = "",
#          i.check.value = "",
#          i.check.issue_id = "logic_c_testing_data",
#          i.check.issue = "testing_data",
#          i.check.other_text = "",
#          i.check.checked_by = "",
#          i.check.checked_date = as_date(today()),
#          i.check.comment = "", 
#          i.check.reviewed = "1",
#          i.check.adjust_log = "",
#          i.check.so_sm_choices = "") %>% 
#   dplyr::select(starts_with("i.check.")) %>% 
#   rename_with(~str_replace(string = .x, pattern = "i.check.", replacement = ""))
# 
# add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_testing_data")

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

df_c_outliers_hh_roster <- checksupporteR::check_outliers_cleaninginspector_repeats(input_tool_data = df_repeat_hh_roster_data,
                                                                                    input_sheet_name = "hh_roster", input_repeat_cols = c("age"))

add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_c_outliers_hh_roster") 


# checks on hh_ids ------------------------------------------------------

sample_hhid_nos <- df_sample_data %>%
  pull(hh_id) %>%
  unique()

# # duplicate hh_ids
# df_c_duplicate_hhid_nos <- check_duplicate_hhid_numbers(input_tool_data = df_tool_data,
#                                                         input_sample_hhid_nos_list = sample_hhid_nos)
# 
# add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_c_duplicate_hhid_nos")


# hh_id does not exist in sample
# df_c_hhid_not_in_sample <- check_hhid_number_not_in_samples(input_tool_data = df_tool_data,
#                                                             input_sample_hhid_nos_list = sample_hhid_nos)
# 
# 
# 
# add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_c_hhid_not_in_sample")

# others checks -----------------------------------------------------------

df_others_data <- extract_other_specify_data(input_tool_data = df_tool_data, 
                                             input_survey = df_survey, 
                                             input_choices = df_choices)

add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_others_data")


# logical checks ----------------------------------------------------------


# Respondent age not given in the hh roster. i.e. respondent_age != age in the hh roster

df_hoh_details_and_hh_roster_1 <- df_repeat_hh_roster_data %>%
  filter(status == "refugee")  %>%
  group_by(`_uuid`) %>%
  mutate(int.hoh_bio = ifelse(respondent_age == HH07|respondent_age == age, "given", "not")) %>% 
  filter(!str_detect(string = paste(int.hoh_bio, collapse = ":"), pattern = "given")) %>% 
  filter(row_number() == 1) %>% 
  ungroup() %>% 
  mutate(i.check.type = "change_response",
         i.check.name = "respondent_age ",
         i.check.current_value = as.character(respondent_age),
         i.check.value = "",
         i.check.issue_id = "logic_c_hoh_details_and_hh_roster_1",
         i.check.issue = glue("respondent_age : {respondent_age}, respondent age not given in the hh_roster"),
         i.check.other_text = "",
         i.check.checked_by = "",
         i.check.checked_date = as_date(today()),
         i.check.comment = "",
         i.check.reviewed = "",
         i.check.adjust_log = "",
         i.check.so_sm_choices = "") %>%
  dplyr::select(starts_with("i.check.")) %>%
  rename_with(~str_replace(string = .x, pattern = "i.check.", replacement = ""))

add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_hoh_details_and_hh_roster_1")


# Respondent select "pre/post natal check up" or "giving birth" in HACC02 but is a man. i.e. ${HACC02} = 4 or ${HACC02} = 5 yet 
#  ${HH04} = 2

# df_reason_sought_consultation <- df_repeat_hh_roster_data %>%
#   filter(status == "refugee")  %>%
#   group_by(`_uuid`) %>%
#   mutate(int.hoh_bio = ifelse(respondent_age == age, "given", "not")) %>% 
#   filter(!str_detect(string = paste(int.hoh_bio, collapse = ":"), pattern = "not")) %>% 
#   filter(row_number() == 1) %>% 
#   ungroup()%>% 
#   filter(sex %in% c("2") & str_detect(string = HACC02, pattern = 4|5)) %>% 
#   mutate(i.check.type = "change_response",
#          i.check.name = "sex",
#          i.check.current_value = sex,
#          i.check.value = "",
#          i.check.issue_id = "logic_c_hoh_details_and_hh_roster_2",
#          i.check.issue = glue("sex : {sex}, HACC02 : {HACC02}"),
#          i.check.other_text = "",
#          i.check.checked_by = "",
#          i.check.checked_date = as_date(today()),
#          i.check.comment = "",
#          i.check.reviewed = "",
#          i.check.adjust_log = "",
#          i.check.so_sm_choices = "") %>%
#   dplyr::select(starts_with("i.check.")) %>%
#   rename_with(~str_replace(string = .x, pattern = "i.check.", replacement = ""))

# add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_hoh_details_and_hh_roster_1")


# If EVD06 = "Ebola is not real, there are no symptoms", then EVD12 cannot be "Ebola can be cured on its own, 
# without action" or "Traditional treatments are better" or Religious treatments are better"
df_evd_treatment_2 <- df_tool_data %>%
  filter(str_detect(string = EVD_symptoms, pattern = "ebola_is_not_real_there_are_no_symptoms") &
           str_detect(string = EVD_recm_no_healthfac, pattern = "ebola_can_be_cured_on_its_own_without_action|
                      traditional_treatments_are_better|religious_treatments_are_better"))%>%
  mutate(i.check.type = "remove_option",
         i.check.name = "EVD_recm_no_healthfac",
         i.check.current_value = EVD_recm_no_healthfac,
         i.check.value = "",
         i.check.issue_id = "logic_c_evd_treatment_2",
         i.check.issue = glue("EVD_symptoms: {EVD_symptoms},
                              but EVD_recm_no_healthfac: {EVD_recm_no_healthfac}"),
         i.check.other_text = "",
         i.check.checked_by = "",
         i.check.checked_date = as_date(today()),
         i.check.comment = "",
         i.check.reviewed = "",
         i.check.adjust_log = "",
         i.check.so_sm_choices = "") %>%
  dplyr::select(starts_with("i.check.")) %>%
  rename_with(~str_replace(string = .x, pattern = "i.check.", replacement = ""))

add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_evd_treatment_2") 


# If EVD08, or EVD09, or EVD11, or EVD12 = "Ebola is not real", then EVD13 cannot be any of the answers except 
# "The ebola pandemic in Uganda is not real, therefore there are no transmissions (exclusive)" and "other"
df_evd_transmission_modes_3 <- df_tool_data %>%
  filter(!str_detect(string = EVD_transmission_modes, pattern = "the_ebola_pandemic_in_uganda_is_not_real|other") & 
           if_any(c(EVD_recm_no_hotline, EVD_recm_no_centre, EVD_recm_usual, EVD_recm_no_healthfac), 
                  ~str_detect(., "ebola_is_not_real"))) %>%
  mutate(i.check.type = "remove_option",
         i.check.name = "EVD_transmission_modes",
         i.check.current_value = EVD_transmission_modes,
         i.check.value = "",
         i.check.issue_id = "logic_c_evd_transmission_modes_3",
         i.check.issue = glue("EVD_transmission_modes: {EVD_transmission_modes}, but has previously said ebola is not real"),
         i.check.other_text = "",
         i.check.checked_by = "",
         i.check.checked_date = as_date(today()),
         i.check.comment = "",
         i.check.reviewed = "",
         i.check.adjust_log = "",
         i.check.so_sm_choices = "") %>%
  dplyr::select(starts_with("i.check.")) %>%
  rename_with(~str_replace(string = .x, pattern = "i.check.", replacement = ""))

add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_evd_transmission_modes_3")


# If EVD08, or EVD09, or EVD11, or EVD12 = "Ebola is not real", then EVD14 cannot be any of the answers except "do not want to answer" and "other"
df_evd_prevention_modes_4 <- df_tool_data %>%
  filter(!str_detect(string = EVD_prevention_modes, pattern = "no_answer|other") & 
           if_any(c(EVD_recm_no_hotline, EVD_recm_no_centre, EVD_recm_usual, EVD_recm_no_healthfac), 
                  ~str_detect(., "ebola_is_not_real"))) %>%
  mutate(i.check.type = "remove_option",
         i.check.name = "EVD_prevention_modes",
         i.check.current_value = EVD_prevention_modes,
         i.check.value = "",
         i.check.issue_id = "logic_c_evd_prevention_modes_4",
         i.check.issue = glue("EVD_prevention_modes: {EVD_prevention_modes}, but has previously said ebola is not real"),
         i.check.other_text = "",
         i.check.checked_by = "",
         i.check.checked_date = as_date(today()),
         i.check.comment = "",
         i.check.reviewed = "",
         i.check.adjust_log = "",
         i.check.so_sm_choices = "") %>%
  dplyr::select(starts_with("i.check.")) %>%
  rename_with(~str_replace(string = .x, pattern = "i.check.", replacement = ""))

add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_evd_prevention_modes_4")


# If EVD08, or EVD09, or EVD11, or EVD12 = "Ebola is not real" or if EVD06 = "Ebola is not real, there are no symptoms", then EVD18 cannot be 
# "Survivors certified to be cured of Ebola could infect others through casual contact (e.g., hugging or shaking hands)" or "Would not buy fresh 
# vegetables from survivor certified by government to be cured of Ebola" or "You cannot survive Ebola, it is a certain death"
df_evd_misconceptions_5 <- df_tool_data %>%
  filter(str_detect(string = EVD_misconceptions, pattern = "survivors_certified_to_be_cured_of_ebola_could_infect_others_through_casual_contact|
                     would_not_buy_fresh_vegetables_from_survivor_certified_by_government_to_be_cured_of_ebola | you_cannot_survive_ebola_it_is_a_certain_death") & 
           (if_any(c(EVD_recm_no_hotline, EVD_recm_no_centre, EVD_recm_usual, EVD_recm_no_healthfac), 
                   ~str_detect(., "ebola_is_not_real"))) | str_detect(string = EVD_symptoms, pattern = "ebola_is_not_real_there_are_no_symptoms")) %>%
  mutate(i.check.type = "remove_option",
         i.check.name = "EVD_misconceptions",
         i.check.current_value = EVD_misconceptions,
         i.check.value = "",
         i.check.issue_id = "logic_c_evd_misconceptions_5",
         i.check.issue = glue("EVD_misconceptions: {EVD_misconceptions}, but has previously said ebola is not real"),
         i.check.other_text = "",
         i.check.checked_by = "",
         i.check.checked_date = as_date(today()),
         i.check.comment = "",
         i.check.reviewed = "",
         i.check.adjust_log = "",
         i.check.so_sm_choices = "") %>%
  dplyr::select(starts_with("i.check.")) %>%
  rename_with(~str_replace(string = .x, pattern = "i.check.", replacement = ""))

add_checks_data_to_list(input_list_name = "logic_output", input_df_name = "df_evd_misconceptions_5")

# combine and output checks -----------------------------------------------

# combine checks
df_combined_checks <- bind_rows(logic_output) %>% 
  rename(hh_id = point_number) %>% 
  mutate(name = case_when(name %in%c("point_number") ~ "hh_id",
                          TRUE ~ name))

# output the combined checks
write_csv(x = df_combined_checks, file = paste0("outputs/", butteR::date_file_prefix(), "combined_checks_RMS.csv"), na = "")

