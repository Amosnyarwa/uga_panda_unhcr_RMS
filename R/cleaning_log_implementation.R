# Applying the cleaning log to clean the data
library(tidyverse)
library(lubridate)
library(glue)

source("R/support_functions.R")

# Read data and checking log
df_full_cl_log <- read_csv("inputs/combined_checks_RMS.csv", col_types = cols(sheet = "c", index = "i")) 

df_cleaning_log <- df_full_cl_log |> 
  filter(!adjust_log %in% c("delete_log")) |>
  mutate(adjust_log = ifelse(is.na(adjust_log), "apply_suggested_change", adjust_log),
         value = ifelse(is.na(value) & str_detect(string = issue_id, pattern = "logic_c_"), "blank", value),
         value = ifelse(type %in% c("remove_survey"), "blank", value),
         name = ifelse(is.na(name) & type %in% c("remove_survey"), "point_number", name)
  ) |> 
  filter(!is.na(value), !is.na(uuid)) |>
  mutate(value = ifelse(value %in% c("blank"), NA, value),
         relevant = NA) |>
  select(uuid, type, name, value, issue_id, sheet, index, relevant, issue)


# handle data -------------------------------------------------------------

data_path <- "inputs/RMS_Uganda_2022_Data.xlsx"

# main data
cols_to_escape <- c("index", "start", "end", "today", "starttime",	"endtime", "_submission_time", "_submission__submission_time", "REF12a", "REF12b")

data_nms <- names(readxl::read_excel(path = data_path, n_max = 2000))
c_types <- ifelse(str_detect(string = data_nms, pattern = "_other$|REF12a|REF12b"), "text", "guess")

df_raw_data <- readxl::read_excel(path = data_path, col_types = c_types) |> 
  mutate(number = "NA",
         number_confirm = "NA",
         mm_name = "NA",
         name_individual = "NA",
         complainant_name = "NA",
         respondent_telephone = "NA",
         complainant_id = "NA",
         name_pers_recording = "NA") |> 
  mutate(across(.cols = -c(contains(cols_to_escape)), 
                .fns = ~ifelse(str_detect(string = ., 
                                          pattern = fixed(pattern = "N/A", ignore_case = TRUE)), "NA", .))) |> 
  mutate(EVD_misinformation_who = ifelse(str_detect(string = EVD_misinformation_who, pattern = "minitry_of_health"), str_replace(string = EVD_misinformation_who, pattern = "minitry_of_health", replacement = "ministry_of_health"), EVD_misinformation_who),
         `EVD_misinformation_who/ministry_of_health` = ifelse(!is.na(`EVD_misinformation_who/minitry_of_health`) & is.na(`EVD_misinformation_who/ministry_of_health`), `EVD_misinformation_who/minitry_of_health`, `EVD_misinformation_who/ministry_of_health`),
         EVD_recm_no_centre = ifelse(str_detect(string = EVD_recm_no_centre, pattern = "minitry_of_health"), str_replace(string = EVD_recm_no_centre, pattern = "minitry_of_health", replacement = "ministry_of_health"), EVD_recm_no_centre),
         `EVD_recm_no_centre/there_is_an_increased_chance_of_getting_ebola_at_the_ebola_treatment_centres` = ifelse(!is.na(`EVD_recm_no_centre/there_is_an_increased_chance_of_getting_ebola_at_the_ebola_treatment_centres_`) & is.na(`EVD_recm_no_centre/there_is_an_increased_chance_of_getting_ebola_at_the_ebola_treatment_centres`), `EVD_recm_no_centre/there_is_an_increased_chance_of_getting_ebola_at_the_ebola_treatment_centres_`, `EVD_recm_no_centre/there_is_an_increased_chance_of_getting_ebola_at_the_ebola_treatment_centres`)
  ) %>% 
  select(-c(`EVD_misinformation_who/minitry_of_health`, `EVD_recm_no_centre/there_is_an_increased_chance_of_getting_ebola_at_the_ebola_treatment_centres_`)) |> 
  mutate(across(.cols =c("namechild2less", "women_name_b":"fam_name20"), .fns = ~na_if(., .)))

# loops
# S1 loop
hh_roster <- readxl::read_excel(path = data_path, sheet = "S1") |> 
  mutate(HH02 = openssl::md5(HH02)) |> 
  mutate(across(.cols =c("women_b":"adult"), .fns = ~na_if(., .)))

df_raw_data_hh_roster <- df_raw_data |> 
  select(-`_index`) |> 
  inner_join(hh_roster, by = c("_uuid" = "_submission__uuid") )

# tool
df_survey <- readxl::read_excel("inputs/RMS_tool.xlsx", sheet = "survey")
df_choices <- readxl::read_excel("inputs/RMS_tool.xlsx", sheet = "choices")

# main dataset ------------------------------------------------------------

df_cleaning_log_main <-  df_cleaning_log |> 
  filter(is.na(sheet))

df_cleaned_data <- implement_cleaning_support(input_df_raw_data = df_raw_data,
                                              input_df_survey = df_survey,
                                              input_df_choices = df_choices,
                                              input_df_cleaning_log = df_cleaning_log_main)|> 
mutate(latestson = ifelse(latestson %in% c(","), NA, latestson))

# clean repeats -----------------------------------------------------------

other_repeat_col <- c("start", "end", "today", "settlement", "hoh_equivalent", "respondent_gender", 
                      "respondent_age", "status")

df_cleaning_log_roster <- df_cleaning_log |> 
  filter(uuid %in% df_raw_data_hh_roster$`_uuid`, name %in% colnames(df_raw_data_hh_roster))

df_cleaned_data_hh_roster <- implement_cleaning_support(input_df_raw_data = df_raw_data_hh_roster,
                                                        input_df_survey = df_survey,
                                                        input_df_choices = df_choices,
                                                        input_df_cleaning_log = df_cleaning_log_roster) |> 
  select(any_of(other_repeat_col), any_of(colnames(hh_roster)), `_index` = index, `_submission__uuid` = uuid) |> 
  # mutate(across(.cols = -c(any_of(cols_to_escape), matches("_age$|^age_|uuid")),
  #               .fns = ~ifelse(str_detect(string = ., pattern = "^[9]{2,9}$"), "NA", .))) |> 
  filter(`_submission__uuid` %in% df_cleaned_data$uuid)


# deletion log ------------------------------------------------------------

df_deletion_log <- df_cleaning_log |> 
  filter(type %in% c("remove_survey")) |> 
  group_by(uuid) |> 
  filter(row_number() == 1) |> 
  ungroup()


# write final modified data -----------------------------------------------

list_of_clean_datasets <- list("Raw_main" = df_raw_data,
                               "Raw_roster" = hh_roster,
                               "cleaning_log" = df_full_cl_log,
                               "deletion_log" = df_deletion_log,
                               "RMS Uganda 2022 UNHCR" = df_cleaned_data,
                               "hh_roster" = df_cleaned_data_hh_roster
)

openxlsx::write.xlsx(x = list_of_clean_datasets,
                     file = paste0("outputs/", butteR::date_file_prefix(), 
                                   "_clean_data_unhcr_rms.xlsx"), 
                     overwrite = TRUE, keepNA = TRUE, na.string = "NA")

