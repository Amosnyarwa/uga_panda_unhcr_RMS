
# Applying the cleaning log to clean the data
library(tidyverse)
library(lubridate)
library(glue)

source("R/support_functions.R")

# Read data and checking log

df_cleaning_log <- read_csv("inputs/combined_checks_RMS_test.csv", col_types = cols(sheet = "c", index = "i")) %>% 
  filter(!adjust_log %in% c("delete_log")) %>%
  mutate(adjust_log = ifelse(is.na(adjust_log), "apply_suggested_change", adjust_log),
         value = ifelse(is.na(value) & str_detect(string = issue_id, pattern = "logic_c_"), "blank", value),
         value = ifelse(type %in% c("remove_survey"), "blank", value),
         name = ifelse(is.na(name) & type %in% c("remove_survey"), "hh_id", name)
  ) %>% 
  filter(!is.na(value), !is.na(uuid)) %>%
  mutate(value = ifelse(value %in% c("blank"), NA, value),
         relevant = NA) %>%
  select(uuid, type, name, value, issue_id, sheet, index, relevant, issue)


data_path <- "inputs/RMS_Uganda_2022_Data.xlsx"

# main data
cols_to_escape <- c("index", "start", "end", "today", "starttime",	"endtime", "_submission_time", "_submission__submission_time")

data_nms <- names(readxl::read_excel(path = data_path, n_max = 2000))
c_types <- ifelse(str_detect(string = data_nms, pattern = "_other$"), "text", "guess")

df_raw_data <- readxl::read_excel(path = data_path, col_types = c_types) %>% 
  mutate(number = "NA") %>% 
  mutate(across(.cols = -c(contains(cols_to_escape)), 
                .fns = ~ifelse(str_detect(string = ., 
                                          pattern = fixed(pattern = "N/A", ignore_case = TRUE)), "NA", .)))

# loops
# S1 loop
hh_roster <- readxl::read_excel(path = data_path, sheet = "S1") %>% 
  mutate(HH02 = openssl::md5(HH02))

df_raw_data_hh_roster <- df_raw_data %>% 
  select(-`_index`) %>% 
  inner_join(hh_roster, by = c("_uuid" = "_submission__uuid"))
  
# tool
df_survey <- readxl::read_excel("inputs/RMS_tool.xlsx", sheet = "survey")
df_choices <- readxl::read_excel("inputs/RMS_tool.xlsx", sheet = "choices")


# main dataset ------------------------------------------------------------

df_cleaning_log_main <-  df_cleaning_log %>% 
  filter(is.na(sheet))

df_cleaned_data <- implement_cleaning_support(input_df_raw_data = df_raw_data,
                                              input_df_survey = df_survey,
                                              input_df_choices = df_choices,
                                              input_df_cleaning_log = df_cleaning_log_main) %>% 
  mutate(across(.cols = -c(any_of(cols_to_escape), matches("_age$|^age_|uuid")),
                .fns = ~ifelse(str_detect(string = ., pattern = "^[9]{2,9}$"), "NA", .)))


# clean repeats -----------------------------------------------------------
other_repeat_col <- c("start", "end", "today", "settlement", "hoh_equivalent", "respondent_gender", 
                      "respondent_age", "status")

df_cleaning_log_roster <- df_cleaning_log %>% 
  filter(uuid %in% df_raw_data_hh_roster$`_uuid`, name %in% colnames(df_raw_data_hh_roster))

df_cleaned_data_hh_roster <- implement_cleaning_support(input_df_raw_data = df_raw_data_hh_roster,
                                                        input_df_survey = df_survey,
                                                        input_df_choices = df_choices,
                                                        input_df_cleaning_log = df_cleaning_log_roster) %>% 
  select(any_of(other_repeat_col), any_of(colnames(hh_roster)), `_index` = index, `_submission__uuid` = uuid) %>% 
  mutate(across(.cols = -c(any_of(cols_to_escape), matches("_age$|^age_|uuid")),
                .fns = ~ifelse(str_detect(string = ., pattern = "^[9]{2,9}$"), "NA", .)))



# write final modified data -----------------------------------------------

list_of_clean_datasets <- list("RMS Uganda 2022 UNHCR" = df_cleaned_data,
                               "hh_roster" = df_cleaned_data_hh_roster,
                               
)

openxlsx::write.xlsx(x = list_of_clean_datasets,
                     file = paste0("outputs/", butteR::date_file_prefix(), 
                                   "_clean_data_unhcr_rms.xlsx"), 
                     overwrite = TRUE, keepNA = TRUE, na.string = "NA")






