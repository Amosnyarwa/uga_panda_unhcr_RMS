
# average time of survey --------------------------------------------------
  
  get_average_survey_time <- function(input_tool_data) {
    
    df_tool_data_with_time_interval <- input_tool_data %>% 
      mutate(int.survey_time_interval = lubridate::time_length(end - start, unit = "min"),
             int.survey_time_interval = ceiling(int.survey_time_interval)
      )%>% 
      filter(int.survey_time_interval > 0 ) %>% # consider only surveys with positive time interval
      select(int.survey_time_interval)
    
    # lower and upper quantiles of survey duration
    lower_limit = quantile(df_tool_data_with_time_interval$int.survey_time_interval, 0.025, na.rm =TRUE)
    upper_limit = quantile(df_tool_data_with_time_interval$int.survey_time_interval, 0.975, na.rm =TRUE)
    
    df_tool_data_with_time_interval %>% 
      filter(int.survey_time_interval > lower_limit | int.survey_time_interval < upper_limit) %>% 
      summarise(average_time = round(mean(int.survey_time_interval, na.rm = TRUE), 0)) %>% 
      pull()
  }
  
  # cleaning support for different datasets -----------------------
  
  implement_cleaning_support <- function(input_df_raw_data, input_df_survey, input_df_choices, input_df_cleaning_log) {
    
    # find all new choices to add to choices sheet
    
    # gather choice options based on unique choices list
    df_grouped_choices<- input_df_choices %>% 
      group_by(list_name) %>% 
      summarise(choice_options = paste(name, collapse = " : "))
    
    # get new name and choice pairs to add to the choices sheet
    new_vars <- input_df_cleaning_log %>% 
      filter(type %in% c("change_response", "add_option")) %>% 
      left_join(input_df_survey, by = "name") %>% 
      filter(str_detect(string = type.y, pattern = "select_one|select one|select_multiple|select multiple")) %>% 
      separate(col = type.y, into = c("select_type", "list_name"), sep =" ", remove = TRUE, extra = "drop") %>% 
      left_join(df_grouped_choices, by = "list_name") %>%
      filter(!str_detect(string = choice_options, pattern = value ) ) %>%
      dplyr::rename(choice = value ) %>%
      select(name, choice) %>%
      distinct() %>% # to make sure there are no duplicates
      arrange(name)
    
    # create kobold object
    
    kbo <- kobold::kobold(survey = input_df_survey, 
                          choices = input_df_choices, 
                          data = input_df_raw_data, 
                          cleaning = input_df_cleaning_log)
    
    # modified choices for the survey tool
    df_choises_modified <- butteR:::xlsform_add_choices(kobold = kbo, new_choices = new_vars)
    
    # special treat for variables for select_multiple, we need to add the columns to the data itself
    df_survey_sm <- input_df_survey %>% 
      mutate(q_type = case_when(str_detect(string = type, pattern = "select_multiple|select multiple") ~ "sm",
                                str_detect(string = type, pattern = "select_one|select one") ~ "so",
                                TRUE ~ type)) %>% 
      select(name, q_type)
    
    # construct new columns for select multiple
    new_vars_sm <- new_vars %>% 
      left_join(df_survey_sm, by = "name") %>% 
      filter(q_type == "sm") %>% 
      mutate(new_cols = paste0(name,"/",choice))
    
    # add new columns to the raw data
    df_raw_data_modified <- input_df_raw_data %>% 
      butteR:::mutate_batch(nm = new_vars_sm$new_cols, value = F )
    
    # make some cleanup
    kbo_modified <- kobold::kobold(survey = input_df_survey %>% filter(name %in% colnames(df_raw_data_modified)), 
                                   choices = df_choises_modified, 
                                   data = df_raw_data_modified, 
                                   cleaning = input_df_cleaning_log)
    kbo_cleaned <- kobold::kobold_cleaner(kbo_modified)
    
    # handling Personally Identifiable Information(PII)
    input_vars_to_remove_from_data <- c("deviceid", 
                                        "audit",
                                        "audit_URL",
                                        "instance_name",
                                        "complainant_name",
                                        "complainant_id",
                                        "respondent_telephone",
                                        "name_pers_recording",
                                        "geopoint",
                                        "_geopoint_latitude",
                                        "_geopoint_longitude",
                                        "_geopoint_altitude",
                                        "_geopoint_precision")
    
    df_handle_pii <- kbo_cleaned$data %>% 
      mutate(across(any_of(input_vars_to_remove_from_data), .fns = ~na_if(., .)))
    
    # handling added responses after starting data collection and added responses in the cleaning process
    
    sm_colnames <-  df_handle_pii %>% 
      select(contains("/")) %>% 
      colnames() %>% 
      str_replace_all(pattern = "/.+", replacement = "") %>% 
      unique()
    
    df_handle_sm_data <- df_handle_pii
    
    for (cur_sm_col in sm_colnames) {
      df_updated_data <- df_handle_sm_data %>% 
        mutate(
          across(contains(paste0(cur_sm_col, "/")), .fns = ~ifelse(!is.na(!!sym(cur_sm_col)) & is.na(.) , FALSE, .)),
          across(contains(paste0(cur_sm_col, "/")), .fns = ~ifelse(is.na(!!sym(cur_sm_col)), NA, .))
        )
      df_handle_sm_data <- df_updated_data
    }
    
    df_final_cleaned_data <- df_handle_sm_data
  }
  
  
  # similarity analysis -----------------------------------------------------
  
  # for each survey, it finds the closest matching survey with the minimum number of different columns
  calculateDifferences <- function(data, input_df_survey){
    # 1) convert all columns to character
    data <- mutate_all(data, as.character)
    # 2) remove columns that are naturally different in each survey
    cols <- data.frame(column=colnames(data)) %>% 
      left_join(select(input_df_survey, name, type), by=c("column"="name")) %>% 
      filter(!(type %in% c("_uuid", "enumerator_id")) &
               !str_starts(column, "_other$") &
               !str_detect(column, "_specify$"))
    data <- data[, all_of(cols$column)]
    # 3) convert NA to "NA" and all columns to factor
    data[is.na(data)] <- "NA"
    data <- data %>% mutate_if(is.character, factor)
    # 4) calculate gower distance
    gower_dist <- daisy(data, metric="gower", warnBin=F, warnAsym=F, warnConst=F)
    gower_mat <- as.matrix(gower_dist)
    # 5) convert distance to number of differences and determine closest matching survey
    r <- c()
    for (i in 1:nrow(data)) r <- c(r, sort(gower_mat[i,]*ncol(data))[2])
    # 6) store results
    data[["n.col.not.NA"]] <- rowSums(data!="NA")
    data[["survey.id"]] <- 1:dim(data)[1]
    data[["most.similar.survey"]] <- names(r)
    data[["number.different.columns"]] <- as.numeric(r)
    data <- data %>% arrange(number.different.columns, survey.id)
    return(data)
  }
  
  
  # silhouette analysis based on gower distance between surveys -------------
  
  calculateEnumeratorSimilarity <- function(data, input_df_survey, col_enum, col_admin){
    
    # helper function
    convertColTypes <- function(data, input_df_survey){
      # select_multiple: numeric or factor?
      col.types <- data.frame(column=colnames(data)) %>% 
        left_join(select(input_df_survey, name, type), by=c("column"="name")) %>% 
        mutate(type.edited = case_when(type %in% c("integer", "decimal", "calculate") ~ "numeric",
                                       str_starts(type, "select_") ~ "factor",
                                       str_detect(column, "/") ~ "factor",
                                       TRUE ~ "text"))
      
      cols <- col.types[col.types$type.edited=="numeric", "column"]
      data[,cols] <- lapply(data[,cols], as.numeric)
      cols <- col.types[col.types$type.edited=="text", "column"]
      data[,cols] <- lapply(data[,cols], as.character)
      cols <- col.types[col.types$type.edited=="factor", "column"]
      data[,cols] <- lapply(data[,cols], as.factor)
      
      return(data)
    }
    
    # convert columns using the tool
    data <- convertColTypes(data, input_df_survey)
    # keep only relevant columns
    cols <- data.frame(column=colnames(data)) %>% 
      left_join(select(input_df_survey, name, type), by=c("column"="name")) %>% 
      filter(!(type %in% c("_uuid", "enumerator_id")) &
               !str_starts(column, "_other$") &
               !str_detect(column, "_specify$"))
    # convert character columns to factor and add enum.id
    data <- data[, all_of(cols$column)] %>% 
      mutate_if(is.character, factor) %>% 
      arrange(!!sym(col_enum)) %>%
      mutate(enum.id=as.numeric(!!sym(col_enum)), .after=!!sym(col_enum))
    # add "SY" column in case col_admin is not specified
    #if (col_admin=="adm") data <- mutate(data, adm="DRC", .before=cols$column[1])
    # calculate similarity (for enumerators who completed at least 5 surveys)
    res <- data %>% split(data[[col_admin]]) %>% 
      lapply(function(gov){
        df <- gov %>% 
          group_by(enum.id) %>% 
          mutate(n=n()) %>% 
          filter(n>=5) %>% 
          ungroup() %>% 
          select_if(function(x) any(!is.na(x)))
        
        if (length(unique(df$enum.id)) > 1){
          # calculate gower distance
          gower_dist <- daisy(select(df, -c(!!sym(col_enum), enum.id)), 
                              metric = "gower", warnBin = F, warnAsym = F, warnConst = F)
          # gower_mat <- as.matrix(gower_dist)
          # calculate silhouette
          si <- silhouette(df$enum.id, gower_dist)
          res.si <- summary(si)
          # create output
          r <- data.frame(enum.id=as.numeric(names(res.si$clus.avg.widths)), si=res.si$clus.avg.widths) %>% 
            left_join(distinct(select(df, !!sym(col_admin), !!sym(col_enum), enum.id)), by="enum.id") %>% 
            left_join(group_by(df, enum.id) %>% summarise(num.surveys=n(), .groups="drop_last"), by="enum.id") %>% 
            select(!!sym(col_admin), !!sym(col_enum), num.surveys, si) %>% 
            arrange(-si)
          return(r)
        }
      })
    do.call(rbind, res)
  }
  
  