library(tidyverse)
library(jsonlite)
library(lubridate)
library(readxl)
library(zoo)


filter_event_fields <- read_excel("master_data/list of events to filter.xlsx",sheet = "filter")


# Read data ----------
#__________________________

users_df <- read_csv("raw_data/users-8dc8b10e6dc60e61.csv.gz")
customers_df <- read_csv("raw_data/customers-dfe40aa03a5b76e9.csv.gz")
raw_events_df <- read_csv("raw_data/events-554c5b3e561b5eb4.csv.gz")


# Rename ids
users_df <- users_df %>% 
  rename(user_id = id)

customers_df <- customers_df %>% 
  rename(customer_id = id) 

raw_events_df <- raw_events_df %>% 
  rename(event_id = id,
         event_name = name)


# Filter data that dont't fulfill considerations
users_df <- users_df %>% 
  mutate(first_app_version = ifelse(first_app_version == "5.9.0","5.09.0",first_app_version),
         last_app_version = ifelse(last_app_version == "5.9.0","5.09.0",last_app_version)) %>% 
  filter(first_app_version >=  "5.09.0", last_app_version >= "5.09.0")

raw_events_df <- raw_events_df %>% 
  mutate(app_version = ifelse(app_version == "5.9.0","5.09.0",app_version)) %>% 
  filter(app_version >= "5.09.0",game == "lk")




users_df %>% write_csv("tables/users.csv")
customers_df %>% write_csv("tables/customers.csv")
raw_events_df %>% write_csv("tables/raw_events.csv")


# Session - Events --------
# ________________________

session_event_df <- raw_events_df %>% 
  arrange(user_id,timestamp) %>% 
  group_by(user_id) %>% 
  mutate(child_id = na_if(child_id,0) %>% na.locf(na.rm = F) %>% na.locf(na.rm = F, fromLast = T)) %>% 
  mutate(change_session = (event_id == first(event_id) | event_name %in% c("session_start","signup_successful"))) %>% 
  mutate(change_session = (change_session & !lag(change_session,default = 0)) | (difftime(timestamp, lag(timestamp, default=min(timestamp)),units = "hours") >= 1)) %>%
  ungroup %>% 
  mutate(session_id = cumsum(change_session)) %>% 
  select(session_id,everything(),-data,-change_session)


session_event_df %>% write_csv("tables/session_event.csv")

# Sessions ---------
# ________________________

sessions_df <- session_event_df %>% 
  group_by(session_id,user_id) %>% 
  mutate(child_id = na_if(child_id,0) %>% na.locf(na.rm = F) %>% na.locf(na.rm = F, fromLast = T)) %>% 
  # fill(child_id) %>% 
  # fill(child_id,.direction = "up") %>% 
  group_by(session_id,user_id,child_id) %>% 
  summarise(platform= first(platform),
            app_version = first(app_version),
            timestamp_ini = first(timestamp),
            timestamp_fin = last(timestamp),
            session_duration = difftime(timestamp_fin, timestamp_ini,units = "mins")) %>% 
  ungroup

sessions_df %>% write_csv("tables/sessions.csv")
  

# Activity enter ----------
#_____________________________

activity_enter_df <- raw_events_df %>% 
  filter(event_name == "activity_enter") %>% 
  select(event_id,data) %>% 
  mutate(data = map(data,~ .x %>% 
                      fromJSON %>% 
                      .[["data"]] %>% 
                      map_if(is.data.frame, list) %>% 
                      discard(is_empty) %>%
                      as_tibble %>% 
                      select(one_of(filter_event_fields %>% 
                                      filter(name == "activity_enter",load) %>% 
                                      pull(field)))
  )) %>%
  unnest

activity_enter_df %>% write_csv("tables/activity_enter.csv")


# Activity exit ----------
#_____________________________

vars_filter <- filter_event_fields %>% 
  filter(name == "activity_exit",load) %>% 
  pull(field)

vars_numeric <- filter_event_fields %>% 
  filter(name == "activity_exit",numeric) %>% 
  pull(field)

activity_exit_df <- raw_events_df %>% 
  filter(event_name == "activity_exit") %>% 
  select(event_id,data) %>% 
  mutate(data = map(data,~ .x %>% 
                      fromJSON %>% 
                      .[["data"]] %>% 
                      map_if(is.data.frame, list) %>% 
                      discard(is_empty) %>%
                      as_tibble %>% 
                      select(one_of(vars_filter)) %>% 
                      distinct %>% 
                      mutate_all(funs(ifelse(.=="",NA,.))) %>% 
                      mutate_at(vars(one_of(vars_numeric)), funs(as.numeric))
  )) %>%
  unnest

activity_exit_df %>% write_csv("tables/activity_exit.csv")



# Stickers awarded-------------
#_____________________________

vars_filter <- filter_event_fields %>% 
  filter(name == "child_sticker_awarded",load) %>% 
  pull(field)

vars_numeric <- filter_event_fields %>% 
  filter(name == "child_sticker_awarded",numeric) %>% 
  pull(field)

child_sticker_awarded_df <- raw_events_df %>% 
  filter(event_name == "child_sticker_awarded") %>% 
  select(event_id,data) %>% 
  # slice(1:1000) %>%
  mutate(data = map(data,~ .x %>% 
                      fromJSON %>% 
                      .[["data"]] %>% 
                      map_if(is.data.frame, list) %>% 
                      discard(is_empty) %>%
                      as_tibble %>% 
                      select(one_of(vars_filter)) %>% 
                      distinct %>% 
                      mutate_all(funs(ifelse(.=="",NA,.))) %>% 
                      mutate_at(vars(one_of(vars_numeric)), funs(as.numeric))
  )) %>%
  unnest


child_sticker_awarded_df %>% write_csv("tables/child_sticker_awarded.csv")

# Stickers accepted -------------
#_____________________________

vars_filter <- filter_event_fields %>% 
  filter(name == "child_sticker_accepted",load) %>% 
  pull(field)

vars_numeric <- filter_event_fields %>% 
  filter(name == "child_sticker_accepted",numeric) %>% 
  pull(field)

child_sticker_accepted_df <- raw_events_df %>% 
  filter(event_name == "child_sticker_accepted") %>% 
  select(event_id,data) %>% 
  # slice(1:1000) %>%
  mutate(data = map(data,~ .x %>% 
                      fromJSON %>% 
                      .[["data"]] %>% 
                      map_if(is.data.frame, list) %>% 
                      discard(is_empty) %>%
                      as_tibble %>% 
                      select(one_of(vars_filter)) %>% 
                      distinct %>% 
                      mutate_all(funs(ifelse(.=="",NA,.))) %>% 
                      mutate_at(vars(one_of(vars_numeric)), funs(as.numeric))
  )) %>%
  unnest


child_sticker_accepted_df %>% write_csv("tables/child_sticker_accepted.csv")


# Stickeralbum session ------
#_____________________________


session_stickeralbum_enter_df <- session_event_df %>% 
  group_by(session_id) %>% 
  filter(event_name %>% str_detect("stickeralbum")| event_id == last(event_id)) %>% 
  filter(n()>1) %>% 
  mutate(event_name_next = lead(event_name),
         timestamp_next = lead(timestamp),
         event_id_next = lead(event_id)) %>% 
  filter(event_name == "stickeralbum_enter") %>% 
  mutate(duration = timestamp_next -timestamp) %>% 
  mutate(duration = coalesce(duration,0))


session_stickeralbum_enter_df %>% 
  filter(is.na(timestamp_next))

stickeralbum_enter_df <- session_stickeralbum_enter_df %>% 
  group_by(session_id) %>% 
  summarise(n_views_album = n(),
            duration_view_album_sum = sum(duration),
            duration_view_album_mean = mean(duration)
  )

