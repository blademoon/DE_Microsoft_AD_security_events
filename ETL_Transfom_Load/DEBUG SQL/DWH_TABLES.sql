-- Создаём базу данных
CREATE DATABASE win_evtx_logs
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

-- Создаём стейджинг
CREATE TABLE IF NOT EXISTS STG_EVENTLOGS (
	event_id	text,
	workstation_name	text,
	ip_address	text,
	ip_port	text,
	log_source	text,
	logon_process_name	text,
	logon_type	text,
	process_id	text,
	process_name	text,
	restricted_admin_mode	text,
	target_domain_name	text,
	target_logon_id	text,
	target_user_sid	text,
	target_user_name	text,
	time_created	text,
	evtx_file_name	text
);

-- Создаём таблицу хранилища для фактов прохождения аутентификации (ID 4624)
CREATE TABLE IF NOT EXISTS DWH_FACT_EVENT_4624 (
time_created    timestamp with time zone,
log_source	text,
event_id	numeric,
target_user_name	text,
target_user_sid text,
target_domain_name	text,
workstation_name    text,
ip_address	text,
ip_port	text,
logon_process_name	text,
process_name	text,
process_id	numeric,
logon_type	numeric,
restricted_admin_mode	text,
target_logon_id text,
evtx_file_name  text
);

-- Создаём таблицу хранилища для фактов завершения сессии (ID 4634)
CREATE TABLE IF NOT EXISTS DWH_FACT_EVENT_4634 (
time_created    timestamp with time zone,
log_source	text,
event_id	numeric,
target_user_name	text,
target_domain_name	text,
logon_type	numeric,
target_logon_id text,
evtx_file_name  text
);









