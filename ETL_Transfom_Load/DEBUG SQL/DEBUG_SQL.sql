# Оистка стейджинга
DELETE FROM STG_EVENTLOGS;

# Запрос для отлова таблиц в старом формате
SELECT
	*
FROM stg_eventlogs
WHERE 1=1
AND	time_created = '-'
AND evtx_file_name != 'Archive-Security-2021-05-11-03-46-57-557'


# Тестируем этот запрос для получения таблицы с нужными полями.
CREATE TABLE DWH_FACT_EVENT_4624 AS
SELECT
	time_created::timestamp at time zone 'Etc/Zulu' as time_created,
	log_source,
	TO_NUMBER(event_id,'9999') as event_id,
	target_user_name,
	CASE
		WHEN target_domain_name = 'NFRN-I'
			THEN 'NFRN-I.INTERNAL'
		ELSE target_domain_name
	END as target_domain_name,
	ip_address,
	ip_port,
	logon_process_name,
	process_name,
	TO_NUMBER(process_id,'99999') as process_id,
	TO_NUMBER(logon_type,'9999') as logon_type,
	CASE
		WHEN strpos(restricted_admin_mode, '-') != 0
			THEN 'Y'
		ELSE 'N'
		END as restricted_admin_mode,
	target_logon_id,
	evtx_file_name
FROM stg_eventlogs
WHERE 1=1
AND event_id = '4624'
AND	target_user_name not like 'PC-%'
AND	target_user_name not like 'ANONYMOUS LOGON'
AND	target_user_name not like 'SYSTEM'
AND	target_user_name not like 'MasterKip%'


















# Более сложные случаи JOIN
# https://www.sisense.com/blog/4-ways-to-join-only-the-first-row-in-sql/
# https://stackoverflow.com/questions/2043259/how-to-join-to-first-row