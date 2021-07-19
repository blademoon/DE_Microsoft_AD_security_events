# Загрузка информации из стейджинга в DWH
# Отлажена, работает

-- Загружаем из стейджинга в хранилище записи с ID 4624 (Прохождение аутентификации)
INSERT INTO DWH_FACT_EVENT_4624
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
AND	target_user_name not like 'SYSTEM';
-- В последней части исключаем различный мусор (авторизации пользователя SYSTEM и служебные УЗ)

-- Загружаем из стейджинга в хранилище записи с ID 4634 (завершение сессии пользователя)
INSERT INTO DWH_FACT_EVENT_4634
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
	TO_NUMBER(logon_type,'9999') as logon_type,
	target_logon_id,
	evtx_file_name
FROM stg_eventlogs
WHERE 1=1
AND event_id = '4634'
AND	target_user_name not like 'PC-%'
AND	target_user_name not like 'ANONYMOUS LOGON'
AND	target_user_name not like 'SYSTEM';
-- В последней части исключаем различный мусор (авторизации пользователя SYSTEM и служебные УЗ)