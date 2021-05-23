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
AND	target_user_name not like 'RC-%'
AND	target_user_name not like 'RNI-%'
AND	target_user_name not like 'ANONYMOUS LOGON'
AND	target_user_name not like 'NC-%'
AND	target_user_name not ilike 'kipcdng%'
AND	target_user_name not ilike 'ARMCXBZPPN-3'
AND	target_user_name not ilike 'DezhCPP%'
AND	target_user_name not like 'SYSTEM'
AND	target_user_name not like 'DezhDNS%'
AND	target_user_name not like 'SERVERFORADMINS$'
AND	target_user_name not ilike 'MasterKip%'
AND	target_user_name not ilike 'DezhASUcppn%'
AND	target_user_name not ilike 'ArmCKSB'
AND	target_user_name not ilike 'BrigadaKIPKSUgut'
AND	target_user_name not ilike 'Uch_KSB'
AND	target_user_name not ilike 'kipCSPTG%'
AND	target_user_name not ilike 'KIPDNS%'
AND	target_user_name not ilike 'EngineerKip%'
AND	target_user_name not ilike 'Ingener_cksb'
AND	target_user_name not ilike 'EnginePriobka'
AND	target_user_name not ilike 'EngineerCKSB'
AND	target_user_name not ilike 'TOiOUU'
AND	target_user_name not ilike 'UtoKSB'
AND	target_user_name not ilike 'NachKIPiAKS2'
AND	target_user_name not ilike 'kipupgCSPTG4'
AND	target_user_name not ilike 'DezhCDNG%'
AND	target_user_name not ilike 'UTSMon'
AND	target_user_name not ilike 'NachSmenCDS'
AND	target_user_name not ilike 'kipCPPN%'
AND target_user_name not ilike 'kipCSPTG%'
AND target_user_name not ilike 'PriemnayaPytPU'
AND target_user_name not ilike 'DezhASUupg'
AND target_user_name not ilike 'netmonitor';














# Мусор!!!
CREATE TABLE DWH_FACT_EVENT_4624 AS
SELECT
	TO_NUMBER(event_id,'9999') as event_id,
	log_source,
	time_created::timestamp at time zone 'Etc/Zulu' as time_created,
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
	process_id,
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
AND	target_user_name not like 'RC-%'
AND	target_user_name not like 'RNI-%'
AND	target_user_name not like 'ANONYMOUS LOGON'
AND	target_user_name not like 'NC-%'
AND	target_user_name not ilike 'kipcdng%'
AND	target_user_name not ilike 'ARMCXBZPPN-3'
AND	target_user_name not ilike 'DezhCPP%'
AND	target_user_name not like 'SYSTEM'
AND	target_user_name not like 'DezhDNS%'
AND	target_user_name not like 'SERVERFORADMINS$'
AND	target_user_name not ilike 'MasterKip%'
AND	target_user_name not ilike 'DezhASUcppn%'
AND	target_user_name not ilike 'ArmCKSB'
AND	target_user_name not ilike 'BrigadaKIPKSUgut'
AND	target_user_name not ilike 'Uch_KSB'
AND	target_user_name not ilike 'kipCSPTG%'
AND	target_user_name not ilike 'KIPDNS%'
AND	target_user_name not ilike 'EngineerKip%'
AND	target_user_name not ilike 'Ingener_cksb'
AND	target_user_name not ilike 'EnginePriobka'
AND	target_user_name not ilike 'EngineerCKSB'
AND	target_user_name not ilike 'TOiOUU'
AND	target_user_name not ilike 'UtoKSB'
AND	target_user_name not ilike 'NachKIPiAKS2'
AND	target_user_name not ilike 'kipupgCSPTG4'
AND	target_user_name not ilike 'DezhCDNG%'
AND	target_user_name not ilike 'UTSMon'
AND	target_user_name not ilike 'NachSmenCDS'
AND	target_user_name not ilike 'kipCPPN%'
AND target_user_name not ilike 'kipCSPTG%'
AND target_user_name not ilike 'PriemnayaPytPU'
AND target_user_name not ilike 'DezhASUupg'
AND target_user_name not ilike 'netmonitor';


CREATE TABLE DWH_FACT_EVENT_4634 AS
SELECT
	src_server,
	TO_NUMBER(event_id,'9999') as event_id,
	TO_NUMBER(logon_type,'9999') as logon_type,
	CASE
		WHEN target_domain_name = 'NFRN-I'
			THEN 'NFRN-I.INTERNAL'
		ELSE target_domain_name
	END as target_domain_name,
	target_logon_id,
	target_user_name,
	time_created::timestamp at time zone 'Etc/Zulu' as time_created
FROM stg_eventlog
WHERE 1=1
AND event_id = '4634'
AND	target_user_name not like 'RC-%'
AND	target_user_name not like 'RNI-%'
AND	target_user_name not like 'ANONYMOUS LOGON'
AND	target_user_name not like 'NC-%'
AND	target_user_name not ilike 'kipcdng%'
AND	target_user_name not ilike 'ARMCXBZPPN-3'
AND	target_user_name not ilike 'DezhCPP%'
AND	target_user_name not like 'SYSTEM'
AND	target_user_name not like 'DezhDNS%'
AND	target_user_name not like 'SERVERFORADMINS$'
AND	target_user_name not ilike 'MasterKip%'
AND	target_user_name not ilike 'DezhASUcppn%'
AND	target_user_name not ilike 'ArmCKSB'
AND	target_user_name not ilike 'BrigadaKIPKSUgut'
AND	target_user_name not ilike 'Uch_KSB'
AND	target_user_name not ilike 'kipCSPTG%'
AND	target_user_name not ilike 'KIPDNS%'
AND	target_user_name not ilike 'EngineerKip%'
AND	target_user_name not ilike 'Ingener_cksb'
AND	target_user_name not ilike 'EnginePriobka'
AND	target_user_name not ilike 'EngineerCKSB'
AND	target_user_name not ilike 'TOiOUU'
AND	target_user_name not ilike 'UtoKSB'
AND	target_user_name not ilike 'NachKIPiAKS2'
AND	target_user_name not ilike 'kipupgCSPTG4'
AND	target_user_name not ilike 'DezhCDNG%'
AND	target_user_name not ilike 'UTSMon'
AND	target_user_name not ilike 'NachSmenCDS'
AND	target_user_name not ilike 'kipCPPN%'
AND target_user_name not ilike 'kipCSPTG%'
AND target_user_name not ilike 'PriemnayaPytPU'
AND target_user_name not ilike 'DezhASUupg'
AND target_user_name not ilike 'netmonitor';

SELECT
	t1.*,
	t2.*
FROM test_event_4624 t1
LEFT JOIN test_event_4634 t2
ON t1.target_logon_id = t2.target_logon_id
WHERE
	t1.target_user_name = 'BykasovNA';


SELECT *
FROM   test_event_4624
WHERE
	time_created BETWEEN '2021-04-29 20:27:00'
                 AND '2021-04-29 20:28:00';


SELECT *
FROM   test_event_4624
WHERE 1=1
AND (time_created BETWEEN '2021-04-29 20:27:00' AND '2021-04-29 20:28:00')
AND target_user_name = 'KisliySV';

# Сколько времени в период 2021-05-01 21:30 по 2021-05-01 21:35:00 кто-то пользовался каким-либо рессурсом.
SELECT
	*
FROM (
	SELECT
		t1.time_created as start_timestamp,
		t2.time_created as end_timestamp,
		t2.time_created - t1.time_created as difference,
		t1.log_source as log_source,
		t1.event_id as login_event_id,
		t2.event_id as logout_event_id,
		t1.target_user_name as user_name,
		t1.target_domain_name as domain_name,
		t1.ip_address as ip_address,
		t1.ip_port as ip_port,
		t1.logon_process_name as logon_process_name,
		t1.process_name	as local_full_process_path,
		t1.process_id as process_id,
		t1.logon_type as logon_type,
		t1.restricted_admin_mode as restricted_admin_mode,
		t1.target_logon_id as target_logon_id,
		t1.evtx_file_name as login_evtx_file_name,
		t2.evtx_file_name as logout_evtx_file_name
	FROM dwh_fact_event_4624 t1
	INNER JOIN dwh_fact_event_4634 t2
	ON t1.target_logon_id = t2.target_logon_id
	AND t1.target_user_name = t2.target_user_name
	WHERE 1=1
	AND (t1.time_created BETWEEN '2021-05-01 21:30:00' AND '2021-05-01 21:35:00')) n1
WHERE 1=1
AND n1.difference > INTERVAL '1 second';

# Более сложные случаи JOIN
# https://www.sisense.com/blog/4-ways-to-join-only-the-first-row-in-sql/
# https://stackoverflow.com/questions/2043259/how-to-join-to-first-row