# Загрузка информации из стейджинга в DWH
# Отлажена, работает

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