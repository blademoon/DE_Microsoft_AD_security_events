# https://naysan.ca/2020/05/31/postgresql-to-pandas/
import logging
import pandas as pd
from datetime import date
import time

from dwh_sql import *

# Настраиваемые параметры
report_start_timestamp = '2021-05-01 00:00:00'
report_end_timestamp = '2021-05-03 00:00:00'
username = 'BoykoAN'
filtered_min_conn_interval = 2

db_usrn = "postgres"
db_usrn_pass = "pa$$word"
db_host = "192.168.1.100"
db_port = "5432"
db_name = "win_evtx_logs"
DEBUG = True
script_name = "DWH_REPORT_"

today = date.today()
# 0.1 OPEN LOG FILE. Откроем журнал.

# Засечем время начала исполнения скрипта
script_start_time = time.time()

try:
    logging.basicConfig(filename=(script_name + today.strftime("%d_%m_%Y") + '.log'),
                        filemode='a',
                        level=logging.DEBUG,
                        encoding='utf-8',
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%d-%m-%Y %H:%M:%S')
    logging.info("0.1 OPEN LOG FILE: Successfully completed.")
except Exception as exc:
    print("0.1 OPEN LOG FILE: Exception occured: {}".format(exc))
    exit(1)

# 0.2 CONNECT DWH. Подключимся к хранилищу и получим курсор
try:
    dwh_connection = dwh_connect(db_usrn, db_usrn_pass, db_host, db_port, db_name)
    dwh_cursor = dwh_connection.cursor()
    logging.info("0.2 CONNECT DWH: Successfully completed.")
except Exception as exc:
    if DEBUG:
        print("0.2 CONNECT DWH: Exception occured: {} \n".format(exc))
    if not DEBUG:
        logging.critical("0.2 CONNECT DWH: Exception occured: {} \n".format(exc))
        exit(1)

# 1. GENERATING REPORT. Генерируем отчёт.
sql_req = """
SELECT
	start_time,
	end_time,
	start_name as username,
	dest_ip,
	duration,
	start_target_logon_id as session_target_logon_id
FROM
	(SELECT 
		t1.time_created as start_time,
		t1.target_user_name as start_name,
	 	t1.target_logon_id as start_target_logon_id,
	 	t1.ip_address as dest_ip,
	 	(t2.time_created - t1.time_created) as duration,
	 	t2.time_created as end_time,
		t2.target_user_name as end_name,
		t2.target_logon_id as end_target_logon_id,
	 	ROW_NUMBER() OVER(PARTITION BY t1.time_created, t1.target_user_name ORDER BY t2.time_created) AS rang
	FROM dwh_fact_event_4624 t1
	INNER JOIN dwh_fact_event_4634 t2
	ON t1.target_logon_id = t2.target_logon_id
    AND t1.log_source = t2.log_source
	AND t1.time_created < t2.time_created) sub_req
WHERE 1=1
AND rang = 1
AND start_time BETWEEN '{start_date}' and '{end_date}'
AND end_time BETWEEN '{start_date}' and '{end_date}'
AND start_name = '{ad_username}'
AND duration > INTERVAL '{minutes} minute';
""".format(start_date=report_start_timestamp,
           end_date=report_end_timestamp,
           ad_username=username,
           minutes=filtered_min_conn_interval)

# Засечем время исполнения скрипта
sql_start_time = time.time()

try:
    dwh_cursor.execute(sql_req)
    tupples = dwh_cursor.fetchall()
    column_names = [desc[0] for desc in dwh_cursor.description]
    df = pd.DataFrame(tupples, columns=column_names)
    df = df.astype(str)
    logging.info("1. GENERATING REPORT: Successfully completed.")
except Exception as exc:
    if DEBUG:
        print("1. GENERATING REPORT: {} Exception occured: {} \n".format(exc))
    if not DEBUG:
        logging.critical("1. GENERATING REPORT: Exception occured: {} \n".format(exc))
        exit(1)

# 2. SAVING REPORT TO FILE. Сохраняем отчёт в файл.
try:
    df.to_excel("REPORT.xlsx", index=False)
    logging.info("2. SAVING REPORT TO FILE: Successfully completed.")
except Exception as exc:
    if DEBUG:
        print("2. SAVING REPORT TO FILE: {} Exception occured: {} \n".format(exc))
    if not DEBUG:
        logging.critical("2. SAVING REPORT TO FILE: Exception occured: {} \n".format(exc))
        exit(1)

# 3. CLOSING DWH CONNECTION. Закрываем соединение с хранилищем данных.
try:
    dwh_close(dwh_cursor, dwh_connection)
    logging.info("3. CLOSING DWH CONNECTION: Successfully completed.")
except Exception as exc:
    if DEBUG:
        print("3. CLOSING DWH CONNECTION: {} Exception occured: {} \n".format(exc))
    if not DEBUG:
        logging.critical("3. CLOSING DWH CONNECTION: Exception occured: {} \n".format(exc))
        exit(1)

# Выведем время выполнения скрипта
if DEBUG:
    print("4. PROFILING: Total query runtime: {} seconds.".format(time.time() - sql_start_time))
if not DEBUG:
    logging.info("4. PROFILING: Total query runtime: {} seconds.".format(time.time() - sql_start_time))

# Выведем время выполнения скрипта
if DEBUG:
    print("4. PROFILING: Script execution time: {} seconds.".format(time.time() - script_start_time))
if not DEBUG:
    logging.info("4. PROFILING: Script execution time: {} seconds.".format(time.time() - script_start_time))

exit(0)
