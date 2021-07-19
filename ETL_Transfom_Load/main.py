import logging
import pandas as pd
#from glob import glob
from datetime import date
import time
from file_operations import *
from dwh_sql import *

# Настраиваемые параметры
csv_files_path = "D:\\WORKSPACE\\DATA\\*.csv"
csv_achive_folder = "D:\\WORKSPACE\\ETL_Transfom_Load\\ARCHIVE\\"
db_usrn = "postgres"
db_usrn_pass = "pa$$word"
db_host = "192.168.1.100"
db_port = "5432"
db_name = "win_evtx_logs"
DEBUG = True
script_name = "ETL_Transform_Load_"

# Засечем время испольнения скрипта
script_start_time = time.time()

# 0.1 OPEN LOG FILE. Откроем журнал.
try:
    # Получим сегоднящнюю дату
    today = date.today()

    logging.basicConfig(filename=(script_name + today.strftime("%d_%m_%Y") + '.log'),
                        filemode='a',
                        format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.DEBUG,
                        encoding='utf-8',
                        datefmt='%d-%m-%Y %H:%M:%S')

    logging.info("0.1 OPEN LOG FILE: Successfully completed.")
except Exception as exc:
    print("0.1 OPEN LOG FILE: Exception occured: {}".format(exc))

    exit(1)

# 0.2 CONNECT DWH. Подключимся к хранилищу и получим курсор
try:
    dwh_connection = dwh_connect(db_usrn, db_usrn_pass, db_host, db_port, db_name)

    # Выключим autocommit
    dwh_connection.autocommit = False

    # Получим SQL курсор
    dwh_cursor = dwh_connection.cursor()

    logging.info("0.2 CONNECT DWH: Successfully completed.")
except Exception as exc:
    logging.critical("0.2 CONNECT DWH: Exception occured: {} \n".format(exc))
    exit(1)

# 0.3 FORMATING OF A LIST OF DATA FILES. Получим список всех имеющихся файлов csv в правильном хронологическом порядке.
try:
    recursive_flg = True
    reverse_sort_flg = False
    csv_files_ordered_list = order_files_by_date(csv_files_path, recursive_flg, reverse_sort_flg)
    logging.info("0.3 FORMATION ORDERED LIST OF DATA FILES: Successfully completed.")
except Exception as exc:
    logging.critical("0.3 FORMATION ORDERED LIST OF DATA FILES: Exception occured: {} \n".format(exc))
    dwh_close(dwh_cursor, dwh_connection)
    exit(1)

    # Получим кол-во обрабатываемых файлов.
    files_num = len(csv_files_ordered_list)

for csv_file in csv_files_ordered_list:
    # 1. LOADING DATA FROM A FILE. Считаем текущий файл
    try:
        df = pd.read_csv(csv_file, sep=',', header=0)
        logging.info("1. FILE -> DATA: {}: Successfully completed.".format(csv_file))
    except Exception as exc:
        logging.critical("1. FILE -> DATA: {file}: Exception occured: {ex}".format(file=csv_file,
                                                                                   ex=exc))
        dwh_close(dwh_cursor, dwh_connection)
        exit(1)

    # Конвертируем все серии датафрейма в строковый тип
    df = df.astype(str)

    # 2. Загружаем данные в стейджинг
    sql_req = """
INSERT INTO stg_eventlogs (
    event_id,
    workstation_name,
    ip_address,
    ip_port,
    log_source,
    logon_process_name,
    logon_type,
    process_id,
    process_name,
    restricted_admin_mode,
    target_domain_name,
    target_logon_id,
    target_user_sid,
    target_user_name,
    time_created,
    evtx_file_name
)
        VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

    try:
        dwh_cursor.executemany(sql_req, df.values.tolist())
        #dwh_connection.commit()
        logging.info("2. DATA -> STG: {} Successfully completed.".format(csv_file))

    except Exception as exc:
        logging.critical("2. DATA -> STG: {filename}: Exception happend: {ex}"
                         "{ex}".format(filename=csv_file, ex=exc))
        dwh_close(dwh_cursor, dwh_connection)
        exit(1)

    # 3. Извлекаем данные из стейджинга, фильтруем, трансформируем в нужный тип, загружаем в фактовые таблицы
    # хранилища данных.

    # 3.1 Заполняем таблицу фактов DWH_FACT_EVENT_4624
    try:
        sql_req = """
INSERT INTO DWH_FACT_EVENT_4624
SELECT
	time_created::timestamp at time zone 'Etc/Zulu' AS time_created,
	log_source,
	TO_NUMBER(event_id,'9999') AS event_id,
	target_user_name,
	target_user_sid,
	CASE
		WHEN target_domain_name = 'NFRN-I'
		THEN 'NFRN-I.INTERNAL'
		WHEN target_domain_name = 'UNG'
		THEN 'YUNGJSC.COM'
		ELSE target_domain_name
    END AS target_domain_name,
	CASE
		WHEN workstation_name = '-'
		THEN NULL
		ELSE workstation_name
	END AS workstation_name,
	ip_address,
	ip_port,
	logon_process_name,
	process_name,
	TO_NUMBER(process_id,'99999') AS process_id,
	TO_NUMBER(logon_type,'9999') AS logon_type,
    CASE
		WHEN strpos(restricted_admin_mode, '-') != 0
        THEN 'Y'
        ELSE 'N'
    END AS restricted_admin_mode,
    target_logon_id,
    evtx_file_name
FROM stg_eventlogs
WHERE 1=1
AND event_id = '4624'
AND	target_user_name NOT LIKE 'PC-%'
AND	target_user_name NOT LIKE 'ANONYMOUS LOGON'
AND	target_user_name NOT LIKE 'SYSTEM';
"""
        dwh_cursor.execute(sql_req)
        logging.info("3.1 STG -> DWH: DWH_FACT_EVENT_4624 {} Successfully completed.".format(csv_file))
    except Exception as exc:
        logging.critical("3.1 STG -> DWH: DWH_FACT_EVENT_4624 {filename}. Exception happend:"
                         " {ex}".format(filename=csv_file, ex=exc))
        dwh_close(dwh_cursor, dwh_connection)
        exit(1)

    # 3.2 Заполняем таблицу фактов DWH_FACT_EVENT_4634
    try:
        sql_req = """
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
AND	target_user_name NOT LIKE 'PC-%'
AND	target_user_name NOT LIKE 'ANONYMOUS LOGON'
AND	target_user_name NOT LIKE 'SYSTEM';
"""
        dwh_cursor.execute(sql_req)
        logging.info("3.2 STG -> DWH: DWH_FACT_EVENT_4634 {} Data from staging was successfully transferred to the data "
                     "warehouse.".format(csv_file))
    except Exception as exc:
        logging.critical("3.2 STG -> DWH: DWH_FACT_EVENT_4634 {filename}. An error occurred when filling in the "
                         "data store. Exception happend: {ex}".format(filename=csv_file, ex=exc))
        dwh_close(dwh_cursor, dwh_connection)
        exit(1)

    # 4. Очищаем стейджинг
    try:
        sql_req = """DELETE FROM stg_eventlogs"""
        dwh_cursor.execute(sql_req)
        logging.info("4. STG CLEAR: {} Successfully completed.".format(csv_file))
    except Exception as exc:
        logging.critical("4. STG CLEAR: {filename} Successfully completed. Exception happend: {ex}"
                         "{ex}".format(filename=csv_file, ex=exc))
        dwh_close(dwh_cursor, dwh_connection)
        exit(1)

    # 5. Завершаем загрузку данных в хранилище, фиксируем изменения.
    try:
        dwh_connection.commit()
        logging.info("5. DATA FIXATION: {} Successfully completed.".format(csv_file))
    except Exception as exc:
        logging.critical("5. DATA FIXATION: {filename}: Exception happend: {ex}".format(filename=csv_file, ex=exc))
        dwh_close(dwh_cursor, dwh_connection)
        exit(1)

    # 6. Архивируем обработанный файл и перемещаем в архивную папку.
    try:
        zip_processed_file(csv_file, csv_achive_folder)
        logging.info("6. ARCHIVING PROCESSED FILE: {} Successfully completed.".format(csv_file))
    except Exception as exc:
        logging.critical("6. ARCHIVING PROCESSED FILE: {filename}: Exception happend: {ex}".format(filename=csv_file, ex=exc))
        dwh_close(dwh_cursor, dwh_connection)
        exit(1)

# Выведем время выполнения скрипта
if DEBUG:
    print("4. PROFILING: Script execution time: {} seconds.".format(time.time() - script_start_time))
    print("4. PROFILING: Total files processed: {}".format(files_num))
if not DEBUG:
    logging.info("4. PROFILING: Script execution time: {} seconds.".format(time.time() - script_start_time))
    logging.info("4. PROFILING: Total files processed: {}".format(files_num))

# Завершаем работу с хранилищем.
dwh_close(dwh_cursor, dwh_connection)
