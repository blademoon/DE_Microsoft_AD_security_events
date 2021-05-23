# https://tableplus.com/blog/2018/07/postgresql-how-to-extract-date-from-timestamp.html
# https://www.opennet.ru/tips/2639_postgresql_timezone_zoneinfo.shtml

import psycopg2

# Функция подключения к хранилищу данных
def dwh_connect(username, password, dwh_host, dwh_port, database):
    conn = psycopg2.connect(user=username,
                            password=password,
                            host=dwh_host,
                            port=dwh_port, database=database)
    return conn

# Функция закрытия подключения к хранилищю
def dwh_close(cursor, connection):
    cursor.close()
    connection.close()