#!/bin/bash

# Обновим операционную систему
apt-get update && apt-get -y upgrade

# Установим необходимые пакеты
apt-get install python3-dev python3-pip htop iotop mc tmux postgresql net-tools postgresql-client aptitude

# Установим необходимые библиотеки
#pip3 --proxy=127.0.0.1:3128 install pandas python-evtx jaydebeapi multiprocess
pip3 install pandas python-evtx jaydebeapi multiprocess

# Сконфигурируем СУБД PostgreSql для работы в качестве Datawearhouse
# Необходимую начальную конфигурацию можно сгенерировать с помощью калькулятора
# https://pgtune.leopard.in.ua/#/
sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" /etc/postgresql/12/main/postgresql.conf
sed -i 's/max_connections = 100	/max_connections = 30/' /etc/postgresql/12/main/postgresql.conf
sed -i 's/shared_buffers = 128MB/shared_buffers = 1GB/' /etc/postgresql/12/main/postgresql.conf
sed -i 's/#effective_cache_size = 4GB/effective_cache_size = 3GB/' /etc/postgresql/12/main/postgresql.conf
sed -i 's/#maintenance_work_mem = 64MB/maintenance_work_mem = 512MB/' /etc/postgresql/12/main/postgresql.conf
sed -i 's/#checkpoint_completion_target = 0.5/checkpoint_completion_target = 0.9/' /etc/postgresql/12/main/postgresql.conf
sed -i 's/#wal_buffers = -1/wal_buffers = 16MB/' /etc/postgresql/12/main/postgresql.conf
sed -i 's/#default_statistics_target = 100/default_statistics_target = 500/' /etc/postgresql/12/main/postgresql.conf
sed -i 's/#random_page_cost = 4.0/random_page_cost = 4.0/' /etc/postgresql/12/main/postgresql.conf
sed -i 's/#effective_io_concurrency = 1/effective_io_concurrency = 2/' /etc/postgresql/12/main/postgresql.conf
sed -i 's/#work_mem = 4MB/work_mem = 8738kB/' /etc/postgresql/12/main/postgresql.conf
sed -i 's/min_wal_size = 80MB/min_wal_size = 4GB/' /etc/postgresql/12/main/postgresql.conf
sed -i 's/max_wal_size = 1GB/max_wal_size = 16GB/' /etc/postgresql/12/main/postgresql.conf
sed -i 's/#max_worker_processes = 8/max_worker_processes = 4/' /etc/postgresql/12/main/postgresql.conf
sed -i 's/#max_parallel_workers_per_gather = 2/max_parallel_workers_per_gather = 2/' /etc/postgresql/12/main/postgresql.conf
sed -i 's/#max_parallel_workers = 8/max_parallel_workers = 4/' /etc/postgresql/12/main/postgresql.conf
sed -i 's/#max_parallel_maintenance_workers = 2/max_parallel_maintenance_workers = 2/' /etc/postgresql/12/main/postgresql.conf

# Зададим пароль для пользователя postgresq
read -p 'Lets set a password for POSTGRES user: ' pass
command="ALTER USER postgres with encrypted password '$pass';"
echo "Formed to execute the command: $command"
#sudo -u postgres psql template1 -c "$command"
postgres psql template1 -c "$command"


# Перезапустим сервер СУБД для вступления изменений в силу
sudo systemctl restart postgresql.service

# Установим веб интерфейс консоли администрирования
#curl -x http://127.0.0.1:3128 -L https://www.pgadmin.org/static/packages_pgadmin_org.pub | apt-key add
curl -L https://www.pgadmin.org/static/packages_pgadmin_org.pub | apt-key add
sh -c 'echo "deb https://ftp.postgresql.org/pub/pgadmin/pgadmin4/apt/$(lsb_release -cs) pgadmin4 main" > /etc/apt/sources.list.d/pgadmin4.list && apt update'
apt install pgadmin4-web
export PGADMIN_PLATFORM_TYPE="debian"
sh /usr/pgadmin4/bin/setup-web.sh

# Конфигурационный файл pgadmin 4 находится по пути /usr/pgadmin4/web/config.py
# Для того, чтобы он стал доступен по сети необходимо установить параметр DEFAULT_SERVER = '0.0.0.0'

# Разрешим доступ к серверу из сети
sed -i 's/host    all             all             127.0.0.1/32            md5/32            md5/host    all             all             0.0.0.0/0            md5/' /etc/postgresql/12/main/pg_hba.conf


# Перезапустим сервер для вступления изменений в силу
reboot