from multiprocessing.managers import BaseManager
from multiprocessing import Process, current_process, cpu_count, Pool, log_to_stderr
from threading import Event
import logging
import time
import sys
from Evtx_parser import *

# Настраиваемые параметры
address = "127.0.0.1"
port = 50001
password = "secret"
processed_files_path = '\\\\rni-10\\LogArchive\\'
log_file_path = 'C:\\WORK\\ETL_Data_Extractor\\Worker\\LOGS\\'
DEBUG = False


class QueueManager(BaseManager):
    pass


shutdown_event = Event()
logger = log_to_stderr()
logger.setLevel(logging.INFO)

if not DEBUG:
    cur_date = date.today()
    log_file_name = 'Data_extractor_' + cur_date.strftime("%d_%m_%Y_%H_%M_%S") + '.log'
    log_file_full_path = log_file_path + log_file_name
    sys.stderr = open(log_file_full_path, 'w')


def connect_to_manager():
    QueueManager.register('work_tasks_queue')
    QueueManager.register('done_task_queue')
    manager = QueueManager(address=(address, port), authkey=password.encode('utf-8'))
    manager.connect()
    return manager.work_tasks_queue(), manager.done_task_queue()


def worker():
    if shutdown_event.isSet():
        logger.info("Worker process detect shutdown event. Shutdown...")
        sys.exit(0)

    try:
        work_queue, done_queue = connect_to_manager()
    except Exception as msg:
        logger.error("Can't connect to manager! Exception: {}".format(msg))
        sys.exit(1)

    while True:
        if shutdown_event.isSet():
            break

        if work_queue.empty():
            shutdown_event.set()
            break

        task = work_queue.get()

        if task is None:  # signal to terminate
            shutdown_event.set()
            work_queue.task_done()
            break

        # Выполняем полученную задачу
        logger.info("Starting the task processing: {}".format(task))

        task_key = list(task.keys())[0]
        result_file_full_path = task[task_key]["result_file_path"]

        try:
            # Извлекаем данные из журнала
            parse_evtx_file(task_key, result_file_full_path, processed_files_path)

            # Сообщаем менеджеру о выполнении задачи.
            task[task_key]['task_state'] = 'DONE'

        except Exception as exc_msg:
            logger.warning("An exception occurred while processing task {task}: {message}".format(task=task,
                                                                                                  message=exc_msg))
            task[task_key]['task_state'] = 'EXCP'

        # Сообщаем в журнал о завершении обработки.
        logger.info("Finishing the task processing: {}".format(task))

        done_queue.put(task)
        work_queue.task_done()

    # Завершаем работу процесса, сообщаем ОС об отсутсвии ошибок.
    sys.exit(0)


if __name__ == '__main__':

    # Засечем время испольнения скрипта
    start_time = time.time()

    # Кол-во процессов парсинга логов = кол-во ядер CPU. Оставим себе 1 ядро, чтобы не тормозил графический нитерфейс на сервере.
    process_count = cpu_count() - 1
    workers = []

    for core in range(process_count):
        if shutdown_event.isSet():
            logger.warning("Shutdown detected!")
            break

        p = Process(target=worker, name=("Worker " + str(core)))
        workers.append(p)
        p.start()

    logger.info("Waiting completion of worker processes.")

    for worker in workers:
        worker.join()

    # Выведем время выполнения скрипта
    print("Profiling. The time taken to execute the script: {} seconds.".format(time.time() - start_time))

    sys.stderr.close()
