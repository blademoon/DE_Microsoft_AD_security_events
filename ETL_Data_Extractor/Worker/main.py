from multiprocessing.managers import BaseManager
from multiprocessing import Process, current_process, cpu_count, Pool, log_to_stderr
from threading import Event
import logging
import os
import time
import sys
from Evtx_parser import *

address = "127.0.0.1"
port = 50001
password = "secret"


class QueueManager(BaseManager):
    pass


shutdown_event = Event()
logger = log_to_stderr()
logger.setLevel(logging.INFO)


def connect_to_manager():
    QueueManager.register('work_tasks_queue')
    QueueManager.register('done_task_queue')
    manager = QueueManager(address=(address, port), authkey=password.encode('utf-8'))
    manager.connect()
    return manager.work_tasks_queue(), manager.done_task_queue()


def worker():
    if shutdown_event.isSet():
        logger.info("SHUTDOWN EVENT DETECTED!")
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
        parse_evtx_file(task_key, result_file_full_path)

        # Сообщаем в журнал о завершении обработки.
        logger.info("Finishing the task processing: {}".format(task))

        # Отмечаем выполнение задачи.
        key = list(task.keys())[0]
        task[key]['task_state'] = 'DONE'

        done_queue.put(task)
        work_queue.task_done()
        # logging.info("End of process code reached")
        # sys.exit(0)


if __name__ == '__main__':
    # logger = log_to_stderr()
    # logger.setLevel(logging.INFO)
    #
    # process_count = cpu_count()
    #
    # p = Process(target=worker, name="Worker")
    # p.start()
    # p.join()

    process_count = cpu_count()
    workers = []

    for core in range(process_count):
        if shutdown_event.isSet():
            logger.warning("Shutdown detected!")
            break

        p = Process(target=worker, name=("Worker " + str(core)))
        workers.append(p)
        p.start()
    print("Waiting shutdows event...")
    shutdown_event.wait()
    print("Work process terminated due to receiving a shutdown event")
