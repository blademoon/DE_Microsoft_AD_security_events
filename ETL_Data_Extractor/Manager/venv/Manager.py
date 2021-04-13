from multiprocessing.managers import BaseManager
from threading import Thread, Event
from queue import Queue
from threading import Thread


address = "127.0.0.1"
port = 50001
password = "secret"


class QueueManager(BaseManager):
    pass


def connect_to_manager():
    QueueManager.register('work_tasks_queue')
    QueueManager.register('done_task_queue')
    manager = QueueManager(address=(address, port), authkey=password.encode('utf-8'))
    manager.connect()
    return manager.work_tasks_queue(), manager.done_task_queue()


# So that queues are not unnecessarily created by worker processes under Windows:
work_tasks_queue = None
done_task_queue = None


def get_work_tasks_queue():
    global work_tasks_queue
    # singleton:
    if work_tasks_queue is None:
        work_tasks_queue = Queue()
    return work_tasks_queue


def get_done_task_queue():
    global done_task_queue
    # singleton:
    if done_task_queue is None:
        done_task_queue = Queue()
    return done_task_queue


def server(started_event, shutdown_event):
    # Don't seem to be able to use a lambda or nested function when using net_manager.start():
    QueueManager.register('work_tasks_queue', callable=get_work_tasks_queue)
    QueueManager.register('done_task_queue', callable=get_done_task_queue)

    net_manager = QueueManager(address=(address, port), authkey=password.encode('utf-8'))

    net_manager.start()

    # Сообщаем основному потоку, что менеджер очередей стартовал.
    started_event.set()

    # Ожидаем появления собыитя завершения работы
    shutdown_event.wait()  # wait to be told to shutdown
    net_manager.shutdown()