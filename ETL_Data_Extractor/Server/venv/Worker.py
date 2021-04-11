from multiprocessing import Process, Manager, current_process
from multiprocessing.managers import BaseManager
from queue import Queue
from threading import Thread
from multiprocessing import Event
import time
import sys

address = "127.0.0.1"
port = 50000
password = "secret"

class QueueManager(BaseManager):
    pass

def worker(in_q, out_q):
    # Проходим аутентификацию для получения доступа к очередям менеджера
    current_process().authkey = password.encode('utf-8')

    # Выполняем задачи поставленные в очередь менеджера до тех пор,
    # Пока не будет получен сигнал о завершении работы
    while True:
        #
        x = in_q.get()

        #
        if x is None: # signal to end up
            in_q.task_done()
            break

        # Здесь вставить код для обработки чего-либо
        # сформировать сообщение о результате (хорошем или плохом) выполнения задачи

        # Отправить сообщение о выполнении задачи менеджеру.
        out_q.put((x, x ** 2))

        # Сообщить менеджеру о выполнении задачи.
        in_q.task_done()

def connect_to_manager():
    QueueManager.register('work_tasks_queue')
    QueueManager.register('done_task_queue')
    manager = QueueManager(address=(address, port), authkey=password.encode('utf-8'))
    manager.connect()
    return manager.work_tasks_queue(), manager.done_task_queue()

def create_workers(in_q, out_q, n_workers):
    processes = [Process(target=worker, args=(in_q, out_q)) for _ in range(n_workers)]
    for process in processes:
        process.start()
    for process in processes:
        process.join()

def client(in_q, out_q):
    for x in range(1, 10):
        in_q.put(x)
    # get results as they become available:
    for x in range(1, 10):
        x, result = out_q.get()
        print(x, result)

if __name__ == '__main__':
    shutdown_event = Event()

    in_q, out_q = connect_to_manager()
    N_WORKERS = 3
    t = Thread(target=create_workers, args=(in_q, out_q, N_WORKERS,))
    t.start()
    client(in_q, out_q)

    # tell workers we are through:
    for _ in range(N_WORKERS):
        in_q.put(None)
    # in_q.join() # не обязательно! предполагает, что работа клиента тоже завершена
    t.join()
    # скажите менеджеру, что мы прошли:
    shutdown_event.set()
    p.join()