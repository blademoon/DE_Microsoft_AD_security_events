

from multiprocessing import Process, Manager, current_process
from multiprocessing.managers import BaseManager
from queue import Queue
from threading import Thread
from multiprocessing import Event
import time
import sys
import glob

# Настраиваемые параметры
address = "127.0.0.1"
port = 50000
password = "secret"
path_to_files = "..\\DATA\\"
files_mask = "*.evtx"
path_to_results = "..\\RESULTS\\"


class QueueManager(BaseManager):
    pass

# Фукнция предназначенная для запуска менеджера очередей.
def run_server(s):
    s.serve_forever()

# Функция - процесс менеджера очередей.
def server(shutdown_event):
    work_tasks_queue = Queue()
    done_task_queue = Queue()

    QueueManager.register('work_tasks_queue', callable=lambda: work_tasks_queue)
    QueueManager.register('done_task_queue', callable=lambda: done_task_queue)

    net_manager = QueueManager(address=(address, port), authkey=password.encode('utf-8'))

    s = net_manager.get_server()
    Thread(target=run_server, args=(s,), daemon=True).start()
    # Работаем до тех пор, пока не возникнет событие shutdown_event()
    shutdown_event.wait()

# Функция для подключения к менеджеру очередей.
def connect_to_manager():
    QueueManager.register('work_tasks_queue')
    QueueManager.register('done_task_queue')
    manager = QueueManager(address=(address, port), authkey=password.encode('utf-8'))
    manager.connect()
    return manager.work_tasks_queue(), manager.done_task_queue()

# Функция для формирования основной очереди и заполнения ее задачами
def fill_main_queue_tasks(path_to_files):
    # Сформируем основную очередь в которой будем хранить и отслеживать состояние задач
    main_queue = []

    # Получим пути ко всем обрабатываемым файлам
    processed_files = glob.glob(path_to_files+files_mask)

    for file in processed_files:
        # Создадим задачу в необходимом нам формате
        task = {"processed_file_path": file, "result_file_path": path_to_results, "task_state": ""}

        # Поместим задачу в основную очередь
        main_queue.append(task)

    return (main_queue)

# Функция выбирающая новые задачи из основной очереди менеджера задач
# и выполняющая постановку новых задач в работу.
def fill_tasks_queue(manager_main_queue,manager_work_queue):
    for task in manager_main_queue:
        # Если задача не отправленна на обработку, то
        if not task["task_state"]:
            # Поставим задачу на выполнение в рабочую очередь
            manager_work_queue.put(task)

            # Отметим что задача поставлена на выполнение в основоной очереди
            task["task_state"] = "IN_QE"
# Функция отмечающая выполненные задачи в основной очереди.
def mark_completed_tasks(manager_main_queue,manager_done_task_queue):
    for task in manager_done_task_queue:




# Основная функция программы.
if __name__ == '__main__':
    # Данный процесс одновременно является и менеджером очередей и управляющим ими.
    # В основном процессе мы будем управлять очередью (отслеживать выполнение задания и добавлять новые задания)
    # В процессе Server() будет работать сам менеджер очередей.

    shutdown_event = Event()

    # Запускаем сервер управления как отдельный процесс. Это позвоилт нам реализовать логику управления очредями
    # в рамках одного скрипта.
    p = Process(target=server, args=(shutdown_event,))
    p.start()

    # Ожидаем старта менеджера очередей.
    time.sleep(1)

    # Получим очереди для дальнейшей работы с ними.
    work_task_queue, done_task_queue = connect_to_manager()

    # Заполним основную очередь задачами
    main_queue = fill_main_queue_tasks(path_to_files+files_mask)

    # Пока не все задачи прошли обработку, выполняем выборку задач из основной очереди и их постановку
    # в очередь на выполнение. Так же осуществляем выборку и контроль выполненных заданий из очереди с уже завершенными заданиями.
    # В случае поступления события завершения работы, выполняем выход из цикла.
    while True:

        # Если основной список задач не нулевой,
        if main_queue.count() != 0:
            # то помещаем задачи в очередь для рабочих процессов.
            fill_tasks_queue(main_queue,work_task_queue)
            # Обновляем состояние задач в основном списке задач.
            mark_completed_tasks(main_queue,done_task_queue)

        # Если основная очередь задач пуста (не удалось получить файлы),то завершаем процессы
        else:
            # Сообщаем рабочим процессам что нужно завершать работу и немного их подождём.
            work_task_queue.put(None)
            time.sleep(60)
            # Сообщаем менеджеру очередей, что необходимо завершить работу.
            shutdown_event.set()
            # Вызодим из цикла
            break






