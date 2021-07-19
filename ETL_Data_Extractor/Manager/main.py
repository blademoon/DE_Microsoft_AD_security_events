from Manager import *
from QueueOperations import *
import time

# Менеджер очередей взаимодействующий с рабочими процессами будет отдельным потоком.
# В основном потоке будем ваполнять очередь и отслеживать пояления событий.

#path_to_files = '\\\\fileserver\\SystemLogs\\SecurityLogsBackup\\**\\*.evtx'
path_to_files = 'C:\\TEMP\\SystemLogs\\SecurityLogsBackup\\**\\*.evtx'
path_to_results = 'C:\\ETL_PROCESS\\DATA\\'

if __name__ == '__main__':
    started_event = Event()
    shutdown_event = Event()
    server_thread = Thread(target=server, args=(started_event, shutdown_event,))
    server_thread.start()

    # Ожидаем запуска менеджера очередей.
    started_event.wait()

    # Подключаемся к менеджеру
    work_queue, done_queue = connect_to_manager()

    # Создаём и заполняем основной перечень задач.
    main_tasks_list = fill_main_tasks_list(path_to_files, path_to_results)

    while True:

        # Ставим в рабочую очередь задания из основной очереди на выполнение.
        fill_work_queue(main_tasks_list, work_queue)

        # Проверяем завершенные задания и отмечаем их в основной очереди.
        mark_completed_tasks(main_tasks_list, done_queue)

        print(main_tasks_list)

        # Проверяем, все ли задачи выполнены
        if not main_task_done(main_tasks_list):
            print("All tasks complete. Exiting...")
            break
        time.sleep(1)

    # Сообщаем менеджеру очередь о завершении работы
    # Ожидаем завершения работы и сообщаем ОС об успешном заверщении работы.
    print("Exiting...")
    shutdown_event.set()
    server_thread.join()
    sys.exit(0)




