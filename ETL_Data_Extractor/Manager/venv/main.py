from Manager import *
from QueueOperations import *

# Менеджер очередей взаимодействующий с рабочими процессами будет отдельным потоком.
# В основном потоке будем хаполнять очередь и отслеживать пояления событий.

path_to_files = '..\\DATA\\*.evtx'
path_to_results = '..\\RESULTS\\'

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
    main_tasks_list = fill_main_tasks_queue(path_to_files,path_to_results)

    # Ставим в рабочую очередь задания из основной очереди на выполнение.
    fill_work_queue(main_tasks_list,work_queue)

    # Проверяем завершенные задания и отмечаем их в основной очереди.



    # Сообщаем менеджеру очередtq о завершении работы
    # Ожидаем завершения работы и сообщаем ОС об успешном заверщении работы.
    print("Exiting...")
    shutdown_event.set()
    server_thread.join()
    sys.exit(0)




