import time
import sys
import glob

# Функция для формирования основной очереди и заполнения ее задачами
def fill_main_tasks_queue(path_to_files, path_to_result):
    # Сформируем основную очередь в которой будем хранить и отслеживать состояние задач
    temp_task_list = {}

    # Получим пути ко всем обрабатываемым файлам
    processed_files = glob.glob(path_to_files)

    for file in processed_files:
        # Добавляем задачу в необходимом нам формате
        temp_task_list[file] = {"result_file_path": path_to_result, "task_state": ""}

    return (temp_task_list)

# Функция выбирающая новые задачи из основной очереди менеджера задач
# и выполняющая постановку новых задач в рабочую очередь.
def fill_work_queue(main_list,work_queue):
    for task,prop in main_list.items():
        # Если задача не отправленна на обработку, то
        if not prop["task_state"]:
            # Поставим задачу на выполнение в рабочую очередь
            work_queue.put(task)

            # Отметим что задача поставлена на выполнение в основоной очереди
            prop["task_state"] = "IN_QE"


# Функция отмечающая выполненные задачи в основной очереди.
# def mark_completed_tasks(manager_main_queue,manager_done_task_queue):
#     for task in manager_done_task_queue: