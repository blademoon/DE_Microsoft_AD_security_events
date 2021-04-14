main_queue = {
    '.\\DATA\\file00.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'IN_QE'},
    '.\\DATA\\file01.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'IN_QE'},
    '.\\DATA\\file02.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'IN_QE'},
    '.\\DATA\\file04.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'IN_QE'},
    '.\\DATA\\file05.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'IN_QE'},
    '.\\DATA\\file06.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'IN_QE'},
    '.\\DATA\\file07.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'IN_QE'},
    '.\\DATA\\file08.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'IN_QE'},
    '.\\DATA\\file09.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'IN_QE'},
    '.\\DATA\\file10.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'IN_QE'}
}
done_queue = {
    '.\\DATA\\file00.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'DONE'},
    '.\\DATA\\file01.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'DONE'},
    '.\\DATA\\file02.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'DONE'},
    '.\\DATA\\file04.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'DONE'},
    '.\\DATA\\file05.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'DONE'},
    '.\\DATA\\file06.evtx': {'result_file_path': '.\\RESULTS\\', 'task_state': 'DONE'}
}
for k, v in main_queue.items():
    statement = False
    if not ((v['task_state'] == 'DONE') or (v['task_state'] == 'EXCP')):
        statement = True
        print(k)
        print(v)
        print("Any detected")

    print(statement)