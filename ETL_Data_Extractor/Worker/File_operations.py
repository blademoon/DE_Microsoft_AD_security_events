import zipfile
import os


# Фукнция получения имени файла из текущей задачи
def get_file_name(full_path_to_file):
    full_file_name = os.path.basename(full_path_to_file)
    filename, file_extension = os.path.splitext(full_file_name)
    return filename


# Сжать и переместить обработанный файл evtx
# Получаем на вход только задачу.
def clean_up_processed_file(current_task_file_path, processed_files_path, src_server_name):
    zip_file_full_path = processed_files_path + src_server_name + "_" + get_file_name(current_task_file_path) + ".zip"

    file_zip = zipfile.ZipFile(zip_file_full_path, 'w')
    file_zip.write(current_task_file_path, compress_type=zipfile.ZIP_DEFLATED, )
    file_zip.close()

    os.remove(current_task_file_path)

    return None
