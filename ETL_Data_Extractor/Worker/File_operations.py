import zipfile
import os


# Сжать и переместить обработанный файл evtx
# Получаем на вход только задачу.
def clean_up_processed_file(current_task_file_path, processed_files_path):
    full_file_name = os.path.basename(current_task_file_path)
    filename, file_extension = os.path.splitext(full_file_name)

    zip_file_full_path = processed_files_path + filename + ".zip"

    file_zip = zipfile.ZipFile(zip_file_full_path, 'w')
    file_zip.write(current_task_file_path, compress_type=zipfile.ZIP_DEFLATED,)
    file_zip.close()

    os.remove(current_task_file_path)

    return None
