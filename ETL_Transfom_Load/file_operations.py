# https://stackoverflow.com/questions/72899/how-do-i-sort-a-list-of-dictionaries-by-a-value-of-the-dictionary
# https://stackoverflow.com/questions/7271482/getting-a-list-of-values-from-a-list-of-dicts
import re
import glob
import datetime
import zipfile
import os


# Функция получения имени файла из текущей задачи
def get_file_name(full_path_to_file):
    full_file_name = os.path.basename(full_path_to_file)
    filename, file_extension = os.path.splitext(full_file_name)
    return filename


# Сжать и переместить обработанный файл в архивную папку
# Получаем на вход только задачу.
def zip_processed_file(file_path, processed_files_path):
    zip_file_full_path = processed_files_path + get_file_name(file_path) + ".zip"

    file_zip = zipfile.ZipFile(zip_file_full_path, 'w')
    file_zip.write(file_path, compress_type=zipfile.ZIP_DEFLATED, )
    file_zip.close()

    os.remove(file_path)

    return None


# Функция получения timestamp из имени файла.
def get_date_from_string(filename):
    str_date = re.search("\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}", filename)
    if (str_date):
        str_date = str_date.group(0)
    date_time_obj = datetime.datetime.strptime(str_date, "%Y-%m-%d-%H-%M-%S")
    return date_time_obj


# Функция сортировки списка файлов по timestamp в имени файла.
def order_files_by_date(path_to_files, recursive_flag, reverse_sort_flag):
    files_paths_list = glob.glob(path_to_files, recursive=True)
    temp_list = []

    for file in files_paths_list:
        file_date = get_date_from_string(file)
        temp_dict = {}
        temp_dict["Path"] = file
        temp_dict["date"] = file_date
        temp_list.append(temp_dict)

    temp_list.sort(key=lambda element: element['date'], reverse=False)
    temp_list = [item['Path'] for item in temp_list]

    return temp_list
