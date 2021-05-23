# https://stackoverflow.com/questions/72899/how-do-i-sort-a-list-of-dictionaries-by-a-value-of-the-dictionary
import re
import glob
import datetime


def get_date_from_string(filename):
    str_date = re.search("\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}", filename)
    if (str_date):
        str_date = str_date.group(0)
    date_time_obj = datetime.datetime.strptime(str_date, "%Y-%m-%d-%H-%M-%S")
    return date_time_obj


def order_files_by_date(path_to_files):
    files_paths_list = glob.glob(path_to_files, recursive=True)
    temp_list = []

    for file in files_paths_list:
        file_date = get_date_from_string(file)
        temp_dict = {}
        temp_dict["Path"] = file
        temp_dict["date"] = file_date
        temp_list.append(temp_dict)

    temp_list.sort(key=lambda element: element['date'], reverse=False)

    return temp_list


# Тестирование функции
csv_files_path = "D:\\WORKSPACE\\ETL_Transfom_Load\\DATA\\*.csv"
csv_files_ordered_list = order_files_by_date(csv_files_path)

print(csv_files_ordered_list)
