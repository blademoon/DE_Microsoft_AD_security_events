from evtx import PyEvtxParser
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import date
import random as rnd

from File_operations import *

def get_event_4624(temp_system, temp_event_data):
    temp_dict = {"TimeCreated": "",
                 "EventID": temp_system.find('./{http://schemas.microsoft.com/win/2004/08/events/event}EventID').text,
                 "Log_source": temp_system.find('./{http://schemas.microsoft.com/win/2004/08/events/event}Computer').text,
                 "TargetUserSid": "",
                 "TargetUserName": "",
                 "TargetDomainName": "",
                 "TargetLogonID": "",
                 "LogonType": "",
                 "LogonProcessName": "",
                 "ProcessName": "",
                 "ProcessId": "",
                 "WorkstationName": "",
                 "IpAddress": "",
                 "IpPort": "",
                 "RestrictedAdminMode": ""}

    # Время сохраняется в формате timestamp часовой пояс GMT (часовой пояс по Гринвичу)
    TimeCreated = temp_system.find("./{http://schemas.microsoft.com/win/2004/08/events/event}TimeCreated")
    temp_dict["TimeCreated"] = TimeCreated.attrib['SystemTime']

    for Data in temp_event_data:

        if list(Data.attrib.values())[0] == "TargetUserSid":
            temp_dict["TargetUserSid"] = str(Data.text)

        if list(Data.attrib.values())[0] == "TargetUserName":
            temp_dict["TargetUserName"] = str(Data.text)

        if list(Data.attrib.values())[0] == "TargetDomainName":
            temp_dict["TargetDomainName"] = str(Data.text)

        if list(Data.attrib.values())[0] == "TargetLogonId":
            temp_dict["TargetLogonID"] = str(Data.text)

        if list(Data.attrib.values())[0] == "LogonType":
            temp_dict["LogonType"] = str(Data.text)

        if list(Data.attrib.values())[0] == "LogonProcessName":
            temp_dict["LogonProcessName"] = str(Data.text)

        if list(Data.attrib.values())[0] == "ProcessName":
            temp_dict["ProcessName"] = str(Data.text)

        if list(Data.attrib.values())[0] == "ProcessId":
            temp_dict["ProcessId"] = str(int(Data.text, 0))

        if list(Data.attrib.values())[0] == "WorkstationName":
            temp_dict["WorkstationName"] = str(Data.text)

        if list(Data.attrib.values())[0] == "IpAddress":
            temp_dict["IpAddress"] = str(Data.text)

        if list(Data.attrib.values())[0] == "IpPort":
            temp_dict["IpPort"] = str(Data.text)

        if list(Data.attrib.values())[0] == "RestrictedAdminMode":
            temp_dict["RestrictedAdminMode"] = str(Data.text)

    return temp_dict


def get_event_4634(temp_system, temp_event_data):
    temp_dict = {"TimeCreated": "",
                 "EventID": temp_system.find('./{http://schemas.microsoft.com/win/2004/08/events/event}EventID').text,
                 "Log_source": temp_system.find('./{http://schemas.microsoft.com/win/2004/08/events/event}Computer').text,
                 "TargetUserName": "",
                 "TargetDomainName": "",
                 "TargetLogonID": "",
                 "LogonType": "",
                 "LogonProcessName": "",
                 "IpAddress": "",
                 "IpPort": "",
                 "RestrictedAdminMode": ""}

    # Время сохраняется в формате timestamp часовой пояс GMT (часовой пояс по Гринвичу)
    TimeCreated = temp_system.find("./{http://schemas.microsoft.com/win/2004/08/events/event}TimeCreated")
    temp_dict["TimeCreated"] = TimeCreated.attrib['SystemTime']

    for Data in temp_event_data:
        if list(Data.attrib.values())[0] == "TargetUserName":
            temp_dict["TargetUserName"] = str(Data.text)

        if list(Data.attrib.values())[0] == "TargetDomainName":
            temp_dict["TargetDomainName"] = str(Data.text)

        if list(Data.attrib.values())[0] == "TargetLogonId":
            temp_dict["TargetLogonID"] = str(Data.text)

        if list(Data.attrib.values())[0] == "LogonType":
            temp_dict["LogonType"] = str(Data.text)

    return temp_dict


def get_event_4647(temp_system, temp_event_data):
    temp_dict = {"TimeCreated": "",
                 "EventID": temp_system.find('./{http://schemas.microsoft.com/win/2004/08/events/event}EventID').text,
                 "Log_source": temp_system.find('./{http://schemas.microsoft.com/win/2004/08/events/event}Computer').text,
                 "TargetUserName": "",
                 "TargetDomainName": "",
                 "TargetLogonID": "",
                 "LogonType": "",
                 "LogonProcessName": "",
                 "IpAddress": "",
                 "IpPort": "",
                 "RestrictedAdminMode": ""}

    # Время сохраняется в формате timestamp часовой пояс GMT (часовой пояс по Гринвичу)
    TimeCreated = temp_system.find("./{http://schemas.microsoft.com/win/2004/08/events/event}TimeCreated")
    temp_dict["TimeCreated"] = TimeCreated.attrib['SystemTime']

    for Data in temp_event_data:
        if list(Data.attrib.values())[0] == "TargetUserName":
            temp_dict["TargetUserName"] = str(Data.text)

        if list(Data.attrib.values())[0] == "TargetDomainName":
            temp_dict["TargetDomainName"] = str(Data.text)

        if list(Data.attrib.values())[0] == "TargetLogonId":
            temp_dict["TargetLogonID"] = str(Data.text)

    return temp_dict


def parse_evtx_file(evtx_file_fullpath, result_file_fullpath, process_file_full_path):
    # Имя сервера с которого был взят журнал
    Log_source = ""

    # Датафрейм в который накапливаем информацию из логов
    #df = pd.DataFrame()

    df = pd.DataFrame(columns=['EventID', 'WorkstationName', 'IpAddress', 'IpPort', 'Log_source', 'LogonProcessName',
                               'LogonType', 'ProcessId', 'ProcessName', 'RestrictedAdminMode', 'TargetDomainName',
                               'TargetLogonID', 'TargetUserSid', 'TargetUserName', 'TimeCreated', 'evtx_file_name'])

    # Откроем файл логов в формате evtx и распарсим его
    parser = PyEvtxParser(evtx_file_fullpath)

    # Получим имя текущего обрабатываемого файла
    current_evtx_file_name = get_file_name(evtx_file_fullpath)

    for record in parser.records():

        xml_event = ET.fromstring(record['data'])
        event_elements = list(xml_event)

        System = None
        EventData = None

        for event in event_elements:
            if (event.tag == "{http://schemas.microsoft.com/win/2004/08/events/event}System"):
                System = event
            elif (event.tag == "{http://schemas.microsoft.com/win/2004/08/events/event}EventData"):
                EventData = event

        if Log_source == "":
            Log_source = System.find('./{http://schemas.microsoft.com/win/2004/08/events/event}Computer').text

        EventID = System.find('./{http://schemas.microsoft.com/win/2004/08/events/event}EventID').text

        if EventID == "4624":
            event = get_event_4624(System, EventData)
            df = df.append(event, ignore_index=True)

        if EventID == "4634":
            event = get_event_4634(System, EventData)
            df = df.append(event, ignore_index=True)

        if EventID == "4647":
            event = get_event_4647(System, EventData)
            df = df.append(event, ignore_index=True)

        else:
            continue

    df['evtx_file_name'] = current_evtx_file_name

    df.to_csv(result_file_fullpath + Log_source + "_" + current_evtx_file_name + "_output.csv", index=False)

    # Архивируем обработанный журнал и перемещаем его в папку для хранения.
    clean_up_processed_file(evtx_file_fullpath, process_file_full_path, Log_source)
