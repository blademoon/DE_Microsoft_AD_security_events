import Evtx.Evtx as evtx
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import date
import random as rnd


def get_event_4624(temp_system, temp_event_data):
    temp_dict = {"TimeCreated": "",
                 "EventID": temp_system.find('./{http://schemas.microsoft.com/win/2004/08/events/event}EventID').text,
                 "Computer": temp_system.find('./{http://schemas.microsoft.com/win/2004/08/events/event}Computer').text,
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

        if list(Data.attrib.values())[0] == "LogonProcessName":
            temp_dict["LogonProcessName"] = str(Data.text)

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
                 "Computer": temp_system.find('./{http://schemas.microsoft.com/win/2004/08/events/event}Computer').text,
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
                 "Computer": temp_system.find('./{http://schemas.microsoft.com/win/2004/08/events/event}Computer').text,
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


def parse_evtx_file(evtx_file_fullpath, result_file_fullpath):
    # Имя сервера с которого был взят журнал
    Computer_name = ""

    # Датафрейм в который накапливаем информацию
    df = pd.DataFrame()

    # Откроем файл
    with evtx.Evtx(evtx_file_fullpath) as log:

        Events_signal = list(log.records())

        for record in Events_signal:
            File = ET.fromstring(record.xml())
            Event = list(File)

            System = None
            EventData = None

            for Event in File:
                if (Event.tag == "{http://schemas.microsoft.com/win/2004/08/events/event}System"):
                    System = Event
                elif (Event.tag == "{http://schemas.microsoft.com/win/2004/08/events/event}EventData"):
                    EventData = Event

            if Computer_name == "":
                Computer_name = System.find('./{http://schemas.microsoft.com/win/2004/08/events/event}Computer').text

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

    today = date.today()
    today = today.strftime("%d_%m_%Y_%H24_%M_%S")

    random_name = str(rnd.randrange(1000))
    df.to_csv(result_file_fullpath + Computer_name + "-" + today + random_name + "_output.xlsx", index=False)