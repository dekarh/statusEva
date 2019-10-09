# -*- coding: utf-8 -*-
# Заполняем поле статуса в монго конвертируя его из Excel

import sys, argparse
from _datetime import datetime, timedelta, date
import time
import os
from mysql.connector import MySQLConnection, Error
from collections import OrderedDict
import openpyxl
from pymongo import MongoClient
import psycopg2

from lib import read_config, fine_phone

STATUSES = {
'BANK REFUSAL': 430,
'APPROVED': 140,
'CLIENT REFUSAL': 400,
'ISSUED': 210}


st = """
Bank refusal - Отказ банка
Approved - Одобрен
Client refusal - Отказ клиента
issued - Выдан

NONE = 0;
QUEUED = 100;
CONFIRM = 110;
RETRY = 120;
PROCESSING = 130;
APPROVED = 140;
PRE_APPROVED = 150;
COMPLETED = 160;

DONE = 200;
ISSUED = 210;
DOUBLE_ISSUED = 220;
ISSUED_CALLCENTER = 230;

DELETED = 400;
UNKNOWN = 410;
TRANSACTION_ERROR = 420;
DENIED = 430;

DEBUG = 500;
DRAFT = 510;
"""

# подключаемся к серверу
cfg = read_config(filename='anketa.ini', section='Mongo')
conn = MongoClient('mongodb://' + cfg['user'] + ':' + cfg['password'] + '@' + cfg['ip'] + ':' + cfg['port'] + '/'
                   + cfg['db'])
# выбираем базу данных
db = conn.saturn_v

# выбираем коллекцию документов
colls = db.Products

# увеличиваем всем Петрам возраст на 5 лет
#coll.update({"name": "Петр"}, {"$inc": {"age": 5}})
# или
# всем Петрам делаем фамилию Новосельцев и возраст 25 лет
#coll.update({"name": "Петр"}, {"surname": "Новосельцев", "age": 25})


path = "./"
# Sort file names with path
file_list = os.listdir(path)
full_list = [os.path.join(path, i) for i in file_list if i.startswith('Raiffeisen_Finfort_') and i.endswith('.xlsx')]
xlsxs = sorted(full_list, key = os.path.getmtime)

for xlsx in xlsxs:
    wb = openpyxl.load_workbook(filename=xlsx, read_only=True)
    ws = wb[wb.sheetnames[0]]
    wbo = openpyxl.Workbook(write_only=True)
    wso_ish = wbo.create_sheet('Исходный')
    wso_task = wbo.create_sheet('Задание')
    wso_rez = wbo.create_sheet('Результат')
    #ws_rez.append(['Город Агента', 'Юр.лицо Агента', 'ФИО Агента', 'Подразделение', 'Город Клиента', 'ФИО Клиента'])

    ids = []
    column_id = -1
    column_status = -1
    for i, row in enumerate(ws.rows):
        if not i:
            for j, cell in enumerate(row):
                if cell.value == 'UTM_TERM':
                    column_id = j
                if cell.value == 'APPROVAL':
                    column_status = j
        else:
            if column_id < 0 or column_status < 0:
                print('Нет колонки с id или колонки со статусом')
                sys.exit()
            if str(type(row[column_id].value)).find('str') > -1:
                remote_id = row[column_id].value[row[column_id].value.find('_') + 1:]
                status = STATUSES[row[column_status].value.upper()]
                print(remote_id, status)
                colls.update({'remote_id': remote_id}, {'$set': {'state_code': status}})
    wbo.save(xlsx.split('Raiffeisen_Finfort_')[0] + time.strftime('%Y-%m-%d_%H-%M', time.gmtime(os.path.getmtime(xlsx)))
             + '_' + xlsx.split('Raiffeisen_Finfort_')[1])
    #os.remove(xlsx)


