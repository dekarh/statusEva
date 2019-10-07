# -*- coding: utf-8 -*-
# Заполняем поле статуса в монго конвертируя его из Excel

import sys, argparse
from _datetime import datetime, timedelta, date
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

if len(sys.argv) < 2:
    print('Укажите имя .xlsx файла отчета')
    sys.exit()
if not sys.argv[1].endswith('.xlsx'):
    print('Это не .xlsx файл')
    sys.exit()

wb = openpyxl.load_workbook(filename=sys.argv[1], read_only=True)
ws = wb[wb.sheetnames[0]]
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


