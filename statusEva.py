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

from lib import read_config, s

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
def filter_x00(inp):
    inp = s(inp)
    inp = inp.replace('_x0020_',' ')
    inp = inp.replace('_X0020_',' ')
    while inp.upper().find('_X0') > -1:
        if inp.find('_x0') > -1:
            inp = inp.split('_x0')[0] + inp.split('_x0')[1].split('_')[1]
        else:
            inp = inp.split('_X0')[0] + inp.split('_X0')[1].split('_')[1]
    return inp

if __name__ == '__main__':
    # подключаемся к серверу
    cfg = read_config(filename='anketa.ini', section='Mongo')
    conn = MongoClient('mongodb://' + cfg['user'] + ':' + cfg['password'] + '@' + cfg['ip'] + ':' + cfg['port'] + '/'
                       + cfg['db'])
    # выбираем базу данных
    db = conn.saturn_v
    # выбираем коллекцию документов
    colls = db.Products

    # Sort file names with path
    path = "./"
    file_list = os.listdir(path)
    full_list = [os.path.join(path, i) for i in file_list if i.startswith('Raiffeisen_Finfort_') and i.endswith('.xlsx')]
    xlsxs = sorted(full_list, key = os.path.getmtime)

    for xlsx in xlsxs:
        print('\n', xlsx,'\n')
        wb = openpyxl.load_workbook(filename=xlsx, read_only=True)
        ws = wb[wb.sheetnames[0]]
        wbo = openpyxl.Workbook(write_only=True)
        wso_ish = wbo.create_sheet('Исходный')
        wso_task = wbo.create_sheet('Задание')
        wso_rez = wbo.create_sheet('Результат')
        ids = []
        column_utm_source = -1
        column_approval = -1
        column_remote_id = -1
        column_result = -1
        for i, row in enumerate(ws.rows):
            # заполняем вкладку задания
            fields_task = []
            for cell in row:
                fields_task.append(cell.value)
            wso_task.append(fields_task)
            # определяем колонку в которой id
            if not i:
                for j, cell in enumerate(row):
                    if cell.value == 'UTM_TERM':
                        column_utm_source = j
                    if cell.value == 'APPROVAL':
                        column_approval = j
                    if cell.value == 'remote_id':
                        column_remote_id = j
                    if cell.value == 'RESULT':
                        column_result = j
            else:
                # Если нет нужной информации - выходим
                if (column_utm_source < 0 and column_remote_id < 0) or (column_approval < 0 and column_result < 0):
                    print('Нет колонки с id или колонки со статусом')
                    sys.exit()

                # Если не смогли расшифровать статус - пропускаем строчку
                if column_approval > -1 \
                        and str(type(row[column_approval].value)).find('str') > -1 \
                        and len(s(row[column_approval].value.strip())):
                    status = STATUSES[filter_x00(row[column_approval].value).upper().strip()]
                elif column_result > -1 \
                        and str(type(row[column_result].value)).find('str') > -1\
                        and len(filter_x00(row[column_result].value.strip())):
                    status = STATUSES[filter_x00(row[column_result].value).upper().strip()]
                else:
                    prints = ''
                    if column_approval > -1:
                        prints += str(row[column_approval].value).strip() + ' '
                    if column_result > -1:
                        prints += str(row[column_result].value).strip()
                    #print('В строке', i,'в колонке со статусом некоректная информация (', prints, ') - пропускаем строку')
                    continue

                # Если не смогли расшифровать id - пропускаем строчку
                if column_utm_source > -1 \
                        and str(type(row[column_utm_source].value)).find('str') > -1 \
                        and len(filter_x00(row[column_utm_source].value)[filter_x00(row[column_utm_source].value).find('_') + 1:].strip()) == 36:
                    remote_id = filter_x00(row[column_utm_source].value)[filter_x00(row[column_utm_source].value).find('_') + 1:].strip()
                elif column_remote_id > -1 \
                        and str(type(row[column_remote_id].value)).find('str') > -1\
                        and len(filter_x00(row[column_remote_id].value.strip())) == 36:
                    remote_id = row[column_remote_id].value.strip()
                else:
                    prints = ''
                    if column_utm_source > -1:
                        prints += str(row[column_utm_source].value).strip() + ' '
                    if column_remote_id > -1:
                        prints += str(row[column_remote_id].value).strip()
                    print('В строке', i,'в колонке с id некоректная информация (', prints, ') - пропускаем строку')
                    continue
                # заполняем вкладку исходника
                for j, coll in enumerate(colls.find({'remote_id': remote_id})):
                    if not j:
                        fields_ish = []
                        for field in coll.keys():
                            if str(type(coll.get(field))).find('str') < 0 and str(type(coll.get(field))).find(
                                    'int') < 0:
                                fields_ish.append(str(coll.get(field)))
                            else:
                                fields_ish.append(coll.get(field))
                        wso_ish.append(fields_ish)
                # обновляем
                colls.update({'remote_id': remote_id}, {'$set': {'state_code': status}})
                # заполняем вкладку результата
                for j, coll in enumerate(colls.find({'remote_id': remote_id})):
                    if not j:
                        fields_rez = []
                        for field in coll.keys():
                            if str(type(coll.get(field))).find('str') < 0 and str(type(coll.get(field))).find(
                                    'int') < 0:
                                fields_rez.append(str(coll.get(field)))
                            else:
                                fields_rez.append(coll.get(field))
                        wso_rez.append(fields_rez)
        wbo.save(xlsx.split('Raiffeisen_Finfort_')[0] + 'loaded/' +
                 time.strftime('%Y-%m-%d_%H-%M', time.gmtime(os.path.getmtime(xlsx))) + '_' +
                 xlsx.split('Raiffeisen_Finfort_')[1])
        os.remove(xlsx)


