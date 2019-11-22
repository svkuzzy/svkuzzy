#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import getopt
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

if __name__ == '__main__':

# объявление входящих параметров    
    unixOptions = 's:e'
    gnuOptions = ['start_dt=','end_dt=']
    
    fullCmdArguments = sys.argv
    argumentsList = fullCmdArguments[1:]

# получение значений входящих параметров
    
    try:
        arguments, values = getopt.getopt(argumentsList, unixOptions, gnuOptions)
    except getopt.error as err:
        print(str(err))
        sys.exit(2)

# значения входящих параметров по умолчанию

    start_dt = '2019-09-24'
    end_dt = '2019-09-25'
    
    for currentArgument, currentValue in arguments:
        if currentArgument in ('-s', '--start_dt'):
            start_dt = currentValue
        elif currentArgument in ('-e','--end_dt'):
            end_dt = currentValue
   
# параметры подключения к базе данных 
    
    db_config = {'user' : 'user_name',
                 'pwd' : 'user_password',
                 'host' : 'localhost',
                 'port' : '5432',
                 'db' : 'zen'}

# строка подключения

    connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(
                                                        db_config['user'],
                                                        db_config['pwd'],
                                                        db_config['host'],
                                                        db_config['port'],
                                                        db_config['db'])
# создание движка
    
    engine = create_engine(connection_string)

# запрос данных из базы: все значения + колонка со временем, конвертированным в UTC в столбце dt
         
    query = ''' SELECT *,
                TO_TIMESTAMP (ts/1000) AT TIME ZONE 'Etc/UTC' AS dt
                FROM log_raw
                WHERE TO_TIMESTAMP (ts/1000) AT TIME ZONE 'Etc/UTC' BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP
            '''.format(start_dt, end_dt)

# получение сырых данных по запросу 

    raw = pd.io.sql.read_sql(query, con = engine)

# определение форматов данных    
    
    columns_str = ['age_segment','event','item_topic','item_type','source_topic','source_type']
    columns_numeric = ['event_id','item_id','source_id','ts','user_id']
    columns_datetime = ['dt']

# приведение данных к формату 
    
    for column in columns_str:
        raw[column] = raw[column].astype(str)
    for column in columns_numeric:
        raw[column] = pd.to_numeric(raw[column], errors = 'coerce')

# для времени - дополнительно округление до минут    
    for column in columns_datetime:
        raw[column] = pd.to_datetime(raw[column].dt.round('min'))
        
# группировка для датафреймов событий
# Группировка по теме карточки, теме источника, возрастной категории и времени. Подсчет числа событий

    dash_visits = (raw.groupby(['item_topic',
                                'source_topic',
                                'age_segment',
                                'dt'])
                    .agg({'user_id':'count'})
                    .reset_index()
                    .rename(columns = {'user_id':'visits'})
    )
    
# группировка для воронки событий
# группировка по времени, теме карточки, событию, возрастной категории. Подсчет числа уникальных пользователей

    dash_engagement = (raw.groupby(['dt',
                                    'item_topic',
                                    'event',
                                    'age_segment'])
                        .agg({'user_id':'nunique'})
                        .reset_index()
                        .rename(columns = {'user_id':'unique_users'})
    )

    
    tables = {'dash_visits' : dash_visits,
              'dash_engagement' : dash_engagement
            }
    
    for key, value in tables.items():
        query = '''DELETE FROM {}
                   WHERE dt BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP
                '''.format(key, start_dt, end_dt)
        engine.execute(query)
        value.to_sql(name = key, con = engine, if_exists = 'append', index = False)
    
    print('All_done {}'.format(end_dt))
