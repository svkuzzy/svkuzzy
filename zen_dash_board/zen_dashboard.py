#!/usr/bin/python
# -*- coding: utf-8 -*-

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

if __name__ == '__main__':
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
# список таблиц
    dash_visits = []
    dash_engagement = []
    tables = {'dash_visits' : dash_visits,
              'dash_engagement' : dash_engagement
            }
# получение данных
    query = '''
            SELECT *
            FROM dash_visits
            '''
    query_II =  '''
                SELECT *
                FROM dash_engagement
                '''
    dash_visits = pd.io.sql.read_sql(query, con = engine)
    dash_engagement = pd.io.sql.read_sql(query_II, con = engine)
# преобразование данных

    columns_datetime = ['dt']
    
    for column in columns_datetime:
        dash_visits[column] = pd.to_datetime(dash_visits[column])
        dash_engagement[column] = pd.to_datetime(dash_engagement[column])

# layout
    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
    app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
    app.layout = html.Div(children = [

# заголовок и описание
    html.H1(children = 'Дзен.Статистика'),
    html.Label('История событий, относительные доли тем источников, усредненная воронка событий. Данные обновляются ежедневно'),
    html.Br(),
    
    html.Div([

#dt_selector    
        html.Div([
            html.Label('Фильтр даты'),
            dcc.DatePickerRange(
                start_date = dash_visits['dt'].min(),
                end_date = dash_visits['dt'].max(),
                display_format = 'YYYY-MM-DD',
                id = 'dt_selector'       
            ),
#age_segment_selector              
            html.Label('Фильтр возрастных категорий'),
            dcc.Dropdown(
                options = [{'label':i, 'value':i} for i in dash_visits['age_segment'].unique()],
                value = dash_visits['age_segment'].unique(),
                multi = True,
                id = 'age_segment_selector'
            )
        ], className = 'six columns'),

#topic_selector                    
        html.Div([
            html.Label('Фильтр карточек'),
            dcc.Dropdown(
                options = [{'label':i,'value':i} for i in dash_visits['item_topic'].unique()],
                value = dash_visits['item_topic'].unique(),
                multi = True,                
                id = 'topic_selector'       
            )
        ], className = 'six columns'),   
    ], className = 'row'),

    html.Br(),

#graph: events_history. Количество событий по времени.
    html.Div([
        html.Div([
            html.Label('История событий'),
            dcc.Graph(
                id = 'events_history',
                style = {'height' : '60vw'},
            ),
        ], className = 'six columns'),

#graph: events_ratio. Доля событий в зависимости от темы источника
        html.Div([
            html.Label('Доля событий по темам источника'),
            dcc.Graph(
                id = 'events_ratio',
                style = {'height' : '30vw'},
            ),
        
#graph: depth. Воронка событий
            html.Label('Средняя глубина взаимодействия'),
            dcc.Graph(
                id = 'depth',
                style = {'height':'27vw'},
            ),
        ], className = 'six columns')
        
    ], className = 'row')  
    ])    

# логика дашборда
    @app.callback(
        [Output('events_history','figure'),
        Output('events_ratio','figure'),
        Output('depth','figure')
        ],
        [Input('dt_selector','start_date'),
        Input('dt_selector','end_date'),
        Input('topic_selector','value'),
        Input('age_segment_selector','value')
        ])
    def update_figures(start_date, end_date, selected_topics, selected_age_segment):
# преобразование входных параметров к нужным типам
        start_date = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S')
        end_date = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S')
        
# фильтрация
        date_filter_I = dash_visits['dt'] >= start_date
        date_filter_II = dash_visits['dt'] <= end_date
        age_filter = dash_visits['age_segment'].isin(selected_age_segment)
        topics_filter = dash_visits['item_topic'].isin(selected_topics)
        
        filtered_data = dash_visits[
                                    date_filter_I & 
                                    date_filter_II & 
                                    age_filter &
                                    topics_filter
                                    ]
        
        date_filter_III = dash_engagement['dt'] >= start_date 
        date_filter_IV = dash_engagement['dt'] <= end_date
        age_filter_II = dash_engagement['age_segment'].isin(selected_age_segment)
        topics_filter_II = dash_engagement['item_topic'].isin(selected_topics)
                
        filtered_data_II = dash_engagement[
                                        date_filter_III &
                                        date_filter_IV &
                                        age_filter_II &
                                        topics_filter_II
                                        ]
    # группировка отфильтрованных данных
        events_graph = filtered_data.groupby([
                                            'item_topic',
                                            'dt' 
                                            ]).agg({'visits':'sum'}).reset_index() 
        ratio_graph = filtered_data.groupby([
                                            'source_topic'
                                            ]).agg({'visits':'sum'}).reset_index()
        depth_graph = filtered_data_II.groupby([
                                            'event'
                                            ]).agg({'unique_users':'mean'}).reset_index().rename(
                                            columns = {'unique_users':'avg_unique_users'}).sort_values(
                                            by = 'avg_unique_users', ascending = False)
        depth_graph['ratio'] = (depth_graph['avg_unique_users'] / depth_graph['avg_unique_users'].max()).round(2)
        
#графики для отрисовки
        events_history = []  
        for item in events_graph['item_topic'].unique():
            events_history += [go.Scatter(
                                    x = events_graph.query('item_topic == @item')['dt'],
                                    y = events_graph.query('item_topic == @item')['visits'],
                                    mode = 'lines',
                                    stackgroup = 'one',
                                    name = item
                                    )]
        events_ratio = [go.Pie(
                            labels = ratio_graph['source_topic'],
                            values = ratio_graph['visits']
                            )]
        depth = [go.Bar(
                        x = depth_graph['event'],
                        y = depth_graph['ratio']
                        )]
                            
# формирование результата для отображения
        return (
                {
                'data': events_history,
                'layout': go.Layout(
                                    xaxis = {'title': 'Дата'},
                                    yaxis = {'title': 'Число визитов'}
                                    )
                },
                {
                'data': events_ratio
                },
                {
                'data': depth,
                'layout': go.Layout(
                                    xaxis = {'title': 'Тип события'},
                                    yaxis = {'title': 'Доля от показов'}
                                    )
                },
        )  


    app.run_server(debug = True)
