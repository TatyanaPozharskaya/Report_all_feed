Отчет по приложению
Соберите отчет по работе всего приложения как единого целого. 

Продумайте, какие метрики необходимо отобразить в этом отчете? Как можно показать их динамику?  Приложите к отчету графики или файлы, чтобы сделать его более наглядным и информативным. Отчет должен быть не просто набором графиков или текста, а помогать отвечать бизнесу на вопросы о работе всего приложения совокупно. В отчете обязательно должны присутствовать метрики приложения как единого целого, можно так же отобразить метрики по каждой из частей приложения - по ленте новостей и по мессенджеру.  

Автоматизируйте отправку отчета с помощью Airflow. Код для сборки отчета разместите в GitLab, для этого:

Отчет должен приходить ежедневно в 11:00 в чат. 


import telegram
import pandas as pd
import pandahouse as ph
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import os
import sys
from datetime import date, datetime, timedelta


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 't-pozharskaja',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 6, 12),
}

# Интервал запуска DAG (каждый день в 11:00)
schedule_interval = '0 11 * * *'

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230520',
                      'user':'student',
                    'password':'dpo_python_2020'
              }


chat_id = -938659451  # id канала   
bot = telegram.Bot(token='5981322866:AAE-nxcLJKljsaeR5WC00bYHTdmMGeg8duA') # получаем доступ

  
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def report_alerts_7_2():
    @task()
    # выгрузим данные для отчета 
    def create_data():
        query = '''           
            select feed.date,
                   feed.user_id,
                   feed.views,
                   feed.likes,
                   feed.ctr,
                   feed.gender,
                   feed.age,
                   feed.country,
                   feed.city,
                   feed.os,
                   feed.source,
                   mes.messages
            FROM
              (select toDate(t1.time) as date,
                      t1.user_id,
                      if(gender=1, 'male', 'female') as gender,
                      age,
                      country,
                      city,
                      os,
                      source,
                      countIf(action = 'view') as views,
                      countIf(action = 'like') as likes,
                      round(likes / views, 2) as ctr
               from simulator_20230520.feed_actions t1
               where user_id in
                   (select distinct(user_id)
                    from simulator_20230520.feed_actions)
               group by toDate(t1.time) as date,
                        t1.user_id,
                        if(gender=1, 'male', 'female') as gender,
                        age,
                        country,
                        city,
                        os,
                        source
               having toDate(time) = yesterday()           
               order by date,
                        user_id) feed
            inner join
                     (select toDate(t2.time) as date,
                             t2.user_id,
                             count(reciever_id) as messages
                      from simulator_20230520.message_actions t2
                      group by toDate(t2.time) as date, 
                               t2.user_id
                      having toDate(time) = yesterday() 
                      order by date) mes on feed.date = mes.date and feed.user_id = mes.user_id
                      order by feed.date,
                      feed.user_id 
             '''
                
        data_text = ph.read_clickhouse(query=query, connection=connection)    
        return data_text
        
    @task()
    # посчитаем метрики на пользователя, сформируем отчет для сообщения в бот 
    def report_message(data_text, chat_id=None):
        chat_id = -938659451
        bot = telegram.Bot(token='5981322866:AAE-nxcLJKljsaeR5WC00bYHTdmMGeg8duA')
                   
        data_text['views_per_user'] = (data_text['views'] / data_text['user_id'].nunique()).round(2)
        data_text['likes_per_user'] = (data_text['likes'] / data_text['user_id'].nunique()).round(2)
        data_text['messages_per_user'] = (data_text['messages'] / data_text['user_id'].nunique()).round(2)
        
        yesterday_metrics = data_text.groupby('date', as_index=False)\
                                                    .agg({'user_id' : 'count', 
                                                          'views' : 'sum',
                                                          'likes' : 'sum', 
                                                          'ctr' : 'mean',
                                                          'messages': 'sum',
                                                          'views_per_user': 'sum',
                                                          'likes_per_user': 'sum',
                                                          'messages_per_user': 'sum'
                                                 })
        
        dau = yesterday_metrics.iloc[0]['user_id']
        views = yesterday_metrics.iloc[0]['views']
        likes = yesterday_metrics.iloc[0]['likes']
        ctr = yesterday_metrics.iloc[0]['ctr']
        views_per_user = yesterday_metrics.iloc[0]['views_per_user']
        likes_per_user = yesterday_metrics.iloc[0]['likes_per_user']
        messages = yesterday_metrics.iloc[0]['messages']
        messages_per_user = yesterday_metrics.iloc[0]['messages_per_user']
        users_android = len(data_text[data_text['os'] == 'Android'])
        users_ios = len(data_text[data_text['os'] == 'iOS'])
        users_organic = len(data_text[data_text['source'] == 'organic'])
        users_ads = len(data_text[data_text['source'] == 'ads'])
        users_write_only = len(data_text[data_text['messages'] > 0])
        
        message = "-" * 20 + '\n' \
                  + f'\nОбщая статистика за вчера:\n\n' \
                  + f'DAU: {dau}\n' \
                  + f'Просмотры: {views}\n' \
                  + f'Лайки: {likes}\n' \
                  + f'CTR: {ctr:.2f}\n' \
                  + f'\nПросмотры на 1 пользователя: {views_per_user:.2f}\n' \
                  + f'Лайки на 1 пользователя: {likes_per_user:.2f}\n' \
                  + f'Сообщения на 1 пользователя: {messages_per_user:.2f}\n' \
                  + f'Сообщения: {messages}\n' \
                  + '\n' + "-" * 20 + '\n' \
                  + f'Пользователи по трафику и источникам:\n' \
                  + f'\nAndroid: {users_android}\n' \
                  + f'iOS: {users_ios}\n' \
                  + f'Органические: {users_organic}\n' \
                  + f'Из рекламного трафика: {users_ads}\n' \
                  + f'\nКоличество активных пользователей в мессенджере: {users_write_only}\n' \
                  + '\n' + "-" * 20 + '\n'
            
         
        bot.sendMessage(chat_id=chat_id, text=message)
        return message
    
    @task
    def create_charts():
        # сформируем таблицу активностей за прошедшую неделю
        query = '''           
            select feed.date,
                   feed.user_id,
                   feed.views,
                   feed.likes,
                   feed.ctr,
                   feed.gender,
                   feed.age,
                   feed.country,
                   feed.city,
                   feed.os,
                   feed.source,
                   mes.messages
            FROM
              (select toDate(t1.time) as date,
                      t1.user_id,
                      if(gender=1, 'male', 'female') as gender,
                      age,
                      country,
                      city,
                      os,
                      source,
                      countIf(action = 'view') as views,
                      countIf(action = 'like') as likes,
                      round(likes / views, 2) as ctr
               from simulator_20230520.feed_actions t1
               where user_id in
                   (select distinct(user_id)
                    from simulator_20230520.feed_actions)
               group by toDate(t1.time) as date,
                        t1.user_id,
                        if(gender=1, 'male', 'female') as gender,
                        age,
                        country,
                        city,
                        os,
                        source
               having toDate(time) between today() - 7 and yesterday()           
               order by date,
                        user_id) feed
            inner join
                      (select toDate(t2.time) as date,
                              t2.user_id,
                              count(reciever_id) as messages
                       from simulator_20230520.message_actions t2
                       group by toDate(t2.time) as date, 
                                t2.user_id
                       having toDate(time) between today() - 7 and yesterday() 
                       order by date) mes on feed.date = mes.date and feed.user_id = mes.user_id
                       order by feed.date,
                       feed.user_id 
            '''
        data_charts = ph.read_clickhouse(query=query, connection=connection)
        return data_charts
           

        
    @task
    # посчитаем метрики на пользователя, сформируем отчет для графиков в бот 
    def report_charts(data_charts, chat_id=None):
        chat_id = -938659451
        bot = telegram.Bot(token='5981322866:AAE-nxcLJKljsaeR5WC00bYHTdmMGeg8duA')
                
        data_charts['views_per_user'] = (data_charts['views'] / data_charts['user_id'].nunique()).round(2)
        data_charts['likes_per_user'] = (data_charts['likes'] / data_charts['user_id'].nunique()).round(2)
        data_charts['messages_per_user'] = (data_charts['messages'] / data_charts['user_id'].nunique()).round(2)
                       
        data_charts_groupe = data_charts.groupby('date', as_index=False).agg({'user_id' : 'count', 
                                                                              'views' : 'sum',
                                                                              'likes' : 'sum', 
                                                                              'ctr' : 'mean',
                                                                              'views_per_user' : 'sum',
                                                                              'likes_per_user' : 'sum',
                                                                              'messages': 'sum',
                                                                              'messages_per_user': 'sum'
                                                                            })
        
        
        fig, axes = plt.subplots(2, 3, figsize=(20, 14))

        fig.suptitle('Динамика показателей за последнюю неделю', fontsize=30)

        sns.lineplot(ax = axes[0, 0], data = data_charts_groupe, x = 'date', y = 'user_id')
        axes[0, 0].set_title('DAU')
        axes[0, 0].grid()

        sns.lineplot(ax = axes[0, 1], data = data_charts_groupe, x = 'date', y = 'views')
        axes[0, 1].set_title('Просмотры')
        axes[0, 1].grid()

        sns.lineplot(ax = axes[0, 2], data = data_charts_groupe, x = 'date', y = 'likes')
        axes[0, 2].set_title('Лайки')
        axes[0, 2].grid()

        sns.lineplot(ax = axes[1, 0], data = data_charts_groupe, x = 'date', y = 'ctr')
        axes[1, 0].set_title('CTR')
        axes[1, 0].grid()

        sns.lineplot(ax = axes[1, 1], data = data_charts_groupe, x = 'date', y = 'views_per_user')
        axes[1, 1].set_title('Views per user')
        axes[1, 1].grid()

        sns.lineplot(ax = axes[1, 2], data = data_charts_groupe, x = 'date', y = 'likes_per_user')
        axes[1, 2].set_title('Likes per user')
        axes[1, 2].grid()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Metrics.png'
        plt.close()
            
         
        bot.sendPhoto(chat_id=chat_id, photo=plot_object) 
        return plot_object

    data_text = create_data()
    data_charts  = create_charts()
    message = report_message(data_text)
    report_message(data_text, chat_id)
    report_charts(data_charts, chat_id)

        
    
report_alerts_7_2 = report_alerts_7_2()