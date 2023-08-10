import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime
import csv
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_data():
    data = pd.read_csv('google table')
    return data

def process_data(**kwargs):
    ti = kwargs["ti"]
    ti.xcom_push("data", get_data())

    

def print_data(ti):
    data = ti.xcom_pull(task_ids="get_data", key="data")  
    species = data.Вид.unique().tolist()
    species.remove('отловы временно прекращены')
    species.remove('отъезд в Сайгон')
    

    captures = data.query('Вид not in ["отловы временно прекращены", "отъезд в Сайгон"] & Клюв != "повтор"')\
    .groupby('Вид', as_index = False).agg({'Дата':'count'})\
    .rename(columns = {'Дата':'Кол-во'})
    
    print(*sorted(species), sep = '\n') #Перечень пойманных видов
    print
    print(f'Всего поймано видов: {len(species)}')
    print
    print(captures)

def save_data(ti):
    data = ti.xcom_pull(task_ids="get_data", key="data")
    with open('bidoup.csv', 'w') as f:
        f.write(data.to_csv())


default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 16),
}
schedule_interval = '0 12 * * *'

dag = DAG('bidoup_captures', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=process_data,
                    dag=dag)

t2 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

        
t3 = PythonOperator(task_id='save_data',
                    python_callable=save_data,
                    dag=dag)

t1 >> t2 >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
