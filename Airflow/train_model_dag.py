from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine, select, Table, MetaData
import requests as req
import pandas as pd
import numpy as np
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
# from airflow.utils.db import provide_session
# from airflow.models import XCom
import json
from airflow.exceptions import AirflowNotFoundException
from io import StringIO
import scipy.stats
import pickle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
## GET ALL ROWS FROM DATA BASE
## UPLOAD TO WAREHOUSE

locations = pd.read_csv(Variable.get('_e_location'))


def get_cons_data():

    try:
        postgres = PostgresHook(postgres_conn_id="_energy_db")
        conn = postgres.get_conn()

        with conn.cursor() as c:
            c.execute(f"SELECT * FROM transformed_data Where is_consumption = 1")
            columns = [desc[0] for desc in c.description]
            t_data = c.fetchall()
            t_df = pd.DataFrame(t_data, columns=columns)

            conn.commit()

            with open(Variable.get('final_df_cons_pkl'), 'wb') as file:
                 pickle.dump(t_df, file)

    except AirflowNotFoundException:
        print("Postrgesql database not found")


def get_gen_data():

    try:
        postgres = PostgresHook(postgres_conn_id="_energy_db")
        conn = postgres.get_conn()

        with conn.cursor() as c:
            c.execute(f"SELECT * FROM transformed_data Where is_consumption = 0")
            columns = [desc[0] for desc in c.description]
            t_data = c.fetchall()
            t_df = pd.DataFrame(t_data, columns=columns)

            conn.commit()

            with open(Variable.get('final_df_gen_pkl'), 'wb') as file:
                 pickle.dump(t_df, file)

    except AirflowNotFoundException:
        print("Postrgesql database not found")



def train_model():
    with open(Variable.get('final_df_cons_pkl'), 'rb') as file:
        data_cons = pickle.load(file)

    with open(Variable.get('final_df_gen_pkl'), 'rb') as file:
        data_gen = pickle.load(file)
    with pd.option_context('display.max_columns', None,
                           'display.precision', 3,
                           ):
        print(data_cons.info())
        print(data_gen.info())
    x_is_consumption = data_cons.drop(['target','target_predicting'], axis=1)
    y_is_consumption = data_cons['target']
    x_isnt_consumption = data_gen.drop(['target','target_predicting'], axis=1)
    y_isnt_consumption = data_gen['target']

    x_is_consumption_train, x_is_consumption_test, y_is_consumption_train, y_is_consumption_test = train_test_split(
        x_is_consumption, y_is_consumption, test_size=0.3, shuffle=True, random_state=5)
    x_isnt_consumption_train, x_isnt_consumption_test, y_isnt_consumption_train, y_isnt_consumption_test = train_test_split(
        x_isnt_consumption, y_isnt_consumption, test_size=0.3, shuffle=True, random_state=51)

    rf_is_consumption = RandomForestRegressor()
    rf_is_consumption.fit(x_is_consumption_train, y_is_consumption_train)

    rf_isnt_consumption = RandomForestRegressor()
    rf_isnt_consumption.fit(x_isnt_consumption_train, y_isnt_consumption_train)

    with open(Variable.get('model_consume_pkl'), 'wb') as file:
        pickle.dump(file)

    with open(Variable.get('model_generate_pkl'), 'wb') as file:
        pickle.dump(file)

with DAG(
        dag_id='_energy_train_model',
        schedule_interval=timedelta(days=30),
        # start_date=datetime(2023,12,30,10,0)
        start_date=datetime(2023, 2, 1),
        catchup=False
) as dag:
    task_1_1 = PythonOperator(
        task_id='get_cons_data_',
        python_callable=get_cons_data,
        #do_xcom_push=True
    )
    task_1_2 = PythonOperator(
        task_id='get_gen_data_',
        python_callable=get_gen_data,
        #do_xcom_push=True
    )
    task_2 = PythonOperator(
        task_id='train_model_',
        python_callable=train_model,
        #do_xcom_push=True
    )

task_1_1 >> task_2
task_1_2 >> task_2
