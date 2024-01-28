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
import json
from io import StringIO
from airflow.exceptions import AirflowNotFoundException
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


c_df = pd.read_csv(Variable.get('_e_client'))
e_df = pd.read_csv(Variable.get('_e_electric'))
f_df = pd.read_csv(Variable.get('_e_forecast'))
g_df = pd.read_csv(Variable.get('_e_gas'))
h_df = pd.read_csv(Variable.get('_e_historical'))
t_df = pd.read_csv(Variable.get('_e_train'))

trans_df = pd.read_csv(Variable.get('_e_transformed'))

def upload_transform():
    postgres = PostgresHook(postgres_conn_id="_energy_db")
    conn = postgres.get_conn()

    with conn.cursor() as c:
        work_time = Variable.get('working_time')
        datetime_format = "%Y-%m-%d %H:%M:%S"
        date_format = "%Y-%m-%d"
        print(work_time)
        # Convert the string to a datetime using strptime
        datetime_object = datetime.strptime(work_time, datetime_format)
        work_hour = datetime_object.hour
        work_day_string = datetime_object.strftime(date_format)

        last_day = datetime_object - timedelta(days=1)
        last_day_string = last_day.strftime(date_format)

        last_week = datetime_object - timedelta(days=7)
        last_week_string = last_week.strftime(datetime_format)

        # e_current_data = e_df
        # print(e_current_data)
        sio = StringIO()
        trans_df.to_csv(sio, index=None, header=None)
        sio.seek(0)
        c.copy_expert(sql="""COPY transformed_data(
                                county, is_business, product_type, target, is_consumption,
                               year, month, day, hour, eic_count, installed_capacity,
                               lowest_price_per_mwh, highest_price_per_mwh, euros_per_mwh,
                               temperature_hist_mean, dewpoint_hist_mean,
                               cloudcover_total_hist_mean, cloudcover_low_hist_mean,
                               cloudcover_mid_hist_mean, cloudcover_high_hist_mean,
                               direct_solar_radiation_hist_mean, snowfall_hist_mean,
                               temperature_hist_mean_by_county, dewpoint_hist_mean_by_county,
                               cloudcover_total_hist_mean_by_county,
                               cloudcover_low_hist_mean_by_county,
                               cloudcover_mid_hist_mean_by_county,
                               cloudcover_high_hist_mean_by_county,
                               direct_solar_radiation_hist_mean_by_county,
                               snowfall_hist_mean_by_county, target_2_days_ago,
                               target_3_days_ago, target_4_days_ago, target_5_days_ago,
                               target_6_days_ago, target_7_days_ago, sin_hour, cos_hour,
                               target_mean, target_std, target_var) FROM STDIN WITH CSV""",
                      file=sio)


        conn.commit()

def upload_one_time():
    #try:
    postgres = PostgresHook(postgres_conn_id="_energy_db")
    conn = postgres.get_conn()

    with conn.cursor() as c:
        work_time = Variable.get('working_time')
        datetime_format = "%Y-%m-%d %H:%M:%S"
        date_format = "%Y-%m-%d"
        print(work_time)
        # Convert the string to a datetime using strptime
        datetime_object = datetime.strptime(work_time, datetime_format)
        work_hour = datetime_object.hour
        work_day_string = datetime_object.strftime(date_format)

        last_day = datetime_object - timedelta(days=1)
        last_day_string = last_day.strftime(date_format)

        last_week = datetime_object - timedelta(days=7)
        last_week_string = last_week.strftime(datetime_format)

        e_current_data = e_df.query(f"forecast_date <'{work_time}'")
        #e_current_data = e_df
        #print(e_current_data)
        sio = StringIO()
        e_current_data.to_csv(sio, index=None, header=None)
        sio.seek(0)
        c.copy_expert(
            sql="COPY electricity(forecast_date,euros_per_mwh,origin_date,data_block_id) From STDIN WITH CSV",
            file=sio)

        sio = StringIO()

        h_current_data = h_df.query(f"datetime < '{work_time}'")
        #h_current_data = h_df
        #print(h_current_data)
        h_current_data.to_csv(sio, index=None, header=None)
        sio.seek(0)
        c.copy_expert(sql="""COPY hist_weather(datetime, temperature, dewpoint, rain, snowfall,
                       surface_pressure, cloudcover_total, cloudcover_low,
                       cloudcover_mid, cloudcover_high, windspeed_10m,
                       winddirection_10m, shortwave_radiation, direct_solar_radiation,
                       diffuse_radiation, latitude, longitude, data_block_id) FROM STDIN WITH CSV""", file=sio)


        sio = StringIO()

        t_current_data = t_df.query(f"datetime < '{work_time}'")
        #t_current_data = t_df

        t_current_data.to_csv(sio, index=None, header=None)
        sio.seek(0)
        table_name = 'train'
        if (check_existence(table_name, c)):
            c.copy_expert(sql=f"""COPY {table_name}(county, is_business, product_type, target, is_consumption,
                                                datetime, data_block_id,row_id, prediction_unit_id) FROM STDIN WITH CSV""",
                          file=sio)
        if (work_hour == 0):
            c_current_data = c_df.query(f"date < '{work_day_string}'")
            #c_current_data = c_df
            sio = StringIO()

            c_current_data.to_csv(sio, index=None, header=None)
            sio.seek(0)
            table_name = 'client'
            c.copy_expert(
                sql=f"""COPY {table_name}(product_type,county,eic_count,installed_capacity,is_business,date,data_block_id) From STDIN WITH CSV""",
                file=sio)

            sio = StringIO()

            g_current_data = g_df.query(f"forecast_date < '{work_day_string}'")
            #g_current_data = g_df
            g_current_data.to_csv(sio, index=None, header=None)
            sio.seek(0)
            table_name = 'gas_price'
            c.copy_expert(sql=f"""COPY {table_name}(forecast_date, lowest_price_per_mwh, highest_price_per_mwh,
                                       origin_date, data_block_id) FROM STDIN WITH CSV""", file=sio)

            sio = StringIO()

            f_df['date'] = f_df['forecast_datetime']
            f_df['date'] = pd.to_datetime(f_df['date'])
            print(f_df.shape)
            f_current_data = f_df.query(f"date < '{work_day_string}'")
            #f_current_data = f_df
            f_current_data.drop(['date'], inplace=True, axis=1)
            f_current_data.to_csv(sio, index=None, header=None)
            sio.seek(0)
            table_name = 'forecast_weather'
            c.copy_expert(sql=f"""COPY {table_name}(latitude, longitude, origin_datetime, hours_ahead,
                               temperature, dewpoint, cloudcover_high, cloudcover_low,
                               cloudcover_mid, cloudcover_total, _10_metre_u_wind_component,
                               _10_metre_v_wind_component, data_block_id, forecast_datetime,
                               direct_solar_radiation, surface_solar_radiation_downwards,
                               snowfall, total_precipitation) FROM STDIN WITH CSV""", file=sio)

        conn.commit()

def check_existence(table_name,c):
    c.execute(
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' ) AS table_exists")
    exists = c.fetchall()[0][0]
    return exists

def upload_data(**kwargs):
    ### HANDLE USING LOG FILE
    execution_date = kwargs['execution_date']

    # Perform your tasks using the execution date/time
    print(f"Current execution date/time: {execution_date}")
    try:
        postgres = PostgresHook(postgres_conn_id="_energy_db")
        conn = postgres.get_conn()
        with conn.cursor() as c:
            variable_key = 'working_time'
            try:
                work_time = Variable.get(variable_key)
                print(f"Value of {variable_key}: {work_time}")

            except KeyError:
                work_time = execution_date
                print(f"Variable with key {variable_key} does not exist.")

            except Exception as e:
                work_time = execution_date
                print(f"An error occurred: {e}")

            datetime_format = "%Y-%m-%d %H:%M:%S"
            date_format = "%Y-%m-%d"

            datetime_object = datetime.strptime(work_time, datetime_format)
            work_hour = datetime_object.hour
            work_day_string = datetime_object.strftime(date_format)


            e_current_data = e_df.query(f"forecast_date =='{work_time}'")
            sio = StringIO()
            if(e_current_data.shape[0]!=0):
                e_current_data.to_csv(sio, index=None, header=None)
                sio.seek(0)
                table_name = 'electricity'
                if(check_existence(table_name,c)):
                    c.copy_expert(
                        sql=f"COPY {table_name}(forecast_date,euros_per_mwh,origin_date,data_block_id) From STDIN WITH CSV",
                        file=sio)
                else:
                    print('ERROR: TABLE NOT EXISTS, NAME: ',table_name)
            else:
                print('ERROR: empty query, Name:electricity')
            sio = StringIO()

            h_current_data = h_df.query(f"datetime =='{work_time}'")
            if(h_current_data.shape[0]!=0):
                h_current_data.to_csv(sio, index=None, header=None)
                sio.seek(0)
                table_name = 'hist_weather'
                if(check_existence(table_name,c)):
                    c.copy_expert(sql=f"""COPY {table_name}(datetime, temperature, dewpoint, rain, snowfall,
                       surface_pressure, cloudcover_total, cloudcover_low,
                       cloudcover_mid, cloudcover_high, windspeed_10m,
                       winddirection_10m, shortwave_radiation, direct_solar_radiation,
                       diffuse_radiation, latitude, longitude, data_block_id) FROM STDIN WITH CSV""", file=sio)
                else:
                    print('ERROR: TABLE NOT EXISTS, NAME: ',table_name)
            else:
                print('ERROR: empty query, Name:hist_weather')
            sio = StringIO()

            t_current_data = t_df.query(f"datetime =='{work_time}'")
            if (t_current_data.shape[0] != 0):
                t_current_data.to_csv(sio, index=None, header=None)
                sio.seek(0)
                table_name='train'
                if(check_existence(table_name,c)):
                    c.copy_expert(sql=f"""COPY {table_name}(county, is_business, product_type, target, is_consumption,
                                            datetime, data_block_id,row_id, prediction_unit_id) FROM STDIN WITH CSV""",
                                  file=sio)
                else:
                    print('ERROR: TABLE NOT EXISTS, NAME: ',table_name)
            else:
                print('ERROR: empty query, Name:train')

            sio = StringIO()

            f_current_data = f_df.query(f"forecast_datetime == '{work_time}'")
            if (f_current_data.shape[0] != 0):
                f_current_data.to_csv(sio, index=None, header=None)
                sio.seek(0)
                table_name = 'forecast_weather'
                if (check_existence(table_name, c)):
                    c.copy_expert(sql=f"""COPY {table_name}(latitude, longitude, origin_datetime, hours_ahead,
                                           temperature, dewpoint, cloudcover_high, cloudcover_low,
                                           cloudcover_mid, cloudcover_total, _10_metre_u_wind_component,
                                           _10_metre_v_wind_component, data_block_id, forecast_datetime,
                                           direct_solar_radiation, surface_solar_radiation_downwards,
                                           snowfall, total_precipitation) FROM STDIN WITH CSV""", file=sio)
                else:
                    print('ERROR: TABLE NOT EXISTS, NAME: ', table_name)
            else:
                print('ERROR: empty query, Name:forecast_weather')
            if (work_hour == 0):
                c_current_data = c_df.query(f"date =='{work_day_string}'")
                sio = StringIO()

                if (c_current_data.shape[0] != 0):

                    c_current_data.to_csv(sio, index=None, header=None)
                    sio.seek(0)
                    table_name = 'client'
                    if (check_existence(table_name, c)):
                        c.copy_expert(
                           sql=f"""COPY {table_name}(product_type,county,eic_count,installed_capacity,is_business,date,data_block_id) From STDIN WITH CSV""",
                            file=sio)
                    else:
                        print('ERROR: TABLE NOT EXISTS, NAME: ', table_name)
                else:
                    print('ERROR: empty query, Name:client')

                sio = StringIO()

                g_current_data = g_df.query(f"forecast_date =='{work_day_string}'")
                if (g_current_data.shape[0] != 0):
                    g_current_data.to_csv(sio, index=None, header=None)
                    sio.seek(0)
                    table_name = 'gas_price'
                    if (check_existence(table_name, c)):
                        c.copy_expert(sql=f"""COPY {table_name}(forecast_date, lowest_price_per_mwh, highest_price_per_mwh,
                                       origin_date, data_block_id) FROM STDIN WITH CSV""", file=sio)
                    else:
                        print('ERROR: TABLE NOT EXISTS, NAME: ', table_name)
                else:
                    print('ERROR: empty query, Name:gas_price')

            conn.commit()

    except AirflowNotFoundException as e:
        print(f"Postrgesql database not found Error:{e}")


default_args = {
    'owner': 'moayad',
    'email': ['altlawy19@gmail.com'],
    'email_on_failure': True
}

with DAG(
        dag_id='_energy_upload_data_v01',
        schedule_interval=timedelta(hours=1),
        # start_date=datetime(2023,12,30,10,0)
        start_date=datetime(2023, 1, 1),
        default_args=default_args,
        catchup=False
) as dag:
    check_file_1 = FileSensor(
        task_id='c_file_sense_',
        filepath = Variable.get('_e_client'),
        poke_interval = 1,
        timeout=10
    )
    check_file_2 = FileSensor(
        task_id='e_file_sense_',
        filepath = Variable.get('_e_electric'),
        poke_interval = 1,
        timeout=30
    )
    check_file_3 = FileSensor(
        task_id='f_file_sense_',
        filepath = Variable.get('_e_forecast'),
        poke_interval = 1,
        timeout=30
    )
    check_file_4 = FileSensor(
        task_id='g_file_sense_',
        filepath = Variable.get('_e_gas'),
        poke_interval = 1,
        timeout=30
    )
    check_file_5 = FileSensor(
        task_id='h_file_sense_',
        filepath = Variable.get('_e_historical'),
        poke_interval = 1,
        timeout=30
    )
    check_file_6 = FileSensor(
        task_id='t_file_sense_',
        filepath = Variable.get('_e_train'),
        poke_interval = 1,
        timeout=30
    )
    #task_0 = PythonOperator(
    #    task_id='upload_once_data_',
    #    python_callable=upload_transform,
    #    provide_context=True
    #)
    task_1 = PythonOperator(
        task_id='upload_data_',
        python_callable=upload_data,
        provide_context=True
    )
    trigger_download = TriggerDagRunOperator(
        task_id="trigger_download_",
        trigger_dag_id="_energy_get_data2",
    )

[check_file_1,check_file_2,check_file_3,check_file_4,check_file_5,check_file_6] >> task_1 >> trigger_download

