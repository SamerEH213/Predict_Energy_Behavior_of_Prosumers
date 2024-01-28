from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine, select, Table, MetaData
import requests as req
import pandas as pd
import numpy as np
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import json
from airflow.exceptions import AirflowNotFoundException
from io import StringIO
import scipy.stats
import pickle
from sklearn.ensemble import RandomForestRegressor

locations = pd.read_csv(Variable.get('_e_location'))


def check_existence(table_name,c):
    c.execute(
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' ) AS table_exists")
    exists = c.fetchall()[0][0]
    return exists


def get_data(ti):

    try:
        postgres = PostgresHook(postgres_conn_id="_energy_db")
        conn = postgres.get_conn()

        with conn.cursor() as c:
            work_time = Variable.get('working_time')
            datetime_format = "%Y-%m-%d %H:%M:%S"
            date_format = "%Y-%m-%d"


            # Convert the string to a datetime using strptime
            datetime_object = datetime.strptime(work_time, datetime_format)
            work_hour = datetime_object.hour
            work_day_string = datetime_object.strftime(date_format)
            zero_hour = datetime_object - timedelta(hours=work_hour)
            zero_hour_string = zero_hour.strftime(datetime_format)
            zero_hour = zero_hour.hour


            last_day = datetime_object - timedelta(days=1)
            last_day_string = last_day.strftime(date_format)

            last_week = datetime_object - timedelta(days=7)
            last_week_string = last_week.strftime(date_format)

            table_name = 'electricity'
            #if (check_existence(table_name, c))
            c.execute(f"SELECT * FROM {table_name} WHERE forecast_date = '{work_time}'")
            columns = [desc[0] for desc in c.description]
            e_data = c.fetchall()
            e_df = pd.DataFrame(e_data, columns=columns)
            #else:
            #print('ERROR: TABLE NOT EXISTS, NAME: ', table_name)

            table_name = 'hist_weather'
            c.execute(f"SELECT * FROM {table_name} WHERE datetime = '{work_time}'")
            columns = [desc[0] for desc in c.description]
            h_data = c.fetchall()
            h_df = pd.DataFrame(h_data,columns = columns)


            table_name = 'train'
            if (check_existence(table_name, c)):
                c.execute(f"SELECT * FROM {table_name} WHERE datetime = '{work_time}'")
                columns = [desc[0] for desc in c.description]
                t_data = c.fetchall()
                t_df = pd.DataFrame(t_data,columns = columns)
            else:
                print('ERROR: TABLE NOT EXISTS, NAME: ', table_name)

            table_name = 'forecast_weather'
            if (check_existence(table_name, c)):
                # c.execute(f"SELECT * FROM {table_name} WHERE cast(forecast_datetime AS DATE) = '{work_day_string}'")
                c.execute(f"SELECT * FROM {table_name} WHERE forecast_datetime = '{work_time}'")
                columns = [desc[0] for desc in c.description]
                f_data = c.fetchall()
                f_df = pd.DataFrame(f_data, columns=columns)
            else:
                print('ERROR: TABLE NOT EXISTS, NAME: ', table_name)

            if (zero_hour == 0):
                table_name = 'client'
                if (check_existence(table_name, c)):
                    c.execute(f"SELECT * FROM {table_name} WHERE date = '{zero_hour_string}'")
                    columns = [desc[0] for desc in c.description]
                    c_data = c.fetchall()
                    c_df = pd.DataFrame(c_data, columns=columns)
                else:
                    print('ERROR: TABLE NOT EXISTS, NAME: ', table_name)

                table_name = 'gas_price'
                if (check_existence(table_name, c)):
                    c.execute(f"SELECT * FROM {table_name} WHERE forecast_date = '{zero_hour_string}'")
                    columns = [desc[0] for desc in c.description]
                    g_data = c.fetchall()
                    g_df = pd.DataFrame(g_data, columns=columns)
                else:
                    print('ERROR: TABLE NOT EXISTS, NAME: ', table_name)

                table_name = 'train'
                if (check_existence(table_name, c)):
                    c.execute(f"SELECT * FROM {table_name} WHERE cast(datetime AS DATE) >= '{last_week_string}' AND cast(datetime AS DATE) < '{work_day_string}'")
                    columns = [desc[0] for desc in c.description]
                    last_data = c.fetchall()
                    last_df = pd.DataFrame(last_data, columns=columns)
                    t_df = pd.concat([t_df,last_df],axis=0)
                    print('SIZEEE === ', t_df.shape)
                else:
                    print('ERROR: TABLE NOT EXISTS, NAME: ', table_name)
            conn.commit()


            final_df = preprocessing_data(t_df, c_df, locations, f_df, e_df, g_df)
            print(final_df.info())
            with open(Variable.get('dataframe_pkl'), 'wb') as file:
                 pickle.dump(final_df, file)
        ti.xcom_push(key='task1_status',value='Succeed')
    except AirflowNotFoundException:
        ti.xcom_push(key='task1_status', value='Failed')
        print("Postrgesql database not found")



def preprocessing_client(client):
    client['data_block_id'] -= 2
    return client

def preprocessing_weather(weather, locations):
    hist_weather = weather[['temperature', 'dewpoint', 'cloudcover_total', 'cloudcover_low', 'latitude', 'longitude',
                            'cloudcover_mid', 'cloudcover_high', 'direct_solar_radiation', 'snowfall', 'forecast_datetime',
                            'data_block_id']]
    locations = locations.drop('Unnamed: 0', axis=1)
    hist_weather[['latitude', 'longitude']] = hist_weather[['latitude', 'longitude']].astype(float).round(1)

    hist_weather = hist_weather.merge(locations, how='left', on=['longitude', 'latitude'])
    hist_weather.dropna(axis=0, inplace=True)
    hist_weather.drop(['latitude', 'longitude'], axis=1, inplace=True)
    hist_weather['county'] = hist_weather['county'].astype('int64')
    hist_weather['forecast_datetime'] = pd.to_datetime(hist_weather['forecast_datetime'], utc=True)
    hist_weather_datetime = hist_weather.groupby([hist_weather['forecast_datetime'].dt.to_period('h')])[
        list(hist_weather.drop(['county', 'forecast_datetime', 'data_block_id'], axis=1).columns)].mean().reset_index()
    hist_weather_datetime['forecast_datetime'] = pd.to_datetime(hist_weather_datetime['forecast_datetime'].dt.to_timestamp(), utc=True)
    hist_weather_datetime = hist_weather_datetime.merge(hist_weather[['forecast_datetime', 'data_block_id']], how='left',
                                                        on='forecast_datetime')
    hist_weather_datetime_county = hist_weather.groupby(['county', hist_weather['forecast_datetime'].dt.to_period('h')])[
        list(hist_weather.drop(['county', 'forecast_datetime', 'data_block_id'], axis=1).columns)].mean().reset_index()
    hist_weather_datetime_county['forecast_datetime'] = pd.to_datetime(
        hist_weather_datetime_county['forecast_datetime'].dt.to_timestamp(), utc=True)

    hist_weather_datetime_county = hist_weather_datetime_county.merge(hist_weather[['forecast_datetime', 'data_block_id']],
                                                                      how='left', on='forecast_datetime')
    hist_weather_datetime['hour'] = hist_weather_datetime['forecast_datetime'].dt.hour
    hist_weather_datetime_county['hour'] = hist_weather_datetime_county['forecast_datetime'].dt.hour
    hist_weather_datetime.drop_duplicates(inplace=True)
    hist_weather_datetime_county.drop_duplicates(inplace=True)
    hist_weather_datetime.drop('forecast_datetime', axis=1, inplace=True)
    hist_weather_datetime_county.drop('forecast_datetime', axis=1, inplace=True)
    return (hist_weather_datetime_county, hist_weather_datetime)


def preprocessing_electricity(electricity):
    electricity = electricity.rename(columns={'forecast_date': 'datetime'})
    electricity['datetime'] = pd.to_datetime(electricity['datetime'], utc=True)
    electricity['hour'] = electricity['datetime'].dt.hour
    return electricity


def create_n_day_lags(data, N_day_lags):

    original_datetime = data['datetime']
    revealed_targets = data[['datetime', 'prediction_unit_id', 'is_consumption', 'target']].copy()
    for day_lag in range(2, N_day_lags + 1):

        revealed_targets['datetime'] = original_datetime + pd.DateOffset(day_lag)
        data = data.merge(revealed_targets,
                          how='left', on=['datetime', 'prediction_unit_id', 'is_consumption'],
                          suffixes=('', f'_{day_lag}_days_ago'))
    data['sin_hour'] = (np.pi * np.sin(data['hour']) / 12)
    data['cos_hour'] = (np.pi * np.cos(data['hour']) / 12)
    data['target_mean'] = data[[f'target_{i}_days_ago' for i in range(2, N_day_lags + 1)]].mean(1)
    data['target_std'] = data[[f'target_{i}_days_ago' for i in range(2, N_day_lags + 1)]].std(1)
    data['target_var'] = data[[f'target_{i}_days_ago' for i in range(2, N_day_lags + 1)]].var(1)
    print(data.info())
    return data


def skew_data(data):
    skew_df = pd.DataFrame(data.select_dtypes(np.number).columns, columns=['Feature'])
    skew_df['Skew'] = skew_df['Feature'].apply(lambda feature: scipy.stats.skew(data[feature]))
    skew_df['Absolute Skew'] = skew_df['Skew'].apply(abs)
    skew_df['Skewed'] = skew_df['Absolute Skew'].apply(lambda x: True if x >= 0.5 else False)
    skew_df = skew_df[~skew_df['Feature'].isin(['year', 'prediction_unit_id'])]
    columns = skew_df[skew_df['Skewed'] == True].Feature.values
    data = distribution_preprocessing(data.copy(), columns)
    return (data)


def distribution_preprocessing(data, columns):
    for i in columns:
        data[f"{i}"] = np.where((data[i]) != 0, np.log(data[i]), 0)
        return data


def preprocessing_data(data, client, locations, weather, electricity, gas):
    client = preprocessing_client(client.copy())
    electricity = preprocessing_electricity(electricity.copy())
    hist_weather_datetime_county, hist_weather_datetime = preprocessing_weather(weather.copy(),
                                                                                     locations.copy())
    electricity['data_block_id'] -= 1
    gas['data_block_id'] -= 1
    data = data[data['target'].notnull()]
    data['datetime'] = pd.to_datetime(data['datetime'], utc=True)
    data['year'] = data['datetime'].dt.year
    data['month'] = data['datetime'].dt.month
    data['day'] = data['datetime'].dt.day
    data['hour'] = data['datetime'].dt.hour
    data = data.merge(client.drop(columns=['date']), how='left',
                      on=['data_block_id', 'county', 'is_business', 'product_type'])
    data = data.merge(gas[['data_block_id', 'lowest_price_per_mwh', 'highest_price_per_mwh']], how='left',
                      on='data_block_id')

    data = data.merge(electricity[['euros_per_mwh', 'hour', 'data_block_id']], how='left', on=['hour', 'data_block_id'])

    data = data.merge(hist_weather_datetime, how='left', on=['data_block_id', 'hour'])

    data = data.merge(hist_weather_datetime_county, how='left', on=['data_block_id', 'county', 'hour'],
                      suffixes=('_hist_mean', '_hist_mean_by_county'))

    data = create_n_day_lags(data.copy(), 7)
    data = skew_data(data)
    data.drop(['row_id','datetime','prediction_unit_id', 'data_block_id'], axis=1, inplace=True)
    data.dropna(inplace=True,axis=0)
    return data

def model_predict(ti):
    ti.xcom_push(key='task2_status', value='Failed')
    with open(Variable.get('dataframe_pkl'), 'rb') as file:
        loaded_data = pickle.load(file)

    #print(loaded_data.datetime.min())
    print(loaded_data.info())
    print(loaded_data.shape)
    consume_data = loaded_data[loaded_data['is_consumption'] == 1]
    generate_data = loaded_data[loaded_data['is_consumption'] == 0]
    with open(Variable.get('model_consume_pkl'), 'rb') as file:
        model_consume = pickle.load(file)

    with open(Variable.get('model_generate_pkl'), 'rb') as file:
        model_generate = pickle.load(file)

    consume_data_new = consume_data.drop('target',axis=1)
    generate_data_new = generate_data.drop('target',axis=1)
    try:
        c_pred_target = model_consume.predict(consume_data_new)
        g_pred_target = model_generate.predict(generate_data_new)
        consume_data['target_predicting'] = np.exp(c_pred_target)
        generate_data['target_predicting'] = np.exp(g_pred_target)

        final_df = pd.concat([consume_data, generate_data], axis=0)

        with open(Variable.get('final_df_pkl'), 'wb') as file:
            ti.xcom_push(key='task2_status', value='Succeed')
            pickle.dump(final_df, file)
    except ValueError as e:
        ti.xcom_push(key='task2_status', value='Failed')
        print(e)


def upload_transformed_data(ti):
    try:
        postgres = PostgresHook(postgres_conn_id="_energy_db")
        conn = postgres.get_conn()

        with conn.cursor() as c:
            sio = StringIO()
            try:
                with open(Variable.get('final_df_pkl'), 'rb') as file:
                    final_df = pickle.load(file)
                    final_df['is_business'] = final_df['is_business'].astype('int')
                    final_df['is_consumption'] = final_df['is_consumption'].astype('int')
                    ti.xcom_push(key='upload_count', value=f', {final_df.shape[0]} rows was uploaded')
                    final_df.to_csv(sio, index=None, header=None)
                sio.seek(0)
                print("UPLOOOAAAAAAAAAAADDDDDINNGGGGGG")
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
                       target_mean, target_std, target_var,target_predicting) FROM STDIN WITH CSV""",
                              file=sio)
                conn.commit()
                work_time = Variable.get('working_time')
                datetime_format = "%Y-%m-%d %H:%M:%S"

                # Convert the string to a datetime using strptime
                datetime_object = datetime.strptime(work_time, datetime_format)

                next_hour = datetime_object + timedelta(hours=1)
                next_hour_string = next_hour.strftime(datetime_format)

                ti.xcom_push(key='working_time', value= Variable.get('working_time'))
                Variable.update(key='working_time', value=next_hour_string)
                ti.xcom_push(key='task3_status', value='Succeed')
            except Exception as e:
                ti.xcom_push(key='task3_status', value='Failed')
                ti.xcom_push(key='upload_count', value='')
                print('ERROR WHILE UPLOADING', e)

    except AirflowNotFoundException:
        ti.xcom_push(key='task3_status',value='Failed')
        ti.xcom_push(key='upload_count', value='')
        print("Postrgesql database not found")

with DAG(
        dag_id='_energy_get_data2',
        schedule_interval=timedelta(hours=1),
        # start_date=datetime(2023,12,30,10,0)
        start_date=datetime(2023, 1, 1),
        catchup=False
) as dag:
    task_1 = PythonOperator(
        task_id='get_data_',
        python_callable=get_data,
        #do_xcom_push=True
    )
    task_2 = PythonOperator(
        task_id='model_predict_',
        python_callable=model_predict,
        #do_xcom_push=True
    )
    task_3 = PythonOperator(
        task_id='upload_transformed_data_',
        python_callable=upload_transformed_data,
        #do_xcom_push=True
    )
    report = EmailOperator(
        task_id = 'report_',
        to='altlawy19@gmail.com',
        subject='ETL ALERT',
        html_content = f"""<p>Your Airflow (get_data_dag) job has finished</p>
        <p>Started at {{{{ti.xcom_pull(key='working_time')}}}}</p>
        <p>Tasks Status ==>>></p>
        <p>Task 1 (get_data): {{{{ti.xcom_pull(key='task1_status')}}}}</p>
        <p>Task 2 (model_predict): {{{{ti.xcom_pull(key='task2_status')}}}}</p>
        <p>Task 3 (upload_transformed_data): {{{{ti.xcom_pull(key='task3_status')}}}} 
        {{{{ti.xcom_pull(key='upload_count')}}}}</p>"""
    )

task_1 >> task_2 >> task_3 >> report