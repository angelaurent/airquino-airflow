"""

"""
from datetime import datetime, timedelta
# import json
import logging
import requests
import pandas as pd
import pymongo
from airflow.operators.python_operator import PythonOperator

from airflow import DAG


# initialisons la DAG
dag = DAG(
    dag_id="airquino",
    start_date=datetime(2023, 12, 28),
    schedule_interval="0 * * * *",
    default_args={
            "owner": "Airflow",
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "email": "monemail@host.com",
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
    }
)


def extract_data():
    """
    azefdsddjklkkjhgfd
    """
    api_url = 'https://airqino-api.magentalab.it/v3/getStationHourlyAvg/283164601'
    # headers = {"Accept": "application/json"}
    # reponse = requests.get(api_url, headers=headers)
    reponse = requests.get(api_url)
    data = reponse.json().get("data")
    logging.info(f'Données extraites : {data}')
    return data


def transform(**kwargs):
    """
    qsdsefrgthgyjiklkjhgfd
    """
    data = kwargs['ti'].xcom_pull(task_ids='extract_tache')
    # logging.info(f'Données extraites : {data}')
    # print(data)
    data = pd.DataFrame(data)
    # logging.info(f'Données extraites : {data}')
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    # logging.info(f'Données extraites : {data}')
    data['date'] = data['timestamp'].dt.date
    # logging.info(f'Données extraites : {data}')
    data = data.groupby('date').agg({"CO": "mean", "PM2.5": "mean"})\
        .reset_index()
    logging.info(f'Données extraites : {data}')
#     # print("transformation effectuée avec succes")
    return data


def load_data(**kwargs):
    """
    adfghgjklosdfrtgyhujk
    """
    data = kwargs['ti'].xcom_pull(task_ids='transform_tache')
    logging.info(f'Données extraites : {data}')

    # data['date'] = data['date'].astype(str)
    # logging.info(f'Données extraites : {data}')
    # data = data.to_dict(orient='records')
    # logging.info(f'Données extraites : {data}')

    client = pymongo.MongoClient("mongodb://172.19.0.8:27017")
    db = client["air"]
    collection = db["station15"]
    try:
        data['date'] = data['date'].astype(str)
        logging.info(f'Données extraites : {data}')

        data = data.to_dict(orient='records')
        logging.info(f'Données extraites : {data}')

        collection.insert_many(data)
        print('bravo,vos données sont bien stockée')

    except Exception as e:
        print("desole l'insertion n'a pas ete effective")
        logging.error(f'Erreur dans la fonction transform_data : {e}')
    finally:
        client.close()


extract_tache = PythonOperator(
    task_id="extract_tache",
    python_callable=extract_data,
    dag=dag,
)

transform_tache = PythonOperator(
    task_id="transform_tache",
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_tache = PythonOperator(
    task_id="load_tache",
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)


extract_tache >> transform_tache >> load_tache

