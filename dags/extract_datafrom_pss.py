from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from xml.etree import ElementTree
from bs4 import BeautifulSoup
import urllib
import xmltodict
from datetime import datetime , timedelta

url = "http://192.168.1.128:80/services/user/records.xml/?begin=31012024230000&end=31042024230000&var=R$CAL_Saisi de la production.BAHIA_1_5_L 4_DH_AP&period=900"
HTTPS_STATUS_OK=200


def get_data_from_pss(**kwargs):
    response=requests.get(url)
    response.raise_for_status()
    xml_data=response.content
    soup=BeautifulSoup(xml_data,'xml')
    print(xml_data)
    with open("/opt/airflow/data/test.csv","w") as file:
        file.write("datetime,Saisie_prod_Bahia_1_5\n")

        for record in soup.find_all('record'):

            dateTime=record.find('dateTime').text
            value=record.find("value").text
            dt = datetime.strptime(dateTime, '%d%m%Y%H%M%S%f')
            formatted_dt = dt.strftime('%Y-%m-%d %H:%M:%S.%f')

            # Écrire dans le fichier
            file.write(f"{formatted_dt}, {value}\n")


default_args={

    'owner':'airflow',
    'retries': 5,
    'email_on_failure':False,
    'email_on_retry':False,
    'retry_daily':timedelta(minutes=5)
}

with DAG(dag_id='get_data_from_pss', default_args=default_args, description="A test for data analysing", start_date=datetime(2024, 5, 27), schedule_interval='0 0 * * *') as dag:
    task1 = PythonOperator(
        task_id='fetch_and_store_data',
        provide_context=True,
        python_callable=get_data_from_pss,
        dag=dag,
    )

    task1