from airflow.decorators import dag, task
from datetime import datetime ,timedelta

default_args={
    'owner':'Nabil',
    "retries":5,
    'retry_delay':timedelta(minutes=5)
}

@dag(dag_id="taskflow_demo_v1",default_args=default_args,start_date=datetime(2024,5,27),schedule_interval='@daily' ) #"0 0 * * *")
def hello_word_etl():
    @ task()
    def get_name():
        return "Test"
    @task()
    def get_age():
        return 25
    def greet(name,age):
        print(f"{name} {age}")

    name = get_name()
    age = get_age()
    greet(name,age)

greet_dag=hello_word_etl()
