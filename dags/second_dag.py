from datetime import datetime, timedelta
import random
import statistics

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'test',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# Fonction pour générer une liste de nombres aléatoires
def generate_random_numbers(ti, n=100):
    random_numbers = [random.randint(1, 100) for _ in range(n)]
    ti.xcom_push(key='random_numbers', value=random_numbers)

# Fonction pour calculer la moyenne et la variance
def calculate_statistics(ti):
    random_numbers = ti.xcom_pull(task_ids='generate_random_numbers', key='random_numbers')
    mean = statistics.mean(random_numbers)
    variance = statistics.variance(random_numbers)
    ti.xcom_push(key='mean', value=mean)
    ti.xcom_push(key='variance', value=variance)

# Fonction pour afficher les statistiques calculées
def print_statistics(ti):
    mean = ti.xcom_pull(task_ids='calculate_statistics', key='mean')
    variance = ti.xcom_pull(task_ids='calculate_statistics', key='variance')
    print(f"Les nombres générés ont une moyenne de {mean} et une variance de {variance}.")

with DAG(
    default_args=default_args,
    dag_id='statistical_calculations_dag',
    description='DAG pour effectuer des calculs statistiques et probabilistes',
    start_date=datetime(2021, 10, 6),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='generate_random_numbers',
        python_callable=generate_random_numbers,
        op_kwargs={'n': 100}  # Vous pouvez changer n pour générer un autre nombre de nombres aléatoires
    )

    task2 = PythonOperator(
        task_id='calculate_statistics',
        python_callable=calculate_statistics
    )

    task3 = PythonOperator(
        task_id='print_statistics',
        python_callable=print_statistics
    )

    task1 >> task2 >> task3
