from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.trigger_rule import TriggerRule as tr
from datetime import datetime, timedelta
import random
import time


# Параметри DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
    # "retry_delay": timedelta(minutes=5),
}


# Функція для випадкового вибору медалі
def pick_medal():
    medals = ["Bronze", "Silver", "Gold"]
    return random.choice(medals)


# Функція для розгалуження задач
def choose_task_based_on_medal(chosen_medal, **kwargs):
    if chosen_medal == "Bronze":
        return "calc_Bronze"
    elif chosen_medal == "Silver":
        return "calc_Silver"
    elif chosen_medal == "Gold":
        return "calc_Gold"


# Функція для затримки
def delay_task():
    time.sleep(33)  # затримка в секундах


# SQL для сенсора
check_latest_record_sql = """
SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30
FROM preart_dag_results;
"""

# Визначення DAG
with DAG(
    dag_id="preart_dag",
    default_args=default_args,
    description="DAG для створення таблиці, розгалуження задач та перевірки часу останнього запису",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Створення таблиці (якщо вона не існує)
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id="mysql_default",
        sql="""
        CREATE TABLE IF NOT EXISTS preart_dag_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # Завдання для вибору медалі
    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    # Завдання для розгалуження
    branching_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=choose_task_based_on_medal,
        op_kwargs={
            "chosen_medal": "{{ task_instance.xcom_pull(task_ids='pick_medal') }}"
        },
        provide_context=True,
    )

    # Завдання для рахування записів і вставки даних для кожної медалі
    calc_Bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id="mysql_default",
        sql="""
        INSERT INTO preart_dag_results (medal_type, count)
        SELECT 'Bronze', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Bronze';
        """,
    )
    calc_Silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id="mysql_default",
        sql="""
        INSERT INTO preart_dag_results (medal_type, count)
        SELECT 'Silver', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Silver';
        """,
    )
    calc_Gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id="mysql_default",
        sql="""
        INSERT INTO preart_dag_results (medal_type, count)
        SELECT 'Gold', COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Gold';
        """,
    )

    # Завдання для затримки
    delay_task = PythonOperator(
        task_id="delay_task",
        python_callable=delay_task,
        trigger_rule=tr.ONE_SUCCESS,
    )

    # Завдання для перевірки часу останнього запису
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        sql=check_latest_record_sql,
        conn_id="mysql_default",
        poke_interval=5,  # Перевірка кожні 5 секунд
        timeout=6,  # Тайм-аут після 6 секунд (1 повторна перевірка)
        mode="poke",  # Режим перевірки: періодична перевірка умови
    )

    # Створення зв'язків між завданнями
    create_table >> pick_medal_task
    pick_medal_task >> branching_task
    branching_task >> calc_Bronze
    branching_task >> calc_Silver
    branching_task >> calc_Gold
    calc_Bronze >> delay_task
    calc_Silver >> delay_task
    calc_Gold >> delay_task
    delay_task >> check_for_correctness
