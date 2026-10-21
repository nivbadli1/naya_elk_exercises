"""
Step 2 - Airflow DAG that runs the producer over SSH
----------------------------------------------------
Airflow does not run the producer itself. It opens an SSH connection to the
machine where the code lives and runs the Python producer script there.

Setup: create an SSH connection in Airflow (Admin -> Connections) named "ssh_default",
and update the script path in "command" to where coins_producer.py is on that machine.
"""

# datetime - for the DAG start date; timedelta - for retry delays (commented out below)
from datetime import datetime, timedelta
# DAG is the object that groups tasks and their schedule
from airflow import DAG  # type: ignore
# SSHOperator runs a shell command on a remote machine over SSH
from airflow.contrib.operators.ssh_operator import SSHOperator
# TriggerDagRunOperator can trigger another DAG (imported for later exercises, not used here)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Default arguments applied to every task in the DAG
default_args = {
    # Owner name shown in the Airflow UI
    'owner': 'airflow',
    # A task run does not depend on the success of its previous run
    'depends_on_past': False,
    # The first date the DAG is valid from
    'start_date': datetime(2024, 5, 27),
    # Uncomment to retry a failed task once, after 5 minutes
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

# Define the DAG: no schedule (run manually), and don't backfill missed runs
with DAG('produce_coins_to_kafka', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    # A single task that runs the producer on the remote machine
    run_python_producer = SSHOperator(
        # Task name shown in the Airflow UI
        task_id='run_python_producer',
        # Name of the SSH connection configured in Airflow
        ssh_conn_id='ssh_default',
        # The shell command to run remotely: python3 + full path to the producer script
        command='/bin/python3 /home/developer/projects/spark-course-python/BigDataETL/coins/coins_python_producer.py'
    )
