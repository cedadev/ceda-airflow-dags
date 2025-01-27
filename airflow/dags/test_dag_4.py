from airflow import DAG
from datetime import datetime, timedelta, timezone
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.slack.notifications.slack_webhook import send_slack_webhook_notification


dag_failure_slack_webhook_notification = send_slack_webhook_notification(
    slack_webhook_conn_id="Slack", text="The dag {{ dag.dag_id }} failed"
)
dag_success_slack_webhook_notification = send_slack_webhook_notification(
    slack_webhook_conn_id="Slack", text="The dag {{ dag.dag_id }} succeeded"
)
task_failure_slack_webhook_notification = send_slack_webhook_notification(
    slack_webhook_conn_id="Slack",
    text="The task {{ ti.task_id }} failed",
)
task_success_slack_webhook_notification = send_slack_webhook_notification(
    slack_webhook_conn_id="Slack",
    text="The task {{ ti.task_id }} succeeded",
)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 17),
    'schedule_interval' : 'None',
    'email_on_failure': True,
    'email_on_success': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

# Instantiate the DAG

dag = DAG(
    'alan_test_with_slack_pythonfail',
    default_args = default_args,
    description = 'Test dag for email notifications (and also to check DAGS sync with GitSync), no schedule, run manually',
    on_failure_callback=[dag_failure_slack_webhook_notification],
    on_success_callback=[dag_success_slack_webhook_notification],
    schedule_interval = None, #you can set any schedule interval you want.
    catchup = False,
)

#Define python operator to execute simple python command
def python_command1():
    text = "something went wrong!"
    print(text)
    with open("/opt/airflow/logs/python_log", "a") as fout:
        fout.write(f"{text}\n")
    raise Exception(text)

task1 = PythonOperator(
     task_id = 'execute_python_command',
     python_callable = python_command1,
     on_success_callback = [task_success_slack_webhook_notification],
     on_failure_callback = [task_failure_slack_webhook_notification],
     provide_context = True,
     dag = dag
)

# Define BashOperators to execute simple bash commands
task2 = BashOperator(
    task_id = 'execute_bash_command',
    bash_command = 'echo "Hello, world!" | tee -a /opt/airflow/logs/bash_log',
    on_success_callback = lambda context: task_success_slack_webhook_notification(context),
    on_failure_callback = lambda context: task_failure_slack_webhook_notification(context),
    dag = dag
)

# Define tasks dependencies
task1 >> task2
