from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.email import send_email
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def success_email(context):
    task_instance = context['task_instance']
    task_status = 'Success' 
    subject = f'Airflow Task {task_instance.task_id} {task_status}'
    body = f'The task {task_instance.task_id} completed with status : {task_status}. \n\n'\
        f'The task execution date is: {context["execution_date"]}\n'\
        f'Log url: {task_instance.log_url}\n\n'
    to_email = 'alan.iwi@stfc.ac.uk' #recepient mail
    send_email(to = to_email, subject = subject, html_content = body)

def failure_email(context):
    task_instance = context['task_instance']
    task_status = 'Failed'
    subject = f'Airflow Task {task_instance.task_id} {task_status}'
    body = f'The task {task_instance.task_id} completed with status : {task_status}. \n\n'\
        f'The task execution date is: {context["execution_date"]}\n'\
        f'Log url: {task_instance.log_url}\n\n'
    to_email = 'alan.iwi@stfc.ac.uk' #recepient mail
    send_email(to = to_email, subject = subject, html_content = body)

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
    'dag_id',
    default_args = default_args,
    description = 'Test dag for email notifications (and also to check DAGS sync with GitSync), no schedule, run manually',
    schedule_interval = None, #you can set any schedule interval you want.
    catchup = False,
)

#Define python operator to execute simple python command
def python_command1():
    text = "How are you?!"
    print(text)
    with open("/opt/airflow/logs/python_log", "a") as fout:
        fout.write(f"{text}\n")

task1 = PythonOperator(
     task_id = 'execute_python_command',
     python_callable = python_command1,
     on_success_callback = success_email,
     on_failure_callback = failure_email,
     provide_context = True,
     dag = dag
)

# Define BashOperators to execute simple bash commands
task2 = BashOperator(
    task_id = 'execute_bash_command',
    bash_command = 'echo "Hello, world!" | tee -a /opt/airflow/logs/bash_log',
    on_success_callback = lambda context: success_email(context),
    on_failure_callback = lambda context: failure_email(context),
    dag = dag
)

# Define tasks dependencies
task1 >> task2
