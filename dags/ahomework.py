from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import random as random
from airflow.models import Variable
from airflow.sensors.python import PythonSensor
from os.path import exists
from airflow.hooks.base import BaseHook
from airflow.operators.python import BranchPythonOperator
import psycopg2
from airflow.exceptions import AirflowException
from http import HTTPStatus
from airflow.operators.postgres_operator import PostgresOperator

numbers_to_run = int(Variable.get("numbers_to_run"))

def hello():
	print("Airflow")

def random2_print():
	print(random.random(), random.random())

def random2_print_2file():
	fname = Variable.get("random_filename")
	with open(fname, 'a') as f:
		print(random.random(), random.random(), file=f)

def sum_columns():
	sum1 = 0
	sum2 = 0
	fname = Variable.get("random_filename")
	with open(fname, 'r+') as f:
		for line in f:
			sum1 = sum1 + float(line.split()[0])
			sum2 = sum2 + float(line.split()[1])
		print(sum1-sum2, file=f)

def random2_print_2file_nolastline():
	fname = Variable.get("random_filename")
	with open(fname, 'a') as f:
		pass
	with open(fname, 'r') as f:
		lines = f.readlines()
	with open(fname, 'w') as f:
		f.writelines(lines[:-1])
		print(random.random(), random.random(), file=f)

def is_file_correct():
	fname = Variable.get("random_filename")
	with open(fname, 'r') as f:
		for count, line in enumerate(f):
			pass
	sum1 = 0
	sum2 = 0
	with open(fname, 'r') as f:
		for cnt, line in enumerate(f):
			if (cnt != count):
				sum1 = sum1 + float(line.split()[0])
				sum2 = sum2 + float(line.split()[1])
			else:
				file_sum = float(line)
		new_sum = sum1-sum2
	if (exists(fname) and (count == numbers_to_run) and (new_sum == file_sum)):
		return True
	else:
		return False

def get_conn_credentials(conn_id) -> BaseHook.get_connection:
	conn_to_airflow = BaseHook.get_connection(conn_id)
	return conn_to_airflow

def file4pg_branch():
	fname = Variable.get("random_filename")
	with open(fname, 'r') as f:
		for count, line in enumerate(f):
			pass
	if (exists(fname) and (count > 0)):
		return 'create_pg_table_task'
	else:
		return 'throw_file4pg_error_task'

def create_pg_table(**kwargs):

	conn_id = Variable.get("conn_id")
	conn_to_airflow = get_conn_credentials(conn_id)

	pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, conn_to_airflow.login,\
		conn_to_airflow.password, conn_to_airflow.schema
	pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port,\
							user=pg_username, password=pg_pass,\
							database=pg_db)
	cursor = pg_conn.cursor()
	
	cursor.execute("CREATE TABLE IF NOT EXISTS numbers_table (id serial PRIMARY KEY, value_1 float, value_2 float);")
	fname = Variable.get("random_filename")
	with open(fname, 'r') as f:
		lines = f.readlines()
	for line in lines[:-1]:
		val1 = float(line.split()[0])
		val2 = float(line.split()[1])
		cursor.execute("INSERT INTO numbers_table (value_1, value_2) VALUES (%s, %s)", (val1, val2))

	pg_conn.commit()

	cursor.close()
	pg_conn.close()

class PGFileException(AirflowException):
	status_code = HTTPStatus.NOT_FOUND
	def __str__(self):
		return "No file to load!"

def throw_file4pg_error():
	raise PGFileException()

# Устанавливаем стартовую дату в прошлом, интервал выполнения задач 1 минута,
# время завершения работы = начальное время + 5 минут
# чтобы одновременно выполнялся только один экземпляр дага устанавливаем
# max_active_runs=1
# согласно ЧаВо по Airflow
# max_active_runs defines how many running concurrent instances of a DAG there are allowed to be.
# https://airflow.apache.org/docs/apache-airflow/stable/faq.html
# Таким образом шедулер  планирует 5 запусков Дага в прошлом
# и начинает их выполнять последовательно с интервалом в 1 минуту
with DAG(dag_id="ahomework", start_date=datetime(2022, 1, 1),\
		schedule_interval=timedelta(seconds=60),\
		max_active_runs=1,\
		end_date=datetime(2022, 1, 1)+timedelta(seconds=60)*(numbers_to_run-1)) as dag:

	bash_task = BashOperator(task_id="hello", bash_command="echo hello")
	python_task = PythonOperator(task_id="world", python_callable = hello)
	random2_print_task = PythonOperator(task_id="random2_print", python_callable = random2_print)
	random2_print_2file_nolastline_task = PythonOperator(\
		task_id="random2_print_2file_nolastline",\
		python_callable = random2_print_2file_nolastline)
	sum_columns_task = PythonOperator(\
		task_id="sum_columns",\
		python_callable = sum_columns)

	is_file_correct_sensor = PythonSensor(\
		task_id="is_file_correct_task", python_callable=is_file_correct,\
		poke_interval=4, timeout=2)

	create_pg_table_task = PythonOperator(task_id="create_pg_table_task", python_callable = create_pg_table)
	throw_file4pg_error_task = PythonOperator(task_id="throw_file4pg_error_task", python_callable = throw_file4pg_error)

	file4pg_branch_task = BranchPythonOperator(
		task_id = 'file4pg_branch_task',
		python_callable = file4pg_branch
	)

	add_coef_col_task = PostgresOperator(
		task_id="add_coef_col_task",
		sql="""
			CREATE VIEW numbers_diff AS (
			with sums AS (
			SELECT id, value_1, value_2, SUM(value_1) OVER (ORDER BY id) AS s1, SUM(value_2) OVER (ORDER BY id) AS s2 FROM numbers_table
			),
			diff_t AS (
			SELECT id, s1-s2 as diff FROM sums
			)
			SELECT s.id, s.value_1, s.value_2, d.diff FROM sums s
			JOIN diff_t d ON s.id=d.id
			);
		""",
		postgres_conn_id=Variable.get("conn_id")
	)

	bash_task >> python_task >> random2_print_task >> random2_print_2file_nolastline_task \
		>> sum_columns_task >> is_file_correct_sensor >> file4pg_branch_task \
		>> [create_pg_table_task, throw_file4pg_error_task]
	create_pg_table_task >> add_coef_col_task
