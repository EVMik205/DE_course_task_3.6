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

# Количество запусков определяется переменной Airflow
numbers_to_run = int(Variable.get("numbers_to_run"))

def hello():
	print("Airflow")

# a. Создайте еще один PythonOperator, который генерирует два произвольных
# числа и печатает их. Добавьте вызов нового оператора в конец вашего pipeline
# с помощью >>.
def random2_print():
	print(random.random(), random.random())

# b. Попробуйте снова запустить ваш DAG и убедитесь, что все работает верно.

# Вывод из лога дага:
# [2022-12-11, 06:12:21 UTC] {logging_mixin.py:137} INFO - 0.6317758458473081 0.2859545870177578

# c. Если запуск произошел успешно, попробуйте изменить логику вашего
# Python-оператора следующим образом – сгенерированные числа должны
# записываться в текстовый файл – через пробел. При этом должно соблюдаться
# условие, что каждые новые два числа должны записываться с новой строки не
# затирая предыдущие.

# Для хранения имени файла будем использовать переменную Airflow random_filename = "dags/random_numbers.txt
def random2_print_2file():
	fname = Variable.get("random_filename")
	with open(fname, 'a') as f:
		print(random.random(), random.random(), file=f)

# d. Создайте новый оператор, который подключается к файлу и вычисляет сумму
# всех чисел из первой колонки, затем сумму всех чисел из второй колонки и
# рассчитывает разность полученных сумм. Вычисленную разность необходимо
# записать в конец того же файла, не затирая его содержимого.
def sum_columns():
	sum1 = 0
	sum2 = 0
	fname = Variable.get("random_filename")
	with open(fname, 'r+') as f:
		for line in f:
			sum1 = sum1 + float(line.split()[0])
			sum2 = sum2 + float(line.split()[1])
		print(sum1-sum2, file=f)

# e. Измените еще раз логику вашего оператора из пунктов 12.а – 12.с. При
# каждой новой записи произвольных чисел в конец файла, вычисленная сумма
# на шаге 12.d должна затираться.
def random2_print_2file_nolastline():
	fname = Variable.get("random_filename")
	with open(fname, 'a') as f:
		pass
	with open(fname, 'r') as f:
		lines = f.readlines()
	with open(fname, 'w') as f:
		f.writelines(lines[:-1])
		print(random.random(), random.random(), file=f)

# g. *Добавьте новый Sensor-оператор, который возвращает True в том случае, если выполнены следующие условия:
#	i. Файл существует
#	ii. Количество строк в файле минус одна последняя - соответствует кол-ву запусков
#	iii. Финальное число рассчитано верно.
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
		print("TRUE!!!")
		return True
	else:
		print("FALSE!!!")
		return False

# i. *Добавьте образ Postgres в docker-compose файл. Для подключения к
# Postgres из Airflow зарегистрируйте подключение и используйте BaseHooks.
# Важно: использовать уже существующий образ Postgres, в котором развернута
# база Airflow – нельзя.

# В Airflow создаём заранее соединение с постгресом
# и переменную conn_id с именем соединения
def get_conn_credentials(conn_id) -> BaseHook.get_connection:
	conn_to_airflow = BaseHook.get_connection(conn_id)
	return conn_to_airflow

# j. *Создайте оператор вейтвления, который в случае, если текстовый файл
# существует и не пустой - создает таблицу аналогичной структуры в Postgres и
# наполняет ее данными из файла(за иск. последнего числа). В случае если файл не
# существует или в нем нет данных – печатает сообщение об ошибке(*** для любителей
# экстрима – выбросьте в данном случае собственное исключение).
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
	print("======================= ERROR!!! ====================")
	raise PGFileException()

# A DAG represents a workflow, a collection of tasks
#with DAG(dag_id="ahomework", start_date=datetime(2022, 1, 1), schedule="1 0 * * *") as dag:
# f. Настройте ваш DAG таким образом, чтобы он запускался каждую минуту в
# течение 5 минут.
with DAG(dag_id="ahomework", start_date=datetime(2022, 1, 1),\
		schedule_interval=timedelta(seconds=60),\
		max_active_runs=1,\
		end_date=datetime(2022, 1, 1)+timedelta(seconds=60)*(numbers_to_run-1)) as dag:
# Для отладки можно поменять на запуск каждые 5 секунд
#with DAG(dag_id="ahomework", start_date=datetime(2022, 1, 1),\
#		schedule_interval=timedelta(seconds=5),\
#		max_active_runs=1,\
#		end_date=datetime(2022, 1, 1)+timedelta(seconds=5)*(numbers_to_run-1)) as dag:

	# Tasks are represented as operators
	bash_task = BashOperator(task_id="hello", bash_command="echo hello")
	python_task = PythonOperator(task_id="world", python_callable = hello)
	random2_print_task = PythonOperator(task_id="random2_print", python_callable = random2_print)
#	random2_print_2file_task = PythonOperator(\
#		task_id="random2_print_2file",\
#		python_callable = random2_print_2file)
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

	# Как добавить в существующую таблицу столбец с разницей между
	# двумя кумулятивными суммами используя только SQL не придумал,
	# поэтому сделал представление (вьюшку) на основе существующей таблицы
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

	# Set dependencies between tasks
#	bash_task >> python_task >> random2_print_task >> random2_print_2file_task >> sum_columns_task
	bash_task >> python_task >> random2_print_task >> random2_print_2file_nolastline_task \
		>> sum_columns_task >> is_file_correct_sensor >> file4pg_branch_task \
		>> [create_pg_table_task, throw_file4pg_error_task]
	create_pg_table_task >> add_coef_col_task
