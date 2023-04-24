# https://betterdatascience.com/apache-airflow-rest-api/
# had to install apache-airflow-providers-http version 2.1.2
# had to restart airflow for the connections and objects to be avaialble.
import json
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

#save_data is called by the python operator below.
def save_data(ti) -> None:
    #pull the json results out of the airflow database.
	data=ti.xcom_pull(task_ids=['get_data'])
	#write the file to local storage as a .json
	with open('/home/omx-raspberry/Downloads/data.json','w') as f:
		json.dump(data[0], f)

#the DAG object defines the callable aspect of the script for airflow.
with DAG(
    dag_id='my_swapi_dag',
    #the @once, @daily, @weekly, @monthly keywords are built into chron format
    #but you can also specify a much more specific chron string.
    #https://www.baeldung.com/cron-expressions
    # <minute> <hour> <day-of-month> <month> <day-of-week> * * * * *
    schedule_interval='@once',
    #start dates can be used to hold the schedule until a certain day.
    start_date=datetime(2023, 4, 23),
    #catchup false tells the scheduler not to execute historical jobs after the dag is uploaded.
    catchup=False
) as dag:
	# 1. Check if the API is active
	# the sensor class checks to see if we get a good response from the api
	task_is_api_active = HttpSensor(
		task_id='is_api_active',
		http_conn_id='swapi',
		endpoint='/people/1'
	)
	# 2. Get the Data
	# the simplehttpoperator completes the get request, but can also do post or other types
	task_get_data = SimpleHttpOperator(
		task_id='get_data',
		http_conn_id='swapi',
		endpoint='/people/1',
		method='GET',
		response_filter=lambda response: json.loads(response.text),
		log_response=True
	)
	# 3. Save the Data
	# python operator calls the function specified above.
	task_save_data = PythonOperator(
		task_id='save_data',
		python_callable=save_data
	)

    #this line defines the order in which the tasks will execute.
	task_is_api_active >> task_get_data >> task_save_data
