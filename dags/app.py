# import libraries
import psycopg2
import pandas as pd
import datetime as dt
from sklearn.preprocessing import LabelEncoder
from imblearn.over_sampling import RandomOverSampler
import schedule
from datetime import time, timedelta

import warnings
warnings.filterwarnings('ignore')
from elasticsearch import Elasticsearch

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def fetch():
    # configure database
    db_name = 'airflow'
    db_user = 'airflow'
    db_pass = 'airflow'
    db_host = 'postgres'
    db_port = '5432'
    
    # connect to database
    connection = psycopg2.connect(
        database = db_name,
        user = db_user,
        password = db_pass,
        host = db_host,
        port = db_port
    )
    
    # get all data
    select_query = 'SELECT * FROM final_project'
    data = pd.read_sql(select_query, connection)
    
    # close the connection
    connection.close()
    
    # save into csv
    data.to_csv('/opt/airflow/dags/data_raw.csv')

def data_cleaning():
    
    data = pd.read_csv('/opt/airflow/dags/data_raw.csv')
    
    data = pd.get_dummies(data, columns=['Gender'], drop_first=True)

    label_encoder = LabelEncoder()
    data['Geography_LabelEncoded'] = label_encoder.fit_transform(data['Geography'])

    ros = RandomOverSampler(sampling_strategy=1)
    x = data.drop(['RowNumber', 'CustomerId', 'Surname', 'Geography', 'Exited', 'HasCrCard'], axis=1)
    y = data[['Exited']]
    x, y = ros.fit_resample(x, y)
    
    data_final = pd.concat([x, y], axis=1)
    
    data_final.rename(columns={'CreditScore':'credit_score', 'Age':'age', 'Tenure':'tenure', 'Balance':'balance', 'NumOfProducts':'num_of_products', 'IsActiveMember':'is_active_member', 'EstimatedSalary':'estimated_salary', 'Gender_Male':'gender_male', 'Geography_LabelEncoded':'geography_labeled', 'Exited':'exited'}, inplace=True)
    
    data_final.to_csv('/opt/airflow/dags/data_clean.csv', index=False)

# def insert_to_elastic():
    
#     # load the clean data
#     data1 = pd.read_csv('/opt/airflow/dags/data_clean.csv')
    
#     # check connection
#     es = Elasticsearch('http://elasticsearch:9200')
#     print('Connection Status: ', es.ping())
    
#     # insert csv file to elastic search
#     failed_insert = []
#     for i, r in data1.iterrows():
#         doc = r.to_json()
#         try:
#             print(i, r['customer_name'])
#             res = es.index(index='customer_churn', doc_type='doc', body=doc)
#         except:
#             print('Failed Index: ', failed_insert)
#             pass
        
# create a default argument
default_args = {
    'owner' : 'Adriel',
    'start_date' : dt.datetime(2024, 2, 4, 14, 30, 0) - dt.timedelta(hours=8),
    'retries' : 1,
    'retry_delay' : dt.timedelta(minutes=5)
}

# create a dag function
with DAG(
    'final_project',
    default_args=default_args,
    schedule_interval= '7 * * * *',
    catchup=False) as dag:
    
    start = BashOperator(
        task_id = 'start',
        bash_command = 'echo "Currently reading the .csv file now"')
    
    fetch_data = PythonOperator(
        task_id = 'fetch-data',
        python_callable = fetch)
    
    clean_data = PythonOperator(
        task_id = 'clean-data',
        python_callable = data_cleaning)
    
    # elastic = PythonOperator(
    #     task_id = 'insert-to-elastic',
    #     python_callable = insert_to_elastic)

# create the order of execution
start >> fetch_data >> clean_data