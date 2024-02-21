# import libraries
import psycopg2
import pandas as pd
import datetime as dt
from imblearn.over_sampling import RandomOverSampler
import time as tm
import schedule
from schedule import repeat, every
from datetime import time, timedelta

import warnings
warnings.filterwarnings('ignore')
from elasticsearch import Elasticsearch

def data_cleaning():
    
    data = pd.read_csv('churn.csv')

    ros = RandomOverSampler(sampling_strategy=1)
    x = data.drop(['RowNumber', 'CustomerId', 'Surname', 'Geography', 'Exited', 'HasCrCard'], axis=1)
    y = data[['Exited']]
    x, y = ros.fit_resample(x, y)
    
    data_final = pd.concat([x, y], axis=1)
    
    data_final.rename(columns={'CreditScore':'credit_score', 'Age':'age', 'Tenure':'tenure', 'Balance':'balance', 'NumOfProducts':'num_of_products', 'IsActiveMember':'is_active_member', 'EstimatedSalary':'estimated_salary', 'Gender_Male':'gender_male', 'Geography_LabelEncoded':'geography_labeled', 'Exited':'exited'}, inplace=True)
    
    data_final.to_csv('data_clean_scheduled.csv', index=False)

schedule.every().day.at('21:54').do(data_cleaning)

while True:
    schedule.run_pending()
    tm.sleep(1)