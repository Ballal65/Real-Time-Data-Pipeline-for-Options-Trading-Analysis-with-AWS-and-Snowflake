import requests
from bs4 import BeautifulSoup
from datetime import datetime
import csv
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Define the path to the CSV file
CSV_FILE = '/opt/airflow/extracted_data/nifty_data/nifty_data.csv'  # Adjust path as needed

# Define your extraction function
def extract_nifty_data():
    ticker = 'NIFTY_50'
    url = f'https://www.google.com/finance/quote/{ticker}:INDEXNSE'

    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        class1 = 'YMlKec fxKbKc'  # Class containing the index value
        value_element = soup.find('div', class_=class1)
        if value_element:
            val = value_element.text.replace(',', '')
            val = float(val)
            print(f"The value of {ticker} is: {val}")
        else:
            print("Value not found. The structure of the page may have changed.")
            return

        class2 = soup.find('div', class_='ygUjEc')  # Class containing date/time
        if class2:
            datetime_string = class2.text.split(' · ')[0].split(' GMT')[0]
            current_year = datetime.now().year
            datetime_string = f"{datetime_string} {current_year}"
            date_format = "%b %d, %I:%M:%S %p %Y"
            dt = datetime.strptime(datetime_string, date_format)
            sql_datetime = dt.strftime("%Y-%m-%d %H:%M:%S")
            print(f"SQL-compatible datetime: {sql_datetime}")
        else:
            print("Datetime not found. The structure of the page may have changed.")
            return

        # Save data to CSV
        file_exists = os.path.isfile(CSV_FILE)
        with open(CSV_FILE, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if not file_exists:
                writer.writerow(['datetime', 'value'])  # Write header if file doesn't exist
            writer.writerow([sql_datetime, val])
            print(f"Data saved to {CSV_FILE}")

    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'nifty_data_extraction',
    default_args=default_args,
    description='A simple DAG to extract Nifty 50 data and save it to a CSV file',
    schedule_interval='* * * * *',  # Run every minute
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_nifty_data',
        python_callable=extract_nifty_data,
    )

# Set task dependencies
extract_task