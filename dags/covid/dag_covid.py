from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime

import requests
import csv
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

default_args = {
    'owner': 'Yuliya Pak',
    'start_date': datetime.datetime(2020, 6, 6)
}

dag = DAG(
    'get_covid_data_russai',
    catchup=False,
    schedule_interval='0 14 * * *',
    default_args=default_args
)


def get_covid_data_russia():
    russia_covid_data = []
    list_of_covid_data = ['10', '9', '8']
    for covid_data in list_of_covid_data:
        response = requests.get(
            f"https://yastat.net/s3/milab/2020/covid19-stat/data/data_struct_{covid_data}.json",
            verify=False,
        )
        response.raise_for_status()
        all_russia_covid_data = response.json()['russia_stat_struct']
        dates = all_russia_covid_data['dates']
        regions_data = all_russia_covid_data['data']
        for region in regions_data:
            region_name = regions_data[region]['info']['name']
            region_cases = regions_data[region]['cases']
            region_deaths = regions_data[region]['deaths']
            region_cured = regions_data[region]['cured']
            res = list(zip(dates, region_cases, region_deaths, region_cured))
            for i in res:
                row = {
                    'date': i[0],
                    'region': region_name,
                    'infected': i[1]['v'],
                    'recovered': i[3]['v'],
                    'dead': i[2]['v']
                }
                russia_covid_data.append(row)

        with open('russia_covid.csv', 'w', encoding='utf-8') as f:
            fields = ['date', 'region', 'infected', 'recovered', 'dead']
            write = csv.DictWriter(f, fields, delimiter=';')
            write.writeheader()
            for ln in russia_covid_data:
                write.writerow(ln)


get_covid_data = PythonOperator(
    task_id='get_covid_data_russia_task',
    dag=dag,
    python_callable=get_covid_data_russia,
)
