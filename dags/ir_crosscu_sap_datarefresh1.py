from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from plugins.helpers.call_microservice import call_microservice

with DAG(
    dag_id="ir_crosscu_sap_datarefresh1",
    start_date=datetime(2025, 12, 23),
    schedule="*/5 * * * *",
    catchup=False,
    tags=['ir', 'crosscu', 'sap', 'datarefresh1'],
) as dag:

    task_1_IRIS_IG_SAPHANA_SAPHANA = PythonOperator(
        task_id="task_IRIS_IG_SAPHANA_SAPHANA",
        python_callable=call_microservice,
        op_kwargs={
            "MicroService": "IRIS-IG-SAPHANA",
            "EndPoint": "SAPHANA",
            "Base_Url": "https://iris-saphana.braveflower-de2e6201.westeurope.azurecontainerapps.io",
            "ConnectionString": "KV-SAPHANA-PROD-P63",
            "Attributes": {
            "File_Formate": "parquet",
            "PrefixFileName": "111",
            "QueryBody": "",
            "SourceObject": "",
            "StorageAccount_Connection": "KV_IRIS_UAT_BRONZE",
            "StorageAccount_Container": "cu-uk",
            "StorageAccount_Directory": "111"
}
        }
    )
