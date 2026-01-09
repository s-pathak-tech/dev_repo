from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from plugins.helpers.fetch_workflow_config import fetch_workflow_config
from plugins.helpers.call_microservice_generic import call_microservice_generic

with DAG(
    dag_id="MyWorflow",
    start_date=datetime(2026, 1, 9),
    schedule="*/5 * * * *",
    catchup=False,
    tags=['myworflow'],
) as dag:

    task1_fetch_config = PythonOperator(
        task_id="task1_fetch_workflow_config",
        python_callable=fetch_workflow_config,
        op_kwargs={
            "workflow_name": "MyWorflow",
            "collection_name": "UC_IR_CROSSCU",
        },
        provide_context=True,
    )

    task_2_IRIS_EDX_DOWNLOAD = PythonOperator(
        task_id="task_2_IRIS_EDX_DOWNLOAD",
        python_callable=call_microservice_generic,
        provide_context=True,
    )

    task1_fetch_config >> task_2_IRIS_EDX_DOWNLOAD