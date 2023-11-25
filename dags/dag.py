# pylint: skip-file
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from custom_operator import custom_operator

path_parent_folder_file = Path(__file__).resolve().parents[1]
path_file_clinical_trials = path_parent_folder_file / "file" / "clinical_trials.csv"
path_drugs = path_parent_folder_file / "file" / "drugs.csv"
path_file_pubmed_csv = path_parent_folder_file / "file" / "pubmed.csv"
path_file_pubmed_json = path_parent_folder_file / "file" / "pubmed.json"

all_paths_local = {
    path_file_clinical_trials,
    path_drugs,
    path_file_pubmed_csv,
    path_file_pubmed_json,
}

with DAG(
    "dag_servier",
    default_args={
        "email": ["jonathan.ndamba.gcp@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
    },
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["Servier"],
) as dag_servier:
    start_point = DummyOperator(task_id="start_point", dag=dag_servier)
    end_point = DummyOperator(task_id="end_point", dag=dag_servier)

    task_push_gold_data = custom_operator.PreprossecingGoldData(
        task_id="push_gold_data_to_gcs_reconcialted_data",
        bucket_name="servier-gold",
        path_elements_drug=path_drugs,
        path_elements_pubmed_json=path_file_pubmed_json,
        path_elements_pubmed_csv=path_file_pubmed_csv,
        path_elements_clinical_trials=path_file_clinical_trials,
        dag=dag_servier,
    )

    for path_local_file in all_paths_local:
        task_push_bronze_data = custom_operator.PreprossecingBronzeData(
            task_id=f"push_raw_data_to_gcs_{path_local_file.name}".replace(".", "_"),
            type_of_operation="push",
            bucket_name="servier-bronze",
            local_file_name=str(path_local_file),
            gcs_file_name=f"{path_local_file.stem}/{path_local_file.name}",
            dag=dag_servier,
        )

        task_push_silver_data = custom_operator.PreprossecingSilverData(
            task_id=f"push_silver_data_to_gcs_{path_local_file.name}".replace(".", "_"),
            bucket_name="servier-silver",
            type_of_schema=path_local_file.stem,
            local_file_name=path_local_file,
            prefix_gcs_file_name=path_local_file.stem,
            dag=dag_servier,
        )

        (
            start_point
            >> task_push_bronze_data
            >> task_push_silver_data
            >> task_push_gold_data
            >> end_point
        )
