#________________________________________________________________________________________
# imports
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator

#________________________________________________________________________________________
# config dag
dag_name = 'orquestracao_rf_cnpj'
schedule_interval = None
start_date = datetime(2021, 9, 20)

project_id = 'gcp_project_id'
region = 'us-central1'
zone = 'us-central1-c'
cluster_name = 'proc-rf-cnpj'

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(dag_name, catchup=False, default_args=default_args, description='orquestração do pipeline de CNPJ Receita Federal', schedule_interval=schedule_interval, tags=['rf_cnpj'])


#________________________________________________________________________________________
# -------------------- set pyspark job params --------------------
pyspark_jobs = {
    'aqs_cnpj_receita_federal': {
        'reference': {'project_id': project_id},
        'placement': {'cluster_name': cluster_name},
        'pyspark_job': {'main_python_file_uri': 'file:///opt/process_cnpj/aqs_cnpj_receita_federal.py'},
        "labels" : {'project': 'rf_cnpj', 'process':'aquisicao', 'task':'aqs_cnpj_receita_federal'}
    },
    'trm_cnpj_receita_federal': {
        'reference': {'project_id': project_id},
        'placement': {'cluster_name': cluster_name},
        'pyspark_job': {'main_python_file_uri': 'file:///opt/process_cnpj/trm_cnpj_receita_federal.py'},
        "labels" : {'project': 'rf_cnpj', 'process':'tratamento', 'task':'trm_cnpj_receita_federal'}
    }
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-8",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-8",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "initialization_actions": [{
            "executable_file": 'gs://bucket/app/custom_init.sh'
        }
    ],
    "software_config": {
        "image_version": "2.0-debian10",
    },
    "gce_cluster_config": {
        "tags" : {'dataproc', 'rfcnpj'},
        "zone_uri" : zone
    },    
    "lifecycle_config": {
        "auto_delete_ttl": {"seconds": int(10000)}
    }
}

task_orchestration_session = DummyOperator(task_id='orchestration_session', dag=dag)

task_create_dataproc_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    project_id=project_id,
    cluster_name=cluster_name,
    cluster_config=CLUSTER_CONFIG,
    region=region,
    labels = {'project':'rf_cnpj', 'cluster':cluster_name},
    dag=dag)

task_aqs_cnpj_receita_federal = DataprocSubmitJobOperator(task_id='aqs_cnpj_receita_federal', job=pyspark_jobs['aqs_cnpj_receita_federal'], location=region, project_id=project_id, dag=dag)

task_trm_cnpj_receita_federal = DataprocSubmitJobOperator(task_id='trm_cnpj_receita_federal', job=pyspark_jobs['trm_cnpj_receita_federal'], location=region, project_id=project_id, dag=dag)

task_delete_dataproc_cluster = DataprocDeleteClusterOperator(task_id='delete_dataproc_cluster', project_id=project_id, region=region, cluster_name=cluster_name, dag=dag)

task_orchestration_session >> task_create_dataproc_cluster >> task_aqs_cnpj_receita_federal >> task_trm_cnpj_receita_federal >>task_delete_dataproc_cluster