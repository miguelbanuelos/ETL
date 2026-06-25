from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'sarimax_telemetry_resampling_etl',
    default_args=default_args,
    description='ETL Avanzado con Pandas para alimentar el modelo SARIMAX',
    schedule_interval='*/5 * * * *', # Expresión CRON: Cada 5 minutos exactamente
    catchup=False,
    tags=['telemetry', 'etl', 'sarimax', 'pandas'],
) as dag:

    run_sarimax_etl_task = DockerOperator(
        task_id='extract_resample_load_sarimax',
        image='etl_sarimax_telemetry:latest',
        container_name='task_etl_sarimax',
        api_version='auto',
        auto_remove=True,
        command="python etl.py",
        network_mode="shared_network", 
        
        # IMPORTANTE: Asumo que la carpeta de tu nuevo repo se llama "etl_sarimax"
        # Si se llama diferente, ajusta esta ruta
        mounts=[
            Mount(source="/docker/etl_sarimax/.env", target="/app/.env", type="bind")
        ],
        docker_url="unix://var/run/docker.sock"
    )

    run_sarimax_etl_task