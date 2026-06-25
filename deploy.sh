#!/bin/bash

# ==============================================================================
# SCRIPT DE DESPLIEGUE CONTINUO - ETL POSTGRES (DOCKER OPERATOR)
# ==============================================================================

# 1. Moverse al directorio del repositorio en el host

cd /docker/etl || exit

# 2. Configuración de seguridad para Git
git config --global --add safe.directory /docker/etl

# 3. Descargar los últimos cambios desde GitHub
echo "[$(date)] Sincronizando repositorio..."
git fetch --all
git reset --hard origin/main

# 4. Sincronizar el orquestador (DAG) con Airflow
# Copiamos el archivo del DAG hacia la carpeta que Airflow está leyendo
echo "[$(date)] Actualizando DAG en Airflow..."
cp sarimax_dag.py /docker/airflow/dags/

# 5. Construir la imagen del trabajador (Worker)
# Ya no hacemos 'docker compose up'. Solo preparamos la imagen (la "receta") 
# para que Airflow levante el contenedor cuando sea la hora.
echo "[$(date)] Construyendo nueva imagen de Docker: etl_sarimax_telemetry:latest..."
docker build -t etl_sarimax_telemetry:latest .

# 6. Mantenimiento del servidor
# Borra las capas de imágenes viejas que ya no se usan para liberar espacio
echo "[$(date)] Limpiando Docker..."
docker image prune -f

echo "[$(date)] ¡Despliegue del ETL Postgres finalizado con éxito!"