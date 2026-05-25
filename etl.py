import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine
from dotenv import load_dotenv
import urllib.parse

# 1. Cargar credenciales (.env local en desarrollo, o inyectadas por Docker Compose)
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_DB = os.getenv("POSTGRES_DB")
DB_PORT = os.getenv("TIMESCALEDB_PORT", "5432")
DB_HOST = os.getenv("TIMESCALEDB_HOST", "192.168.3.155")

PROMETHEUS_HOST = os.getenv("PROMETHEUS_HOST", "192.168.3.155")
PROMETHEUS_PORT = os.getenv("PROMETHEUS_PORT", "9090")
PROMETHEUS_URL = f"http://{PROMETHEUS_HOST}:{PROMETHEUS_PORT}/api/v1/query_range"

# 2. Diccionario estructurado por Servidor
SERVERS = {
    'Zephyrus': {
        'cpu': '100 - (avg(rate(windows_cpu_time_total{mode="idle"}[1m])) * 100)',
        'mem': '100 - (avg(windows_memory_physical_free_bytes) / avg(windows_memory_physical_total_bytes) * 100)'
    },
    'ThinkCentre': {
        'cpu': '100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)',
        'mem': '100 * (1 - (avg(node_memory_MemAvailable_bytes) / avg(node_memory_MemTotal_bytes)))'
    }
}

def fetch_metric_stats(query, start, end):
    """Extrae puntos de Prometheus y devuelve el min, max y avg matemáticos."""
    try:
        response = requests.get(PROMETHEUS_URL, params={
            'query': query, 'start': start, 'end': end, 'step': '15s'
        })
        response.raise_for_status()
        results = response.json().get('data', {}).get('result', [])
        
        if results:
            df_temp = pd.DataFrame(results[0]['values'], columns=['time', 'value'])
            df_temp['value'] = pd.to_numeric(df_temp['value'])
            return {
                'min': round(df_temp['value'].min(), 2),
                'max': round(df_temp['value'].max(), 2),
                'avg': round(df_temp['value'].mean(), 2)
            }
    except Exception as e:
        print(f"Error consultando Prometheus: {e}")
        
    return {'min': None, 'max': None, 'avg': None}

def run_etl_cycle():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Iniciando extracción ETL (Ventana de 5 min)...")
    
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=5)
    
    datos_para_bd = []

    # --- FASE 1 y 2: EXTRACT & TRANSFORM ---
    for server_name, queries in SERVERS.items():
        print(f"Procesando métricas para: {server_name}")
        
        cpu_stats = fetch_metric_stats(queries['cpu'], start_time.timestamp(), end_time.timestamp())
        mem_stats = fetch_metric_stats(queries['mem'], start_time.timestamp(), end_time.timestamp())
        
        fila = {
            'timestamp': end_time,
            'ServerName': server_name,
            'cpu_min': cpu_stats['min'],
            'cpu_avg': cpu_stats['avg'],
            'cpu_max': cpu_stats['max'],
            'memory_min': mem_stats['min'],
            'memory_avg': mem_stats['avg'],
            'memory_max': mem_stats['max']
        }
        datos_para_bd.append(fila)

    df_final = pd.DataFrame(datos_para_bd)
    df_final.set_index('timestamp', inplace=True)
    
    print("\nVisualización de los datos a inyectar:......")
    print(df_final)

    # --- FASE 3: LOAD (TimescaleDB) ---
    print("\nConectando a TimescaleDB...")

    if DB_USER and DB_PASSWORD:
        safe_user = urllib.parse.quote_plus(DB_USER)
        safe_password = urllib.parse.quote_plus(DB_PASSWORD)
    else:
        print("Error: Credenciales no cargadas.")
        return

    db_url = f"postgresql://{safe_user}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_DB}?sslmode=disable"
    engine = create_engine(db_url)

    try:
        df_final.to_sql('Servers', engine, if_exists='append', index=True)
        print("¡Inyección exitosa en TimescaleDB!")
    except Exception as e:
        print(f"Error al escribir en base de datos: {e}")

def wait_until_next_grid_interval(interval_minutes=5):
    """Calcula el tiempo exacto que falta para el próximo minuto divisible por interval_minutes."""
    now = datetime.now()
    minutes_past_last_interval = now.minute % interval_minutes
    minutes_to_next_interval = interval_minutes - minutes_past_last_interval
    
    next_run = now.replace(second=0, microsecond=0) + timedelta(minutes=minutes_to_next_interval)
    
    if next_run <= now:
        next_run += timedelta(minutes=interval_minutes)
        
    sleep_seconds = (next_run - now).total_seconds()
    
    print(f"\n⏰ Sincronización de reloj activa. Próxima ingesta: {next_run.strftime('%H:%M:%S')}")
    time.sleep(sleep_seconds)

if __name__ == "__main__":
    print("Iniciando Orquestador Dockerizado... Presiona Ctrl+C para detener.")
    run_etl_cycle()
    while True:
        wait_until_next_grid_interval(interval_minutes=5)
        run_etl_cycle()