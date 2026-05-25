import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import urllib.parse

# 1. Cargar credenciales
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

def fetch_and_resample(query, start_ts, end_ts):
    """Extrae datos crudos y delega la compresión temporal a Pandas."""
    try:
        response = requests.get(PROMETHEUS_URL, params={
            'query': query, 'start': start_ts, 'end': end_ts, 'step': '15s'
        })
        response.raise_for_status()
        results = response.json().get('data', {}).get('result', [])
        
        if not results:
            return pd.DataFrame()

        # Cargar a Pandas
        df = pd.DataFrame(results[0]['values'], columns=['timestamp', 'value'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        df['value'] = pd.to_numeric(df['value'])
        df.set_index('timestamp', inplace=True)

        # Magia Analítica: Agrupar la serie de tiempo en cajones exactos de 5 minutos
        df_resampled = df.resample('5min').agg(
            min=('value', 'min'),
            avg=('value', 'mean'),
            max=('value', 'max')
        ).round(2)
        
        return df_resampled.dropna()
    except Exception as e:
        print(f"Error consultando API: {e}")
        return pd.DataFrame()

def run_incremental_etl():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Iniciando ciclo de Ingesta Incremental...")
    
    safe_user = urllib.parse.quote_plus(DB_USER)
    safe_password = urllib.parse.quote_plus(DB_PASSWORD)
    db_url = f"postgresql://{safe_user}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_DB}?sslmode=disable"
    engine = create_engine(db_url)
    
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    for server_name, queries in SERVERS.items():
        print(f"\nSincronizando {server_name}...")
        
        # --- FASE 1: CONSULTA DE ESTADO (Catch-up) ---
        try:
            with engine.connect() as conn:
                # Preguntamos por la fecha más reciente registrada para este servidor
                query_sql = text(f"SELECT MAX(timestamp) FROM \"Servers\" WHERE \"ServerName\" = '{server_name}'")
                last_ts = conn.execute(query_sql).scalar()
                
                if last_ts:
                    # Si existe, continuamos a partir de los 5 minutos siguientes para no duplicar
                    start_time = last_ts.replace(tzinfo=timezone.utc) + timedelta(minutes=5)
                else:
                    # Si no hay historia, retrocedemos al máximo (30 días)
                    print("No hay historia previa detectada. Iniciando recuperación a 30 días...")
                    start_time = now - timedelta(days=30)
        except Exception:
            start_time = now - timedelta(days=30)

        # --- FASE 2: PAGINACIÓN HISTÓRICA ---
        chunk_start = start_time
        while chunk_start < now:
            # Descargamos en bloques máximos de 7 días para no saturar
            chunk_end = min(chunk_start + timedelta(days=7), now)
            
            # Evitar requests basura si el rango es menor a un bloque de 5 minutos
            if (chunk_end - chunk_start).total_seconds() < 300:
                break
                
            df_cpu = fetch_and_resample(queries['cpu'], chunk_start.timestamp(), chunk_end.timestamp())
            df_mem = fetch_and_resample(queries['mem'], chunk_start.timestamp(), chunk_end.timestamp())

            # --- FASE 3: TRANSFORMACIÓN Y CARGA ---
            if not df_cpu.empty and not df_mem.empty:
                # Unimos CPU y Memoria horizontalmente alineando el timestamp
                df_final = df_cpu.join(df_mem, lsuffix='_cpu', rsuffix='_memory', how='inner')
                
                # Renombramos para igualar la estructura de la tabla
                df_final.rename(columns={
                    'min_cpu': 'cpu_min', 'avg_cpu': 'cpu_avg', 'max_cpu': 'cpu_max',
                    'min_memory': 'memory_min', 'avg_memory': 'memory_avg', 'max_memory': 'memory_max'
                }, inplace=True)
                
                df_final['ServerName'] = server_name
                
                # Inyección a TimescaleDB
                df_final.to_sql('Servers', engine, if_exists='append', index=True)
                print(f"  -> ✅ Lote inyectado: {chunk_start.strftime('%Y-%m-%d %H:%M')} a {chunk_end.strftime('%Y-%m-%d %H:%M')} ({len(df_final)} filas)")
            else:
                print(f"  -> ⚠️ Sin métricas en Prometheus para: {chunk_start.strftime('%Y-%m-%d')} a {chunk_end.strftime('%Y-%m-%d')}")
                
            # Avanzar al siguiente bloque
            chunk_start = chunk_end

def wait_until_next_grid_interval(interval_minutes=5):
    """Calcula el tiempo exacto que falta para el próximo minuto divisible por la rejilla."""
    now = datetime.now()
    minutes_past_last_interval = now.minute % interval_minutes
    minutes_to_next_interval = interval_minutes - minutes_past_last_interval
    
    next_run = now.replace(second=0, microsecond=0) + timedelta(minutes=minutes_to_next_interval)
    
    if next_run <= now:
        next_run += timedelta(minutes=interval_minutes)
        
    sleep_seconds = (next_run - now).total_seconds()
    
    print(f"\n⏰ Rejilla activa. Próxima evaluación de estado: {next_run.strftime('%H:%M:%S')}")
    time.sleep(sleep_seconds)

if __name__ == "__main__":
    print("Iniciando Orquestador Incremental... Presiona Ctrl+C para detener.")
    # La primera ejecución hace el backfill pesado
    run_incremental_etl()
    
    # A partir de aquí, solo insertará los nuevos bloques de 5 minutos
    while True:
        wait_until_next_grid_interval(interval_minutes=5)
        run_incremental_etl()