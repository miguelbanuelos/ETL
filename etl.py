import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import urllib.parse

# =========================================================================
# 1. CREDENCIALES Y CONFIGURACIÓN
# =========================================================================
load_dotenv('.env')
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_DB = os.getenv("POSTGRES_DB")
DB_PORT = os.getenv("TIMESCALEDB_PORT", "5432")
DB_HOST = os.getenv("TIMESCALEDB_HOST", "192.168.3.155")

PROMETHEUS_HOST = os.getenv("PROMETHEUS_HOST", "192.168.3.155")
PROMETHEUS_PORT = os.getenv("PROMETHEUS_PORT", "9090")
PROMETHEUS_URL = f"http://{PROMETHEUS_HOST}:{PROMETHEUS_PORT}/api/v1/query_range"

# --- VARIABLE GLOBAL DE LA TABLA ---
DB_TABLE_NAME = "Servers" 

# =========================================================================
# 2. MOTOR DE CONSULTAS PROMQL POR ARQUITECTURA
# =========================================================================
SERVERS = {
    'Zephyrus': {
        'cpu_percent': '100 - (avg(rate(windows_cpu_time_total{mode="idle"}[1m])) * 100)',
        'cpu_cores': 'max(windows_cpu_logical_processor)',
        'cpu_freq_mhz': 'max(windows_cpu_core_frequency_mhz)',
        'cpu_mhz_used': '(100 - (avg(rate(windows_cpu_time_total{mode="idle"}[1m])) * 100)) / 100 * max(windows_cpu_logical_processor) * max(windows_cpu_core_frequency_mhz)',
        'mem_percent': '100 - (avg(windows_memory_physical_free_bytes) / avg(windows_memory_physical_total_bytes) * 100)',
        'mem_total_gb': 'avg(windows_memory_physical_total_bytes) / 1073741824',
        'mem_used_gb': '(avg(windows_memory_physical_total_bytes) - avg(windows_memory_physical_free_bytes)) / 1073741824'
    },
    'ThinkCentre': {
        'cpu_percent': '100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)',
        'cpu_cores': 'count(count(node_cpu_seconds_total{mode="idle"}) by (cpu))',
        'cpu_freq_mhz': 'avg(node_cpu_scaling_frequency_hertz) / 1000000',
        'cpu_mhz_used': '(100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)) / 100 * count(count(node_cpu_seconds_total{mode="idle"}) by (cpu)) * (avg(node_cpu_scaling_frequency_hertz) / 1000000)',
        'mem_percent': '100 * (1 - (avg(node_memory_MemAvailable_bytes) / avg(node_memory_MemTotal_bytes)))',
        'mem_total_gb': 'avg(node_memory_MemTotal_bytes) / 1073741824',
        'mem_used_gb': '(avg(node_memory_MemTotal_bytes) - avg(node_memory_MemAvailable_bytes)) / 1073741824'
    },
    'Nano_Jetson': {
        'cpu_percent': '100 - (avg(rate(node_cpu{mode="idle"}[1m])) * 100)',
        'cpu_cores': 'count(count(node_cpu{mode="idle"}) by (cpu))',
        'cpu_freq_mhz': 'avg(node_cpu_frequency_hertz) / 1000000',
        'cpu_mhz_used': '(100 - (avg(rate(node_cpu{mode="idle"}[1m])) * 100)) / 100 * count(count(node_cpu{mode="idle"}) by (cpu)) * (avg(node_cpu_frequency_hertz) / 1000000)',
        'mem_percent': '100 * (1 - (avg(node_memory_MemAvailable) / avg(node_memory_MemTotal)))',
        'mem_total_gb': 'avg(node_memory_MemTotal) / 1073741824',
        'mem_used_gb': '(avg(node_memory_MemTotal) - avg(node_memory_MemAvailable)) / 1073741824'
    }
}

# =========================================================================
# 3. FUNCIONES CORE ETL
# =========================================================================
def ensure_database_schema(engine):
    primer_servidor = list(SERVERS.keys())[0]
    metricas_base = SERVERS[primer_servidor].keys()
    
    columnas_sql = []
    for metrica in metricas_base:
        columnas_sql.append(f'"{metrica}_min" FLOAT')
        columnas_sql.append(f'"{metrica}_avg" FLOAT')
        columnas_sql.append(f'"{metrica}_max" FLOAT')
    
    columnas_formateadas = ",\n        ".join(columnas_sql)
    
    table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{DB_TABLE_NAME}" (
        "timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
        "ServerName" TEXT NOT NULL,
        {columnas_formateadas}
    );
    """
    
    hypertable_sql = f"""
    SELECT create_hypertable('"{DB_TABLE_NAME}"', 'timestamp', if_not_exists => TRUE);
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(table_sql))
            conn.execute(text(hypertable_sql))
        print(f"[+] Estructura de la tabla '{DB_TABLE_NAME}' verificada correctamente.")
    except Exception as e:
        print(f"[*] Nota DDL: {e}")

def fetch_and_resample(query, start_ts, end_ts):
    try:
        response = requests.get(PROMETHEUS_URL, params={
            'query': query, 'start': start_ts, 'end': end_ts, 'step': '15s'
        })
        response.raise_for_status()
        results = response.json().get('data', {}).get('result', [])
        
        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results[0]['values'], columns=['timestamp', 'value'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        df['value'] = pd.to_numeric(df['value'])
        df.set_index('timestamp', inplace=True)

        df_resampled = df.resample('5min').agg(
            min=('value', 'min'),
            avg=('value', 'mean'),
            max=('value', 'max')
        ).round(2)
        
        return df_resampled.dropna()
    except Exception as e:
        print(f"Error consultando API: {e}")
        return pd.DataFrame()

def run_incremental_etl(engine):
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Iniciando ciclo de Ingesta Incremental...")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    for server_name, metrics in SERVERS.items():
        print(f"\nSincronizando {server_name}...")
        
        try:
            with engine.connect() as conn:
                query_sql = text(f"SELECT MAX(timestamp) FROM \"{DB_TABLE_NAME}\" WHERE \"ServerName\" = '{server_name}'")
                last_ts = conn.execute(query_sql).scalar()
                
                if last_ts:
                    start_time = last_ts.replace(tzinfo=timezone.utc) + timedelta(minutes=5)
                else:
                    print("No hay historia previa detectada. Iniciando recuperación a 30 días...")
                    start_time = now - timedelta(days=30)
        except Exception:
            start_time = now - timedelta(days=30)

        chunk_start = start_time
        while chunk_start < now:
            chunk_end = min(chunk_start + timedelta(days=1), now)
            
            if (chunk_end - chunk_start).total_seconds() < 300:
                break
                     
            chunk_grid = pd.date_range(start=chunk_start, end=chunk_end, freq='5min', inclusive='left')
            
            dfs = []
            for metric_alias, query in metrics.items():
                df_temp = fetch_and_resample(query, chunk_start.timestamp(), chunk_end.timestamp())
                if not df_temp.empty:
                    df_temp.rename(columns={
                        'min': f"{metric_alias}_min",
                        'avg': f"{metric_alias}_avg",
                        'max': f"{metric_alias}_max"
                    }, inplace=True)
                    dfs.append(df_temp)

            if dfs:
                df_final = pd.concat(dfs, axis=1, join='outer')
            else:
                df_final = pd.DataFrame()
                
            df_final = df_final.reindex(chunk_grid)
            df_final.index.name = 'timestamp'
            df_final['ServerName'] = server_name
            
            df_final.to_sql(DB_TABLE_NAME, engine, if_exists='append', index=True)
            
            if dfs:
                print(f"  -> ✅ Lote inyectado: {chunk_start.strftime('%Y-%m-%d %H:%M')} a {chunk_end.strftime('%Y-%m-%d %H:%M')} ({len(df_final)} filas)")
            else:
                print(f"  -> 💤 Servidor en reposo. Rellenando con NULLs: {chunk_start.strftime('%Y-%m-%d %H:%M')} a {chunk_end.strftime('%Y-%m-%d %H:%M')}")
                
            chunk_start = chunk_end

if __name__ == "__main__":
    # Eliminamos el While True infinito. 
    # Airflow orquestará las corridas, esto se ejecuta una vez y se apaga limpiamente.
    print("Inicializando Motor ETL Multidimensional SARIMAX (Ejecución Airflow)...")
    
    safe_user = urllib.parse.quote_plus(DB_USER)
    safe_password = urllib.parse.quote_plus(DB_PASSWORD)
    db_url = f"postgresql://{safe_user}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_DB}?sslmode=disable"
    engine = create_engine(db_url)
    
    ensure_database_schema(engine)
    run_incremental_etl(engine)
    
    print("Ejecución finalizada con éxito. Airflow cerrará el contenedor.")