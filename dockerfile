# 1. Imagen base ligera de Python
FROM python:3.11-slim

# 2. Configurar variables de entorno para evitar retrasos en logs de consola
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Crear y establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# 4. Copiar e instalar dependencias primero (aprovecha la caché de capas de Docker)
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiar el script de Python al contenedor
COPY etl.py .

# 6. Comando de ejecución por defecto
CMD ["python", "etl.py"]