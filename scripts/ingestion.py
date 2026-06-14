import os
import re
import pandas as pd
from sqlalchemy import create_engine

def run_ingestion():
    # 1. Ruta del dataset dentro del contenedor
    csv_path = '/opt/airflow/data/ecommerce_data.csv'
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"El dataset no se encuentra en: {csv_path}")

    print("Iniciando la lectura del dataset...")
    df = pd.read_csv(csv_path, encoding='ISO-8859-1')

    # 2. Limpieza tipográfica con Regex
    print("Aplicando reglas de limpieza...")

    # Regla 1: limpiar caracteres especiales
    def clean_special_chars(text):
        if pd.isna(text):
            return "SIN_DESCRIPCION"
        return re.sub(r'[^a-zA-Z0-9\s\-]', '', str(text))
    df['Description'] = df['Description'].apply(clean_special_chars)

    # Regla 2: normalizar espacios
    def normalize_spacing(text):
        if pd.isna(text):
            return text
        return re.sub(r'\s+', ' ', str(text)).strip()
    df['Description'] = df['Description'].apply(normalize_spacing)

    # Regla 3: validar facturas
    def validate_invoice_format(invoice):
        inv_str = str(invoice).strip()
        if re.match(r'^C?\d{6}$', inv_str):
            return inv_str
        return "INVALID_INVOICE"
    df['InvoiceNo'] = df['InvoiceNo'].apply(validate_invoice_format)

    # 3. Conexión a PostgreSQL en Docker
    DB_USER = "uteq_user"
    DB_PASSWORD = "uteq_password"
    DB_DB = "dw_analytics"
    DB_PORT = "5432"
    DB_HOST = "postgres_staging"  # IMPORTANTE: nombre del contenedor

    conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DB}"
    engine = create_engine(conn_str)

    # 4. Carga masiva por chunks
    print("Cargando datos en staging...")
    df.to_sql(
        name='stg_ecommerce_sales',
        con=engine,
        if_exists='replace',
        index=False,
        chunksize=10000,
        method='multi'
    )
    print("¡Ingesta finalizada con éxito!")

if __name__ == "__main__":
    run_ingestion()
