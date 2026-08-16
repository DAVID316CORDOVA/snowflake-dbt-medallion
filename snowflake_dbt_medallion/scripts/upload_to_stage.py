#!/usr/bin/env python3
"""Sube un archivo local al stage interno de Snowflake usando PUT,
y luego dispara manualmente el Snowpipe (simulando lo que la
notificacion automatica de SQS haria en un escenario con S3 real)."""

import os
import sys
import snowflake.connector

SNOWFLAKE_CONFIG = {
    'account':   'bic01670.us-east-1',
    'user':      'david',
    'password':  os.environ['DBT_SNOWFLAKE_PASSWORD'],
    'role':      'ACCOUNTADMIN',
    'warehouse': 'COMPUTE_WH',
    'database':  'SNOWFLAKE_MEDALLION',
    'schema':    'RAW',
}

if len(sys.argv) < 2:
    print("Uso: python scripts/upload_to_stage.py <archivo.csv>")
    sys.exit(1)

filepath = sys.argv[1]

conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cursor = conn.cursor()

# Sube el archivo local al stage interno
put_command = f"PUT file://{os.path.abspath(filepath)} @raw_stage AUTO_COMPRESS=TRUE"
cursor.execute(put_command)
print(f"Archivo subido al stage: {filepath}")

# Dispara el pipe manualmente (equivalente a lo que SQS haria automatico)
cursor.execute("ALTER PIPE pedidos_pipe REFRESH")
print("Snowpipe disparado manualmente.")

cursor.close()
conn.close()
