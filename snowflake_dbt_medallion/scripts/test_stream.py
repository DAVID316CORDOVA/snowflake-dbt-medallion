#!/usr/bin/env python3
"""
Script dedicado para probar el Stream sobre pedidos_snowpipe.
Genera un CSV nuevo con nombre unico (timestamp), lo sube al stage,
dispara el pipe, y luego consulta el Stream para confirmar que
detecto los cambios. Todo en un solo script, aislado de los demas.
"""

import os
import csv
import random
from datetime import date, datetime, timedelta
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

PRODUCTOS = ['Laptop HP 15', 'Monitor Samsung 27"', 'Teclado Logitech MX',
             'Mouse Razer DeathAdder', 'SSD Kingston 1TB']
ESTADOS = ['pendiente', 'procesando', 'enviado', 'entregado']


def generar_csv() -> str:
    """Genera un CSV con nombre unico (timestamp completo), para que
    Snowpipe nunca lo confunda con un archivo ya procesado."""
    filename = f"pedidos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'cliente_id', 'producto', 'cantidad',
                          'precio_unitario', 'fecha_pedido', 'estado'])
        base_id = random.randint(200000, 299999)
        for i in range(10):
            writer.writerow([
                base_id + i,
                random.randint(1, 2400),
                random.choice(PRODUCTOS),
                random.randint(1, 10),
                round(random.uniform(10, 1000), 2),
                (date.today() - timedelta(days=random.randint(0, 3))).isoformat(),
                random.choice(ESTADOS),
            ])
    print(f"[1/4] CSV generado: {filename} (10 filas)")
    return filename


def subir_y_disparar(cursor, filepath: str):
    put_command = f"PUT file://{os.path.abspath(filepath)} @raw_stage AUTO_COMPRESS=TRUE"
    cursor.execute(put_command)
    print(f"[2/4] Archivo subido al stage.")

    cursor.execute("ALTER PIPE pedidos_pipe REFRESH")
    print(f"[3/4] Pipe disparado (REFRESH).")


def revisar_stream(cursor):
    cursor.execute("SELECT * FROM pedidos_snowpipe_stream")
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    print(f"[4/4] Stream detecto {len(rows)} fila(s) nueva(s):\n")
    if not rows:
        print("  (vacio -- puede que el pipe aun no haya terminado de cargar, "
              "espera unos segundos y vuelve a correr este script, o consulta "
              "el stream manualmente en Snowflake)")
        return

    print(f"  {colnames}")
    for row in rows[:5]:
        print(f"  {row}")
    if len(rows) > 5:
        print(f"  ... y {len(rows) - 5} filas mas")


def main():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    filepath = generar_csv()
    subir_y_disparar(cursor, filepath)

    print("\nEsperando unos segundos a que el pipe procese...")
    import time
    time.sleep(15)

    revisar_stream(cursor)

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
