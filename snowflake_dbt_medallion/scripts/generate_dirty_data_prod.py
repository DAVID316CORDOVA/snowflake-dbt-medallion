#!/usr/bin/env python3
"""
Script incremental de datos sucios — capa RAW en Snowflake
Proyecto Medallion: Snowflake + dbt

Cada ejecucion agrega UN NUEVO LOTE de datos, sin borrar los anteriores.
Simula llegadas diarias para practicar Snowpipe / carga incremental.
Incluye columna VARIANT (atributos_extra) con JSON de forma variable,
para practicar schema evolution en datos semi-estructurados y LATERAL FLATTEN.

Uso: python scripts/generate_dirty_data_snowflake.py
"""

import os
import json
import random
import snowflake.connector
from datetime import date, timedelta

# ── CONFIGURACION ──────────────────────────────────────────────────────────────
SNOWFLAKE_CONFIG = {
    'account':   'bic01670.us-east-1',
    'user':      'david',
    'password':  os.environ['DBT_SNOWFLAKE_PASSWORD'],  # lee del .env cargado
    'role':      'ACCOUNTADMIN',
    'warehouse': 'COMPUTE_WH',
    'database':  'SNOWFLAKE_MEDALLION_PROD',  
    'schema':    'RAW',
}

N_CLIENTES = 3000
N_PEDIDOS  = 9000

CIUDADES  = ['Lima', 'Bogota', 'Buenos Aires', 'Santiago', 'Medellin',
             'Quito', 'Caracas', 'Montevideo', 'Ciudad de Mexico', 'Asuncion']
NOMBRES   = ['Carlos Garcia', 'Maria Lopez', 'Juan Rodriguez', 'Ana Martinez',
             'Pedro Gonzalez', 'Laura Sanchez', 'Roberto Diaz', 'Carmen Hernandez',
             'Jose Fernandez', 'Isabel Torres', 'Diego Ramirez', 'Sofia Herrera',
             'Andres Castro', 'Valentina Mora', 'Felipe Ortega']
PRODUCTOS = ['Laptop HP 15', 'Monitor Samsung 27"', 'Teclado Logitech MX',
             'Mouse Razer DeathAdder', 'Auriculares Sony WH-1000XM5',
             'Webcam Logitech C920', 'SSD Kingston 1TB', 'RAM Corsair 16GB',
             'Cable HDMI 4K', 'Hub USB-C 7en1', 'Disco Externo Seagate 2TB',
             'Router TP-Link AX3000', 'Tablet Samsung Tab A8', 'Impresora Epson L3250']
ESTADOS   = ['pendiente', 'procesando', 'enviado', 'entregado', 'cancelado']


# ── HELPERS ────────────────────────────────────────────────────────────────────
def rnd_date(start: date, end: date) -> str:
    return (start + timedelta(days=random.randint(0, (end - start).days))).isoformat()


def rnd_email(nombre: str) -> str:
    dominios = ['gmail.com', 'hotmail.com', 'yahoo.com', 'outlook.com', 'empresa.com']
    base = nombre.lower().replace(' ', '.')
    return f"{base}.{random.randint(1,99)}@{random.choice(dominios)}"


def rnd_atributos() -> dict:
    """Genera entre 2 y 4 pares clave-valor aleatorios, simulando un
    origen semi-estructurado donde no todos los registros traen
    los mismos campos (schema evolution en datos semi-estructurados)."""
    pool = {
        'color': ['negro', 'blanco', 'gris', 'azul', 'rojo'],
        'garantia_meses': [6, 12, 24, 36],
        'promocion': ['black_friday', 'cyber_monday', 'liquidacion', None],
        'canal_venta': ['web', 'tienda_fisica', 'marketplace', 'app'],
        'descuento_pct': [0, 5, 10, 15, 20],
        'sku_proveedor': [f"PRV-{random.randint(1000, 9999)}"],
    }
    n_keys = random.randint(2, 4)
    keys = random.sample(list(pool.keys()), n_keys)
    return {k: random.choice(pool[k]) for k in keys}


# ── SETUP ──────────────────────────────────────────────────────────────────────
def setup(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clientes (
            id             INT,
            nombre         VARCHAR(200),
            email          VARCHAR(200),
            ciudad         VARCHAR(100),
            fecha_registro VARCHAR(50),
            _batch_id      INT,
            _inserted_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pedidos (
            id               INT,
            cliente_id       INT,
            producto         VARCHAR(300),
            cantidad         VARCHAR(50),
            precio_unitario  VARCHAR(50),
            fecha_pedido     VARCHAR(50),
            estado           VARCHAR(50),
            _batch_id        INT,
            atributos_extra  VARIANT,
            _inserted_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    print("Tablas clientes y pedidos listas en RAW.")


# ── GENERADOR CLIENTES ─────────────────────────────────────────────────────────
def gen_clientes(cursor, batch_id: int):
    cursor.execute("SELECT COALESCE(MAX(id), 0) FROM clientes")
    nxt = cursor.fetchone()[0] + 1

    REG_INI, REG_FIN, HOY = date(2022, 1, 1), date(2025, 6, 30), date.today()
    rows, stats = [], {}

    clean_ids, clean_emails = [], []
    n_clean = round(N_CLIENTES * 0.80)
    for _ in range(n_clean):
        nom = random.choice(NOMBRES) + f" {random.randint(100, 9999)}"
        email = rnd_email(nom)
        rows.append((nxt, nom, email, random.choice(CIUDADES), rnd_date(REG_INI, REG_FIN), batch_id))
        clean_ids.append(nxt); clean_emails.append(email); nxt += 1
    stats['limpios'] = n_clean

    for _ in range(2):
        nom = random.choice(NOMBRES) + f" {random.randint(100, 9999)}"
        rows.append((nxt, nom, None, random.choice(CIUDADES), rnd_date(REG_INI, REG_FIN), batch_id))
        nxt += 1
    stats['email_null'] = 2

    dup_sources = random.sample(clean_ids, min(2, len(clean_ids)))
    for did in dup_sources:
        rows.append(next(r for r in rows if r[0] == did))
    stats['duplicados_exactos'] = len(dup_sources)

    email_dup = random.choice(clean_emails)
    nom2 = random.choice(NOMBRES) + f" {random.randint(100, 9999)}"
    rows.append((nxt, nom2, email_dup, random.choice(CIUDADES), rnd_date(REG_INI, REG_FIN), batch_id))
    nxt += 1
    stats['duplicado_negocio_email'] = 1

    rows.append((nxt, "  " + random.choice(NOMBRES).upper() + "  ", "usuariosinarroba.com",
                 "  " + random.choice(CIUDADES), rnd_date(REG_INI, REG_FIN), batch_id))
    nxt += 1
    stats['formato_nombre_email'] = 1

    fechas_malas = ['1800-03-15', (HOY + timedelta(days=500)).isoformat(), '99/99/9999']
    rows.append((nxt, random.choice(NOMBRES) + f" {random.randint(100, 9999)}",
                 rnd_email("test user"), random.choice(CIUDADES), random.choice(fechas_malas), batch_id))
    nxt += 1
    stats['fecha_invalida'] = 1

    stats['total_filas'] = len(rows)
    return rows, stats, clean_ids


# ── GENERADOR PEDIDOS ──────────────────────────────────────────────────────────
def gen_pedidos(cursor, batch_id: int, valid_cids: list):
    cursor.execute("SELECT COALESCE(MAX(id), 0) FROM pedidos")
    nxt = cursor.fetchone()[0] + 1

    REG_INI, REG_FIN, HOY = date(2022, 1, 1), date(2025, 6, 30), date.today()
    rows, stats = [], {}

    n_clean, clean_ids = round(N_PEDIDOS * 0.78), []
    for _ in range(n_clean):
        rows.append((nxt, random.choice(valid_cids), random.choice(PRODUCTOS),
                     str(random.randint(1, 20)), str(round(random.uniform(10.0, 2000.0), 2)),
                     rnd_date(REG_INI, REG_FIN), random.choice(ESTADOS), batch_id))
        clean_ids.append(nxt); nxt += 1
    stats['limpios'] = n_clean

    for _ in range(3):
        rows.append((nxt, random.choice(valid_cids), random.choice(PRODUCTOS),
                     str(random.randint(1, 10)), None, rnd_date(REG_INI, REG_FIN),
                     random.choice(ESTADOS), batch_id))
        nxt += 1
    stats['precio_null'] = 3

    for _ in range(2):
        fake_cid = random.randint(99000, 99999)
        rows.append((nxt, fake_cid, random.choice(PRODUCTOS), str(random.randint(1, 5)),
                     str(round(random.uniform(10.0, 500.0), 2)), rnd_date(REG_INI, REG_FIN),
                     random.choice(ESTADOS), batch_id))
        nxt += 1
    stats['fk_huerfana'] = 2

    dup_pids = random.sample(clean_ids, min(2, len(clean_ids)))
    for dp in dup_pids:
        rows.append(next(r for r in rows if r[0] == dp))
    stats['duplicados_exactos'] = len(dup_pids)

    for _ in range(2):
        fecha_fut = (HOY + timedelta(days=random.randint(30, 730))).isoformat()
        rows.append((nxt, random.choice(valid_cids), random.choice(PRODUCTOS),
                     str(random.randint(1, 10)), str(round(random.uniform(10.0, 500.0), 2)),
                     fecha_fut, random.choice(ESTADOS), batch_id))
        nxt += 1
    stats['fecha_futura'] = 2

    fechas_raras = ['15/03/2024', '2024.07.22', 'marzo 2024', '32/13/2023']
    for _ in range(2):
        rows.append((nxt, random.choice(valid_cids), "  " + random.choice(PRODUCTOS).lower() + "  ",
                     str(random.randint(1, 5)), str(round(random.uniform(10.0, 200.0), 2)),
                     random.choice(fechas_raras), random.choice(ESTADOS), batch_id))
        nxt += 1
    stats['fecha_formato_raro'] = 2

    for _ in range(2):
        rows.append((nxt, random.choice(valid_cids), random.choice(PRODUCTOS),
                     str(random.choice([0, -1, -3])), str(round(random.uniform(-500.0, 0), 2)),
                     rnd_date(REG_INI, REG_FIN), random.choice(ESTADOS), batch_id))
        nxt += 1
    stats['numericos_invalidos'] = 2

    textos_corruptos = ['N/A', '#ERROR!', 'null_val', 'ABCD$$', '???']
    for _ in range(3):
        rows.append((nxt, random.choice(valid_cids), random.choice(PRODUCTOS),
                     random.choice(textos_corruptos), str(round(random.uniform(10.0, 500.0), 2)),
                     rnd_date(REG_INI, REG_FIN), random.choice(ESTADOS), batch_id))
        nxt += 1
    stats['texto_corrupto_cantidad'] = 3

    stats['total_filas'] = len(rows)

    # Agrega el JSON de atributos_extra (2 a 4 llaves variables) a cada fila
    rows = [r + (json.dumps(rnd_atributos()),) for r in rows]

    return rows, stats


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    setup(cursor)

    cursor.execute("SELECT COALESCE(MAX(_batch_id), 0) + 1 FROM clientes")
    batch_id = cursor.fetchone()[0]
    print(f"\nGenerando LOTE #{batch_id}...")

    # ── Clientes ───────────────────────────────────────────────────────────────
    c_rows, c_stats, new_valid_cids = gen_clientes(cursor, batch_id)
    cursor.executemany(
        "INSERT INTO clientes (id, nombre, email, ciudad, fecha_registro, _batch_id) "
        "VALUES (%s, %s, %s, %s, %s, %s)", c_rows
    )
    print(f"  -> {len(c_rows)} filas insertadas en clientes")

    cursor.execute("SELECT DISTINCT id FROM clientes WHERE id IS NOT NULL")
    all_valid_cids = [r[0] for r in cursor.fetchall()] or new_valid_cids

    # ── Pedidos (incluye atributos_extra como JSON via PARSE_JSON) ──────────────
    p_rows, p_stats = gen_pedidos(cursor, batch_id, all_valid_cids)
    cursor.executemany(
        "INSERT INTO pedidos "
        "(id, cliente_id, producto, cantidad, precio_unitario, fecha_pedido, estado, _batch_id, atributos_extra) "
        "SELECT $1, $2, $3, $4, $5, $6, $7, $8, PARSE_JSON($9) "
        "FROM VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        p_rows
    )
    print(f"  -> {len(p_rows)} filas insertadas en pedidos (con atributos_extra VARIANT)")

    cursor.close()
    conn.close()

    print(f"""
=================================================================
  RESUMEN LOTE #{batch_id}
=================================================================
  CLIENTES ({c_stats['total_filas']} filas)
  -----------------------------------------------------------------
  Limpios                           : {c_stats['limpios']}
  Email NULL                        : {c_stats['email_null']}
  Duplicados exactos                : {c_stats['duplicados_exactos']}
  Duplicado de negocio (mismo email): {c_stats['duplicado_negocio_email']}
  Formato incorrecto                : {c_stats['formato_nombre_email']}
  Fecha invalida                    : {c_stats['fecha_invalida']}
-----------------------------------------------------------------
  PEDIDOS ({p_stats['total_filas']} filas)
-----------------------------------------------------------------
  Limpios                           : {p_stats['limpios']}
  precio_unitario NULL              : {p_stats['precio_null']}
  FK huerfana                       : {p_stats['fk_huerfana']}
  Duplicados exactos                : {p_stats['duplicados_exactos']}
  Fecha futura                      : {p_stats['fecha_futura']}
  Fecha formato raro                : {p_stats['fecha_formato_raro']}
  Cantidad/precio invalidos         : {p_stats['numericos_invalidos']}
  Texto corrupto en cantidad        : {p_stats['texto_corrupto_cantidad']}
  atributos_extra (VARIANT, 2-4 keys variables) en TODAS las filas
=================================================================
  Ejecuta de nuevo para agregar el LOTE #{batch_id + 1}
=================================================================
""")


if __name__ == '__main__':
    main()