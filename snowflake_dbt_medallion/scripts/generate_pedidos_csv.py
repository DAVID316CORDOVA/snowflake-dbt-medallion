#!/usr/bin/env python3
"""Genera un CSV de pedidos nuevos, para simular la llegada de un
archivo externo que Snowpipe va a detectar y cargar automaticamente."""

import csv
import random
from datetime import date, timedelta

PRODUCTOS = ['Laptop HP 15', 'Monitor Samsung 27"', 'Teclado Logitech MX',
             'Mouse Razer DeathAdder', 'SSD Kingston 1TB']
ESTADOS = ['pendiente', 'procesando', 'enviado', 'entregado']

filename = f"pedidos_{date.today().isoformat()}.csv"

with open(filename, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['id', 'cliente_id', 'producto', 'cantidad',
                      'precio_unitario', 'fecha_pedido', 'estado'])
    for i in range(50):
        writer.writerow([
            90000 + i,
            random.randint(1, 2400),
            random.choice(PRODUCTOS),
            random.randint(1, 10),
            round(random.uniform(10, 1000), 2),
            (date.today() - timedelta(days=random.randint(0, 5))).isoformat(),
            random.choice(ESTADOS),
        ])

print(f"Archivo generado: {filename}")
