# Databricks notebook source
# El resto de tu código abajo...
estado = dbutils.widgets.getArgument("estado", "desconocido")
print(f"Estado recibido: {estado}")

print(f"--- Ejecución de prueba en Databricks ---")
print(f"El estado recibido es: {estado}")
print(f"Despliegue desde GitHub Actions: EXITOSO")