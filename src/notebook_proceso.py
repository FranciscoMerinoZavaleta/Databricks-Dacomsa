# Leer el parámetro definido en el YAML
estado = dbutils.widgets.getArgument("estado", "no_recibido")

print(f"--- Ejecución de prueba en Databricks ---")
print(f"El estado recibido es: {estado}")
print(f"Despliegue desde GitHub Actions: EXITOSO")