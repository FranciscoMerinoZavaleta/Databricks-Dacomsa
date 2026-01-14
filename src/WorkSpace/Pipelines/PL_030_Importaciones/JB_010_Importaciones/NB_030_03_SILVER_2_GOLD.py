# Databricks notebook source
# MAGIC %md
# MAGIC # INCLUDES

# COMMAND ----------

# MAGIC %run /Shared/Importaciones/Includes/

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# COMMAND ----------

df_tabla = (spark.read.
            parquet("/mnt/importacion-bo/silver/CTL_importaciones.parquet/"))

# COMMAND ----------

# Agregar columna de mes (ya tiene una de ANIO)
df_tabla = df_tabla.withColumn("MES", month("FECHA_PAGO"))
df_tabla = df_tabla.withColumn("DIA", dayofmonth(col("FECHA_PAGO")))
df_tabla = df_tabla.withColumnRenamed("Id Fecha", "Id_Fecha")

df_tabla = (df_tabla
            .withColumn("FRACCION", col("FRACCION").cast(DoubleType()))
            .withColumn("FRACCION_NICO", col("FRACCION_NICO").cast(DoubleType()))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Proveedor Importador Acumulado (V1.1)

# COMMAND ----------

df_R1 = (
    df_tabla
    .groupBy(
        "ANIO",
        "MES",
        "RFC_CAUSANTE",
        "RAZON_CAUSANTE",
        "DENOMINACION",
        "NOMBRE_ADUANA",
        "ESTADO_CAUSANTE",
        "FRACCION",
        "DESCRIPCION",
        "RAZON_SOCIAL",
        "PAISORIGEN",
        "PAISRUTA",
        "REGISTRO",
        "FRACCION_NICO"
    )
    .agg(
        F.sum(F.col("VALOR_COMERCIAL") / 1000).alias("VALOR_COMERCIAL_K"),
        F.sum(F.col("CANTIDAD")).alias("CANTIDAD_TOTAL")
    )
)

# COMMAND ----------

# Se hace un unpivot de las columnas VALOR_COMERCIAL_K y CANTIDAD_TOTAL para la visualización que se requiere en power bi. Esto duplica el número de registros

df_R1 = df_R1.selectExpr(
    "ANIO",
    "MES",
    "RFC_CAUSANTE",
    "RAZON_CAUSANTE",
    "DENOMINACION",
    "NOMBRE_ADUANA",
    "ESTADO_CAUSANTE",
    "FRACCION",
    "DESCRIPCION",
    "RAZON_SOCIAL",
    "PAISORIGEN",
    "PAISRUTA",
    "REGISTRO",
    "FRACCION_NICO",
    "stack(2, 'VALOR_COMERCIAL_K', VALOR_COMERCIAL_K, 'CANTIDAD_TOTAL', CANTIDAD_TOTAL) as (UNIDAD, CANTIDAD)"
)

# COMMAND ----------

target_path = f"/mnt/importacion-bo/gold/Proveedor_Importador_Acumulado.parquet/"

df_R1=df_R1.withColumn("ANIO_1", col("ANIO"))
#Escritura de los archivos gold
(df_R1.
    write.format("delta").
    partitionBy('ANIO_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Proveedor Importador Acumulado- Direcciones

# COMMAND ----------

df_R2 = (
    df_tabla
    .groupBy(
        "ANIO",
        "MES",
        "DIA",
        "RFC_CAUSANTE",
        "RAZON_CAUSANTE",
        "DENOMINACION",
        "NOMBRE_ADUANA",
        "ESTADO_CAUSANTE",
        "FRACCION",
        "DESCRIPCION",
        "RAZON_SOCIAL",
        "PAISORIGEN",
        "PAISRUTA",
        "REGISTRO",
        "DOMICILIO_CAUSANTE",
        "CP_CAUSANTE",
        "MUNICIPIO_CAUSANTE",
        "FECHA_PAGO",  
        "Id_Fecha",    
        "DIRECCION",
        "ESTADO",
        "CP",
        "FRACCION_NICO"
    )
    .agg(
        F.sum(F.col("VALOR_COMERCIAL") / 1000).alias("VALOR_COMERCIAL"),
        F.sum(F.col("CANTIDAD")).alias("CANTIDAD")
    )
)

# COMMAND ----------

target_path = f"/mnt/importacion-bo/gold/Proveedor_Importador_Acumulado_Direcciones.parquet/"

df_R2=df_R2.withColumn("ANIO_1", col("ANIO"))
#Escritura de los archivos gold
(df_R2.
    write.format("delta").
    partitionBy('ANIO_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Proveedor Importador Meses (V1.1)

# COMMAND ----------

df_R3 = (
    df_tabla
    .groupBy(
        "MES",
        "ANIO",
        "RFC_CAUSANTE",
        "RAZON_CAUSANTE",
        "DENOMINACION",
        "NOMBRE_ADUANA",
        "ESTADO_CAUSANTE",
        "FRACCION",
        "DESCRIPCION",
        "RAZON_SOCIAL",
        "PAISORIGEN",
        "PAISRUTA",
        "REGISTRO",
        "FRACCION_NICO"
    )
    .agg(
        F.sum(F.col("VALOR_COMERCIAL") / 1000).alias("VALOR_COMERCIAL"),
        F.sum(F.col("CANTIDAD")).alias("CANTIDAD")
    )
)

# COMMAND ----------

#from pyspark.sql.functions import col

df_R3 = df_R3.selectExpr(
    "MES",
    "ANIO",
    "RFC_CAUSANTE",
    "RAZON_CAUSANTE",
    "DENOMINACION",
    "NOMBRE_ADUANA",
    "ESTADO_CAUSANTE",
    "FRACCION",
    "DESCRIPCION",
    "RAZON_SOCIAL",
    "PAISORIGEN",
    "PAISRUTA",
    "REGISTRO",
    "FRACCION_NICO",
    "stack(2, 'VALOR_COMERCIAL', VALOR_COMERCIAL, 'CANTIDAD', CANTIDAD) as (UNIDAD, CANTIDAD)"
)

# COMMAND ----------

target_path = f"/mnt/importacion-bo/gold/Proveedor_Importador_Meses_(V1.1).parquet/"

df_R3=df_R3.withColumn("ANIO_1", col("ANIO"))
#Escritura de los archivos gold
(df_R3.
    write.format("delta").
    partitionBy('ANIO_1').
    mode("overwrite").
    parquet(target_path))