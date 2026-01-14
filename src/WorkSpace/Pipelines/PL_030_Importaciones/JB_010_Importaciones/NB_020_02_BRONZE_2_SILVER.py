# Databricks notebook source
# MAGIC %md
# MAGIC # Includes

# COMMAND ----------

# MAGIC %run /Shared/Importaciones/Includes/

# COMMAND ----------

# Leer el archivo parquet (tabla bronce)
df_tabla = spark.read.parquet('/mnt/importacion-bo/bronze/Tabla_ADF/CTL_importaciones.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC # Casteo de columnas

# COMMAND ----------

# Columnas numéricas
numericas = ["CANTIDAD","VALOR_COMERCIAL"]

# Columnas string
string = ["CP","CP_CAUSANTE","DENOMINACION","DESCRIPCION","DIRECCION",
"DOMICILIO_CAUSANTE","ESTADO","ESTADO_CAUSANTE","Id Fecha","MUNICIPIO_CAUSANTE","NOMBRE_ADUANA","PAISORIGEN","PAISRUTA","RAZON_CAUSANTE","RAZON_SOCIAL","REGISTRO","RFC_CAUSANTE","FRACCION_NICO","FRACCION"]

# Columnas de tipo fecha
fechas = ["FECHA_PAGO"]
S =["SEQ_DATE"]


# COMMAND ----------

# Se itera sobre las columnas para emparejarlas usando zip. Transformar a numeric data y date usando función convert_columns
#numericas = ["CANTIDAD" y "VALOR_COMERCIAL"]
#fechas = ["FECHA_PAGO"]
columns = [numericas, fechas]
tipos = ["numerico", "fecha"]

for cols, t in zip(columns, tipos):
    df_tabla = convert_columns(df_tabla, cols,t)

# COMMAND ----------

# MAGIC %md
# MAGIC # Identificación de llaves

# COMMAND ----------

df_grouped = (df_tabla
              .groupBy("CP","CP_CAUSANTE","DENOMINACION","DESCRIPCION","DIRECCION","DOMICILIO_CAUSANTE","ESTADO",
                       "ESTADO_CAUSANTE","Id Fecha","MUNICIPIO_CAUSANTE","NOMBRE_ADUANA","PAISORIGEN","PAISRUTA",
                       "RAZON_CAUSANTE","RAZON_SOCIAL","REGISTRO","RFC_CAUSANTE","FECHA_PAGO","FRACCION","FRACCION_NICO","SEQ_DATE",)
              .agg(sum("CANTIDAD").alias("CANTIDAD"),
                   sum("VALOR_COMERCIAL").alias("VALOR_COMERCIAL")))

# COMMAND ----------

#### borrar genaro tema de ricardo
#df_grouped.count()


# #### borrar genaro tema de ricardo
df_grouped.filter((col("RAZON_CAUSANTE")=="REFACCIONARIA ROGELIO SA DE CV") & (col("FECHA_PAGO").between("2025-05-01", "2025-05-31"))).display()
df_grouped.filter((col("RAZON_CAUSANTE")=="REFACCIONARIA ROGELIO SA DE CV") & (col("FECHA_PAGO").between("2025-06-01", "2025-06-30"))).display()
df_grouped.filter((col("RAZON_CAUSANTE")=="REFACCIONARIA ROGELIO SA DE CV") & (col("FECHA_PAGO").between("2025-07-01", "2025-07-31"))).display()
df_grouped.filter((col("RAZON_CAUSANTE")=="REFACCIONARIA ROGELIO SA DE CV") & (col("FECHA_PAGO").between("2025-08-01", "2025-08-31"))).display()


# COMMAND ----------

## codigo para eliminar duplicados dependiendo del SEQ DATE verifica cual es el mas reciente y elimina el resto para evitar que se dupliquen

from pyspark.sql import functions as F, Window

# Lista de todas tus columnas excepto SEQ_DATE
keys = [c for c in df_grouped.columns if c != "SEQ_DATE"]

# Definir ventana por todas esas columnas
w = Window.partitionBy(*keys).orderBy(F.col("SEQ_DATE").desc())

# Conservar solo el registro más reciente
df_grouped = (df_grouped
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)


# COMMAND ----------

df_grouped.filter(col("FECHA_PAGO").between("2025-05-01", "2025-05-31")).display()
df_grouped.filter(col("FECHA_PAGO").between("2025-06-01", "2025-06-30")).display()
df_grouped.filter(col("FECHA_PAGO").between("2025-07-01", "2025-07-31")).display()
df_grouped.filter(col("FECHA_PAGO").between("2025-08-01", "2025-08-31")).display()

# COMMAND ----------

# Reemplazar valores vacios por null

string_cols = ["CP","CP_CAUSANTE","DENOMINACION","DESCRIPCION","DIRECCION",
"DOMICILIO_CAUSANTE","ESTADO","ESTADO_CAUSANTE","Id Fecha","MUNICIPIO_CAUSANTE","NOMBRE_ADUANA","PAISORIGEN","PAISRUTA","RAZON_CAUSANTE","RAZON_SOCIAL","REGISTRO","RFC_CAUSANTE","FRACCION","FRACCION_NICO"]

for c in string_cols:
    df_grouped = df_grouped.withColumn(
        c,
        when(col(c) == "", lit(None)).otherwise(col(c))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Window partition

# COMMAND ----------


keys = ["CP","CP_CAUSANTE","DENOMINACION","DESCRIPCION","DIRECCION",
"DOMICILIO_CAUSANTE","ESTADO","ESTADO_CAUSANTE","Id Fecha","MUNICIPIO_CAUSANTE","NOMBRE_ADUANA","PAISORIGEN","PAISRUTA","RAZON_CAUSANTE","RAZON_SOCIAL","REGISTRO","RFC_CAUSANTE","FECHA_PAGO","FRACCION","FRACCION_NICO","SEQ_DATE"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df_grouped = (df_grouped
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )

# COMMAND ----------

# Generar variable de año refente al campo de año que usamos para separar (esa columna se usará en el partition by al grabar)
df_grouped = df_grouped.withColumn("ANIO", year(col("FECHA_PAGO")))

# COMMAND ----------

df_grouped = df_grouped.withColumn("SEQ_DATE", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC # Escritura del archivo

# COMMAND ----------

target_path = f"/mnt/importacion-bo/silver/CTL_importaciones.parquet/"

#Escritura de los archivos silver
(df_grouped.
    write.format("delta").
    partitionBy('ANIO').
    mode("overwrite").
    parquet(target_path))