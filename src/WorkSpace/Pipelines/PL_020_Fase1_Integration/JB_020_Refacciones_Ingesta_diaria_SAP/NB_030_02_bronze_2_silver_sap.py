# Databricks notebook source
# MAGIC %md
# MAGIC # Includes

# COMMAND ----------

# MAGIC %run /Shared/FASE1_INTEGRACION/includes

# COMMAND ----------

### Funcion para convertir columnas por tipo de dato usada en la capa bronze to silver
def convert_columns(df, columns, type_column):
    if type_column == "numerico":
        for column in columns:
            df = df.withColumn(column, col(column).cast('float'))
    else:
        for column in columns:
            df = df.withColumn(column, to_date(col(column), "yyyy-MM-dd"))
    return df

# COMMAND ----------

# MAGIC %md 
# MAGIC # Read Log Table

# COMMAND ----------

LOG_TABLAS_SAP = spark.read.parquet("/mnt/adls-dac-data-bi-pr-scus/bronze/LOG_TABLAS_SAP.parquet").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC #Mapping User's columns name

# COMMAND ----------


def limpiar_columna(dataframe, nombre_columna):
    columna_limpia = (
        dataframe[nombre_columna]
        .map(str)
        .str.upper()  # Convertir a mayúsculas
        .str.replace('/', '_')  # Eliminar diagonales
        .str.replace('°', 'O')
        .str.replace('º', 'O')
        .str.replace('.',' ')
        .str.strip()
        .str.replace('  ',' ')
        .str.replace(' ','_')
        .str.replace('=','_')
        .str.replace('-','_')
        .str.replace('(','')
        .str.replace(')','')
        .apply(unidecode) # Eliminar símbolos de grados
    )
    return columna_limpia

# COMMAND ----------

# MAGIC %md
# MAGIC # Load and transform bronze tables to silver tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### KONP (carga delta)

# COMMAND ----------

table = "KONP"
delta_date = (LOG_TABLAS_SAP.filter(col('TABLA') == table)                            
              .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias('max'))                            
              .collect()[0].max)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path)
       .filter(col("SEQ_DATE") >= delta_date))
# Se itera sobre las columnas para emparejarlas usando zip. Transformar a numeric data y date usando función convert_columns
columns_n = ['FKWRT', 'GKWRT', 'KBETR', 'KBRUE', 'KSTBM', 'MXWRT', 'PKWRT', 'RSWRT', 'UKBAS']
columns_d = []
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys = ["MANDT","KNUMH","KOPOS"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR','MONTH','MANDANTE','CLASE_DE_CONDICION').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### MARA (carga delta)

# COMMAND ----------

table = "MARA"
delta_date = (LOG_TABLAS_SAP.filter(col('TABLA') == table)                            
              .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias('max'))                            
              .collect()[0].max)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = (spark.
       read.
       parquet(source_path))
       #.filter(col("SEQ_DATE") >= delta_date))
# Se itera sobre las columnas para emparejarlas usando zip. Transformar a numeric data y date usando función convert_columns
columns_n = ['BRIDGE_MAX_SLOPE', 'BRIDGE_TRESH', 'GEWTO', 'MAXC', 'MAXC_TOL', 'OVERHANG_TRESH',
             'SAPMP_FBDK', 'SAPMP_FBHK', 'SAPMP_KADP', 'SAPMP_KADU', 'SAPMP_KEDU', 'SAPMP_MIFRR',
             'SAPMP_SPBI', 'SAPMP_TRAD', 'VOLTO', 'WESCH','NTGEW','BRGEW'] #columnas numericas
columns_d = ["ERSDA","LAEDA"] #columnas fecha
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys = ["MANDT","MATNR"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))

df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",sheet_name="MARA")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR','MONTH','MANDANTE',"TIPO_MATERIAL").
    mode("overwrite").
    parquet(target_path))


# COMMAND ----------

# MAGIC %md
# MAGIC ### MAKT (carga histórica completa)

# COMMAND ----------

table = "MAKT"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = (spark.
       read.
       parquet(source_path))
       #.filter(col("SEQ_DATE") >= delta_date))
columns_n = [] #columnas numericas
columns_d = [] #columnas fecha
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys = ["MANDT","MATNR","SPRAS"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",sheet_name="MAKT")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR','MONTH','MANDANTE','CLAVE_DE_IDIOMA').
    mode("overwrite").
    parquet(target_path))


# COMMAND ----------

# MAGIC %md
# MAGIC ### MARC (carga delta)

# COMMAND ----------

table = "MARC"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
delta_date = (LOG_TABLAS_SAP.filter(col('TABLA') == table)                            
              .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias('max'))                            
              .collect()[0].max)
df = (spark.
       read.
       parquet(source_path))
       #.filter(col("SEQ_DATE") >= delta_date))
columns_n = ['BWTTY', 'MMSTA', 'MMSTD', 'DISPR', 'SOBSL', 'SBDKZ'] #columnas numericas
columns_d = [] #columnas fecha
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys = ["MANDT","MATNR","WERKS"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",sheet_name="MARC")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR','MONTH','MANDANTE','CENTRO').
    mode("append").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### MBEW (carga mensual completa)

# COMMAND ----------

table = "MBEW"
delta_date = (LOG_TABLAS_SAP.filter(col('TABLA') == table)                            
              .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias('max'))                            
              .collect()[0].max)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = (spark.
       read.
       parquet(source_path)
       .filter(col("SEQ_DATE") >= delta_date))
columns_n = ["LBKUM","SALK3","VERPR", "STPRS","PEINH","SALKV","VJKUM","VJSAL","VJVER","VJSTP","VJSAV","LFGJA","LFMON","STPRV","ZKPRS","BWPRS","BWPRH","VJBWS","VJBWH","VVJSL","VVJLB","VVMLB","VVSAL","ZPLPR","BWPH1","BWPS1","VPLPR","LPLPR"] #columnas numericas
columns_d = ["LAEPR"] #columnas fecha
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)

keys = ["MANDT","MATNR","BWKEY"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",sheet_name="MBEW")
aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR','MONTH','MANDANTE').
    mode("append").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### MLGN (carga histórica completa)

# COMMAND ----------

table = "MLGN"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = (spark.
       read.
       parquet(source_path))
# for c in df.columns:
#     df = df.withColumn(c,
#                        when(col(c)==" ",lit(None)).
#                        otherwise(col(c)))
columns_n = ["LHMG1","LHMG2","LHMG3","MKAPV"] #columnas numericas
columns_d = [] #columnas fecha
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys = ["MANDT","MATNR","LGNUM"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",sheet_name="MLGN")
aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))

(df.
    write.format("delta").
    partitionBy('YEAR','MONTH','MANDANTE').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### VBAP (carga delta)

# COMMAND ----------

table = "VBAP"
delta_date = (LOG_TABLAS_SAP.filter(col('TABLA') == table)                            
              .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias('max'))                            
              .collect()[0].max)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = (spark.
       read.
       parquet(source_path).
       filter(col("SEQ_DATE") >= delta_date))
columns_n = ['NETWR'] #columnas numericas
columns_d = ['ERDAT'] #columnas fecha
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys = ["MANDT","VBELN","POSNR"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
    
aux = pd.read_excel("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",sheet_name="VBAP")
aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))

(df.
    write.format("delta").
    partitionBy('YEAR').
    mode("append").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### A004 (carga delta)

# COMMAND ----------

table = "A004"
delta_date = (LOG_TABLAS_SAP.filter(col('TABLA') == table)                           
              .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias('max'))                            
              .collect()[0].max)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
       #.filter(col("SEQ_DATE") >= delta_date))
columns_n = [] #columnas numericas
columns_d = ["DATAB","DATBI"] #columnas fecha
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys = ["MANDT","KAPPL","MATNR"]
particion = (Window.
             partitionBy(keys).orderBy(desc('DATAB'),desc('SEQ_DATE')))
             
df = (df.withColumn("ROW_NUM", row_number().over(particion))
      .filter(col('ROW_NUM')==1)
      .drop("ROW_NUM"))
    

aux = pd.read_excel("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",sheet_name="A004")
aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR','MONTH','MANDANTE')
    .mode("overwrite")
    .parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### MVKE (carga histórica)

# COMMAND ----------

table = "MVKE"
# delta_date = (
#     LOG_TABLAS_SAP.filter(col("TABLA") == table)
#     .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias("max"))
#     .collect()[0]
#     .max
# )
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = spark.read.parquet(source_path)#.filter(col("SEQ_DATE") >= delta_date)
df = (
    df.withColumn("SEQ_DATE", current_date())
      .withColumn("YEAR", year(col("SEQ_DATE")))
      .withColumn("MONTH", month(col("SEQ_DATE")))
)


columns_n = []  # columnas numericas
columns_d = []  # columnas fecha
columns = [columns_n, columns_d]
tipos = ["numerico", "fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols, t)
keys = ["MANDT", "MATNR", "VKORG", "VTWEG","DWERK"]
particion = Window.partitionBy(keys).orderBy(desc("SEQ_DATE"))
df = (
    df.withColumn("ROW_NUM", row_number().over(particion))
    .filter(col("ROW_NUM") == 1)
    .drop("ROW_NUM")
)
aux = pd.read_excel(
    "/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx", sheet_name="MVKE"
)
aux["USUARIO"] = limpiar_columna(aux, "USUARIO")

for i in range(len(aux["USUARIO"])):
    df = df.withColumnRenamed(aux["TEC"][i], aux["USUARIO"][i])
(
    df.write.format("delta")
    .partitionBy("YEAR", "MONTH", "MANDANTE", "CDIS")
    .mode("overwrite")
    .parquet(target_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNA1 (carga delta)

# COMMAND ----------

table = "KNA1"
delta_date = (
    LOG_TABLAS_SAP.filter(col("TABLA") == table)
    .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias("max"))
    .collect()[0]
    .max
)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = spark.read.parquet(source_path).filter(col("SEQ_DATE") >= delta_date)
columns_n = ["J_3GSTDMON", "J_3GSTDTAG", "J_3GTAGMON"]  # columnas numericas
columns_d = ["ERDAT"]  # columnas fecha
columns = [columns_n, columns_d]
tipos = ["numerico", "fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols, t)
keys = ["MANDT", "KUNNR"]
particion = Window.partitionBy(keys).orderBy(desc("SEQ_DATE"))
df = (
    df.withColumn("ROW_NUM", row_number().over(particion))
    .filter(col("ROW_NUM") == 1)
    .drop("ROW_NUM")
)
aux = pd.read_excel(
    "/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx", sheet_name="KNA1"
)
aux["USUARIO"] = limpiar_columna(aux, "USUARIO")

for i in range(len(aux["USUARIO"])):
    df = df.withColumnRenamed(aux["TEC"][i], aux["USUARIO"][i])
(
    df.write.format("delta")
    .partitionBy("YEAR", "MONTH", "MANDANTE")
    .mode("append")
    .parquet(target_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ADRC (carga delta)

# COMMAND ----------

table = "ADRC"
delta_date = (
    LOG_TABLAS_SAP.filter(col("TABLA") == table)
    .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias("max"))
    .collect()[0]
    .max
)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = spark.read.parquet(source_path).filter(col("SEQ_DATE") >= delta_date)
columns_n = []  # columnas numericas
columns_d = ["DATE_FROM", "DATE_TO"]  # columnas fecha
columns = [columns_n, columns_d]
tipos = ["numerico", "fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols, t)
keys = ["MANDT", "ADDRNUMBER", "DATE_FROM", "NATION"]
particion = Window.partitionBy(keys).orderBy(desc("SEQ_DATE"))
df = (
    df.withColumn("ROW_NUM", row_number().over(particion))
    .filter(col("ROW_NUM") == 1)
    .drop("ROW_NUM")
)
aux = pd.read_excel(
    "/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx", sheet_name="ADRC"
)
aux["USUARIO"] = limpiar_columna(aux, "USUARIO")

for i in range(len(aux["USUARIO"])):
    df = df.withColumnRenamed(aux["TEC"][i], aux["USUARIO"][i])
(
    df.write.format("delta")
    .partitionBy("YEAR", "MONTH", "MANDANTE")
    .mode("overwrite")
    .parquet(target_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### PRCD_ELEMENTS (carga delta)

# COMMAND ----------

table = "PRDC_ELEMENTS"
delta_date = (
    LOG_TABLAS_SAP.filter(col("TABLA") == table)
    .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias("max"))
    .collect()[0]
    .max
)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = spark.read.parquet(source_path).filter(col("SEQ_DATE") >= delta_date)
columns_n = [
    "KAQTY",
    "KAWRT",
    "KAWRT_K",
    "KBETR",
    "KDIFF",
    "KSTBS",
    "KWERT",
    "KWERT_K",
]  # columnas numericas
columns_d = ["KDATU"]  # columnas fecha
columns = [columns_n, columns_d]
tipos = ["numerico", "fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols, t)
keys = ["MANDT", "KNUMV", "KPOSN", "STUNR", "ZAEHK"]
particion = Window.partitionBy(keys).orderBy(desc("SEQ_DATE"))
df = (
    df.withColumn("ROW_NUM", row_number().over(particion))
    .filter(col("ROW_NUM") == 1)
    .drop("ROW_NUM")
)
aux = pd.read_excel(
    "/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",
    sheet_name="PRCD_ELEMENTS",
)
aux["USUARIO"] = limpiar_columna(aux, "USUARIO")

for i in range(len(aux["USUARIO"])):
    df = df.withColumnRenamed(aux["TEC"][i], aux["USUARIO"][i])
(
    df.write.format("delta")
    .partitionBy("YEAR", "MONTH", "MANDANTE")
    .mode("overwrite")
    .parquet(target_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### VBBE (carga delta)

# COMMAND ----------

# table = "VBBE"
# delta_date = (LOG_TABLAS_SAP.filter(col('TABLA') == table)                            .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias('max'))                            .collect()[0].max)
# source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
# target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
# df = (spark.
#        read.
#        parquet(source_path).
#        filter(col("SEQ_DATE") >= delta_date))
# columns_n = ['FSH_RALLOC_QTY', 'OMENG', 'VMENG'] #columnas numericas
# columns_d = ['MBDAT'] #columnas fecha
# columns = [columns_n,columns_d]
# tipos = ["numerico","fecha"]
# for cols, t in zip(columns, tipos):
#     df = convert_columns(df, cols,t)
# keys = ["MANDT","VBELN","POSNR","ETENR"]
# aux = pd.read_excel("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",sheet_name="VBBE")
# aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

# for i in range(len(aux["USUARIO"])):
#        df = (df
#              .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
# (df.
#     write.format("delta").
#     partitionBy('YEAR','MONTH','MANDANTE').
#     mode("overwrite").
#     parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### VBKD (carga delta)

# COMMAND ----------

table = "VBKD"
delta_date = (
    LOG_TABLAS_SAP.filter(col("TABLA") == table)
    .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias("max"))
    .collect()[0]
    .max
)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = spark.read.parquet(source_path).filter(col("SEQ_DATE") >= delta_date)
columns_n = ["AKPRZ", "KURSK", "STCUR"]  # columnas numericas
columns_d = ["FKDAT", "KURRF_DAT", "KURSK_DAT"]  # columnas fecha
columns = [columns_n, columns_d]
tipos = ["numerico", "fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols, t)
keys = ["MANDT", "VBELN", "POSNR"]
particion = Window.partitionBy(keys).orderBy(desc("SEQ_DATE"))
df = (
    df.withColumn("ROW_NUM", row_number().over(particion))
    .filter(col("ROW_NUM") == 1)
    .drop("ROW_NUM")
)
aux = pd.read_excel(
    "/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx", sheet_name="VBKD"
)
aux["USUARIO"] = limpiar_columna(aux, "USUARIO")

for i in range(len(aux["USUARIO"])):
    df = df.withColumnRenamed(aux["TEC"][i], aux["USUARIO"][i])
(
    df.write.format("delta")
    .partitionBy("YEAR", "MONTH", "MANDANTE")
    .mode("overwrite")
    .parquet(target_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###VBAK (carga delta)

# COMMAND ----------

table = "VBAK"
delta_date = (
    LOG_TABLAS_SAP.filter(col("TABLA") == table)
    .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias("max"))
    .collect()[0]
    .max
)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = spark.read.parquet(source_path).filter(col("SEQ_DATE") >= delta_date)
columns_n = [
    "AWAHR",
    "EXT_REV_TMSTMP",
    "VKGRP",
    "VPRGR",
]  # columnas numericas
columns_d = ["AUDAT", "CROSSITEM_PRC_DATE", "ERDAT", "VDATU"]  # columnas fecha
columns = [columns_n, columns_d]
tipos = ["numerico", "fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols, t)
keys = ["MANDT", "KUNNR"]
particion = Window.partitionBy(keys).orderBy(desc("SEQ_DATE"))
df = (
    df.withColumn("ROW_NUM", row_number().over(particion))
    .filter(col("ROW_NUM") == 1)
    .drop("ROW_NUM")
)
aux = pd.read_excel(
    "/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx", sheet_name="VBAK"
)
aux["USUARIO"] = limpiar_columna(aux, "USUARIO")

for i in range(len(aux["USUARIO"])):
    df = df.withColumnRenamed(aux["TEC"][i], aux["USUARIO"][i])
(
    df.write.format("delta")
    .partitionBy("YEAR", "MONTH", "MANDANTE", "ORGVT")
    .mode("overwrite")
    .parquet(target_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###A005 (carga delta)

# COMMAND ----------

table = "A005"
delta_date = (
    LOG_TABLAS_SAP.filter(col("TABLA") == table)
    .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias("max"))
    .collect()[0]
    .max)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = spark.read.parquet(source_path)#.filter(col("SEQ_DATE") >= delta_date)
columns_n = []  # columnas numericas
columns_d = ["DATBI", "DATAB"]  # columnas fecha
columns = [columns_n, columns_d]
tipos = ["numerico", "fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols, t)
keys = ["MANDT", "MATNR", "KUNNR", "VTWEG", "VKORG", "KSCHL"]
particion = Window.partitionBy(keys).orderBy(desc("DATAB"), desc("SEQ_DATE"))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
)
aux = pd.read_excel(
    "/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx", sheet_name="A005"
)
aux["USUARIO"] = limpiar_columna(aux, "USUARIO")

for i in range(len(aux["USUARIO"])):
    df = df.withColumnRenamed(aux["TEC"][i], aux["USUARIO"][i])
(
    df.write.format("delta")
    .partitionBy("YEAR", "MONTH", "MANDANTE", "ORGVT", "CLCD")
    .mode("overwrite")
    .parquet(target_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### VBRK (carga delta)

# COMMAND ----------

table = "VBRK"
delta_date = (
    LOG_TABLAS_SAP.filter(col("TABLA") == table)
    .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias("max"))
    .collect()[0]
    .max
)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = spark.read.parquet(source_path).filter(col("SEQ_DATE") >= delta_date)
columns_n = [
    "GJAHR",
    "NETWR",
    "TAXK1",
    "KUNRG",
    "KUNAG",
    "KNKLI",
    "MWSBK",
]  # columnas numericas
columns_d = ["FKDAT", "ERDAT", "KURRF_DAT"]  # columnas fecha
columns = [columns_n, columns_d]
tipos = ["numerico", "fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols, t)
keys = ["MANDT", "VBELN"]
particion = Window.partitionBy(keys).orderBy(desc("SEQ_DATE"))
df = (
    df.withColumn("ROW_NUM", row_number().over(particion))
    .filter(col("ROW_NUM") == 1)
    .drop("ROW_NUM")
)
aux = pd.read_excel(
    "/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",
    sheet_name=f"{table}",
)
aux["USUARIO"] = limpiar_columna(aux, "USUARIO")

for i in range(len(aux["USUARIO"])):
    df = df.withColumnRenamed(aux["TEC"][i], aux["USUARIO"][i])
(
    df.write.format("overwrite")
    .partitionBy("YEAR", "MONTH", "MANDANTE", "ORGVT")
    .mode("overwrite")
    .parquet(target_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### VBPA (carga delta)

# COMMAND ----------

table = "VBPA"
delta_date = (
    LOG_TABLAS_SAP.filter(col("TABLA") == table)
    .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias("max"))
    .collect()[0]
    .max
)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = spark.read.parquet(source_path).filter(col("SEQ_DATE") >= delta_date)
columns_n = []  # columnas numericas
columns_d = []  # columnas fecha
columns = [columns_n, columns_d]
tipos = ["numerico", "fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols, t)
keys = ["MANDT", "VBELN", "POSNR"]
particion = Window.partitionBy(keys).orderBy(desc("SEQ_DATE"))
df = (
    df.withColumn("ROW_NUM", row_number().over(particion))
    .filter(col("ROW_NUM") == 1)
    .drop("ROW_NUM")
)
aux = pd.read_excel(
    "/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",
    sheet_name=f"{table}",
)
aux["USUARIO"] = limpiar_columna(aux, "USUARIO")

for i in range(len(aux["USUARIO"])):
    df = df.withColumnRenamed(aux["TEC"][i], aux["USUARIO"][i])
(
    df.write.format("delta")
    .partitionBy("YEAR", "MONTH", "MANDANTE")
    .mode("overwrite")
    .parquet(target_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### VBRP (carga delta)

# COMMAND ----------

table = "VBRP"
delta_date = (
    LOG_TABLAS_SAP.filter(col("TABLA") == table)
    .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias("max"))
    .collect()[0]
    .max
)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = spark.read.parquet(source_path).filter(col("SEQ_DATE") >= delta_date)
columns_n = [
    "POSNR",
    "FKIMG",
    "NTGEW",
    "BRGEW",
    "KURSK",
    "VGPOS",
    "AUPOS",
    "POSPA",
    "WAVWR",
    "KZWI1",
    "KZWI2",
]  # columnas numericas
columns_d = ["PRSDT", "FBUDA", "NETWR", "VGPOS", "ERDAT"]  # columnas fecha
columns = [columns_n, columns_d]
tipos = ["numerico", "fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols, t)
keys = ["MANDT", "VBELN", "POSNR"]
particion = Window.partitionBy(keys).orderBy(desc("SEQ_DATE"))
df = (
    df.withColumn("ROW_NUM", row_number().over(particion))
    .filter(col("ROW_NUM") == 1)
    .drop("ROW_NUM")
)
aux = pd.read_excel(
    "/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",
    sheet_name=f"{table}",
)
aux["USUARIO"] = limpiar_columna(aux, "USUARIO")

for i in range(len(aux["USUARIO"])):
    df = df.withColumnRenamed(aux["TEC"][i], aux["USUARIO"][i])
(
    df.write.format("delta")
    .partitionBy("YEAR", "MONTH", "MANDANTE")
    .mode("overwrite")
    .parquet(target_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### VBFA (carga delta)

# COMMAND ----------

table = "VBFA"
delta_date = (
    LOG_TABLAS_SAP.filter(col("TABLA") == table)
    .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias("max"))
    .collect()[0]
    .max
)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = spark.read.parquet(source_path).filter(col("SEQ_DATE") >= delta_date)
columns_n = ["RFMNG"]  # columnas numericas
columns_d = ["ERDAT"]  # columnas fecha
columns = [columns_n, columns_d]
tipos = ["numerico", "fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols, t)
# for cols, t in zip(columns, tipos):
#     df = convert_columns(df, cols,t)
keys = ["MANDT", "VBELN"]
particion = Window.partitionBy(keys).orderBy(desc("SEQ_DATE"))
df = (
    df.withColumn("ROW_NUM", row_number().over(particion))
    .filter(col("ROW_NUM") == 1)
    .drop("ROW_NUM")
)
aux = pd.read_excel(
    "/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",
    sheet_name=f"{table}",
)
aux["USUARIO"] = limpiar_columna(aux, "USUARIO")

for i in range(len(aux["USUARIO"])):
    df = df.withColumnRenamed(aux["TEC"][i], aux["USUARIO"][i])
(
    df.write.format("delta")
    .partitionBy("YEAR", "MONTH", "MANDANTE")
    .mode("overwrite")
    .parquet(target_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### T178T (carga única)

# COMMAND ----------

# table = "T178T"
# #delta_date = (LOG_TABLAS_SAP.filter(col('TABLA') == table)                            .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias('max'))                            .collect()[0].max)
# source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
# target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
# df = (spark.
#        read.
#        parquet(source_path))#.
#        #filter(col("SEQ_DATE") >= delta_date))
# columns_n = [] #columnas numericas
# columns_d = [] #columnas fecha
# columns = [columns_n,columns_d]
# tipos = ["numerico","fecha"]
# for cols, t in zip(columns, tipos):
#     df = convert_columns(df, cols,t)
# # for cols, t in zip(columns, tipos):
# #     df = convert_columns(df, cols,t)
# keys = ["MANDT"]
# aux = pd.read_excel("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")
# aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

# for i in range(len(aux["USUARIO"])):
#        df = (df
#              .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
# (df.
#     write.format("delta").
#     mode("overwrite").
#     parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### T023T (carga única)

# COMMAND ----------

# table = "T023T"
# #delta_date = (LOG_TABLAS_SAP.filter(col('TABLA') == table)                            .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias('max'))                            .collect()[0].max)
# source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
# target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
# df = (spark.
#        read.
#        parquet(source_path))#.
#        #filter(col("SEQ_DATE") >= delta_date))
# columns_n = [] #columnas numericas
# columns_d = [] #columnas fecha
# columns = [columns_n,columns_d]
# tipos = ["numerico","fecha"]
# for cols, t in zip(columns, tipos):
#     df = convert_columns(df, cols,t)
# # for cols, t in zip(columns, tipos):
# #     df = convert_columns(df, cols,t)
# keys = ["MANDT"]
# aux = pd.read_excel("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")
# aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

# for i in range(len(aux["USUARIO"])):
#        df = (df
#              .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
# (df.
#     write.format("delta").
#     mode("overwrite").
#     parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### T151 (carga única)

# COMMAND ----------

# table = "T151"
# #delta_date = (LOG_TABLAS_SAP.filter(col('TABLA') == table)                            .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias('max'))                            .collect()[0].max)
# source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
# target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
# df = (spark.
#        read.
#        parquet(source_path))#.
#        #filter(col("SEQ_DATE") >= delta_date))
# columns_n = [] #columnas numericas
# columns_d = [] #columnas fecha
# columns = [columns_n,columns_d]
# tipos = ["numerico","fecha"]
# for cols, t in zip(columns, tipos):
#     df = convert_columns(df, cols,t)
# # for cols, t in zip(columns, tipos):
# #     df = convert_columns(df, cols,t)
# keys = ["MANDT"]
# aux = pd.read_excel("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")
# aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

# for i in range(len(aux["USUARIO"])):
#        df = (df
#              .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
# (df.
#     write.format("delta").
#     mode("overwrite").
#     parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### TVAU (carga única)

# COMMAND ----------

# table = "TVAU"
# #delta_date = (LOG_TABLAS_SAP.filter(col('TABLA') == table)                            .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias('max'))                            .collect()[0].max)
# source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
# target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
# df = (spark.
#        read.
#        parquet(source_path))#.
#        #filter(col("SEQ_DATE") >= delta_date))
# columns_n = [] #columnas numericas
# columns_d = [] #columnas fecha
# columns = [columns_n,columns_d]
# tipos = ["numerico","fecha"]
# for cols, t in zip(columns, tipos):
#     df = convert_columns(df, cols,t)
# # for cols, t in zip(columns, tipos):
# #     df = convert_columns(df, cols,t)
# keys = ["MANDT"]
# aux = pd.read_excel("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")
# aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

# for i in range(len(aux["USUARIO"])):
#        df = (df
#              .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
# (df.
#     write.format("delta").
#     mode("overwrite").
#     parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### T188T (carga única)

# COMMAND ----------

# table = "T188T"
# #delta_date = (LOG_TABLAS_SAP.filter(col('TABLA') == table)                            .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias('max'))                            .collect()[0].max)
# source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
# target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
# df = (spark.
#        read.
#        parquet(source_path))#.
#        #filter(col("SEQ_DATE") >= delta_date))
# columns_n = [] #columnas numericas
# columns_d = [] #columnas fecha
# columns = [columns_n,columns_d]
# tipos = ["numerico","fecha"]
# for cols, t in zip(columns, tipos):
#     df = convert_columns(df, cols,t)
# # for cols, t in zip(columns, tipos):
# #     df = convert_columns(df, cols,t)
# keys = ["MANDT"]
# aux = pd.read_excel("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")
# aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

# for i in range(len(aux["USUARIO"])):
#        df = (df
#              .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
# (df.
#     write.format("delta").
#     mode("overwrite").
#     parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## VBEP

# COMMAND ----------

table = "VBEP"
delta_date = (
    LOG_TABLAS_SAP.filter(col("TABLA") == table)
    .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias("max"))
    .collect()[0]
    .max
)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
df = spark.read.parquet(source_path).filter(col("SEQ_DATE") >= delta_date)
columns_n = []  # columnas numericas
columns_d = [
    "EDATU",
    "CREA_DLVDATE",
    "HANDOVERTIME",
    "IDNNR",
    "LCCST",
    "LDDAT",
    "TDDAT",
    "TDUHR",
]  # columnas fecha
columns = [columns_n, columns_d]
tipos = ["numerico", "fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols, t)
# for cols, t in zip(columns, tipos):
#     df = convert_columns(df, cols,t)
keys = ["MANDT"]
particion = Window.partitionBy(keys).orderBy(desc("SEQ_DATE"))
df = (
    df.withColumn("ROW_NUM", row_number().over(particion))
    .filter(col("ROW_NUM") == 1)
    .drop("ROW_NUM")
)
aux = pd.read_excel(
    "/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx",
    sheet_name=f"{table}",
)
aux["USUARIO"] = limpiar_columna(aux, "USUARIO")

for i in range(len(aux["USUARIO"])):
    df = df.withColumnRenamed(aux["TEC"][i], aux["USUARIO"][i])
(df.write.format("delta").mode("overwrite").parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### MARA sql

# COMMAND ----------

table = "TMP_IND_ECC_MARA"
delta_date = (
    LOG_TABLAS_SAP.filter(col("TABLA") == table)
    .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias("max"))
    .collect()[0]
    .max
)
source_path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/MARA_SQL.parquet/"
df = spark.read.parquet(source_path)
# Se itera sobre las columnas para emparejarlas usando zip. Transformar a numeric data y date usando función convert_columns
columns_n = ["VOLTO", "WESCH", "NTGEW", "BRGEW"]  # columnas numericas
columns_d = ["ERSDA", "LAEDA"]  # columnas fecha
columns = [columns_n, columns_d]
tipos = ["numerico", "fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols, t)
keys = ["MANDT", "MATNR"]
particion = Window.partitionBy(keys).orderBy(desc("SEQ_DATE"))
df = (
    df.withColumn("ROW_NUM", row_number().over(particion))
    .filter(col("ROW_NUM") == 1)
    .drop("ROW_NUM")
)
aux = pd.read_excel(
    "/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/Mapeo_columns.xlsx", sheet_name="MARA"
)

aux["USUARIO"] = limpiar_columna(aux, "USUARIO")

for i in range(len(aux["USUARIO"])):
    df = df.withColumnRenamed(aux["TEC"][i], aux["USUARIO"][i])
(
    df.write.format("delta")
    .partitionBy("YEAR", "MONTH", "MANDANTE", "TIPO_MATERIAL")
    .mode("overwrite")
    .parquet(target_path)
)