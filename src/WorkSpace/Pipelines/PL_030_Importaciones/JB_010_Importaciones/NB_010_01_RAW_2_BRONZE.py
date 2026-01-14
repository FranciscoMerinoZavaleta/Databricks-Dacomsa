# Databricks notebook source
# MAGIC %md
# MAGIC #INCLUDES

# COMMAND ----------

# MAGIC %run /Shared/Importaciones/Includes/

# COMMAND ----------

# MAGIC %md
# MAGIC #LOG TABLES

# COMMAND ----------

try:
    LOG_TABLAS_ADF = spark.read.parquet("/mnt/importacion-bo/bronze/LOGS/LOG_TABLAS_ADF.parquet")
    print("Cargado LOG_TABLAS_ADF")
except:
    print("Creando LOG_TABLAS_ADF...")
    LOG_TABLAS_ADF = pd.DataFrame({'TABLA':list(get_schema()),
                'LAST_MODIFFIED':[pd.Timestamp('2024-01-01 00:00:00', tz=None)]*len(list(get_schema()))})
    LOG_TABLAS_ADF=spark.createDataFrame(LOG_TABLAS_ADF)
    (LOG_TABLAS_ADF.dropDuplicates()
    .write
    .format("delta")
    .mode("overwrite")
    .parquet("/mnt/importacion-bo/bronze/LOGS/LOG_TABLAS_ADF.parquet"))
    print("LOG_TABLAS_ADF creado")

# COMMAND ----------

LOG_TABLAS_ADF = LOG_TABLAS_ADF.distinct()

# COMMAND ----------

# Funcion para pegar archivos json en uno solo
def union_tables(table,path_origin,path_destiny):
    # max_date = pd.Timestamp('2023-12-17 00:00:00', tz=None)
    max_date=LOG_TABLAS_ADF.filter(col('TABLA')==table).agg(max('LAST_MODIFFIED').alias('max')).collect()[0].max
    emptyRDD = spark.sparkContext.emptyRDD()

    # paths es un tupple que viene de mapear los paths del container posterior a la fecha de actualziación
    paths = lsR(f'/mnt/{path_origin}/{table}/',last_modified = max_date)
    # pd.Timestamp('2023-12-18 00:00:00', tz=None))

    # se construye la ruta destino
    target_path = f"/mnt/{path_destiny}/{table}.parquet"
    schema = get_schema(table)

    # se crea un dataframe vacío utilizando el schema definido. Se itera una lista de rutas de archivos json y se les extrae ruta y timestamp 
    empty_df = spark.createDataFrame(data = spark.sparkContext.emptyRDD(), schema = schema).withColumn('SEQ_DATE', current_timestamp())
    paths_subidos=[]
    seq_date_subidos=[]
    errores = {}

    if len(paths)>0:
        for row in paths:
            path = row.split('*')[0]  
            #path = "/dbfs" + row.split('*')[0]
            seq_date = pd.to_datetime(int(row.split('*')[1]+'000000')) 
            print(path,' - ', seq_date)
            seq_date_subidos.append(seq_date)
            
            try:
                curr_state = spark.read.schema(schema).json(path)
                curr_state = curr_state.withColumn('SEQ_DATE', lit(seq_date))
            except Exception as e:
                curr_state = spark.createDataFrame(emptyRDD, schema = schema)
                curr_state = curr_state.withColumn('SEQ_DATE', lit(seq_date))
                print(e)

        
            #pass
            curr_state = (curr_state
            .withColumn('YEAR', year(col('SEQ_DATE')))
            .withColumn('MONTH', month(col('SEQ_DATE')))
            )
            
            (curr_state
            .write.format("delta")
            .partitionBy('YEAR') 
            .mode("append")
            .parquet(target_path))
            paths_subidos.append(path)
            print("--------------------------")

        maximo=np.max(seq_date_subidos)
        print(maximo)
        (LOG_TABLAS_ADF
        .filter((col("TABLA")==table))
        .withColumn("LAST_MODIFFIED",lit(maximo))
        .write.format("delta")
        .mode("append")
        .parquet("/mnt/importacion-bo/bronze/LOGS/LOG_TABLAS_ADF.parquet")) #cambiar ruta

    else:
        print("No hay datos nuevos")
        dbutils.notebook.exit("No hay datos nuevos")


# COMMAND ----------

path_origin = 'importacion-bo/raw/'
path_destiny = 'importacion-bo/bronze/Tabla_ADF'
tables = list(get_schema()) 


# COMMAND ----------

for t in tables:
    union_tables(t,path_origin,path_destiny)