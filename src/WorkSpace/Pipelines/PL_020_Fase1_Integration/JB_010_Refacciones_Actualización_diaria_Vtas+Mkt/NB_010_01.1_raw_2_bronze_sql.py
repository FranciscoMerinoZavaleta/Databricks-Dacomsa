# Databricks notebook source
# MAGIC %md
# MAGIC # Includes

# COMMAND ----------

# MAGIC %run /Shared/FASE1_INTEGRACION/includes

# COMMAND ----------

# MAGIC %md
# MAGIC # Log table

# COMMAND ----------

#####Crea la tabla logs por primera vez, no descomentar a menos de ser necesario cargar todos los archivos
# LOG_TABLAS=pd.DataFrame({'TABLA':list(get_schema()), 
#                'LAST_MODIFFIED':[pd.Timestamp('2024-01-01 00:00:00', tz=None)]*len(list(get_schema()))})
# LOG_TABLAS=spark.createDataFrame(LOG_TABLAS)

# COMMAND ----------

# LOG_TABLAS.display()

# COMMAND ----------

####Escribe la tabla logs por primera vez
# (LOG_TABLAS
#       .write.format("delta")
#      .mode("overwrite")
#      .parquet("/mnt/adls-dac-data-bi-pr-scus/bronze/LOG_TABLAS.parquet"))

# COMMAND ----------

####Lee tabla logs
LOG_TABLAS = spark.read.parquet("/mnt/adls-dac-data-bi-pr-scus/bronze/LOG_TABLAS.parquet")

# COMMAND ----------

#####Elimina duplicados
LOG_TABLAS = LOG_TABLAS.distinct()

# COMMAND ----------

# LOG_TABLAS = LOG_TABLAS.filter(col("TABLA")!="REBATE_CLIENTE")

# COMMAND ----------

#new_data = spark.createDataFrame([("TMP_IND_ECC_MARA", datetime.now() - timedelta(days=1))], ["TABLA", "LAST_MODIFFIED"])

# COMMAND ----------

# LOG_TABLAS = LOG_TABLAS.unionByName(new_data,allowMissingColumns = True)

# COMMAND ----------

LOG_TABLAS.display()

# COMMAND ----------

#### Funcion para pegar archivos json en uno solo
import datetime
def union_tables(table,path_origin,path_destiny):
    # max_date = pd.Timestamp('2023-12-17 00:00:00', tz=None)
    max_date=LOG_TABLAS.filter(col('TABLA')==table).agg(max('LAST_MODIFFIED').alias('max')).collect()[0].max
    emptyRDD = spark.sparkContext.emptyRDD()

    ### paths es un tupple que viene de mapear los paths del container posterior a la fecha de actualziación
    paths = lsR(f'/mnt/{path_origin}/{table}/',last_modified = max_date)
    # pd.Timestamp('2023-12-18 00:00:00', tz=None))

    target_path = f"/mnt/{path_destiny}/{table}.parquet"
    schema = get_schema(table)
    empty_df = spark.createDataFrame(data = spark.sparkContext.emptyRDD(), schema = schema).withColumn('SEQ_DATE', current_timestamp())
    paths_subidos=[]
    archivos_nulos = []
    seq_date_subidos=[]
    errores = {}
    date = str(datetime.date.today())

    if len(paths)>0:
        for row in paths:
            path = row.split('*')[0]
            seq_date = pd.to_datetime(int(row.split('*')[1]+'000000')) 
            print(path,' - ', seq_date)
            seq_date_subidos.append(seq_date)
            try:
                #pass
                curr_state = spark.read.schema(schema).json(path)
                curr_state=curr_state.withColumn('SEQ_DATE', lit(seq_date ))
            except Exception as e:
                #pass
                curr_state = spark.createDataFrame(data = spark.sparkContext.emptyRDD(), schema = schema).withColumn('SEQ_DATE', current_timestamp())
                archivos_nulos.append(path)
                print(e)
                
            curr_state = (curr_state
            .withColumn('YEAR', year(col('SEQ_DATE')))
            .withColumn('MONTH', month(col('SEQ_DATE')))
                )
                
            (curr_state
                .write.format("delta")
                .partitionBy('YEAR','MONTH')
                .mode("append")
                .parquet(target_path))
            paths_subidos.append(path)
            print("--------------------------")

        maximo=np.max(seq_date_subidos)
        (LOG_TABLAS
        .filter((col("TABLA")==table))
        .withColumn("LAST_MODIFFIED",lit(maximo))
        .write.format("delta")
        .mode("append")
        .parquet("/mnt/adls-dac-data-bi-pr-scus/bronze/LOG_TABLAS.parquet"))
        with open(f"/dbfs/mnt/adls-dac-data-bi-pr-scus/bronze/control_cargas/{t}_archivos_nulos_{date}.txt", "w") as archivo:
            # Itera sobre cada elemento de la lista
            for elemento in archivos_nulos:
                # Escribe el elemento en una nueva línea en el archivo
                archivo.write(elemento + "\n")
    else:
        print("No hay datos nuevos")

# COMMAND ----------

path_origin = 'adls-dac-data-bi-pr-scus/raw'
path_destiny = 'adls-dac-data-bi-pr-scus/bronze/tablas_sql'
tables = list(get_schema())

# COMMAND ----------



# COMMAND ----------

tables

# COMMAND ----------

for t in tables:
    union_tables(t,path_origin,path_destiny)

# COMMAND ----------

# MAGIC %md
# MAGIC # Storm chaser
# MAGIC
# MAGIC En esta seccion se realizará un log para el tablero de strom chaser.
# MAGIC
# MAGIC FACT_PEDIDOS_ESTATUS_TOP_REGISTROS_RECIENTES :  Monitoreo de las ultimas fechas de la facturación y pedidos de la tabla FAC_PEDIDOS_ESTATUS
# MAGIC FACT_PEDIDOS_ESTATUS_REGISTROS_UNICOS_FECHA_PEDIDO : Moniterea las fechas de factura y pedidos unicas para sensar si hay documentos con la fecha actual

# COMMAND ----------

from pyspark.sql.functions import col
df = spark.read.parquet("/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/FACT_PEDIDOS_ESTATUS.parquet") # Lectura de la capa bronze

# METRICA 1: FACT_PEDIDOS_ESTATUS_TOP_REGISTROS_RECIENTES
# Esta primer tabla solo guarda registros de los ultimos 20 datos ordenados por fecha más reciente
dfm1 = df.orderBy(col("FECHA_PEDIDO").desc())
dfm1 = dfm1.limit(10)
dfm1.write.mode("overwrite").parquet("/mnt/adls-dac-data-bi-pr-scus/storm_chaser/FACT_PEDIDOS_ESTATUS_TOP_REGISTROS_RECIENTES.parquet")


# METRICA 2: FACT_PEDIDOS_ESTATUS_REGISTROS_UNICOS_FECHA_PEDIDO
dfm2 = df.select(col("FECHA_PEDIDO")).distinct()
display(dfm2)
dfm2.write.mode("overwrite").parquet("/mnt/adls-dac-data-bi-pr-scus/storm_chaser/FACT_PEDIDOS_ESTATUS_REGISTROS_UNICOS_FECHA_PEDIDO.parquet")





# COMMAND ----------

