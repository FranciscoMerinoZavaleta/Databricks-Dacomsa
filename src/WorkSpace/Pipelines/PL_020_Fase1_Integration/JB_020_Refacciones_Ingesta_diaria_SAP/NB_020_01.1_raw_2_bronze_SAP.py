# Databricks notebook source
# MAGIC %md
# MAGIC # Includes

# COMMAND ----------

# MAGIC %run /Shared/FASE1_INTEGRACION/includes

# COMMAND ----------

def lsRA(path,last_modified ): #**kwargs):
    #last_modified=kwargs.get('last_modified', None)

    # if (last_modified == None):
    #     last_modified=pd.Timestamp('2023-11-08 00:00:00', tz=None)
    flist = []
    for fi in dbutils.fs.ls(path):
            print("A R C H I V O: ", fi)
            print("P A T H: ", path)
            print("FECHA EN CUESTION: ", pd.to_datetime(int(str(fi.modificationTime)+'000000')))
            if  not(fi.isFile())  or pd.to_datetime(int(str(fi.modificationTime)+'000000')) > last_modified :
                print("ES MAYOR")
                flist.append(fi.path+'*'+str(fi.modificationTime))
            else:
                print("ES MENOR")
                

    for fname in flist:
        fname.replace('dbfs:', '').replace('/_SUCCESS', '') 
    display("================================================")
    display("================================================")
    display("ARCHVOS A CARGAR: :::::::::::",flist )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Log Tables

# COMMAND ----------

try:
    LOG_TABLAS_SAP = spark.read.parquet("/mnt/adls-dac-data-bi-pr-scus/bronze/LOG_TABLAS_SAP.parquet")
    print("Cargado LOG_TABLAS_SAP")
except:
    print("Creando LOG_TABLAS_SAP...")
    LOG_TABLAS_SAP = pd.DataFrame({'TABLA':list(get_schema_sap()), 
                'LAST_MODIFFIED':[pd.Timestamp('2025-08-01 09:21:47', tz=None)]*len(list(get_schema_sap()))})
                #Se pusó esa fecha 2025-01-09 porque se rompío el log ese día, para no caragar la historia de nuevo
    LOG_TABLAS_SAP=spark.createDataFrame(LOG_TABLAS_SAP)
    (LOG_TABLAS_SAP.dropDuplicates()
    .write
    .format("delta")
    .mode("overwrite")
    .parquet("/mnt/adls-dac-data-bi-pr-scus/bronze/LOG_TABLAS_SAP.parquet"))
    print("LOG_TABLAS_SAP creado")

# COMMAND ----------

LOG_TABLAS_SAP.display()

# COMMAND ----------

#2
LOG_TABLAS_SAP = spark.read.parquet("/mnt/adls-dac-data-bi-pr-scus/bronze/LOG_TABLAS_SAP.parquet") #1#3#5

# COMMAND ----------

# #1
# (LOG_TABLAS_SAP.dropDuplicates().
#  #filter(~col("TABLA").isin(["MARA","MBEW","MARC"])).
#  write.
#  format("delta").
#  mode("overwrite").
#  parquet("/mnt/adls-dac-data-bi-pr-scus/bronze/LOG_TABLAS_SAP.parquet"))#2 con filter 

# COMMAND ----------

#3
# (LOG_TABLAS_SAP
#       .unionByName(spark.createDataFrame(pd.DataFrame({'TABLA':["VBAP"], 
#                'LAST_MODIFFIED':[pd.Timestamp('2024-01-01 00:00:00', tz=None)]})))
#       .write.format("delta")
#       .mode("append")
#       .parquet("/mnt/adls-dac-data-bi-pr-scus/bronze/LOG_TABLAS_SAP.parquet"))#4

# COMMAND ----------

LOG_TABLAS_SAP = LOG_TABLAS_SAP.distinct()

# COMMAND ----------

LOG_TABLAS_SAP.display()

# COMMAND ----------

#### Funcion para pegar archivos json en uno solo
import datetime
def union_tables(table,path_origin,path_destiny):
    print("Cargando tabla: " + table)
    print("---------------------------")
    print("---------------------------")
    # max_date = pd.Timestamp('2023-12-17 00:00:00', tz=None)
    max_date=LOG_TABLAS_SAP.filter(col('TABLA')==table).agg(max('LAST_MODIFFIED').alias('max')).collect()[0].max
    emptyRDD = spark.sparkContext.emptyRDD()
    print("ULTIMA FECHA DE LA QUE SE TIENE REGISTRO DE ACTUALIZACION: ", max_date)
    ### paths es un tupple que viene de mapear los paths del container posterior a la fecha de actualziación
    paths = lsR(f'/mnt/{path_origin}/{table}/',last_modified = max_date)
    # pd.Timestamp('2023-12-18 00:00:00', tz=None))

    target_path = f"/mnt/{path_destiny}/{table}.parquet"
    schema = get_schema_sap(table)
    empty_df = spark.createDataFrame(data = spark.sparkContext.emptyRDD(), schema = schema).withColumn('SEQ_DATE', current_timestamp())
    paths_subidos=[]
    archivos_nulos = []
    seq_date_subidos=[]
    errores = {}
    date = str(datetime.date.today())
    print("Path a cargar: ",paths )
    print("**************************************")
    if len(paths)>0:
        for row in paths:
            path = row.split('*')[0]
            seq_date = pd.to_datetime(int(row.split('*')[1]+'000000')) 
            print(path,' - ', seq_date)
            seq_date_subidos.append(seq_date)
            try:
                #pass
                # curr_state = spark.read.schema(schema).json(path)
                curr_state = spark.createDataFrame(pd.read_json(f"/dbfs/{path}", dtype=str),schema=schema)
                curr_state = curr_state.withColumn('SEQ_DATE', lit(seq_date))
            except Exception as e:
                #pass
                #curr_state = spark.createDataFrame(data = spark.sparkContext.emptyRDD(), schema = schema).withColumn('SEQ_DATE', current_timestamp())
                archivos_nulos.append(path)
                print(e)
                print("ARCHIVO NULO")
                print("---------------------------")
                continue
                
            curr_state = (curr_state
            .withColumn('YEAR', year(col('SEQ_DATE')))
            .withColumn('MONTH', month(col('SEQ_DATE')))
            .repartition(8))
            # if table in ["ADRC"]:
            #     (curr_state
            #         .withColumn("MANDT", lit("504"))
            #         .write.format("delta")
            #         .partitionBy('YEAR','MONTH','MANDT')
            #         .mode("append")
            #         .parquet(target_path))
            #     paths_subidos.append(path)
            #     print("--------------------------")
            
            if table in ["PRDC_ELEMENTS","ADRC"]:
                (curr_state
                    .withColumnRenamed("CLIENT","MANDT")
                    .write.format("delta")
                    .partitionBy('YEAR')
                    .mode("append")
                    .parquet(target_path))
                paths_subidos.append(path)
                print("--------------------------")          
            else:
                (curr_state
                    .write.format("delta")
                    .partitionBy('YEAR')
                    .mode("append")
                    .parquet(target_path))
                paths_subidos.append(path)
                print("--------------------------")
                
        maximo=np.max(seq_date_subidos)
        (LOG_TABLAS_SAP
        .filter((col("TABLA")==table))
        .withColumn("LAST_MODIFFIED",lit(maximo))
        .write.format("delta")
        .mode("append")
        .parquet("/mnt/adls-dac-data-bi-pr-scus/bronze/LOG_TABLAS_SAP.parquet"))
        with open(f"/dbfs/mnt/adls-dac-data-bi-pr-scus/bronze/control_cargas_SAP/{t}_archivos_nulos_{date}.txt", "w") as archivo:
            # Itera sobre cada elemento de la lista
            for elemento in archivos_nulos:
                # Escribe el elemento en una nueva línea en el archivo
                archivo.write(elemento + "\n")
    else:
        print("No hay datos nuevos")

# COMMAND ----------

# 
# EN SOPORTE ALEJANDRA.ROCHA@DESC.COM / GENARO.LUNA.NAVARRO@ACCENTURE.COM
# LOG: 13/08/2025
# MOTIVO: CORRECCIÓN DE CARGAS DELTAS DE LA TABLA MVKE. SE EXCLUYE DE LA ACTUALIZACIÓN E
# NOTA: REVISAR LAS SIGUIENTES TABLAS:
# MBEW
path_origin = 'adls-dac-data-bi-pr-scus/raw'
path_destiny = 'adls-dac-data-bi-pr-scus/bronze/tablas_SAP'
tables = list(get_schema_sap())
tables = [t for t in tables if t != "MVKE"]  # SE EXCLUVE ESTA TABLA DE LA CARGA INICIAR YA QUE ABAJO SE ATIENDE CON UN METODO OPTIMIZADO

# COMMAND ----------

for t in tables:
    union_tables(t, path_origin, path_destiny)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Carga de FIORI
# MAGIC Se agrega este segmento de código para la lectura optimizada de la tabla FIORI

# COMMAND ----------

#Carga de productos fiori
productos_fiori = spark.createDataFrame(pd.read_excel('/dbfs/mnt/adls-dac-data-bi-pr-scus/powerappslandingzone_productos_fiori/productos_fiori.xlsx'))
productos_fiori = (productos_fiori.withColumnRenamed('Producto',"MATERIAL")
                   .withColumnRenamed('Cód.estad.mercancías',"COD_EST_MERCANCIAS"))
path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/PRODUCTOS_FIORI.parquet"
productos_fiori.write.format("delta").mode("overwrite").parquet(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga de MKVE (Optimizada)
# MAGIC Se agrega este segmento de código para la lectura optimizada de la tabla MKVE
# MAGIC Atendido por : aalejandra.rocha@desc.com
# MAGIC Fecha: 30/10/2025

# COMMAND ----------


# =============================================
#  Inicialización
# =============================================
start_time = time.time()
print("🚀 Iniciando proceso de unión de archivos MVKE...")

# Definir rutas
path_origin = 'adls-dac-data-bi-pr-scus/raw'
input_path = f'/mnt/{path_origin}/MVKE'
output_path =  f'/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/MVKE.parquet'

# Definir schema explícito para ahorrar tiempo en la inferencia on the fly
schema = StructType([
    StructField("MANDT", StringType(), True),
    StructField("MATNR", StringType(), True),
    StructField("VKORG", StringType(), True),
    StructField("VTWEG", StringType(), True),
    StructField("SKTOF", StringType(), True),
    StructField("VMSTD", StringType(), True),
    StructField("AUMNG", DoubleType(), True),
    StructField("LFMNG", DoubleType(), True),
    StructField("EFMNG", DoubleType(), True),
    StructField("SCMNG", DoubleType(), True),
    StructField("MTPOS", StringType(), True),
    StructField("DWERK", StringType(), True),
    StructField("PRODH", StringType(), True),
    StructField("PROVG", StringType(), True),
    StructField("KONDM", StringType(), True),
    StructField("KTGRM", StringType(), True),
    StructField("MVGR1", StringType(), True),
    StructField("MVGR2", StringType(), True),
    StructField("MVGR3", StringType(), True),
    StructField("MVGR4", StringType(), True),
    StructField("MVGR5", StringType(), True),
    StructField("LFMAX", DoubleType(), True),
    StructField("VMSTA", StringType(), True),
    StructField("RDPRF", StringType(), True)
])

# =============================================
# 1️⃣ Lectura de archivos JSON
# =============================================
print(f"📂 Leyendo archivos desde: {input_path}/MVKE*.json ...")
df = (
    spark.read
        .schema(schema)
        .option("multiLine", False)
        .json(f"{input_path}/MVKE*.json")
        .withColumn("SOURCE_FILE", input_file_name())
)
count_raw = df.count()
print(f"✅ Lectura completada. Registros totales leídos: {count_raw:,}")

# =============================================
# 2️⃣ Limpieza y eliminación de duplicados
# =============================================
print("🧹 Eliminando registros duplicados...")
df_clean = df.dropDuplicates()
count_clean = df_clean.count()
print(f"✅ Limpieza completada. Registros únicos: {count_clean:,}")

# =============================================
# 3️⃣ Repartición
# =============================================
print("⚙️ Reparticionando dataset para escritura eficiente...")
df_final = df_clean.repartition(8)
print("✅ Repartición completada (8 particiones).")

# =============================================
# 4️⃣ Escritura en formato Parquet
# =============================================

print(f"💾 Escribiendo dataset final en formato Parquet → {output_path}")
(
    df_final.write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .save(output_path)
)
print("✅ Escritura completada correctamente.")



# COMMAND ----------

# table = "T188T"
# path = f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_SAP/{table}.parquet"
# df = spark.createDataFrame(pd.read_excel(f'/dbfs/mnt/adls-dac-data-bi-pr-scus/dummy/TABLAS_T_SAP/{table}_tec.xlsx'))
# df.write.format("delta").mode("overwrite").parquet(path)