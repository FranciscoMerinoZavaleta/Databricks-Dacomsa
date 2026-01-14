



try:
    LOG_TABLAS_SAP = spark.read.parquet("/mnt/adlsabastosprscus/abastos/bronze/SAP/LOG_TABLAS_SAP.parquet")
    print("Cargado LOG_TABLAS_SAP")
except:
    print("Creando LOG_TABLAS_SAP...")
    LOG_TABLAS_SAP = pd.DataFrame({'TABLA':list(get_schema_sap()), 
                'LAST_MODIFFIED':[pd.Timestamp('2024-01-01 00:00:00', tz=None)]*len(list(get_schema_sap()))})
    LOG_TABLAS_SAP=spark.createDataFrame(LOG_TABLAS_SAP)
    (LOG_TABLAS_SAP.dropDuplicates()
    .write
    .format("delta")
    .mode("overwrite")
    .parquet("/mnt/adlsabastosprscus/abastos/bronze/SAP/LOG_TABLAS_SAP.parquet"))
    print("LOG_TABLAS_SAP creado")








LOG_TABLAS_SAP = spark.read.parquet("/mnt/adlsabastosprscus/abastos/bronze/SAP/LOG_TABLAS_SAP.parquet")


try:
    LOG_TABLAS_SQL = spark.read.parquet("/mnt/adlsabastosprscus/abastos/bronze/SQL/LOG_TABLAS_SQL.parquet")
    print("Cargado LOG_TABLAS_SQL")
except:
    print("Creando LOG_TABLAS_SQL...")
    LOG_TABLAS_SQL = pd.DataFrame({'TABLA':list(get_schema()), 
                'LAST_MODIFFIED':[pd.Timestamp('2024-01-01 00:00:00', tz=None)]*len(list(get_schema()))})
    LOG_TABLAS_SQL=spark.createDataFrame(LOG_TABLAS_SQL)
    (LOG_TABLAS_SQL.dropDuplicates()
    .write
    .format("delta")
    .mode("overwrite")
    .parquet("/mnt/adlsabastosprscus/abastos/bronze/SQL/LOG_TABLAS_SQL.parquet"))
    print("LOG_TABLAS_SQL creado")


LOG_TABLAS_SQL = spark.read.parquet("/mnt/adlsabastosprscus/abastos/bronze/SQL/LOG_TABLAS_SQL.parquet") #1#3#5


LOG_TABLAS_SAP = LOG_TABLAS_SAP.distinct()
LOG_TABLAS_SQL = LOG_TABLAS_SQL.distinct()


LOG_TABLAS_SAP.display()


from pyspark.sql.functions import year, month, current_timestamp, lit
from functools import reduce
import datetime

def union_tables(table, path_origin, path_destiny):
    print(f"Cargando tabla: {table}")
    print("---------------------------")

    max_date = LOG_TABLAS_SAP.filter(col('TABLA') == table) \
                             .agg(max('LAST_MODIFFIED').alias('max')) \
                             .collect()[0]['max']
    
    paths = lsR(f'/mnt/{path_origin}/{table}/', last_modified=max_date)
    target_path = f"/mnt/{path_destiny}/{table}.parquet"
    schema = get_schema_sap(table)

    archivos_nulos = []
    dfs_validos = []

    for row in paths:
        try:
            path, ts = row.split('*')
            seq_date = pd.to_datetime(int(ts + '000000'))
            print(path, '-', seq_date)

            df = spark.read.schema(schema).json(path)
            df = df.withColumn('SEQ_DATE', lit(current_timestamp())) \
                   .withColumn('YEAR', year(col('SEQ_DATE'))) \
                   .withColumn('MONTH', month(col('SEQ_DATE')))
            dfs_validos.append(df)

        except Exception as e:
            print(f"ARCHIVO NULO: {path} - {e}")
            archivos_nulos.append(path)
            continue

    if dfs_validos:
        df_final = reduce(lambda a, b: a.union(b), dfs_validos).repartition(16)

        df_final.write \
                .format("delta") \
                .partitionBy('YEAR') \
                .mode("append") \
                .parquet(target_path)

        maximo = df_final.agg({"SEQ_DATE": "max"}).collect()[0][0]
        LOG_TABLAS_SAP.filter(col("TABLA") == table) \
            .withColumn("LAST_MODIFFIED", lit(maximo)) \
            .write.format("delta") \
            .mode("append") \
            .parquet("/mnt/adlsabastosprscus/abastos/bronze_dummy/SAP/LOG_TABLAS_SAP.parquet")

    else:
        print("No hay datos válidos nuevos.")

    if archivos_nulos:
        date = str(datetime.date.today())
        with open(f"/dbfs/mnt/adlsabastosprscus/abastos/bronze_dummy/SAP/control_cargas_SAP/{table}_archivos_nulos_{date}.txt", "w") as archivo:
            for elemento in archivos_nulos:
                archivo.write(elemento + "\n")







            

            
            
            




import datetime
def union_tables_SQL(table,path_origin,path_destiny):
    print("Cargando tabla: " + table)
    print("---------------------------")
    print("---------------------------")
    max_date=LOG_TABLAS_SQL.filter(col('TABLA')==table).agg(max('LAST_MODIFFIED').alias('max')).collect()[0].max
    emptyRDD = spark.sparkContext.emptyRDD()

    paths = lsR(f'/mnt/{path_origin}/{table}/', last_modified = max_date)

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
            seq_date = pd.to_datetime(int(row.split('*')[1] + '000000')) 
            print(path,' - ', seq_date)
            seq_date_subidos.append(seq_date)

            
            try:
                curr_state = spark.read.schema(schema).json(path)
                curr_state=curr_state.withColumn('SEQ_DATE', lit(seq_date))
            except Exception as e:
                archivos_nulos.append(path)
                print(e)
                print("ARCHIVO NULO")
                print("---------------------------")
                continue
                
            curr_state = (curr_state
            .withColumn('YEAR', year(col('SEQ_DATE')))
            .withColumn('MONTH', month(col('SEQ_DATE')))
            .repartition(16))

            
            (curr_state
                .write.format("delta")
                .partitionBy('YEAR')
                .mode("append")
                .parquet(target_path))
            paths_subidos.append(path)
            print("--------------------------")
            
        maximo=np.max(seq_date_subidos)
        (LOG_TABLAS_SQL
        .filter((col("TABLA")==table))
        .withColumn("LAST_MODIFFIED",lit(maximo))
        .write.format("delta")
        .mode("append")
        .parquet("/mnt/adlsabastosprscus/abastos/bronze/SQL/LOG_TABLAS_SQL.parquet"))
        with open(f"/dbfs/mnt/adlsabastosprscus/abastos/bronze/SQL/control_cargas_SQL/{t}_archivos_nulos_{date}.txt", "w") as archivo:
            for elemento in archivos_nulos:
                archivo.write(elemento + "\n")
    else:
        print("No hay datos nuevos")



path_origin = 'adlsabastosprscus/abastos/raw'
path_destiny = 'adlsabastosprscus/abastos/bronze/SAP/tablas_SAP'


tables_SAP = list(get_schema_sap())
tables_SQL = list(get_schema())


tables_SAP



elementos_a_eliminar = ['RESB']

for elemento in elementos_a_eliminar:
      tables_SAP.remove(elemento)

print(tables_SAP)


for t in tables_SAP:
    union_tables(t, path_origin, path_destiny)


