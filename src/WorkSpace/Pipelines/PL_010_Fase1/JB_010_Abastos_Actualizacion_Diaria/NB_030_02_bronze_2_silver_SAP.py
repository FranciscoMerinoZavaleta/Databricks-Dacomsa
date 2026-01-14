

%pip install openpyxl


def convert_columns(df, columns, type_column):
    if type_column == "numerico":
        for column in columns:
            df = df.withColumn(column, col(column).cast('float'))
    else:
        for column in columns:
            df = df.withColumn(column, to_date(col(column), "yyyy-MM-dd"))
    return df


LOG_TABLAS_SAP = spark.read.parquet("/mnt/adlsabastosprscus/abastos/bronze/SAP/LOG_TABLAS_SAP.parquet").distinct()



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






table = "A017"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['ESOKZ'] # Las que consideres que se necesitan con formato de número para las viz
columns_d =  ['DATBI', 'DATAB'] # Las que veas que son relevantes, Ej: Fecha de creación, fecha de recepción, fecha de ultima modificación 
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)

keys =  ["MATNR", "WERKS", "KNUMH", "DATAB"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df.withColumn("ROW_NUM", row_number().over(particion))
      .filter(col('ROW_NUM')==1)
      .drop("ROW_NUM")
      )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR','MONTH','APLICACION','CLASE_DE_CONDICION','MANDANTE','CENTRO').
    mode("overwrite").
    parquet(target_path))




table = "CDHDR"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path)).withColumnRenamed("MANDANT", "MANDT") 
columns_n =  []
columns_d =  ['UDATE']
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  [ "MANDT","CHANGENR" ]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH','MANDANTE','CLAVE_DE_IDIOMA').
    mode("overwrite").
    parquet(target_path))




table = "EBAN"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['MENGE', 'BUMNG', 'LPEIN', 'WEBAZ', 'PREIS', 'PEINH', 'PSTYP', 'MNG02','PLIFZ', 'BANPR', 'RLWRT' ]
columns_d =  ['BADAT', 'ERDAT', 'LFDAT', 'FRGDT', 'CREATIONDATE']
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["BANFN", "BNFPO"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("overwrite").
    partitionBy('YEAR', 'MONTH').
    mode("overwrite").
    parquet(target_path))




table = "EBKN"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['MENGE', 'SAKTO', 'NETWR', 'DIFFOPTRATE' ]
columns_d =  ['ERDAT', 'CREATIONDATE']
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  [ "MANDT", "BANFN", "BNFPO", "ZEBKN"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH').
    mode("overwrite").
    parquet(target_path))




table = "EINA"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['UMREN', 'UMREZ' ]
columns_d =  ['ERDAT']
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  [ "MATNR", "LIFNR", "INFNR","ERDAT"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH','MANDANTE','UNIDAD_MEDIDA_PEDIDO').
    mode("overwrite").
    parquet(target_path))




table = "EINE"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['APLFZ', 'NETPR', 'PEINH', 'UEBTO', 'UNTTO']
columns_d =  ['DATLB', 'ERDAT', 'PRDAT']
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["EBELN", "EBELP", "INFNR", "LOEKZ", "ESOKZ", "EKORG"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH','MANDANTE', 'TIPO_DOC_COMPRAS','ORGANIZACION_COMPRAS', 'TP_REG_INFO_COMPRAS').
    mode("overwrite").
    parquet(target_path))




table = "EKET"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['AMENG', 'GLMNG', 'MENGE', 'WEMNG' ]
columns_d =  ['BEDAT', 'EINDT', 'SLFDT', 'MBDAT']
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["EBELN", "EBELP", "ETENR"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'INDICADOR_CREACION','MANDANTE').
    mode("overwrite").
    parquet(target_path))


table = "EKET"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}_foto.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['AMENG', 'GLMNG', 'MENGE', 'WEMNG' ]
columns_d =  ['BEDAT', 'EINDT', 'SLFDT', 'MBDAT']
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["EBELN", "EBELP", "ETENR"]

df = df.withColumn("FECHA_SEQ_DATE",date_format(col("BEDAT"),"yyyy-MM-dd"))

first_day= (datetime.now().replace(day=1).strftime('%Y-%m-%d'))

df = df.filter(col("FECHA_SEQ_DATE") == first_day).drop("FECHA_SEQ_DATE")

particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH','MANDANTE').
    mode("overwrite").
    parquet(target_path))


df.count()




table = "EKKO"
delta_date = (LOG_TABLAS_SAP.filter(col('TABLA') == table)                            
              .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias('max'))                            
              .collect()[0].max)
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path)
       .filter(col("SEQ_DATE") >= delta_date))
columns_n =  ['LPONR', 'RLWRT', 'WKURS', 'ZBD1T' ]
columns_d =  ['BEDAT', 'AEDAT', 'DESP_DAT', 'KDATE' ]
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["EBELN", "LIFNR", "BUKRS"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'SOCIEDAD','ORGANIZACION_COMPRAS','MANDANTE','CENTRO_SUMINISTRADOR').
    mode("append").
    parquet(target_path))




table = "KONP"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['KOPOS']
columns_d =  []
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  [ "KAPPL","KNUMH","KOPOS", "KSCHL", "KUNNR"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'MANDANTE', 'CLASE_DE_ESCALA', 'CLASE_DE_CONDICION', 'REGLA_DE_CALCULO').
    mode("overwrite").
    parquet(target_path))




table = "MARC"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['BASMG', 'BSTMI', 'BSTMA', 'BSTRF', 'EISLO', 'MTVER','FXHOR']
columns_d =  ['MMSTD']
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["MATNR", "MANDT", "WERKS"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'EJ_PERIODO_ACTUAL','CENTRO','MANDANTE','CLAVE_DE_DESVIACION').
    mode("overwrite").
    parquet(target_path))










table = "T024"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  []
columns_d =  []
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["EKGRP", "MANDT"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'MANDANTE').
    mode("overwrite").
    parquet(target_path))




table = "T024D"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  []
columns_d =  []
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["WERKS", "DISPO"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'MANDANTE').
    mode("overwrite").
    parquet(target_path))




table = "T160"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  []
columns_d =  []
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["MANDT", "TCODE"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'MANDANTE', 'TIPO_DE_TRANSACCION').
    mode("overwrite").
    parquet(target_path))




table = "T160T"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  []
columns_d =  []
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["SELPA", "SPRAS"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'MANDANTE','CLAVE_DE_IDIOMA').
    mode("overwrite").
    parquet(target_path))




table = "EKPO"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['BONBA', 'BRTWR', 'KTMNG', 'MENGE', 'NTGEW', 'NETPR', 'NETWR', 'PLIFZ', 'UEBTO', 'UMREN', 'UMREZ', 'VOLUM' ]
columns_d =  ['AEDAT', 'CREATIONDATE']
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["EBELN", 'EBELP', "MATNR" ]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'CONVERSION_CANTIDAD','SOCIEDAD','MANDANTE','TOLER_FALTAS_SUMIN').
    mode("overwrite").
    parquet(target_path))




table = "MARA"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  [ 'BRGEW', 'GEWTO', 'MAXC', 'MAXC_TOL', 'OVERHANG_TRESH',
             'SAPMP_FBDK', 'SAPMP_FBHK', 'VOLTO', 'WESCH','NTGEW','BRGEW']
columns_d =  ['ERSDA','LAEDA']
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["MATNR" ]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'MODIFICADO_POR','UNIDAD_MEDIDA_BASE','MANDANTE','TIPO_MATERIAL').
    mode("overwrite").
    parquet(target_path))




table = "S032"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['BKLAS', 'MKOBEST', 'MBWBEST', 'WBWBEST']
columns_d =  []
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["MATNR", "WERKS", "LGORT", "MANDT"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'CENTRO','TIPO_MATERIAL','MANDANTE','SECTOR').
    mode("overwrite").
    parquet(target_path))




table = "T001L"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['SPART', 'VKORG', 'VSTEL', 'VTWEG' ]
columns_d =  []
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["MANDT", "WERKS", "LGORT"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'CENTRO','MANDANTE').
    mode("overwrite").
    parquet(target_path))




table = "MATDOC"
delta_date = (LOG_TABLAS_SAP.filter(col('TABLA') == table)                            
              .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias('max'))                            
              .collect()[0].max)
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path)
       .filter(col("SEQ_DATE") >= delta_date))
columns_n =  ['BPMNG', 'BWART','BWLVS', 'DMBUM', 'DMBTR', 'ERFMG', 'LGNUM', 'SALK3', 'URZEI']
columns_d =  ['BLDAT', 'BUDAT', 'CPUDT', 'HSDAT']
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["KEY1", "KEY2", "KEY3", "KEY4", "KEY5", "KEY6"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.write.format("delta").
    partitionBy('YEAR', 'MONTH','MANDANTE').
    mode("append").
    parquet(target_path))




table = "MAST"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['LOSVN', 'LOSBS']
columns_d =  ['ANDAT']
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  [ "WERKS", "MATNR", "STLNR", "STLAL"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'CENTRO','MANDANTE', 'LISTA_MAT_ALTERNAT').
    mode("overwrite").
    parquet(target_path))




table = "MCHB"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['CLABS']
columns_d =  ['CHDLL', 'ERSDA' ]
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["MATNR", "WERKS", "LGORT", "CHARG", "CLABS"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'BLOQUEADO','CENTRO','MANDANTE','EJ_PERIODO_ACTUAL').
    mode("overwrite").
    parquet(target_path))




table = "MKOL"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['MMENG', 'SLABS' , 'SVMEI']
columns_d =  ['ERSDA', 'KODLL']
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["MATNR", "SOBKZ", "LIFNR", "CHARG", "LGORT"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'MANDANTE', 'CREADO_POR', 'ALMACEN', 'CONSIG_BLOQUEADA' ).
    mode("overwrite").
    parquet(target_path))




table = "S031"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['AAGBB', 'AAGKB', 'AGVBR', 'AMBWG', 'ASTOR', 'AUVBR', 'AZUBB', 'AZUKB', 'MAGBB',  'WAGBB', 'WZUBB']
columns_d =  []
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["MATNR", "PERIV", "LGORT", "SPTAG", "SPBUP", "AMBWG"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'MANDANTE','CENTRO').
    mode("overwrite").
    parquet(target_path))




table = "STKO"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ['STLAL', 'STLNR']
columns_d =  ['AEDAT', 'ANDAT', 'DATUV']
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =  ["MANDT", "BMENG", "STLAL", "STLNR" ]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR', 'MONTH', 'CANTIDAD_BASE','CREADO_POR','MANDANTE','CENTRO_INSTALACION').
    mode("overwrite").
    parquet(target_path))




table = "EKBE"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = spark.read.parquet(source_path)
columns_n = ["ETENS", "MENGE", "LSMNG", "REEWR", "REFWR", "REWRB"]
columns_d = ["BLDAT", "BUDAT", "CPUDT"]
columns = [columns_n, columns_d]
tipos = ["numerico", "fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols, t)
keys = ["MATNR", "EBELN", "EBELP", "BELNR"]
particion = Window.partitionBy(keys).orderBy(desc("SEQ_DATE"))
df = (
    df.withColumn("ROW_NUM", row_number().over(particion))
    .filter(col("ROW_NUM") == 1)
    .drop("ROW_NUM")
)
aux = pd.read_excel(
    "/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",
    sheet_name=f"{table}",
)

aux["USUARIO"] = limpiar_columna(aux, "USUARIO")

for i in range(len(aux["USUARIO"])):
    df = df.withColumnRenamed(aux["TEC"][i], aux["USUARIO"][i])
(
    df.write.format("delta")
    .partitionBy("YEAR", "MONTH", "NUMERO_ACTUAL", "CLASE_DE_OPERACION")
    .mode("overwrite")
    .parquet(target_path)
)


table = "BAPI_CTRO_ALM"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  [] # Las que consideres que se necesitan con formato de número para las viz
columns_d =  [] # Las que veas que son relevantes, Ej: Fecha de creación, fecha de recepción, fecha de ultima modificación 
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)

keys =  ["MATERIAL", "CENTRO", "ALMACEN"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df.withColumn("ROW_NUM", row_number().over(particion))
      .filter(col('ROW_NUM')==1)
      .drop("ROW_NUM")
      )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR','MONTH','CENTRO').
    mode("overwrite").
    parquet(target_path))


table = "BAPI_ENTRYSHEET_GETDETAIL"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  [] # Las que consideres que se necesitan con formato de número para las viz
columns_d =  [] # Las que veas que son relevantes, Ej: Fecha de creación, fecha de recepción, fecha de ultima modificación 
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)

keys =  ["BELNR"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df.withColumn("ROW_NUM", row_number().over(particion))
      .filter(col('ROW_NUM')==1)
      .drop("ROW_NUM")
      )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR','MONTH').
    mode("overwrite").
    parquet(target_path))


table = "LFA1"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  [] # Las que consideres que se necesitan con formato de número para las viz
columns_d =  ['ERDAT', 'GBDAT', 'UPDAT', 'AEDAT'] # Las que veas que son relevantes, Ej: Fecha de creación, fecha de recepción, fecha de ultima modificación 
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)

keys =  ["LIFNR"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df.withColumn("ROW_NUM", row_number().over(particion))
      .filter(col('ROW_NUM')==1)
      .drop("ROW_NUM")
      )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR','MONTH','MANDANTE').
    mode("overwrite").
    parquet(target_path))


table = "LFB1"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  [] # Las que consideres que se necesitan con formato de número para las viz
columns_d =  ['ERDAT', 'UPDAT'] # Las que veas que son relevantes, Ej: Fecha de creación, fecha de recepción, fecha de ultima modificación 
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)

keys =  ["LIFNR"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df.withColumn("ROW_NUM", row_number().over(particion))
      .filter(col('ROW_NUM')==1)
      .drop("ROW_NUM")
      )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.
    write.format("delta").
    partitionBy('YEAR','MONTH','MANDANTE').
    mode("overwrite").
    parquet(target_path))




table = "ZV03RB"
delta_date = (LOG_TABLAS_SAP.filter(col('TABLA') == table)                            
              .agg(max(expr("LAST_MODIFFIED - INTERVAL 1 HOUR")).alias('max'))                            
              .collect()[0].max)
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
df = (spark
       .read
       .parquet(source_path))
columns_n =  ["ANTLF",  # Cantidad planificada de entregas
    "FIXMG",  # Cantidad fija
    "KBMENG",  # Cantidad de material en contrato
    "KWMENG",  # Cantidad convenida
    "NETWR",  # Valor neto
    "OLFMNG",  # Cantidad restante
    ]
columns_d =  ["ERDAT",  # Fecha de creación
    "LFDAT_1"  # Fecha de entrega
    ]
columns = [columns_n,columns_d]
tipos = ["numerico","fecha"]
for cols, t in zip(columns, tipos):
    df = convert_columns(df, cols,t)
keys =["KEY_HD", "KEY_POS"]

particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
df = (df
    .withColumn("ROW_NUM", row_number().over(particion))
    .filter(col('ROW_NUM')==1)
    .drop("ROW_NUM")
    )
aux = pd.read_excel("/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Mapeo_columns.xlsx",sheet_name=f"{table}")

aux["USUARIO"] = limpiar_columna(aux,"USUARIO")

for i in range(len(aux["USUARIO"])):
       df = (df
             .withColumnRenamed(aux["TEC"][i],aux["USUARIO"][i]))
(df.write.format("delta").
    partitionBy('YEAR', 'MONTH','MANDANTE').
    mode("overwrite").
    parquet(target_path))