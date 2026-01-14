# Databricks notebook source
# MAGIC      %run /Shared/FASE_1/includes

# COMMAND ----------

current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month
next_year = current_year + 1

# COMMAND ----------

# MAGIC %md
# MAGIC # Follow_up

# COMMAND ----------

# MAGIC %md
# MAGIC ##Catálogo de artículos

# COMMAND ----------

table = "dim_productos"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"
dim_productos= (spark
       .read
       .parquet(source_path))
       # .filter(col("SEQ_DATE") >= delta_date))


# COMMAND ----------

dim_productos = dim_productos.withColumn(
    "ID_MATERIAL",
    when(
        col("ID_MATERIAL").contains("Nvas"),
        expr("substring(ID_PRD, 1, length(ID_PRD) - 3)")
    ).otherwise(col("ID_MATERIAL"))
)

# COMMAND ----------

dim_productos = dim_productos.select(['ID_MATERIAL',
 'DES_MATERIAL',
 'SISTEMA',
 'COMPAÑIA',
 'LINEA_AGRUPADA']).distinct()

# COMMAND ----------

table = "CATALOGO_DE_ARTICULOS"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/marketing/{table}.parquet/"
catalogo_de_articulos= (spark
       .read
       .parquet(source_path))
       # .filter(col("SEQ_DATE") >= delta_date))

# COMMAND ----------

catalogo_de_articulos = (catalogo_de_articulos.withColumnRenamed(
    "MATERIAL", "ID_MATERIAL"
).withColumnRenamed("CDIS", "ID_CANAL_DISTRIBUCION")
.withColumn("ID_PRD", concat(col("ID_MATERIAL"),lit("-"),col("ID_CANAL_DISTRIBUCION")))
.withColumn("ID_MATERIAL", regexp_replace(col("ID_MATERIAL"), "^0{2,}", "")))


# COMMAND ----------

table = "MBEW"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
keys = ["MATERIAL","ANIO", "MES"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
mbew = (spark.
       read.
       parquet(source_path)
       .where(col("AREA_DE_VALORACION")=="A717")
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .where(col("PRECIO_FISCAL_1") != 0)
       .select("MATERIAL","PRECIO_FISCAL_1","SEQ_DATE").dropDuplicates()
       .withColumn("ANIO", year(col("SEQ_DATE").cast("date")))
       .withColumn("MES", month(col("SEQ_DATE").cast("date")))
       .withColumn("DIA", day(col("SEQ_DATE").cast("date")))
       .withColumn("MES_PRONO_AUX", concat(col("ANIO"), lit("-"), col("MES"),lit("-01")))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM", "SEQ_DATE", "ANIO", "MES", "DIA")
       )

# COMMAND ----------

mbew_max = mbew.groupBy("MATERIAL").agg(min(col("PRECIO_FISCAL_1")).alias("PRECIO_FISCAL_MAX"))

# COMMAND ----------

# mbew.withColumn("ANIO_MES", date_format(col("SEQ_DATE"), "yyyy-MM")).groupBy("ANIO_MES").agg(min("SEQ_DATE")).withColumn(
#     'ANIO_MES2', 
#     date_format(add_months('ANIO_MES', -1), 'yyyy-MM')
# ).display()

# COMMAND ----------

# mbew.withColumn("ANIO_MES", date_format(col("SEQ_DATE"), "yyyy-MM")).groupBy("ANIO_MES").agg(max("SEQ_DATE")).display()

# COMMAND ----------

# mbew.groupBy("SEQ_DATE").agg(count(col("MATERIAL"))).display()

# COMMAND ----------

# mbew.count()

# COMMAND ----------

mbew = mbew.withColumn('MATERIAL', regexp_replace(col('MATERIAL'), r'^[0]*', ''))



# COMMAND ----------

#Aplico a los consignados
# precio_fiscal = mbew.select(col("MATERIAL").alias("ID_MATERIAL"), col("PRECIO_FISCAL_1").cast("float")).distinct()

# COMMAND ----------

#Este afecta a las compras
# precio_medio = catalogo_de_articulos.groupBy("ID_MATERIAL").agg(max("PRECIO_VARIABLE").alias("PRECIO_VARIABLE"))

# COMMAND ----------

table = "T024"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
planeadores= (spark
       .read
       .parquet(source_path))
       # .filter(col("SEQ_DATE") >= delta_date))


# COMMAND ----------

planeadores = planeadores.select('GRUPO_DE_COMPRAS', col('DENOMINACION_GRPCOMP').alias('PLANEADOR')).distinct()

# COMMAND ----------

catalogo_de_articulos = catalogo_de_articulos.join(planeadores,on = ['GRUPO_DE_COMPRAS'], how = 'left')

# COMMAND ----------

catalogo_de_articulos = catalogo_de_articulos.unionByName(dim_productos.join(dim_productos.select("ID_MATERIAL").distinct() \
    .join(
        catalogo_de_articulos.select("ID_MATERIAL").distinct(),
        on=["ID_MATERIAL"],
        how="left_anti"
    ),  on=["ID_MATERIAL"],how = 'inner'), allowMissingColumns=True)

# COMMAND ----------

catalogo_de_articulos = catalogo_de_articulos.select('GRUPO_DE_COMPRAS',
 'ID_MATERIAL',
 'TEXTO_BREVE_DE_MATERIAL',
 'INDICADOR_ABC',
 'CARACT_PLANIF_NEC',
 'TAMANO_LOTE_MINIMO',
 'HORIZ_PLANIF_FIJO',
 'GRUPO_DE_ARTICULOS',
 'PLANIF_NECESIDADES',
 'SISTEMA',
 'COMPANIA',
 'LINEA_AGRUPADA',
 'LINEA_ABIERTA',
 'PLANEADOR',
 'DES_MATERIAL',
 'COMPAÑIA').distinct()

# COMMAND ----------

catalogo_de_articulos = catalogo_de_articulos.groupBy('GRUPO_DE_COMPRAS',
 'ID_MATERIAL',
 'TEXTO_BREVE_DE_MATERIAL',
 'INDICADOR_ABC',
 'CARACT_PLANIF_NEC',
 'TAMANO_LOTE_MINIMO',
 'HORIZ_PLANIF_FIJO',
 'GRUPO_DE_ARTICULOS',
 'PLANIF_NECESIDADES',
 'SISTEMA',
 'COMPANIA',
 'LINEA_AGRUPADA',
 'PLANEADOR',
 'DES_MATERIAL',
 'COMPAÑIA').agg(max("LINEA_ABIERTA"))

# COMMAND ----------

table = "catalogo_de_articulos"
target_path = f"/mnt/adlsabastosprscus/abastos/gold/{table}.parquet/"
(catalogo_de_articulos.
    write.format("delta").
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Inventario Inicial

# COMMAND ----------

table = "fact_hab_inventario_inicio"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/{table}.parquet/"
inventario_disponible =  (spark.read.parquet(source_path)
                          .withColumn("COSTO_UNITARIO", col("STOCK_VALOR") / col("STOCK_UNIDADES"))).fillna(0)


# COMMAND ----------

# Cálculo de precio de demanda y limpieza de columnas no necesarias
precio_inventario = inventario_disponible.select("ID_MATERIAL","STOCK_VALOR","STOCK_UNIDADES","ANIO_MES") \
    .withColumn("PRECIO_INVENTARIO", col("STOCK_VALOR") / col("STOCK_UNIDADES")) \
    .drop("STOCK_VALOR","STOCK_UNIDADES")

# COMMAND ----------

precio_inventario=precio_inventario.join(precio_inventario.groupBy("ID_MATERIAL").agg(max("ANIO_MES").alias("ANIO_MES")),on=["ANIO_MES","ID_MATERIAL"],how="inner").drop("ANIO_MES")

# COMMAND ----------

inventario_disponible_unidades =  (inventario_disponible
                                   .select("ID_MATERIAL","STOCK_UNIDADES","FLAG_CS","COSTO_UNITARIO","ANIO_MES")
                                   .withColumn("ANIO_MES", col("ANIO_MES").cast("date"))
                                   .withColumnRenamed("STOCK_UNIDADES", "STOCK")
                                   .withColumn("TYPE_1", lit("PIEZAS"))
                                   .withColumnRenamed("ANIO_MES", "MES_PRONO_AUX")
                                   .join(mbew.withColumnRenamed("MATERIAL","ID_MATERIAL"), ["ID_MATERIAL", "MES_PRONO_AUX"],"left")
                                   .join(mbew_max.withColumnRenamed("MATERIAL","ID_MATERIAL"), ["ID_MATERIAL"],"left")
                                   .withColumn("PRECIO_FISCAL_1", when(col("PRECIO_FISCAL_1").isNull(), col("PRECIO_FISCAL_MAX")).otherwise(col("PRECIO_FISCAL_1")))
                                   .withColumn("COSTO_UNITARIO", when(col("FLAG_CS") == "CONSIGNADO", col("PRECIO_FISCAL_1")).otherwise(col("COSTO_UNITARIO")))
                                   .drop("PRECIO_FISCAL_1", "PRECIO_FISCAL_MAX")
                                   )
 
 
inventario_disponible_imp = (inventario_disponible
                             .select("ID_MATERIAL","STOCK_UNIDADES","FLAG_CS","COSTO_UNITARIO","ANIO_MES")
                             .withColumn("ANIO_MES", col("ANIO_MES").cast("date"))
                             .withColumnRenamed("STOCK_UNIDADES", "STOCK")
                             .withColumn("TYPE_1", lit("IMPORTE"))
                             .withColumnRenamed("ANIO_MES", "MES_PRONO_AUX")
                             .join(mbew.withColumnRenamed("MATERIAL","ID_MATERIAL"), ["ID_MATERIAL", "MES_PRONO_AUX"],"left")
                             .join(mbew_max.withColumnRenamed("MATERIAL","ID_MATERIAL"), ["ID_MATERIAL"],"left")
                             .withColumn("PRECIO_FISCAL_1", when(col("PRECIO_FISCAL_1").isNull(), col("PRECIO_FISCAL_MAX")).otherwise(col("PRECIO_FISCAL_1")))
                             .withColumn("COSTO_UNITARIO", when(col("FLAG_CS") == "CONSIGNADO", col("PRECIO_FISCAL_1")).otherwise(col("COSTO_UNITARIO")))
                             .withColumn("STOCK", col("STOCK") * col("COSTO_UNITARIO"))
                             .drop("PRECIO_FISCAL_1", "PRECIO_FISCAL_MAX")
                            )
 
inventario_disponible =  inventario_disponible_unidades.unionByName(inventario_disponible_imp).dropDuplicates().withColumn("ANIO_MES", date_format(col("MES_PRONO_AUX"), "yyyy-MM"))

# COMMAND ----------

# fact_inventario_inicial=fact_inventario_inicial.groupBy(
#  'ID_MATERIAL',
#  'ANIO_MES','FLAG_CS').agg(sum('STOCK_UNIDADES').alias('STOCK_UNIDADES'),
#  sum('STOCK_VALOR').alias('STOCK_VALOR'))


# COMMAND ----------

# fact_inventario_inicial = fact_inventario_inicial.join(precio_fiscal, on = ["ID_MATERIAL"], how = "left")

# COMMAND ----------

# fact_inventario_inicial = fact_inventario_inicial.withColumn("STOCK_VALOR",when(col("FLAG_CS")=="CONSIGNADO",col("STOCK_UNIDADES")*col("PRECIO_FISCAL_1")).otherwise(col("STOCK_VALOR")))

# COMMAND ----------

# columns_to_transpose = [
# 'STOCK_UNIDADES',
#  'STOCK_VALOR',]
        
# columns_to_group_by = [
# 'ID_MATERIAL',
#  'ANIO_MES',]   

# COMMAND ----------

# #Se transponen los datos
# fact_inventario_inicial = fact_inventario_inicial.melt(
#     ids=[columns_to_group_by], values=[columns_to_transpose],
#     variableColumnName="CONCEPTO", valueColumnName="STOCK")

# COMMAND ----------

# fact_inventario_inicial = fact_inventario_inicial.withColumn("CONCEPTO",when(col("CONCEPTO") == "STOCK_UNIDADES", lit("PIEZAS")).otherwise(lit("IMPORTE")))

# COMMAND ----------

fact_inventario_inicial = inventario_disponible.select("ID_MATERIAL","ANIO_MES",col("TYPE_1").alias("CONCEPTO"),"STOCK").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Inventario Final
# MAGIC

# COMMAND ----------

# fact_inventario_final = fact_inventario_inicial.withColumn(
#     'ANIO_MES', 
#     date_format(add_months('ANIO_MES', -1), 'yyyy-MM')).withColumnRenamed("STOCK","STOCK_FINAL")

# COMMAND ----------

table = "fact_hab_inventario"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"
fact_inventario_final= (spark
       .read
       .parquet(source_path)).fillna(0)
       # .filter(col("FLAG_CS") == 'PROPIO'))

# COMMAND ----------

fact_inventario_final=fact_inventario_final.groupBy(
 'ID_MATERIAL',
 'ANIO_MES','FLAG_CS').agg(sum('STOCK_UNIDADES').alias('STOCK_UNIDADES'),
 sum('STOCK_VALOR').alias('STOCK_VALOR'))

# COMMAND ----------

precio_medio = inventario_disponible.select("ANIO_MES","ID_MATERIAL","COSTO_UNITARIO").distinct().withColumn("ID_MATERIAL", regexp_replace(col("ID_MATERIAL"), "^0{2,}", ""))

# COMMAND ----------

fact_inventario_final=fact_inventario_final.groupBy(
 'ID_MATERIAL',
 'ANIO_MES','FLAG_CS').agg(sum('STOCK_UNIDADES').alias('STOCK_UNIDADES'),
 sum('STOCK_VALOR').alias('STOCK_VALOR'))

fact_inventario_final = fact_inventario_final.join(precio_medio, on = ["ID_MATERIAL","ANIO_MES"], how = "left")

fact_inventario_final = fact_inventario_final.withColumn("STOCK_VALOR",col("STOCK_UNIDADES")*col("COSTO_UNITARIO"))

# COMMAND ----------

columns_to_transpose = [
'STOCK_UNIDADES',
 'STOCK_VALOR',]
        
columns_to_group_by = [
'ID_MATERIAL',
 'ANIO_MES',]   

# COMMAND ----------

#Se transponen los datos
fact_inventario_final = fact_inventario_final.melt(
    ids=[columns_to_group_by], values=[columns_to_transpose],
    variableColumnName="CONCEPTO", valueColumnName="STOCK_FINAL")

# COMMAND ----------

fact_inventario_final = fact_inventario_final.withColumn("CONCEPTO",when(col("CONCEPTO") == "STOCK_UNIDADES", lit("PIEZAS")).otherwise(lit("IMPORTE")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Demanda Irrestricta

# COMMAND ----------

# table = "HISTORIA_PRONO_MI"
# source_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/{table}.parquet/"
# demanda_irrestricta = (spark
#        .read
#        .parquet(source_path))


# COMMAND ----------

regex_pattern = r"(\d{4}-\d{2})"
keys = ["ID_MATERIAL","ANIO_MES","TYPE_1"] 
particion = (Window.partitionBy(keys).orderBy(desc('datestr')))
table = "HISTORIA_PRONO_MI"
prono_mi = (spark.read.parquet(
    f"/mnt/adls-dac-data-bi-pr-scus/gold/{table}.parquet")
            .withColumn("MES_PRONO_AUX", regexp_extract(col("datestr"), regex_pattern, 1))
            .withColumn("MES_PRONO_AUX", col("MES_PRONO_AUX").cast("date"))
            .withColumnRenamed("DATE","ANIO_MES")
            #.withColumn("ANIO_MES", regexp_extract(col("ANIO_MES"), regex_pattern, 1))
            .withColumnRenamed("MATERIAL","ID_MATERIAL")
            .withColumn("CONCEPTO", when(col("CONCEPTO") == "U_PRONO", "PIEZAS").otherwise("IMPORTE"))
            .withColumnRenamed("CONCEPTO","TYPE_1")
            .withColumn("ROW_NUM", row_number().over(particion))
            .filter(col("ROW_NUM") == 1)
            #.filter(col("TYPE_1") == "PIEZAS")
            .drop("ID_CANAL_DISTRIBUCION","ROW_NUM")
)

keys = ["ID_MATERIAL","MES_PRONO_AUX","ANIO_MES","CONCEPTO"]
particion = (Window.partitionBy(keys).orderBy(desc('datestr')))
table = "prono_mkt_memo"
prono_memo = (spark.read.parquet(
    f"/mnt/adls-dac-data-bi-pr-scus/gold/{table}.parquet/")
            .withColumn("MES_PRONO_AUX", regexp_extract(col("datestr"), regex_pattern, 1))
            .withColumn("MES_PRONO_AUX", col("MES_PRONO_AUX").cast("date"))
            .withColumnRenamed("DATE","ANIO_MES")
            #.withColumn("ANIO_MES", regexp_extract(col("ANIO_MES"), regex_pattern, 1))
            .withColumnRenamed("MATERIAL","ID_MATERIAL")
            .groupBy("ID_MATERIAL","SISTEMA","COMPANIA","LINEA_AGRUPADA","LINEA_ABIERTA", "CLASE", "SEGMENTO","CONCEPTO","MES_PRONO_AUX", "ANIO_MES","datestr")
            .agg(sum("value").alias("value"))
            .withColumn("ROW_NUM", row_number().over(particion))
            .filter(col('ROW_NUM')==1)
            .drop("ROW_NUM")
            .orderBy("ID_MATERIAL","MES_PRONO_AUX","ANIO_MES","ROW_NUM")
            .withColumn("CONCEPTO", when(col("CONCEPTO") == "U_PRONO", "PIEZAS").otherwise("IMPORTE"))
            .withColumnRenamed("CONCEPTO","TYPE_1")
            #.filter(col("TYPE_1") == "PIEZAS")
)

# COMMAND ----------

columns_order = ['ID_MATERIAL',
                 'SISTEMA',
                 'COMPANIA',
                 'LINEA_AGRUPADA',
                 'LINEA_ABIERTA',
                 'CLASE',
                 'SEGMENTO',
                 'MES_PRONO_AUX',
                 'TYPE_1',
                 'value',
                 'ANIO_MES']

# COMMAND ----------

prono_memo = prono_memo.select(columns_order)
prono_mi = prono_mi.select(columns_order)

prono = prono_memo.unionByName(prono_mi)

# COMMAND ----------

keys = ["ID_MATERIAL","MES_PRONO_AUX","TYPE_1"]
window = Window.partitionBy(keys).orderBy("ID_MATERIAL","MES_PRONO_AUX","ANIO_MES","TYPE_1")
prono = (prono
         #.filter(col("TYPE_1") == "PIEZAS")
         .groupBy("ID_MATERIAL", "MES_PRONO_AUX","ANIO_MES","TYPE_1")
         .agg(sum("value").alias("value"))  
         .filter(col("ANIO_MES") >= col("MES_PRONO_AUX"))
         .withColumn("ROW_NUM", row_number().over(window))
         #.filter(col("ROW_NUM") < 13)
         #.drop("ROW_NUM", "MES_PRONO")
         .orderBy("ID_MATERIAL","MES_PRONO_AUX","TYPE_1","ANIO_MES") 
         .withColumn("value", when(col("TYPE_1") == "IMPORTE", col("value") * 1000).otherwise(col("value")))                            
         )

# COMMAND ----------

prono_dic_aux = prono.where((month(col("MES_PRONO_AUX")) == 11) & (month(col("ANIO_MES")) != 11)).withColumn("MES_PRONO_AUX", add_months(col("MES_PRONO_AUX"), 1))
prono = prono.where(month(col("MES_PRONO_AUX")) != 12)

# COMMAND ----------

prono = prono.unionByName(prono_dic_aux)

# COMMAND ----------

# table = "prono_mkt_memo"
# historico_prono_memo = (spark.
#                      read.
#                      parquet(f"/mnt/adls-dac-data-bi-pr-scus/gold/{table}.parquet/"))

# COMMAND ----------

# demanda_irrestricta = demanda_irrestricta.unionByName(historico_prono_memo, allowMissingColumns=True)

# COMMAND ----------

# demanda_irrestricta = demanda_irrestricta.groupBy(['MATERIAL',
#  'SISTEMA',
#  'COMPANIA',
#  'LINEA_ABIERTA',
#  'LINEA_AGRUPADA',
#  'CLASE',
#  'SEGMENTO',
#  'ID_CANAL_DISTRIBUCION',
#  'DATE',
#  'CONCEPTO',
#  'team',
#  'usuario',
#  'datestr']).agg(sum('value').alias('value'))

# COMMAND ----------

# # Obtener la fecha actual
# hoy = datetime.now()

# # Calcular el mes anterior
# primer_dia_mes_actual = datetime(hoy.year, hoy.month, 2)
# mes_anterior = primer_dia_mes_actual - timedelta(days=1)
# año_anterior = mes_anterior.year
# mes_anterior_numero = mes_anterior.month

# # Filtrar el DataFrame por el mes anterior
# demanda_irrestricta= demanda_irrestricta.filter(
#     (year(col("datestr")) == lit("2024")) & 
#     (month(col("datestr")) == lit("11"))
# )


# COMMAND ----------

# demanda_irrestricta = (
#     (
#         demanda_irrestricta.select(
#             col("MATERIAL").alias("ID_MATERIAL"),
#             "ID_CANAL_DISTRIBUCION",
#             col("DATE").alias("ANIO_MES"),
#             col("value").alias("DI"),
#             "CONCEPTO",
#             col("datestr").alias("ANIO_MES_PRONO"),
#         )
#     )
#     .withColumn("ANIO_MES", date_format(col("ANIO_MES"), "yyyy-MM"))
#     .withColumn("ANIO_MES_PRONO", date_format(col("ANIO_MES_PRONO"), "yyyy-MM"))
#     .withColumn("CONCEPTO",  when(col('CONCEPTO') == 'IMP_PRONO', 'IMPORTE').otherwise('PIEZAS'))
#     .withColumn("DI",  when(col('CONCEPTO') == 'IMPORTE', col("DI") * 1000).otherwise(col("DI")))
# )

# COMMAND ----------

# demanda_irrestricta= demanda_irrestricta.groupBy('ID_MATERIAL',
#  'ANIO_MES',
#  'CONCEPTO',
#  'ANIO_MES_PRONO').agg(sum('DI').alias("DI"))

# COMMAND ----------

# demanda_irrestricta.select("ANIO_MES_PRONO").distinct().display()

# COMMAND ----------

# keys = ["ID_MATERIAL","ANIO_MES","TYPE_1"]
# window = Window.partitionBy(keys).orderBy(desc("MES_PRONO_AUX"))

# demanda_irrestricta = (prono.filter(col("MES_PRONO_AUX") < f"{current_year}-{current_month}-01")
#                        .withColumn("ROW_NUM", row_number().over(window))
#                        .filter(col("ROW_NUM") == 1)
#                        .withColumnsRenamed({"MATERAL":"ID_MATERIAL","value": "DI", "TYPE_1": "CONCEPTO"})
#                        .drop("ROW_NUM","MES_PRONO_AUX")
#                        .dropDuplicates()
#                        .fillna(0)
#                        .withColumn("ANIO_MES", regexp_extract(col("ANIO_MES"), regex_pattern, 1))
#                        .dropDuplicates()
#                        ).fillna(0)


# COMMAND ----------

keys = ["ID_MATERIAL","ANIO_MES","TYPE_1"]
window = Window.partitionBy(keys).orderBy(desc("MES_PRONO_AUX"))

demanda_irrestricta = (prono.filter(col("MES_PRONO_AUX") < f"{current_year}-{current_month:02d}-01")
                       .withColumn("ROW_NUM", row_number().over(window))
                       .filter(col("ROW_NUM") == 1)
                       .withColumnsRenamed({"MATERAL":"ID_MATERIAL","value": "DI", "TYPE_1": "CONCEPTO"})
                       .drop("ROW_NUM","MES_PRONO_AUX")
                       .dropDuplicates()
                       .fillna(0)
                       .withColumn("ANIO_MES", regexp_extract(col("ANIO_MES"), regex_pattern, 1))
                       ).fillna(0)

# COMMAND ----------

# demanda_irrestricta.where(col("ID_MATERIAL") == "HR355").display()

# COMMAND ----------

# demanda_irrestricta.where(col("ID_MATERIAL") == "HR355").groupBy("ANIO_MES","CONCEPTO","ROW_NUM").agg(sum("DI")).display()

# COMMAND ----------

# demanda_irrestricta.groupBy("ANIO_MES","CONCEPTO").agg(sum("DI")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Pedidos

# COMMAND ----------

table = "fact_pedidos"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"
pedidos= (spark
       .read
       .parquet(source_path)).fillna(0)
       # .filter(col("SEQ_DATE") >= delta_date))


# COMMAND ----------

pedidos = pedidos.filter(col("FECHA_REGISTRO")>="2023-01-01")

# COMMAND ----------

pedidos= pedidos.select(
 'ID_MATERIAL',
 'PIEZAS',
 'PIEZAS_FACTURADA',
 'PIEZAS_BACKORDER',
 'IMPORTE_FACTURADA',
 'IMPORTE_BACKORDER',
 'IMPORTE',
 'ANIO_MES',
 'FECHA_REGISTRO',
 'ID_CANAL_DISTRIBUCION').filter(col("FECHA_REGISTRO")>='2023-01-01')

# COMMAND ----------

columns_to_transpose = [
 'PIEZAS',
 'PIEZAS_FACTURADA',
 'PIEZAS_BACKORDER',
 'IMPORTE_FACTURADA',
 'IMPORTE_BACKORDER',
 'IMPORTE']
        
columns_to_group_by = [
'ID_MATERIAL',
'ANIO_MES',
'ID_CANAL_DISTRIBUCION'   ]

# COMMAND ----------

#Se agrupan las metricas por tipo
type = {
        'PIEZAS_FACTURADA':"VENTA",
        'IMPORTE_FACTURADA':"VENTA",
        "PIEZAS":"PEDIDOS",
        "PIEZAS_BACKORDER":"BACKORDER",
        "IMPORTE":"PEDIDOS",
        "IMPORTE_BACKORDER":"BACKORDER",
            }

# COMMAND ----------

measures = {
        'PIEZAS_FACTURADA':"PIEZAS",
        'IMPORTE_FACTURADA':"IMPORTE",
        "PIEZAS":"PIEZAS",
        "PIEZAS_BACKORDER":"PIEZAS",
        "IMPORTE":"PIEZAS",
        "IMPORTE_BACKORDER":"IMPORTE",
            }

# COMMAND ----------

#Se genera el data frame 

measures= [(MEASURE, TYPE_1) for MEASURE, TYPE_1 in measures.items()]

# Crear el DataFrame de Spark a partir de la lista de tuplas
measures= spark.createDataFrame(measures, ['MEASURE', 'TYPE_1'])

# COMMAND ----------

#Se genera el data frame

type= [(MEASURE, TYPE_2) for MEASURE, TYPE_2 in type.items()]
# Crear el DataFrame de Spark a partir de la lista de tuplas
type= spark.createDataFrame(type, ['MEASURE', 'TYPE_2'])

# COMMAND ----------

#Join del dataframe
type_measures = type.join(
                                        measures,
                                        on = ['MEASURE'],
                                        how = "inner")
                                        

# COMMAND ----------

#Se transponen los datos
pedidos = (pedidos.melt(
    ids=[columns_to_group_by], values=[columns_to_transpose],
    variableColumnName="MEASURE", valueColumnName="VALUE"))

# COMMAND ----------

#Se unen las metricas por tipo
pedidos= pedidos.join(
                                        type_measures,
                                        on = ['MEASURE'],
                                        how = "inner")

# COMMAND ----------

#Se hace la cross tab
pedidos = pedidos.groupBy('ID_MATERIAL', 'ANIO_MES',
      "TYPE_1").pivot("TYPE_2").agg({"VALUE": "sum"})

# COMMAND ----------

pedidos=pedidos.withColumnRenamed("TYPE_1","CONCEPTO")

# COMMAND ----------

# pedidos = pedidos.withColumn(
#     'ANIO_MES', 
#     date_format(add_months('ANIO_MES', +1), 'yyyy-MM') )

# COMMAND ----------

pedidos = pedidos.withColumn(
    'ANIO_MES', 
    date_format(add_months('ANIO_MES', +1), 'yyyy-MM')
)

# COMMAND ----------

pedidos_fin = pedidos.withColumn(
    'ANIO_MES', 
    date_format(add_months('ANIO_MES', -1), 'yyyy-MM')
)

# COMMAND ----------

pedidos_fin = pedidos_fin.withColumnRenamed("BACKORDER","BACKORDER_FIN").withColumnRenamed("PEDIDOS","PEDIDOS_FIN").withColumnRenamed("VENTA","VENTA_FIN")

# COMMAND ----------

from pyspark.sql.functions import col

# Selecciona las columnas relevantes y renombra "BACKORDER" a "BACKORDER_IMPORTE" donde el CONCEPTO es "IMPORTE", luego elimina la columna "CONCEPTO"
pedidos_importe = pedidos.select(
    "ID_MATERIAL", "ANIO_MES", "CONCEPTO", col("BACKORDER").alias("BACKORDER_IMPORTE")
).filter(col("CONCEPTO") == "IMPORTE").drop("CONCEPTO")

# Selecciona las columnas relevantes y renombra "BACKORDER" a "BACKORDER_PIEZAS" donde el CONCEPTO es "PIEZAS", luego elimina la columna "CONCEPTO"
pedidos_piezas = pedidos.select(
    "ID_MATERIAL", "ANIO_MES", "CONCEPTO", col("BACKORDER").alias("BACKORDER_PIEZAS")
).filter(col("CONCEPTO") == "PIEZAS").drop("CONCEPTO")

# Hace un join de los dos dataframes anteriores sobre las columnas "ID_MATERIAL" y "ANIO_MES", calcula el precio por backorder y luego elimina las columnas innecesarias
precio_pedidos = pedidos_piezas.join(
    pedidos_importe, on=["ID_MATERIAL", "ANIO_MES"], how="inner"
).withColumn(
    "PRECIO_BACKORDER", col("BACKORDER_IMPORTE") / col("BACKORDER_PIEZAS")
).drop("BACKORDER_IMPORTE", "BACKORDER_PIEZAS")



# COMMAND ----------

precio_pedidos=precio_pedidos.join(precio_pedidos.groupBy("ID_MATERIAL").agg(max("ANIO_MES").alias("ANIO_MES")),on=["ANIO_MES","ID_MATERIAL"],how="inner").drop("ANIO_MES")

# COMMAND ----------

# MAGIC %md
# MAGIC ##PPTO

# COMMAND ----------

table = "PPTO_VIGENTE"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"
ppto= (spark
       .read
       .parquet(source_path)).fillna(0)
       # .filter(col("SEQ_DATE") >= delta_date))


# COMMAND ----------

ppto = ppto.withColumn("ID_MATERIAL", when(col("ID_MATERIAL").contains("Nvas Aplica"), left("ID_PRD", length(col("ID_PRD")) - 3)).otherwise(col("ID_MATERIAL")))

# COMMAND ----------

ppto = ppto.filter(~col("ANIO_MES").isin(["2018","2019","2020","2021","2022","2025"]))

# COMMAND ----------

ppto = ppto.groupBy('ID_MATERIAL',
 col('TYPE_1').alias('CONCEPTO'),
 'ANIO_MES').agg(sum('PPTO').alias('PPTO'))

# COMMAND ----------

# ppto_2025 = spark.createDataFrame(
#     pd.read_excel(
#         "/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Inventario Optimo 2025 Productos Propios.xlsx"))

# COMMAND ----------

# ppto = ppto.unionByName(ppto_2025,allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Proveedores

# COMMAND ----------

table = "dim_proveedor"
source_path = f"/mnt/adlsabastosprscus/abastos/gold/compras/{table}.parquet/"
dim_proveedor= (spark
       .read
       .parquet(source_path))
       # .filter(col("SEQ_DATE") >= delta_date))

# COMMAND ----------

dim_proveedor = (dim_proveedor.filter(col("ID_SOCIEDAD")=='A717').select('ID_PAIS',
 'ID_ESTADO',
 'ID_MANDANTE',
 'ID_PROVEEDOR',
 'NOMBRE_1',
 'NOMBRE_2',
 'CIUDAD',
 'ID_SOCIEDAD',
 'PAIS_REG',
 col('Descripción').alias("ESTADO")).distinct())

# COMMAND ----------

table = "inforecord"
source_path = f"/mnt/adlsabastosprscus/abastos/gold/compras/{table}.parquet/"
proveedores= (spark
       .read
       .parquet(source_path))
       # .filter(col("SEQ_DATE") >= delta_date))

# COMMAND ----------

proveedores = proveedores.filter(col("CENTRO")=='A717')

# COMMAND ----------

proveedores = proveedores.select('ID_PROVEEDOR',col("NOMBRE_1").alias("NOMBRE_PROVEEDOR"),
 col('PAIS_REG').alias("NOMBRE_PAIS"),
 col('Descripción').alias('NOMBRE_ESTADO'),
 col('MATERIAL').alias("ID_MATERIAL")).distinct().withColumn("ID_MATERIAL", regexp_replace(col("ID_MATERIAL"), "^0{2,}", ""))

# COMMAND ----------

catalogo_de_articulos = catalogo_de_articulos.select("ID_MATERIAL").distinct()

# COMMAND ----------

proveedores = proveedores.join(proveedores.groupBy("ID_MATERIAL").agg(max("ID_PROVEEDOR").alias("ID_PROVEEDOR")),on = ["ID_MATERIAL","ID_PROVEEDOR"],how="inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Inventario por centro

# COMMAND ----------

table = "fact_inventario_trans"
       # .filter(col("SEQ_DATE") >= delta_date))
source_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"
fact_inventario_trans= (spark
       .read
       .parquet(source_path)).fillna(0)

# COMMAND ----------

fact_inventario_piezas = fact_inventario_trans.filter(col("TYPE_1")=="PIEZAS")

# COMMAND ----------

fact_inventario_importe= fact_inventario_trans.filter(col("TYPE_1")=="PIEZAS")

# COMMAND ----------

fact_inventario_importe_consignados= fact_inventario_trans.filter((col("TYPE_1")=="PIEZAS") & (col("TYPE_3")=="CONSIGNADO"))

fact_inventario_importe_propios = fact_inventario_trans.filter((col("TYPE_1")=="IMPORTE") & (col("TYPE_3")=="PROPIO"))

fact_inventario_importe_consignados = fact_inventario_importe_consignados.join(precio_medio, on = ["ID_MATERIAL","ANIO_MES"], how = "left")

fact_inventario_importe_consignados  = fact_inventario_importe_consignados.withColumn("STOCK",col("STOCK")*col("COSTO_UNITARIO"))

fact_inventario_importe_consignados = fact_inventario_importe_consignados.withColumn("TYPE_1",lit("IMPORTE"))

fact_inventario_importe = fact_inventario_importe_consignados.unionByName(fact_inventario_importe_propios, allowMissingColumns=True)
                                                                                     


# COMMAND ----------

fact_inventario_trans = fact_inventario_piezas.unionByName(fact_inventario_importe, allowMissingColumns=True)

# COMMAND ----------

fact_inventario_trans = (
    fact_inventario_trans
    .withColumn(
        "A717", 
        when(
            (col("ID_ALMACEN") == 'A717') & (col("TYPE_2").isin(["LIBRE UTILIZACION", "EN TRASLADO"])),
            col("STOCK")
        )
    )
    .withColumn(
        "A718", 
        when(
            (col("ID_ALMACEN") == 'A718') & (col("TYPE_2").isin(["LIBRE UTILIZACION", "EN TRASLADO"])),
            col("STOCK")
        )
    )
    .withColumn(
        "A780,785 & 790", 
        when(
            (col("ID_ALMACEN").isin(['A780', 'A785', 'A790'])) & (col("TYPE_2").isin(["LIBRE UTILIZACION", "EN TRASLADO"])),
            col("STOCK")
        )
    )
    .withColumn(
        "A730", 
        when(
            (col("ID_ALMACEN").isin(["A730"])),
            col("STOCK")
        )
    )
    .withColumn(
        "TOTAL", 
        when(
            (col("ID_ALMACEN").isin(['A717', 'A718', 'A780', 'A785', 'A790'])) & (col("TYPE_2").isin(["LIBRE UTILIZACION", "EN TRASLADO"])),
            col("STOCK")
        )
    )
)


# COMMAND ----------

fact_inventario_trans = fact_inventario_trans.groupBy(
 'ANIO_MES',
 'ID_MATERIAL',
 'ID_CANAL_DISTRIBUCION',
 col('TYPE_1').alias("CONCEPTO")
).agg(sum("A717").alias("A717"),
              sum("A718").alias("A718"),
              sum("A780,785 & 790").alias("A780,785 & 790"),
              sum("A730").alias("A730"),
              sum("TOTAL").alias("TOTAL")
              )

# COMMAND ----------

fact_inventario_trans = fact_inventario_trans.groupBy(
 'ANIO_MES',
 'ID_MATERIAL',
 "CONCEPTO"
).agg(first("A717").alias("A717"),
              first("A718").alias("A718"),
              first("A780,785 & 790").alias("A780,785 & 790"),
              first("A730").alias("A730"),
              first("TOTAL").alias("TOTAL")
              )

# COMMAND ----------

# Mostramos el resultado
fact_inventario_trans_final = fact_inventario_trans.withColumnRenamed("A717","A717_FIN").withColumnRenamed("A718","A718_FIN").withColumnRenamed("A780,785 & 790","A780,785 & 790_FIN").withColumnRenamed("A730","A730_FIN").withColumnRenamed("TOTAL","TOTAL_FIN")

# COMMAND ----------

# fact_inventario_trans.groupby("ANIO_MES","TYPE_1","ID_CENTRO").agg(sum("STOCK")).orderBy("ANIO_MES",desc=True).display()

# COMMAND ----------

# Restamos un mes a la columna ANIO_MES y lo asignamos en una nueva columna ANIO_MES_ANTERIOR
fact_inventario_trans = fact_inventario_trans.withColumn(
    'ANIO_MES', 
    date_format(add_months('ANIO_MES', +1), 'yyyy-MM')
)



# COMMAND ----------

# MAGIC %md
# MAGIC ##MARC

# COMMAND ----------

# table = "MARC"
# source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
# marc= (spark
#        .read
#        .parquet(source_path))
#        # .filter(col("SEQ_DATE") >= delta_date))

# COMMAND ----------

keys = ["MANDANTE","MATERIAL","CENTRO"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
table = "MARC"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
marc = (spark.
       read.
       parquet(source_path)
       .where(col("CENTRO")== "A717")
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
       .withColumn("STOCK_ALM_MAX",col("STOCK_ALM_MAX").cast("int"))
       )

# COMMAND ----------

marc = marc.select(col("MATERIAL").alias("ID_MATERIAL"),"STOCK_ALM_MAX").filter(col("CENTRO")=="A717").withColumn("ID_MATERIAL", regexp_replace(col("ID_MATERIAL"), "^0{2,}", "")).withColumn("STOCK_ALM_MAX",col("STOCK_ALM_MAX").cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Compras

# COMMAND ----------

table = "plan_entregas"
source_path = f"/mnt/adlsabastosprscus/abastos/gold/planeacion/{table}.parquet/"
plan_entregas= (spark
       .read
       .parquet(source_path)).fillna(0)
       # .filter(col("SEQ_DATE") >= delta_date))



# COMMAND ----------

table = "plan_entregas_fin"
source_path = f"/mnt/adlsabastosprscus/abastos/gold/planeacion/{table}.parquet/"
plan_entregas_fin= (spark
       .read
       .parquet(source_path)).fillna(0)
       # .filter(col("SEQ_DATE") >= delta_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingresos

# COMMAND ----------

# table = "MATDOC"
# source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
# keys =  ["SPLIT_GUID,_PART_1", "SPLIT_GUID,_PART_2", "SPLIT_GUID,_PART_3", "SPLIT_GUID,_PART_4", "SPLIT_GUID,_PART_5", "SPLIT_GUID,_PART_6"]
# particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
# ingresos =  (spark.read.parquet(source_path)
#              .withColumn("ROW_NUM", row_number().over(particion))
#              .filter(col('ROW_NUM')==1)
#              .drop("ROW_NUM"))

# COMMAND ----------

# ingresos.filter(col("SEQ_DATE").contains("2025-01")).select("LOTE_IDENTIFICADOR_DE_LOTE").distinct().display()

# COMMAND ----------

table = "MATDOC"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
keys =  ["SPLIT_GUID,_PART_1", "SPLIT_GUID,_PART_2", "SPLIT_GUID,_PART_3", "SPLIT_GUID,_PART_4", "SPLIT_GUID,_PART_5", "SPLIT_GUID,_PART_6"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
ingresos =  (spark.read.parquet(source_path)
             .withColumn("ROW_NUM", row_number().over(particion))
             .filter(col('ROW_NUM')==1)
             .drop("ROW_NUM")
             .select("MATERIAL","ALMACEN", "IMPTE_MON_LOCAL","CENTRO","CLASE_DE_MOVIMIENTO",col("CANTIDAD_DE_STOCK").alias("CANTIDAD"),"FECHA_DE_ENTRADA","FE_CONTABILIZACION"
                       )
             .where((col("CLASE_DE_MOVIMIENTO").isin([101,102,311,312]))
                )).fillna(0)

# COMMAND ----------

ingresos = ingresos.filter(col("CENTRO")=="A717").filter(~col("ALMACEN").isin(["A730","A723"]))

# COMMAND ----------

ingresos = ingresos.withColumn("ANIO_MES", date_format(col("FECHA_DE_ENTRADA"), "yyyy-MM"))

# COMMAND ----------

ingresos = ingresos.withColumn("IMPTE_MON_LOCAL",when((col("CLASE_DE_MOVIMIENTO")==102) | (col("CLASE_DE_MOVIMIENTO")==312), col("IMPTE_MON_LOCAL")*-1).otherwise(col("IMPTE_MON_LOCAL")))

# COMMAND ----------

# x = ingresos.where((col("ID_MATERIAL").contains("235376")) & (col("ANIO_MES") == "2025-05"))

# COMMAND ----------



# COMMAND ----------

columns_to_transpose = [
    'CANTIDAD',
    'IMPTE_MON_LOCAL']

columns_to_group_by = [
   ['MATERIAL',
 'ANIO_MES']
 ]

# COMMAND ----------

type = {
    "CANTIDAD": 'INGRESOS',
    'IMPTE_MON_LOCAL': 'INGRESOS'
}

# COMMAND ----------

measures = {
    "CANTIDAD": 'PIEZAS',
    'IMPTE_MON_LOCAL': 'IMPORTE'
}

# COMMAND ----------

#Se genera el data frame 

measures= [(MEASURE, TYPE_1) for MEASURE, TYPE_1 in measures.items()]

# Crear el DataFrame de Spark a partir de la lista de tuplas
measures= spark.createDataFrame(measures, ['MEASURE', 'TYPE_1'])

# COMMAND ----------

#Se genera el data frame

type= [(MEASURE, TYPE_2) for MEASURE, TYPE_2 in type.items()]
# Crear el DataFrame de Spark a partir de la lista de tuplas
type= spark.createDataFrame(type, ['MEASURE', 'TYPE_2'])

# COMMAND ----------

#Join del dataframe
type_measures = type.join(
                                        measures,
                                        on = ['MEASURE'],
                                        how = "inner")

# COMMAND ----------

# Cast columns to a common data type
ingresos = ingresos.withColumn("CANTIDAD", ingresos["CANTIDAD"].cast("float"))
ingresos = ingresos.withColumn("IMPTE_MON_LOCAL", ingresos["IMPTE_MON_LOCAL"].cast("float"))

# Transpose the data
ingresos = ingresos.melt(
    ids=columns_to_group_by,
    values=columns_to_transpose,
    variableColumnName="MEASURE",
    valueColumnName="VALUE"
)


# COMMAND ----------

#Se unen las metricas por tipo
ingresos= ingresos.join(
                                        type_measures,
                                        on = ['MEASURE'],
                                        how = "inner")

# COMMAND ----------

# Se hace la cross tab
ingresos = (
    ingresos.groupBy(
        [
            col("MATERIAL").alias("ID_MATERIAL"),
            "ANIO_MES",
            col("TYPE_1").alias("CONCEPTO"),
        ]
    )
    .pivot("TYPE_2")
    .agg({"VALUE": "sum"})
    .withColumn("ID_MATERIAL", regexp_replace(col("ID_MATERIAL"), "^0{2,}", ""))
)

# COMMAND ----------

ingresos = ingresos.join(precio_medio, on = ["ID_MATERIAL","ANIO_MES"], how = 'left')

# COMMAND ----------


ingresos = ingresos.withColumn(
    "INGRESOS",
    when(
        col("CONCEPTO") == "IMPORTE",
        col("INGRESOS") * col("COSTO_UNITARIO").cast(DoubleType())
    ).otherwise(col("INGRESOS"))
).drop("COSTO_UNITARIO")


# COMMAND ----------

ingresos_fin = ingresos.withColumnRenamed("INGRESOS","INGRESOS_FIN")

# COMMAND ----------

# x = ingresos_fin.where((col("ID_MATERIAL").contains("235376")) & (col("ANIO_MES") == "2025-05"))

# COMMAND ----------

# x.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Pedidos confirmados
# MAGIC

# COMMAND ----------

# from pyspark.sql.functions import col, expr, to_date

# pedidos_apartados.withColumn(
#     "ANIO_MES",
#     to_date(expr("CAST(KEY_HD AS STRING)"), "yyyyMMddHHmmss")  # Convertir a string antes de procesar
# ).filter(col("MATNR") == "HR355").display()


# COMMAND ----------

table = "ZV03RB"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
pedidos_apartados= (spark
       .read
       .parquet(source_path)).fillna(0)

# COMMAND ----------

pedidos_apartados = pedidos_apartados.withColumn(
    "ANIO_MES",
    date_format(from_unixtime(unix_timestamp(col("CLAVE_DE_CABECERA"), "yyyyMMddHHmmss")), "yyyy-MM")
).withColumn(
    "ANIO_MES_DIA",
    date_format(from_unixtime(unix_timestamp(col("CLAVE_DE_CABECERA"), "yyyyMMddHHmmss")), "yyyy-MM-dd")
)




# COMMAND ----------

pedidos_apartados = pedidos_apartados.join(
    pedidos_apartados.groupBy("ANIO_MES")
    .agg(max("ANIO_MES_DIA").alias("ANIO_MES_DIA"))
    .distinct(),
    on=["ANIO_MES", "ANIO_MES_DIA"],
    how="inner"
).distinct()


# COMMAND ----------

pedidos_apartados = pedidos_apartados.filter(col("CENTRO")=="A717")

# COMMAND ----------

pedidos_apartados = pedidos_apartados.groupBy(col("NUMERO_DE_MATERIAL").alias("ID_MATERIAL"),"ANIO_MES").agg(
    sum('CANTIDAD_RESTANTE').alias('CANTIDAD_APARTADA') )

# COMMAND ----------

pedidos_apartados = pedidos_apartados.withColumn('ID_MATERIAL', regexp_replace(col('ID_MATERIAL'), r'^[0]*', ''))

# COMMAND ----------

pedidos_apartados = pedidos_apartados.withColumn("CONCEPTO", lit("PIEZAS"))

# COMMAND ----------

pedidos_apartados_fin = pedidos_apartados.withColumnRenamed("CANTIDAD_APARTADA","CANTIDAD_APARTADA_FIN")

# COMMAND ----------

pedidos_apartados = pedidos_apartados.withColumn(
    'ANIO_MES', 
    date_format(add_months('ANIO_MES', +1), 'yyyy-MM'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Fechas

# COMMAND ----------

table = "dim_fechas"
source_path = f"/mnt/adlsabastosprscus/abastos/gold/compras/{table}.parquet/"
fechas = (spark
       .read
       .parquet(source_path))
       # .filter(col("SEQ_DATE") >= delta_date))

# COMMAND ----------

fechas = fechas.filter((col("LAST_DATE") > "2022-12-31") & (col("LAST_DATE") <= "2029-12-31")).select("ANIO_MES").withColumn("ID",lit("1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##inventario_optimo

# COMMAND ----------

inventario_optimo = spark.createDataFrame(
    pd.read_excel(
        "/dbfs/mnt/adlsabastosprscus/abastos/catalogos/Inventario Optimo 2025 Productos Propios.xlsx"))

# COMMAND ----------

inventario_optimo = inventario_optimo.withColumn('ID_MATERIAL', regexp_replace(col('MATERIAL'), r'^[0]*', '')).withColumn("ID",lit("1"))

# COMMAND ----------

inventario_optimo_2025_piezas = inventario_optimo.select("ID_MATERIAL",col("INV OPT 2025 (REV)").alias("INVENTARIO_OPTIMO_VALIDADO"),"ID").distinct().withColumn("CONCEPTO",lit("PIEZAS"))
inventario_optimo_2025_importe = inventario_optimo.select("ID_MATERIAL",col("Unnamed: 3").alias("INVENTARIO_OPTIMO_VALIDADO"),"ID").distinct().withColumn("CONCEPTO",lit("IMPORTE"))
inventario_optimo_2025 =inventario_optimo_2025_piezas.unionByName(inventario_optimo_2025_importe, allowMissingColumns=True)
inventario_optimo_2025 = inventario_optimo_2025.join(fechas.filter(col("ANIO_MES").contains("2025")),"ID","inner").drop("ID")

# COMMAND ----------

inventario_optimo_2024_piezas = inventario_optimo.select("ID_MATERIAL",col("`Inv. Obj 2024 Pz`").alias("INVENTARIO_OPTIMO_VALIDADO"),"ID").distinct().withColumn("CONCEPTO",lit("PIEZAS"))
inventario_optimo_2024_importe = inventario_optimo.select("ID_MATERIAL",col("`Inv. Obj \n2024 $`").alias("INVENTARIO_OPTIMO_VALIDADO"),"ID").distinct().withColumn("CONCEPTO",lit("IMPORTE"))
inventario_optimo_2024 =inventario_optimo_2024_piezas.unionByName(inventario_optimo_2024_importe, allowMissingColumns=True)
inventario_optimo_2024 = inventario_optimo_2024.join(fechas.filter(col("ANIO_MES").contains("2024")),"ID","inner").drop("ID")

# COMMAND ----------

inventario_optimo_2023_piezas = inventario_optimo.select("ID_MATERIAL",col("`Inv. Obj 2023 Pz`").alias("INVENTARIO_OPTIMO_VALIDADO"),"ID").distinct().withColumn("CONCEPTO",lit("PIEZAS"))
inventario_optimo_2023_importe = inventario_optimo.select("ID_MATERIAL",col("`Inv. Obj \n2023 $`").alias("INVENTARIO_OPTIMO_VALIDADO"),"ID").distinct().withColumn("CONCEPTO",lit("IMPORTE"))
inventario_optimo_2023 =inventario_optimo_2023_piezas.unionByName(inventario_optimo_2024_importe, allowMissingColumns=True)
inventario_optimo_2023 = inventario_optimo_2023.join(fechas.filter(col("ANIO_MES").contains("2023")),"ID","inner").drop("ID")

# COMMAND ----------

inventario_optimo = inventario_optimo_2023.unionByName(inventario_optimo_2024.unionByName(inventario_optimo_2025,allowMissingColumns=True),allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Precio Demanda

# COMMAND ----------

from pyspark.sql.functions import col, date_format, sum

# Selección y renombramiento de columnas, además de filtrado correcto
demanda_piezas = demanda_irrestricta \
    .withColumn("ANIO", date_format(col("ANIO_MES"), "yyyy")) \
    .groupBy("ANIO", "ID_MATERIAL","CONCEPTO") \
    .agg(sum("DI").alias("DI_PIEZAS")) \
    .filter(col("CONCEPTO") == "PIEZAS") \
    .drop("CONCEPTO")

demanda_importe = demanda_irrestricta \
    .withColumn("ANIO", date_format(col("ANIO_MES"), "yyyy")) \
    .groupBy("ANIO", "ID_MATERIAL","CONCEPTO") \
    .agg(sum("DI").alias("DI_IMPORTE")) \
    .filter(col("CONCEPTO") == "IMPORTE") \
    .drop("CONCEPTO")

# Unión de los DataFrames
precio_demanda_anual = demanda_piezas.join(demanda_importe, on=["ANIO", "ID_MATERIAL"], how="left")

# Cálculo de precio de demanda y limpieza de columnas no necesarias
precio_demanda_anual = precio_demanda_anual \
    .withColumn("PRECIO_DEMANDA_ANUAL", col("DI_IMPORTE") / col("DI_PIEZAS")) \
    .drop("DI_IMPORTE", "DI_PIEZAS")
    
precio_demanda_anual = demanda_irrestricta.withColumn("ANIO", date_format(col("ANIO_MES"), "yyyy")).select("ID_MATERIAL","ANIO_MES","ANIO").distinct().join(precio_demanda_anual, on = ["ANIO","ID_MATERIAL"], how = "left").drop("ANIO")


# COMMAND ----------

from pyspark.sql.functions import col

# Selección y renombramiento de columnas, además de filtrado correcto
demanda_piezas = demanda_irrestricta \
    .select("ID_MATERIAL", "ANIO_MES", "CONCEPTO", col("DI").alias("DI_PIEZAS")) \
    .filter(col("CONCEPTO") == "PIEZAS") \
    .drop("CONCEPTO")

demanda_importe = demanda_irrestricta \
    .select("ID_MATERIAL", "ANIO_MES", "CONCEPTO", col("DI").alias("DI_IMPORTE")) \
    .filter(col("CONCEPTO") == "IMPORTE") \
    .drop("CONCEPTO")

# Unión de los DataFrames
# Asegúrate de que los nombres de las columnas en el 'join' coincidan con los nombres reales de las columnas en los DataFrames
precio_demanda_mensual = demanda_piezas.join(
    demanda_importe, 
    on=["ID_MATERIAL", "ANIO_MES"],  # Asegúrate de que ambas columnas estén presentes en ambos DataFrames y sean las correctas para el join
    how="left"
)

# Cálculo de precio de demanda y limpieza de columnas no necesarias
precio_demanda_mensual = precio_demanda_mensual \
    .withColumn("PRECIO_DEMANDA_MENSUAL", col("DI_IMPORTE") / col("DI_PIEZAS")) \
    .drop("DI_IMPORTE", "DI_PIEZAS")


# COMMAND ----------

# MAGIC %md
# MAGIC ##CONCEPTO
# MAGIC

# COMMAND ----------

# Datos para el DataFrame
data = [("IMPORTE",), ("PIEZAS",)]

# Esquema para el DataFrame
schema = ["CONCEPTO"]

# Crear el DataFrame
concepto = spark.createDataFrame(data, schema).withColumn("ID",lit("1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Combinaciones

# COMMAND ----------

# Realizar el join inicial con la tabla 'marc'
catalogo_de_articulos = (
    catalogo_de_articulos
    .join(marc, on=["ID_MATERIAL"], how="left")  # Join con 'marc' usando 'ID_MATERIAL'
    .withColumn("ID", lit("1"))                 # Crear la columna 'ID' con valor constante '1'
    .join(fechas, "ID", "inner")                # Join con 'fechas' usando la columna 'ID'
    .join(concepto, "ID", "inner")              # Join con 'concepto' usando la columna 'ID'
    .drop("ID")                               # Eliminar la columna 'ID' después del join
)


# COMMAND ----------

precio_medio=precio_medio.join(precio_medio.groupBy("ID_MATERIAL").agg(max("ANIO_MES").alias("ANIO_MES")),on=["ANIO_MES","ID_MATERIAL"],how="inner").drop("ANIO_MES")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Follow up

# COMMAND ----------

# x = ingresos_fin.where((col("ID_MATERIAL").contains("235376")) & (col("ANIO_MES") == "2025-05"))

# COMMAND ----------

# x.display()

# COMMAND ----------

follow_up = (
    catalogo_de_articulos
    .join(
         fact_inventario_inicial,on=["ID_MATERIAL","ANIO_MES","CONCEPTO"],how="left"
      )
    .join(
         fact_inventario_final,on=["ID_MATERIAL","ANIO_MES","CONCEPTO"], how="left"
      )
       .join(
         demanda_irrestricta, on=["ID_MATERIAL", "ANIO_MES", "CONCEPTO"], how="left"
      )
     .join(
         pedidos,
         on=["ID_MATERIAL", "ANIO_MES", "CONCEPTO"],
         how="left",
     )
     .join(
         pedidos_fin,
         on=["ID_MATERIAL", "ANIO_MES" , "CONCEPTO"],
         how="left",
     )
    .join(
         ppto,
         on=["ID_MATERIAL", "ANIO_MES", "CONCEPTO"],
         how="left",
     )
     .join(fact_inventario_trans,
         on=["ID_MATERIAL", "ANIO_MES", "CONCEPTO"],
         how="left",    
     )
      .join(fact_inventario_trans_final,
         on=["ID_MATERIAL", "ANIO_MES", "CONCEPTO"],
         how="left",    
     )
      .join(plan_entregas,
         on=["ID_MATERIAL", "ANIO_MES", "CONCEPTO"],
         how="left", 
     )
       .join(plan_entregas_fin,
         on=["ID_MATERIAL", "ANIO_MES", "CONCEPTO"],
         how="left", 
     )
       .join(ingresos,
         on=["ID_MATERIAL","CONCEPTO","ANIO_MES"],
         how="left",   
     )
        .join(ingresos_fin,
         on=["ID_MATERIAL","CONCEPTO","ANIO_MES"],
         how="left",   
      )
        .join(precio_pedidos,
         on=["ID_MATERIAL"],
         how="left",   
      )
        .join(precio_inventario,
         on=["ID_MATERIAL"],
         how="left",   
      )
        .join(precio_demanda_anual,
         on=["ID_MATERIAL","ANIO_MES"],
         how="left",   
      )
       .join(precio_demanda_mensual,
         on=["ID_MATERIAL","ANIO_MES"],
         how="left",   
      )    
        .join(precio_medio,
         on=["ID_MATERIAL"],
         how="left",   
      )      
        .join(pedidos_apartados,
         on=["ID_MATERIAL","CONCEPTO","ANIO_MES"],
         how="left",   
     )
         .join(pedidos_apartados_fin,
         on=["ID_MATERIAL","CONCEPTO","ANIO_MES"],
         how="left",   
     )
         .join(inventario_optimo,
         on=["ID_MATERIAL","CONCEPTO","ANIO_MES"],
         how="left",   
     )
         .join(proveedores,
         on=["ID_MATERIAL"],
         how="left",
     )
 .distinct().withColumn("ANIO_MES_1", col("ANIO_MES")).dropDuplicates()
)


# COMMAND ----------

follow_up.filter(col('ANIO_MES')=='2025-11').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##escribir

# COMMAND ----------

select_cols = ['ID_MATERIAL',
 'CONCEPTO',
 'ANIO_MES',
 'STOCK_ALM_MAX',
 'STOCK',
 'STOCK_FINAL',
 'DI',
 'BACKORDER',
 'PEDIDOS',
 'VENTA',
 'BACKORDER_FIN',
 'PEDIDOS_FIN',
 'VENTA_FIN',
 'PPTO',
 'A717',
 'A718',
 'A780,785 & 790',
 'A730',
 'TOTAL',
 'A717_FIN',
 'A718_FIN',
 'A780,785 & 790_FIN',
 'A730_FIN',
 'TOTAL_FIN',
 'CANTIDAD',
 'CANTIDAD_FIN',
 'ID_PROVEEDOR',
 'NOMBRE_PROVEEDOR',
 'NOMBRE_PAIS',
 'NOMBRE_ESTADO',
 'INGRESOS',
 'INGRESOS_FIN',
 'PRECIO_BACKORDER',
 'PRECIO_INVENTARIO',
 'PRECIO_DEMANDA_ANUAL',
 'PRECIO_DEMANDA_MENSUAL',
 'COSTO_UNITARIO',
 'CANTIDAD_APARTADA',
 'CANTIDAD_APARTADA_FIN',
 'INVENTARIO_OPTIMO_VALIDADO',
 'ANIO_MES_1']

# COMMAND ----------

follow_up = follow_up.withColumn("INVENTARIO_OPTIMO_VALIDADO", when(col("INVENTARIO_OPTIMO_VALIDADO").isNull(), col('STOCK_ALM_MAX')).otherwise(col('INVENTARIO_OPTIMO_VALIDADO'))).withColumn("INVENTARIO_OPTIMO_VALIDADO", col("INVENTARIO_OPTIMO_VALIDADO").cast("int")).dropDuplicates()

# COMMAND ----------

table = "follow_up_completo"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"
follow_up_bu= (spark
       .read
       .parquet(source_path))
       # .filter(col("SEQ_DATE") >= delta_date))

# COMMAND ----------

# MAGIC %md
# MAGIC Quitar si se agrega la demanda irrestricta 171 - 173 

# COMMAND ----------

# follow_up = follow_up.drop("DI")

# COMMAND ----------

# follow_up_bu = follow_up_bu.select("ANIO_MES","CONCEPTO","ID_MATERIAL","DI")

# COMMAND ----------

# follow_up = follow_up.join(follow_up_bu, on=["ID_MATERIAL","ANIO_MES","CONCEPTO"], how="left")

# COMMAND ----------

table = "follow_up"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"

# COMMAND ----------

(follow_up.select(select_cols).
    write.format("delta").
    partitionBy('ANIO_MES_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

table = "dim_proveedor"
target_path = f"/mnt/adlsabastosprscus/abastos/gold/planeacion/{table}.parquet/"

# COMMAND ----------

(dim_proveedor.
    write.format("delta").
    #partitionBy('ID_CANAL_DISTRIBUCION_1','ANIO_MES_1').
    mode("overwrite").
    parquet(target_path))