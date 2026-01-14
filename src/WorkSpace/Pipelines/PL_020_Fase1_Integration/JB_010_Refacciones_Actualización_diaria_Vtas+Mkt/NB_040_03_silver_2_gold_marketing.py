# Databricks notebook source
# MAGIC %md
# MAGIC # Includes

# COMMAND ----------

# MAGIC %run /Shared/FASE1_INTEGRACION/includes

# COMMAND ----------

# MAGIC %md
# MAGIC #Pedidos

# COMMAND ----------

#Hechos
table = "fact_pedidos"
fact_pedidos = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))      

# COMMAND ----------

#fact_pedidos.display()

# COMMAND ----------


fact_pedidos = (
    fact_pedidos
    .withColumn("ANIO_MES_1", col("ANIO_MES"))
    .withColumn("ID_CANAL_DISTRIBUCION_1", col("ID_CANAL_DISTRIBUCION"))  
    .withColumn("ID_PRD", concat(col("ID_MATERIAL"), lit("-"), col("ID_CANAL_DISTRIBUCION")))
    .withColumn("ID_CTE", concat(col("ID_CLIENTE"), lit("-"), col("ID_CANAL_DISTRIBUCION"))))

# COMMAND ----------

# fact_pedidos.join(dim_productos,
#                   on = ['ID_MATERIAL','ID_CANAL_DISTRIBUCION'],
#                   how = 'inner').filter(col("LINEA_AGRUPADA")=='VALVULAS').display()

# COMMAND ----------

table = "fact_pedidos"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"

# COMMAND ----------

fact_pedidos.display()

# COMMAND ----------

#Escritura de los archivos gold
(fact_pedidos.
    write.format('delta').
    partitionBy('ANIO_MES_1','ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

filtro_productos = fact_pedidos.select('ID_MATERIAL','ID_CANAL_DISTRIBUCION').distinct()

# COMMAND ----------


filtro_clientes = fact_pedidos.select('ID_CLIENTE','ID_CANAL_DISTRIBUCION').distinct()

# COMMAND ----------

fact_pedidos = fact_pedidos.select(['FECHA_REGISTRO',
 'ID_CLIENTE',
 'ID_MATERIAL',
 'PIEZAS',
 'PIEZAS_FACTURADA',
 'PIEZAS_RECHAZADA',
 'PIEZAS_BACKORDER',
 'IMPORTE_FACTURADA',
 'IMPORTE_RECHAZADA',
 'IMPORTE_BACKORDER',
 'IMPORTE',
 'ANIO_MES',
 'ID_CANAL_DISTRIBUCION',
 'ANIO_MES_1',
 'ID_CANAL_DISTRIBUCION_1',
 'ID_PRD',
 'ID_CTE'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pedidos_transpose

# COMMAND ----------

columns_to_transpose = [
        'PIEZAS',
 'PIEZAS_FACTURADA',
 'PIEZAS_RECHAZADA',
 'PIEZAS_BACKORDER',
 'IMPORTE_FACTURADA',
 'IMPORTE_RECHAZADA',
 'IMPORTE_BACKORDER',
 'IMPORTE']
        
columns_to_group_by = [
 'ID_CLIENTE',
 'ID_MATERIAL',
 'ANIO_MES',
 'ID_CANAL_DISTRIBUCION',
 'ANIO_MES_1',
 'ID_CANAL_DISTRIBUCION_1',
 'ID_PRD',
 'ID_CTE']   

# COMMAND ----------

#Se transponen los datos
fact_pedidos_trans = fact_pedidos.melt(
    ids=[columns_to_group_by], values=[columns_to_transpose],
    variableColumnName="MEASURE", valueColumnName="VALUE")


# COMMAND ----------

#Se agrupan las metricas por tipo
type = {
        "PIEZAS":"PEDIDOS",
        "PIEZAS_FACTURADA":"FACTURADO",
        "PIEZAS_RECHAZADA":"RECHAZADO",
        "PIEZAS_BACKORDER":"BACKORDER",
        "IMPORTE":"PEDIDOS",
        "IMPORTE_FACTURADA":"FACTURADO",
        "IMPORTE_RECHAZADA":"RECHAZADO",
        "IMPORTE_BACKORDER":"BACKORDER"
            }

# COMMAND ----------

measures = {
        "PIEZAS":"PIEZAS",
        "PIEZAS_FACTURADA":"PIEZAS",
        "PIEZAS_RECHAZADA":"PIEZAS",
        "PIEZAS_BACKORDER":"PIEZAS",
        "IMPORTE":"IMPORTE",
        "IMPORTE_FACTURADA":"IMPORTE",
        "IMPORTE_RECHAZADA":"IMPORTE",
        "IMPORTE_BACKORDER":"IMPORTE"
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
type_measures= type.join(
                                        measures,
                                        on = ['MEASURE'],
                                        how = "inner")

# COMMAND ----------

#Se unen las metricas por tipo
fact_pedidos_trans= fact_pedidos_trans.join(
                                        type_measures,
                                        on = ['MEASURE'],
                                        how = "inner")

# COMMAND ----------

#Se hace la cross tab
fact_pedidos_trans = fact_pedidos_trans.groupBy(
 'ID_CLIENTE',
 'ID_MATERIAL',
 'ANIO_MES',
 'ID_CANAL_DISTRIBUCION',
 'ANIO_MES_1',
 'ID_CANAL_DISTRIBUCION_1',
 'ID_PRD',
 'ID_CTE',
        "TYPE_1").pivot("TYPE_2").agg({"VALUE": "sum"})

# COMMAND ----------

#  fact_pedidos_trans.groupby("ANIO_MES","TYPE_1").agg(sum("FACTURADO")).display()

# COMMAND ----------

# fact_pedidos_trans.count()

# COMMAND ----------

table = "fact_pedidos_trans"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos gold
(fact_pedidos_trans.
    write.format('delta').
    partitionBy('ANIO_MES_1','ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# #Hechos
# table = "fact_pedidos_trans"
# fact_pedidos_trans= (spark.
#                      read.
#                      parquet(f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"))  

# COMMAND ----------

# fact_pedidos_trans.groupBy("ANIO_MES","TYPE_1").agg(sum("FACTURADO")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Clientes

# COMMAND ----------

#Dimensiones
table = "dim_clientes"
dim_clientes = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))     

# COMMAND ----------

dim_clientes = dim_clientes.join(filtro_clientes,
                                   on = ['ID_CLIENTE','ID_CANAL_DISTRIBUCION'],
                                    how = 'inner')

# COMMAND ----------

dim_clientes = (
    dim_clientes
    .withColumn("ID_CANAL_DISTRIBUCION_1", col("ID_CANAL_DISTRIBUCION"))
     .withColumn("ID_CTE", concat(col("ID_CLIENTE"), lit("-"), col("ID_CANAL_DISTRIBUCION"))))

# COMMAND ----------

target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos gold
(dim_clientes.
    write.format('delta').
    partitionBy('ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC #Margen Cliente Producto

# COMMAND ----------

#Hechos
table = "fact_margen_cliente_producto"
fact_margen_cliente_producto = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))

# COMMAND ----------

fact_margen_cliente_producto = (
    fact_margen_cliente_producto
    .withColumn("ANIO_MES_1", col("ANIO_MES"))  # Agregar una nueva columna "ANIO_MES_1" con los valores de "ANIO_MES" 
    .withColumn("ID_CANAL_DISTRIBUCION_1", col("ID_CANAL_DISTRIBUCION"))  # Agregar una nueva columna "ID_CANAL_DISTRIBUCION_1" con los valores de "ID_CANAL_DISTRIBUCION"
    .withColumn("ID_PRD", concat(col("ID_MATERIAL"), lit("-"),col("ID_CANAL_DISTRIBUCION")))
    .withColumn("ID_CTE", concat(col("ID_CLIENTE"), lit("-"), col("ID_CANAL_DISTRIBUCION"))))  # Agregar una nueva columna "ID_PRD" concatenando "ID_MATERIAL" y "ID_CANAL_DISTRIBUCION" separados por un guion "-"

# COMMAND ----------

fact_margen_cliente_producto = fact_margen_cliente_producto.select('ID_COMPANIA',
 'ID_SISTEMA',
 'ID_LINEA_ABIERTA',
 'ID_CLIENTE',
 'ID_MATERIAL',
 'CANTIDAD',
 'VENTA TOTAL',
 'COSTO_VENTA_FACT',
 'MARGEN_CLIENTE',
 'ANIO_MES',
 'ID_CANAL_DISTRIBUCION',
 'ANIO_MES_1',
 'ID_CANAL_DISTRIBUCION_1',
 'ID_PRD',
 'ID_CTE')

# COMMAND ----------

target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos gold
(fact_margen_cliente_producto.
    write.format('delta').
    partitionBy('ANIO_MES_1','ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

fact_margen_cliente_producto.groupBy("ANIO_MES").agg(sum("COSTO_VENTA_FACT")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Inventario Total

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inventario

# COMMAND ----------

table = "fact_inventario"
fact_inventario = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))

#Dimensiones
table = "dim_productos_material"
dim_productos = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))

dim_productos_select=dim_productos.select("ID_MATERIAL","ID_CANAL_DISTRIBUCION").distinct()

fact_inventario = fact_inventario.join(dim_productos_select,
                                       on =["ID_MATERIAL"],
                                       how = "left")
                    
fact_inventario = fact_inventario.select('ID_MATERIAL',
 'ID_CENTRO',
 'ESTATUS',
 'ELIMINACION',
 'INVENTARIO_BLOQUEO',
 'LIBRE_UTILIZACION',
 'EN_TRASLADO',
 'INSPECCION_DE_CALIDAD',
 'STOCK_NO_LIBRE',
 'BLOQUEADO',
 'DEVOLUCIONES',
 'CONSIG_LIBRE_UTILIZACION',
 'CONSIG_CONTROL_CALIDAD',
 'CONSIG_NO_LIBRE',
 'CONSIG_BLOQUEADO',
 'ANIO_MES',
 'ID_ALMACEN',
 'ID_CANAL_DISTRIBUCION')

fact_inventario = (
    fact_inventario
    .withColumn("ANIO_MES_1", col("ANIO_MES"))  # Agregar una nueva columna "ANIO_MES_1" con los valores de "ANIO_MES" 
    .withColumn("ID_CANAL_DISTRIBUCION_1", col("ID_CANAL_DISTRIBUCION")))  # Agregar una nueva columna "ID_CANAL_DISTRIBUCION_1" con los valores de "ID_CANAL_DISTRIBUCION"

from pyspark.sql.functions import substring, col

fact_inventario = fact_inventario.withColumn("ANIO", substring(col("ANIO_MES"), 1, 4)) \
                                .withColumn("MES", substring(col("ANIO_MES"), 6, 7))
                                
display(fact_inventario.limit(5))

# COMMAND ----------

#Seleccionamos los campos a transponer y agrupar
columns_to_transpose = [
 'LIBRE_UTILIZACION',
 'EN_TRASLADO',
 'INSPECCION_DE_CALIDAD',
 'STOCK_NO_LIBRE',
 'BLOQUEADO',
 'DEVOLUCIONES',
 'CONSIG_LIBRE_UTILIZACION',
 'CONSIG_CONTROL_CALIDAD',
 'CONSIG_NO_LIBRE',
 'CONSIG_BLOQUEADO',
        ]
        
columns_to_group_by = ['ID_MATERIAL',
 'ID_CENTRO',
 'ANIO_MES',
 'ANIO',
 'MES',
 'ID_ALMACEN',
 'ID_CANAL_DISTRIBUCION',
 'ANIO_MES_1',
 'ID_CANAL_DISTRIBUCION_1' ]   

#Se transponen los datos
fact_inventario_trans = fact_inventario.melt(
    ids=[columns_to_group_by], values=[columns_to_transpose],
    variableColumnName="MEASURE", valueColumnName="VALUE"
)

#Se agrupan las metricas

origen = {
 'LIBRE_UTILIZACION':'PROPIO',
 'EN_TRASLADO':'PROPIO',
 'INSPECCION_DE_CALIDAD':'PROPIO',
 'STOCK_NO_LIBRE':'PROPIO',
 'BLOQUEADO':'PROPIO',
 'DEVOLUCIONES':'PROPIO',
 'CONSIG_LIBRE_UTILIZACION':'CONSIGNADO',
 'CONSIG_CONTROL_CALIDAD':'CONSIGNADO',
 'CONSIG_NO_LIBRE':'CONSIGNADO',
 'CONSIG_BLOQUEADO':'CONSIGNADO',
            }

type = {
 'LIBRE_UTILIZACION': 'LIBRE UTILIZACION',
 'EN_TRASLADO':'EN TRASLADO',
 'INSPECCION_DE_CALIDAD':'CONTROL DE CALIDAD',
 'STOCK_NO_LIBRE':'NO LIBRE',
 'BLOQUEADO':'BLOQUEADO',
 'DEVOLUCIONES':'DEVOLUCIONES',
 'CONSIG_LIBRE_UTILIZACION': 'LIBRE UTILIZACION',
 'CONSIG_CONTROL_CALIDAD':'CONTROL DE CALIDAD',
 'CONSIG_NO_LIBRE':'NO LIBRE',
 'CONSIG_BLOQUEADO':'BLOQUEADO',
            }

measures = {
 'LIBRE_UTILIZACION':'PIEZAS',
 'EN_TRASLADO':'PIEZAS',
 'INSPECCION_DE_CALIDAD':'PIEZAS',
 'STOCK_NO_LIBRE':'PIEZAS',
 'BLOQUEADO':'PIEZAS',
 'DEVOLUCIONES':'PIEZAS',
 'CONSIG_LIBRE_UTILIZACION':'PIEZAS',
 'CONSIG_CONTROL_CALIDAD':'PIEZAS',
 'CONSIG_NO_LIBRE':'PIEZAS',
 'CONSIG_BLOQUEADO':'PIEZAS',
            }

#Se genera el data frame 

origen = [(MEASURE, TYPE_3) for MEASURE, TYPE_3 in origen.items()]

# Crear el DataFrame de Spark a partir de la lista de tuplas
origen = spark.createDataFrame(origen, ['MEASURE', 'TYPE_3'])

#Se genera el data frame 

measures= [(MEASURE, TYPE_1) for MEASURE, TYPE_1 in measures.items()]

# Crear el DataFrame de Spark a partir de la lista de tuplas
measures= spark.createDataFrame(measures, ['MEASURE', 'TYPE_1'])

#Se genera el data frame

type= [(MEASURE, TYPE_2) for MEASURE, TYPE_2 in type.items()]
# Crear el DataFrame de Spark a partir de la lista de tuplas
type= spark.createDataFrame(type, ['MEASURE', 'TYPE_2'])

# COMMAND ----------

#Join del dataframe
type_measures= type.join( measures,
                          on = ['MEASURE'],
                           how = "inner")

type_measures_origen= type_measures.join(
                                        origen,
                                        on = ['MEASURE'],
                                        how = "inner")
                                    
#Se unen las metricas por tipo
fact_inventario_trans= fact_inventario_trans.join(
                                        type_measures_origen,
                                        on = ['MEASURE'],
                                        how = "inner")

fact_inventario_trans = fact_inventario_trans.withColumn("ID_PRD", concat(col("ID_MATERIAL"), lit("-"), col("ID_CANAL_DISTRIBUCION")))

fact_inventario_trans = fact_inventario_trans.withColumnRenamed("VALUE",'STOCK')

fact_inventario_trans = fact_inventario_trans.filter((col("ANIO") > '2024') | ((col("ANIO") == '2024') & (col("MES") >= '05')))

display(fact_inventario_trans.limit(5))

# COMMAND ----------

fact_inventario_trans.select("ANIO_MES").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Habilidad de Inventario

# COMMAND ----------

table = "fact_hab_inventario"
fact_hab_inventario = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))

fact_hab_inventario = (
    fact_hab_inventario
    .withColumn("ANIO_MES_1", col("ANIO_MES")))  # Agregar una nueva columna "ANIO_MES_1" con los valores de "ANIO_MES" 

fact_hab_inventario = fact_hab_inventario.withColumn("COSTO_INVENTARIO", col("STOCK_VALOR") / col("STOCK_UNIDADES")) \
                                        .withColumn("ANIO", substring(col("ANIO_MES"), 1, 4)) \
                                        .withColumn("MES", substring(col("ANIO_MES"), 6, 7))

fact_hab_inventario = fact_hab_inventario.select(['ID_MATERIAL',
 'STOCK_UNIDADES',
 'STOCK_VALOR',
 'FLAG_CS',
 'ANIO_MES',
 'ANIO_MES_1',
 'COSTO_INVENTARIO',
 'ANIO',
 'MES'])

fact_hab_inventario = fact_hab_inventario.join(dim_productos_select,
                                       on =["ID_MATERIAL"],
                                       how = "left")

fact_hab_inventario = fact_hab_inventario.withColumn("ANIO_MES_1", col("ANIO_MES")) \
                                         .withColumn("ID_CANAL_DISTRIBUCION_1", col("ID_CANAL_DISTRIBUCION"))\
                                         .withColumn("TYPE_2", lit("LIBRE UTILIZACION"))\
                                         .withColumn("ID_CENTRO", lit("A717"))\
                                         .withColumn("ID_ALMACEN", lit("A717"))   # Agregar una nueva columna "ID_CANAL_DISTRIBUCION_1" con los valores de "ID_CANAL_DISTRIBUCION"

fact_hab_inventario = fact_hab_inventario.withColumnRenamed("FLAG_CS","TYPE_3")

# display(fact_hab_inventario.limit(5))

# COMMAND ----------

#Seleccionamos los campos a transponer y agrupar
columns_to_transpose = [
 'STOCK_UNIDADES',
 'STOCK_VALOR',
 'COSTO_INVENTARIO']
        
columns_to_group_by = ['ID_MATERIAL',
 'TYPE_3',
 'ANIO_MES',
 'ANIO_MES_1',
 'ANIO',
 'MES',
 'ID_CANAL_DISTRIBUCION',
 'ID_CANAL_DISTRIBUCION_1',
 'TYPE_2',
 'ID_CENTRO',
 'ID_ALMACEN']   

#Se transponen los datos
fact_hab_inventario_trans = fact_hab_inventario.melt(
    ids=[columns_to_group_by], values=[columns_to_transpose],
    variableColumnName="MEASURE", valueColumnName="VALUE"
)

#Se genera el data frame 

measures = {
 'STOCK_UNIDADES':'PIEZAS',
 'STOCK_VALOR':'IMPORTE',
 'COSTO_INVENTARIO':'IMPORTE'
            }

measures= [(MEASURE, TYPE_1) for MEASURE, TYPE_1 in measures.items()]

# Crear el DataFrame de Spark a partir de la lista de tuplas
measures= spark.createDataFrame(measures, ['MEASURE', 'TYPE_1'])

fact_hab_inventario_trans= fact_hab_inventario_trans.join(
                                        measures,
                                        on = ['MEASURE'],
                                        how = "inner")

display(fact_hab_inventario_trans.limit(5))

# COMMAND ----------

fact_hab_inventario_trans = fact_hab_inventario_trans.withColumn("ID_PRD", concat(col("ID_MATERIAL"), lit("-"), col("ID_CANAL_DISTRIBUCION")))

costo_inventario = fact_hab_inventario_trans.select("ID_PRD","ANIO_MES","VALUE","TYPE_3").filter(col("MEASURE")=="COSTO_INVENTARIO")

fact_hab_inventario_trans = fact_hab_inventario_trans.filter(col("MEASURE")!="COSTO_INVENTARIO")

fact_hab_inventario_trans = fact_hab_inventario_trans.withColumnRenamed("VALUE","STOCK")

fact_hab_inventario_trans = fact_hab_inventario_trans.filter((col("ANIO") < '2024') | ((col("ANIO") == '2024') & (col("MES") < '05'))
)


# COMMAND ----------

fact_hab_inventario_trans.select("ANIO_MES").distinct().display()

# COMMAND ----------

fact_inventario_trans = fact_inventario_trans.join(costo_inventario,
                           on = ["ID_PRD","ANIO_MES","TYPE_3"],
                           how = 'left')

fact_inventario_trans = fact_inventario_trans.withColumn("STOCK_IMPORTE",col("STOCK")*col("VALUE"))

display(fact_inventario_trans.limit(5))

# COMMAND ----------

#Seleccionamos los campos a transponer y agrupar
columns_to_transpose = [
 'STOCK',
 'STOCK_IMPORTE']
        
columns_to_group_by = ['ID_PRD',
 'ANIO_MES',
 'TYPE_3',
 'MEASURE',
 'ID_MATERIAL',
 'ID_CENTRO',
 'ANIO',
 'MES',
 'ID_ALMACEN',
 'ID_CANAL_DISTRIBUCION',
 'ANIO_MES_1',
 'ID_CANAL_DISTRIBUCION_1',
 'TYPE_2']   

#Se transponen los datos
fact_inventario_trans = fact_inventario_trans.melt(
    ids=[columns_to_group_by], values=[columns_to_transpose],
    variableColumnName="MEASURE1", valueColumnName="VALUE"
)

measures = {
  'STOCK':'PIEZAS',
 'STOCK_IMPORTE':'IMPORTE'
            }

#Se genera el data frame 

measures= [(MEASURE1, TYPE_1) for MEASURE1, TYPE_1 in measures.items()]

# Crear el DataFrame de Spark a partir de la lista de tuplas
measures= spark.createDataFrame(measures, ['MEASURE1', 'TYPE_1'])

#Se unen las metricas por tipo
fact_inventario_trans= fact_inventario_trans.join(
                                        measures,
                                        on = ['MEASURE1'],
                                        how = "inner")

fact_inventario_trans = fact_inventario_trans.withColumnRenamed("VALUE","STOCK")

display(fact_inventario_trans.limit(100))

# COMMAND ----------

fact_inventario_trans = fact_inventario_trans.drop("MEASURE1", "MEASURE")

fact_inventario_trans = fact_inventario_trans.unionByName(fact_hab_inventario_trans, allowMissingColumns=True)

fact_inventario_trans= fact_inventario_trans.filter(col("ID_PRD").isNotNull())

# COMMAND ----------

filtron_inventario = fact_inventario_trans.select("ID_MATERIAL","ID_CANAL_DISTRIBUCION").distinct()

# COMMAND ----------

table = "fact_inventario_trans"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"

(fact_inventario_trans.
    write.format('delta').
    partitionBy('ANIO_MES_1', 'ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC #Servicio

# COMMAND ----------

table = "fact_servicio"
fact_servicio = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))

# COMMAND ----------

# fact_servicio.groupBy("ANIO_MES").agg(sum("POS_SOLICITADAS"), sum("POS_ENTREGADAS")).display()

# COMMAND ----------

#Dimensiones
table = "dim_productos_material"
dim_productos = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))

# COMMAND ----------

dim_linea_agrupada =  dim_productos.select("ID_LINEA_ABIERTA","LINEA","LINEA_AGRUPADA").distinct()

# COMMAND ----------

dim_linea_agrupada.display()

# COMMAND ----------

import pyspark.sql.functions as F
fact_servicio= (
    fact_servicio
    .withColumn("ANIO_MES_1", col("ANIO_MES"))  # Agregar una nueva columna "ANIO_MES_1" con los valores de "ANIO_MES" 
    #.withColumn("ID_SISTEMA_1", col("ID_SISTEMA"))  # Agregar una nueva columna "ID_CANAL_DISTRIBUCION_1" con los valores de "ID_CANAL_DISTRIBUCION"
    .withColumn("ID_PRD_NS", concat(col("ID_SISTEMA"), lit("-"),col("LINEA_AGRUPADA"), lit("-"),col("ID_INDICADOR_ABC"))))  # Agregar una nueva columna "ID_PRD" concatenando "ID_MATERIAL" y "ID_CANAL_DISTRIBUCION" separados por un guion "-"

# COMMAND ----------

fact_servicio = fact_servicio.withColumnRenamed("LINEA_AGRUPADA","LINEA")

# COMMAND ----------

fact_servicio = fact_servicio.join(dim_linea_agrupada,
                                    on =["LINEA"],
                                    how = "left"
                                         

)

# COMMAND ----------

fact_servicio.display()

# COMMAND ----------

fact_servicio = fact_servicio.select('LINEA',
 'SISTEMA',
 'ID_CLIENTE',
 'CLIENTE',
 'PLANTA',
 'ID_INDICADOR_ABC',
 'POS_SOLICITADAS',
 'POS_ENTREGADAS',
 'ID_PRD',
 'ANIO_MES',
 'ID_SISTEMA',
 'ANIO_MES_1',
 'ID_PRD_NS',
 'ID_LINEA_ABIERTA',
 'LINEA_AGRUPADA')

# COMMAND ----------

table = "fact_servicio"

# COMMAND ----------

target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"

# COMMAND ----------

# fact_servicio.display()

# COMMAND ----------

(fact_servicio.
    write.format('delta').
    partitionBy('ANIO_MES_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Fechas

# COMMAND ----------

#Dimensiones
table = "dim_fechas"
dim_fechas = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))     

# COMMAND ----------

dim_fechas = (dim_fechas.withColumn("ANIO_1", col("ANIO"))
.withColumn("ANIO_MES", to_date("ANIO_MES", "yyyy-MM-dd"))
.withColumn("ANIO_MES_YM",
                   concat(year("ANIO_MES"), lit("-"), 
                          lpad(month("ANIO_MES").cast("string"), 2, "0")))
.drop("ANIO_MES")
         .withColumnRenamed("ANIO_MES_YM", "ANIO_MES")
)

# COMMAND ----------

table = "dim_fechas"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos gold
(dim_fechas.
    write.format('delta').
    partitionBy('ANIO_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC #Presupuesto por parte 2024
# MAGIC

# COMMAND ----------

table = "PPTO_VIGENTE"
PPTO_VIGENTE = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/marketing/{table}.parquet/"))     

# COMMAND ----------

PPTO_VIGENTE.columns

# COMMAND ----------

PPTO_VIGENTE.groupBy("ANIO_MES","TYPE_1","ID_CANAL_DISTRIBUCION").agg(sum("PPTO")).display()

# COMMAND ----------

table="PPTO_VIGENTE"

target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"


#Escritura de los archivos gold_ marketing
(PPTO_VIGENTE.
    write.format('delta').
    partitionBy('ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC #Productos

# COMMAND ----------

#Dimensiones
table = "dim_productos_material"
dim_productos = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))

# COMMAND ----------

dim_productos.display()

# COMMAND ----------

filtro_productos = filtro_productos.union(filtron_inventario)
filtro_productos = filtro_productos.distinct()

# COMMAND ----------

dim_productos = dim_productos.join(filtro_productos,
                                   on = ['ID_MATERIAL','ID_CANAL_DISTRIBUCION'],
                                    how = 'inner')

# COMMAND ----------

dim_productos = (
    dim_productos
    .withColumn("ID_CANAL_DISTRIBUCION_1", col("ID_CANAL_DISTRIBUCION"))
    .withColumn("ID_PRD", concat(col("ID_MATERIAL"), lit("-"), col("ID_CANAL_DISTRIBUCION"))))

# COMMAND ----------

dim_productos.columns

# COMMAND ----------

table = "catalogo_nvas_aplica"
catalogo_nvas_aplica = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/marketing/{table}.parquet/"))

display(catalogo_nvas_aplica)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# Definir la ventana
window_spec = Window.partitionBy("ID_PRD").orderBy("ID_PRD")

# Añadir la columna 'row_number' para asignar números de fila dentro de cada grupo de 'ID_MATERIAL'
df_with_row_num = catalogo_nvas_aplica.withColumn("row_number", row_number().over(window_spec))

# Filtrar para conservar solo la primera fila de cada grupo de 'ID_MATERIAL'
catalogo_nvas_aplica_distinct = df_with_row_num.filter(col("row_number") == 1).drop("row_number")

catalogo_nvas_aplica_distinct.display()

# COMMAND ----------

# Unir los DataFrames
dim_productos = dim_productos.unionByName(catalogo_nvas_aplica_distinct, allowMissingColumns=True)

display(dim_productos.filter(col('ID_MATERIAL') == 'Nvas Aplica'))


# COMMAND ----------

table= "dim_productos"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos gold
(dim_productos. 
    write.format('delta').
    partitionBy('ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Pronóstico

# COMMAND ----------

table = "prono_mkt_memo"
historico_prono_memo = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/gold/{table}.parquet/"))

# COMMAND ----------

historico_prono_memo = historico_prono_memo.fillna(0)

# COMMAND ----------

historico_prono_memo.display()

# COMMAND ----------

historico_prono_memo = historico_prono_memo.groupBy('MATERIAL',
 'ID_CANAL_DISTRIBUCION',
 col('CONCEPTO').alias('Tipo'),
 col('DATE').alias('ANIO_MES'),
 col('datestr').alias('MES_PRONO')).agg(sum('value').alias('VALUE'))

# COMMAND ----------

table = "prono_mkt_mi"
historico_prono_mi = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/gold/{table}.parquet/"))

# COMMAND ----------

historico_prono_mi.display()

# COMMAND ----------

historico_prono_mi.filter(col("MATERIAL")=="OW-1399-ZV").select("usuario","datestr").distinct().display()

# COMMAND ----------

historico_prono_mi = historico_prono_mi.fillna(0)

# COMMAND ----------

historico_prono_mi = historico_prono_mi.groupBy('MATERIAL',
 'ID_CANAL_DISTRIBUCION',
 col('CONCEPTO').alias('Tipo'),
 col('DATE').alias('ANIO_MES'),
 col('datestr').alias('MES_PRONO')).agg(sum('value').alias('VALUE'))

# COMMAND ----------

historico_prono_mi.display()

# COMMAND ----------

historico_prono_memo = historico_prono_memo.withColumn("MES_PRONO", historico_prono_memo["MES_PRONO"].cast("string"))

display(
  spark.createDataFrame(
    [
      (
        col,
        historico_prono_memo.schema[col].dataType.simpleString() if col in historico_prono_memo.columns else None,
        historico_prono_mi.schema[col].dataType.simpleString() if col in historico_prono_mi.columns else None
      )
      for col in set(historico_prono_memo.columns).union(set(historico_prono_mi.columns))
    ],
    ['columna', 'tipo_historico_prono_memo', 'tipo_historico_prono_mi']
  )
)

pronostico = historico_prono_memo.unionByName(historico_prono_mi, allowMissingColumns=True)

# COMMAND ----------

display(pronostico.filter(pronostico.ID_CANAL_DISTRIBUCION == 'MI'))

# COMMAND ----------

pronostico = pronostico.join(pronostico.groupBy('MATERIAL', 'ID_CANAL_DISTRIBUCION', 'Tipo', 'ANIO_MES').agg(max('MES_PRONO').alias('MES_PRONO')),on=['MATERIAL', 'ID_CANAL_DISTRIBUCION', 'Tipo', 'ANIO_MES','MES_PRONO'], how = 'inner')

# COMMAND ----------

pronostico = pronostico.withColumn("ID_PRD",concat(col("MATERIAL"),lit("-"),"ID_CANAL_DISTRIBUCION"))


# COMMAND ----------

pronostico = pronostico.withColumn('VALUE', when(col('Tipo')=="IMP_PRONO",pronostico['VALUE']*1000).otherwise(col('VALUE')))
pronostico.limit(25).display()

# COMMAND ----------

pronostico.groupBy("ANIO_MES","MES_PRONO","Tipo").agg(sum("VALUE")).display()

# COMMAND ----------

# Crear la columna TYPE_1 basada en el contenido de la columna Tipo
pronostico = pronostico.withColumn("TYPE_1", 
                    when(col("Tipo").contains("U"), "PIEZAS")
                    .when(col("Tipo").contains("IMP"), "IMPORTE")
                    .otherwise(None)).drop("Tipo")

pronostico.limit(10).display()

# COMMAND ----------

# Crear la columna MDO_1
pronostico = (pronostico.withColumn("ID_CANAL_DISTRIBUCION_1", pronostico["ID_CANAL_DISTRIBUCION"])
              .withColumn("MES_PRONO", date_format("MES_PRONO", "yyyy-MM"))
              .withColumn("ANIO_MES", date_format("ANIO_MES", "yyyy-MM")))

# COMMAND ----------

pronostico.limit(100).display()

# COMMAND ----------

aux_1 = pronostico.groupby('ANIO_MES').agg(max('MES_PRONO').alias('MES_PRONO'))
aux_1.display()

from pyspark.sql.functions import col

def schema_comparison(df1, df2, name1="df1", name2="df2"):
    schema1 = [(f.name, f.dataType.simpleString()) for f in df1.schema.fields]
    schema2 = [(f.name, f.dataType.simpleString()) for f in df2.schema.fields]
    comp = []
    for col1, type1 in schema1:
        type2 = dict(schema2).get(col1, None)
        comp.append((col1, type1, type2 if type2 else "Not in " + name2))
    for col2, type2 in schema2:
        if col2 not in dict(schema1):
            comp.append((col2, "Not in " + name1, type2))
    return spark.createDataFrame(comp, [f"Column", f"{name1}_type", f"{name2}_type"])

display(schema_comparison(aux_1, pronostico, "aux_1", "pronostico"))

# COMMAND ----------

pronostico = pronostico.join(
    pronostico.groupby('ANIO_MES').agg(max('MES_PRONO').alias('MES_PRONO')),
    on=['ANIO_MES', 'MES_PRONO'],
    how='inner'
)

# COMMAND ----------

pronostico.groupBy('MES_PRONO','ANIO_MES','TYPE_1','ID_CANAL_DISTRIBUCION').agg(sum("VALUE")).display()


# COMMAND ----------

table="PRONO"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"


#Escritura de los archivos gold_ marketing
(pronostico.
    write.format('delta').
    partitionBy('ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Lista de Precios

# COMMAND ----------

table = "LISTA_DE_PRECIOS_MI"
precios = (spark
              .read
              .parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/marketing/{table}.parquet/")
              .withColumnRenamed("CDIS", "ID_CANAL_DISTRIBUCION")
              .withColumnRenamed("MATERIAL", "ID_MATERIAL")
              .withColumn('ID_MATERIAL',regexp_replace(col('ID_MATERIAL'), r'^[0]*', ''))
              .withColumn("ID_PRD", concat(col("ID_MATERIAL"), lit("-"), col("ID_CANAL_DISTRIBUCION")))
              .withColumn("ANIO_MES", substring("FECHA_INICIO_VALIDEZ", 1, 7))
             )

precios.limit(25).display()

# COMMAND ----------

precios = precios.select(['ID_PRD', 'ANIO_MES', 'IMPORTE_DE_CONDICION', 'MONEDA_CONDICION', 'UM_DE_PRECIO', 'ID_CANAL_DISTRIBUCION'])

precios.limit(25).display()

# COMMAND ----------

table="LISTA_DE_PRECIOS_MI"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"


#Escritura de los archivos gold_ marketing
(precios.
    write.format('delta').
    mode("overwrite").
    parquet(target_path))