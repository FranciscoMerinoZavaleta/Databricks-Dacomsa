# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Includes

# COMMAND ----------

# MAGIC %run /Shared/FASE1_INTEGRACION/includes

# COMMAND ----------

# MAGIC %md
# MAGIC # Ventas_Pedidos
# MAGIC

# COMMAND ----------

#Llamamos tablas de pedidos. Dentro de BO, esta es la tabla que usa ventas y pedidos
#Importe facturado = Venta Neta
#Piezas facturada = Importe Piezas
table = "fact_pedidos"
fact_ventas_pedidos = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))      

# COMMAND ----------

#fact_ventas_pedidos.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### REVISION DE LA CAIDA CORRECTA DEL DATO

# COMMAND ----------

cols_numericas = [f.name for f in fact_ventas_pedidos.schema.fields if str(f.dataType) in ['IntegerType', 'LongType', 'DoubleType', 'FloatType', 'DecimalType', 'ShortType'] and f.name != 'FECHA_REGISTRO']

(
    fact_ventas_pedidos
    .groupBy("FECHA_REGISTRO")
    .sum(*cols_numericas)
    .orderBy(col("FECHA_REGISTRO").desc())
    .display()
)

# COMMAND ----------

from pyspark.sql.functions import year, month, col

cols_numericas = [f.name for f in fact_ventas_pedidos.schema.fields if str(f.dataType) in ['IntegerType', 'LongType', 'DoubleType', 'FloatType', 'DecimalType', 'ShortType'] and f.name != 'FECHA_REGISTRO']

(
    fact_ventas_pedidos
    .withColumn("ANIO", year(col("FECHA_REGISTRO")))
    .withColumn("MES", month(col("FECHA_REGISTRO")))
    .groupBy("ANIO", "MES")
    .sum(*cols_numericas)
    .orderBy(col("ANIO").desc(), col("MES").desc())
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #join

# COMMAND ----------

#Llamamos la tabla de productos
table = "dim_material"
dim_productos= (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))  

# COMMAND ----------

#Como la informacion de ventas es a nivel linea, se hace join de las ventas_pedidos con los productos
fact_ventas_pedidos_productos = (fact_ventas_pedidos.
    join (dim_productos,
          on = ['ID_MATERIAL','ID_CANAL_DISTRIBUCION'],
          how = 'inner'))
                         

# COMMAND ----------

#Agrupamos la tabla a nivel lineas 

fact_ventas_pedidos = (
    fact_ventas_pedidos_productos.groupBy(
        'ANIO_MES',
        'ANIO_FECHA_REGISTRO',
        'MES_FECHA_REGISTRO',
        "ID_CANAL_DISTRIBUCION",
        "ID_CLIENTE",
        "ID_SISTEMA",
        "ID_LINEA_ABIERTA",
        "LINEA",
        "LINEA_AGRUPADA",
        "CLASE"
    )
    .agg(
        sum("IMPORTE_FACTURADA").alias("VENTA_IMPORTE"),
        sum("IMPORTE_FACTURADA_DOLARES").alias("VENTA_IMPORTE_DOLARES"),
        sum("PIEZAS_FACTURADA").alias("VENTA_UNIDADES"),
        sum("PIEZAS").alias("PIEZAS"),
        sum("PIEZAS_RECHAZADA").alias("PIEZAS_RECHAZADA"),
        sum("PIEZAS_BACKORDER").alias("PIEZAS_BACKORDER"),
        sum("IMPORTE").alias("IMPORTE"),
        sum("IMPORTE_RECHAZADA").alias("IMPORTE_RECHAZADA"),
        sum("IMPORTE_BACKORDER").alias("IMPORTE_BACKORDER"),
        sum("IMPORTE_DOLARES").alias("IMPORTE_DOLARES"),
        sum("IMPORTE_RECHAZADA_DOLARES").alias("IMPORTE_RECHAZADA_DOLARES"),
        sum("IMPORTE_BACKORDER_DOLARES").alias("IMPORTE_BACKORDER_DOLARES")
    )
)

# COMMAND ----------

#Se agregan los campos llaves para la lectura en synapse y las llaves para unir con el catálogo de productos y clientes
import pyspark.sql.functions as F
fact_ventas_pedidos = (
    fact_ventas_pedidos
    .withColumn("ANIO_MES_1", col("ANIO_MES"))  # Agregar una nueva columna "ANIO_MES_1" con los valores de "ANIO_MES" 
    .withColumn("ID_CANAL_DISTRIBUCION_1", col("ID_CANAL_DISTRIBUCION"))  # Agregar una nueva columna "ID_CANAL_DISTRIBUCION_1" con los valores de "ID_CANAL_DISTRIBUCION"
    .withColumn("ID_PRD", concat(col("ID_SISTEMA"), lit("-"),col("ID_LINEA_ABIERTA"), lit("-"), col("CLASE"), lit("-"),col("ID_CANAL_DISTRIBUCION")))
    .withColumn("ID_CTE", concat(col("ID_CLIENTE"), lit("-"), col("ID_CANAL_DISTRIBUCION")))  # Agregar una nueva columna "ID_PRD" concatenando "ID_MATERIAL" y "ID_CANAL_DISTRIBUCION" separados por un guion "-"
)

# COMMAND ----------

#Cambiamos la fecha de registro con la fecha de factura
fact_ventas_pedidos = (
    fact_ventas_pedidos
    .withColumnRenamed("ANIO_FECHA_REGISTRO", "ANIO_FECHA_FACTURA")  # Agregar una nueva columna "ANIO_MES_1" con los valores de "ANIO_MES" 
    .withColumnRenamed("MES_FECHA_REGISTRO","MES_FECHA_FACTURA"))

# COMMAND ----------

table = "fact_ventas_pedidos"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos gold
(fact_ventas_pedidos.
    write.format('delta').
    partitionBy('ANIO_MES_1','ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# fact_ventas_pedidos.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Transpose

# COMMAND ----------

#Seleccionamos los campos a transponer y agrupar
columns_to_transpose = [
        "VENTA_UNIDADES",
        "VENTA_IMPORTE",
        "VENTA_IMPORTE_DOLARES",
        "PIEZAS",
        "PIEZAS_RECHAZADA",
        "PIEZAS_BACKORDER",
        "IMPORTE",
        "IMPORTE_RECHAZADA",
        "IMPORTE_BACKORDER",
        "IMPORTE_DOLARES",
        "IMPORTE_RECHAZADA_DOLARES",
        "IMPORTE_BACKORDER_DOLARES"]
        
columns_to_group_by = [
    'ANIO_MES',
        'ANIO_FECHA_FACTURA',
        'MES_FECHA_FACTURA',
        "ID_CANAL_DISTRIBUCION",
        "ID_CLIENTE",
        "ID_SISTEMA",
        "ID_LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "CLASE",
        "ANIO_MES_1",
        "ID_CANAL_DISTRIBUCION_1",
        "ID_PRD",
        "ID_CTE"
        ]   

# COMMAND ----------

#Se transponen los datos
fact_ventas_pedidos_trans = fact_ventas_pedidos.melt(
    ids=[columns_to_group_by], values=[columns_to_transpose],
    variableColumnName="MEASURE", valueColumnName="VALUE"
)


# COMMAND ----------

#Se agrupan las metricas por tipo
type = {
        "VENTA_UNIDADES":"VENTA",
        "VENTA_IMPORTE":"VENTA",
        "VENTA_IMPORTE_DOLARES":"VENTA",
        "PIEZAS":"PEDIDOS",
        "PIEZAS_RECHAZADA":"RECHAZADO",
        "PIEZAS_BACKORDER":"BACKORDER",
        "IMPORTE":"PEDIDOS",
        "IMPORTE_DOLARES":"PEDIDOS",
        "IMPORTE_RECHAZADA":"RECHAZADO",
        "IMPORTE_RECHAZADA_DOLARES":"RECHAZADO",
        "IMPORTE_BACKORDER":"BACKORDER",
        "IMPORTE_BACKORDER_DOLARES":"BACKORDER"
            }

# COMMAND ----------

#Se agrupan las metricas por pieza o importe

measures = {
        "VENTA_UNIDADES":"PIEZAS",
        "VENTA_IMPORTE":"IMPORTE",
        "VENTA_IMPORTE_DOLARES":"IMPORTE",
        "PIEZAS":"PIEZAS",
        "PIEZAS_RECHAZADA":"PIEZAS",
        "PIEZAS_BACKORDER":"PIEZAS",
        "IMPORTE":"IMPORTE",
        "IMPORTE_DOLARES":"IMPORTE",
        "IMPORTE_RECHAZADA":"IMPORTE",
        "IMPORTE_RECHAZADA_DOLARES":"IMPORTE",
        "IMPORTE_BACKORDER":"IMPORTE",
        "IMPORTE_BACKORDER_DOLARES":"IMPORTE"
            }

# COMMAND ----------

currency = {
        "VENTA_UNIDADES":"PIEZAS",
        "VENTA_IMPORTE":"MX",
        "VENTA_IMPORTE_DOLARES":"USD",
        "PIEZAS":"PIEZAS",
        "PIEZAS_RECHAZADA":"PIEZAS",
        "PIEZAS_BACKORDER":"PIEZAS",
        "IMPORTE":"MX",
        "IMPORTE_DOLARES":"USD",
        "IMPORTE_RECHAZADA":"MX",
        "IMPORTE_RECHAZADA_DOLARES":"USD",
        "IMPORTE_BACKORDER":"MX",
        "IMPORTE_BACKORDER_DOLARES":"USD"
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

#Se genera el data frame

currency= [(MEASURE, TYPE_3) for MEASURE, TYPE_3 in currency.items()]
# Crear el DataFrame de Spark a partir de la lista de tuplas
currency= spark.createDataFrame(currency, ['MEASURE', 'TYPE_3'])

# COMMAND ----------

#Join del dataframe
type_measures_currency = type.join(
                                        measures,
                                        on = ['MEASURE'],
                                        how = "inner").join(currency, on = ['MEASURE'],
                                        how = "inner")

# COMMAND ----------

#Se unen las metricas por tipo
fact_ventas_pedidos_trans= fact_ventas_pedidos_trans.join(
                                        type_measures_currency,
                                        on = ['MEASURE'],
                                        how = "inner")

# COMMAND ----------

#Se hace la cross tab
fact_ventas_pedidos_trans = fact_ventas_pedidos_trans.groupBy(   'ANIO_MES',
        'ANIO_FECHA_FACTURA',
        'MES_FECHA_FACTURA',
        "ID_CANAL_DISTRIBUCION",
        "ID_CLIENTE",
        "ID_SISTEMA",
        "ID_LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "CLASE",
        "ANIO_MES_1",
        "ID_CANAL_DISTRIBUCION_1",
        "ID_PRD",
        "ID_CTE",
        "TYPE_1",
        "TYPE_3").pivot("TYPE_2").agg({"VALUE": "sum"})

# COMMAND ----------

# fact_ventas_pedidos_trans.groupby("ANIO_MES","TYPE_1").agg(sum("VENTA")).display()

# COMMAND ----------

table = "fact_ventas_pedidos_trans"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"


# COMMAND ----------

#Escritura de los archivos gold
(fact_ventas_pedidos_trans.
    write.format('delta').
    partitionBy('ANIO_MES_1','ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC #PPTO

# COMMAND ----------

#Hechos
table = "dim_ppto_cliente"
dim_ppto_clientes = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))    


# COMMAND ----------

dim_ppto_clientes = dim_ppto_clientes.withColumnRenamed("LINEA_AGRUPADA","LINEA")

# COMMAND ----------

# clientes_nuevos = dim_ppto_clientes.select('ID_GRUPO_CLIENTE',
#  'GRUPO_CLIENTE',
#  'SHIP_TO',
#  'ID_CLIENTE',
#  'RAZON_SOCIAL',
#  'ESTADO',
#  'ID_ASESOR',
#  'REGION',
#  'ASESOR',
#  'ID_CANAL_DISTRIBUCION').filter(col("GRUPO_CLIENTE").contains("ME Nuevos clientes")).distinct()

# COMMAND ----------

dim_ppto_clientes = dim_ppto_clientes.select('ID_CLIENTE',
 'LINEA',
 'ID_CANAL_DISTRIBUCION',
 'FECHA_PPTO',
 'MES_FECHA_PPTO',
 'ANIO_FECHA_PPTO',
 'IMPORTE_PPTO',
 'PIEZAS_PPTO',
 'ANIO_MES',
 'SISTEMA',
 'INDICE'
).filter(col("ANIO_FECHA_PPTO")>=2018)

# COMMAND ----------

dim_ppto_clientes = dim_ppto_clientes.groupby('ID_CLIENTE',
 'LINEA',
 'ID_CANAL_DISTRIBUCION',
 'FECHA_PPTO',
 'MES_FECHA_PPTO',
 'ANIO_FECHA_PPTO',
 'ANIO_MES',
 'SISTEMA',
 'INDICE'
).agg(sum('IMPORTE_PPTO').alias("IMPORTE_PPTO"),
 sum('PIEZAS_PPTO').alias("PIEZAS_PPTO"))

# COMMAND ----------

dim_ppto_clientes = dim_ppto_clientes \
    .withColumn("LINEA", trim(col("LINEA"))) \
    .withColumn("SISTEMA", trim(col("SISTEMA"))) \
    .withColumn("ID_CANAL_DISTRIBUCION", trim(col("ID_CANAL_DISTRIBUCION")))

# COMMAND ----------

dim_productos_ppto = dim_productos.select("ID_CANAL_DISTRIBUCION","SISTEMA","LINEA","CLASE").distinct()

# COMMAND ----------

dim_productos_ppto = dim_productos_ppto\
    .withColumn("LINEA", trim(col("LINEA"))) \
    .withColumn("SISTEMA", trim(col("SISTEMA"))) \
    .withColumn("ID_CANAL_DISTRIBUCION", trim(col("ID_CANAL_DISTRIBUCION")))

# COMMAND ----------

dim_ppto_clientes = dim_ppto_clientes.join(dim_productos_ppto,
                       on = ["ID_CANAL_DISTRIBUCION","SISTEMA","LINEA"],
                       how = "left")

# COMMAND ----------

# Assuming dim_ppto_clase is a DataFrame already loaded in your Spark session.
clases = dim_ppto_clientes.groupBy("ANIO_MES","ID_CANAL_DISTRIBUCION", "SISTEMA", "LINEA") \
                       .agg(countDistinct("CLASE").alias("DISTINCT_CLASES"))

# COMMAND ----------

dim_ppto_clientes = dim_ppto_clientes.join(clases,
                       on = ["ANIO_MES","ID_CANAL_DISTRIBUCION","SISTEMA","LINEA"],
                       how = "inner")

# COMMAND ----------

dim_ppto_clientes = dim_ppto_clientes.withColumn(
    "IMPORTE_PPTO",
    when(col("DISTINCT_CLASES") == 0, col("IMPORTE_PPTO"))
     .otherwise(col("IMPORTE_PPTO") / col("DISTINCT_CLASES"))
).withColumn(
    "PIEZAS_PPTO",
    when(col("DISTINCT_CLASES") == 0, col("PIEZAS_PPTO"))
     .otherwise(col("PIEZAS_PPTO") / col("DISTINCT_CLASES"))
)



# COMMAND ----------

dim_ppto_clientes = dim_ppto_clientes.drop("DISTINCT_CLASES")

# COMMAND ----------

dim_ppto_clientes = (
   dim_ppto_clientes
    .withColumn("ANIO_MES_1", col("ANIO_MES"))  # Agregar una nueva columna "ANIO_MES_1" con los valores de "ANIO_MES" 
    .withColumn("ID_CANAL_DISTRIBUCION_1", col("ID_CANAL_DISTRIBUCION"))  # Agregar una nueva columna "ID_CANAL_DISTRIBUCION_1" con los valores de "ID_CANAL_DISTRIBUCION"
    .withColumn("ID_PRD_PPTO", concat(col("SISTEMA"), lit("-"),col("LINEA"), lit("-"), col("CLASE"), lit("-"), col("ID_CANAL_DISTRIBUCION")))
    .withColumn("ID_CTE", concat(col("ID_CLIENTE"), lit("-"), col("ID_CANAL_DISTRIBUCION"))))  # Agregar una nueva columna "ID_PRD" concatenando "ID_MATERIAL" y "ID_CANAL_DISTRIBUCION" separados por un guion "-"


# COMMAND ----------

fact_ventas_pedidos_ppto = (
    fact_ventas_pedidos_productos.groupBy(
        'ANIO_MES',
        'ANIO_FECHA_REGISTRO',
        'MES_FECHA_REGISTRO',
        "ID_CANAL_DISTRIBUCION",
        "ID_CLIENTE",
        "ID_SISTEMA",
        "SISTEMA",
        "LINEA",
        "CLASE"
    )
    .agg(
        sum("IMPORTE_FACTURADA").alias("VENTA_IMPORTE"),
        sum("PIEZAS_FACTURADA").alias("VENTA_UNIDADES"),
        sum("PIEZAS").alias("PIEZAS"),
        sum("PIEZAS_RECHAZADA").alias("PIEZAS_RECHAZADA"),
        sum("PIEZAS_BACKORDER").alias("PIEZAS_BACKORDER"),
        sum("IMPORTE").alias("IMPORTE"),
        sum("IMPORTE_RECHAZADA").alias("IMPORTE_RECHAZADA"),
        sum("IMPORTE_BACKORDER").alias("IMPORTE_BACKORDER"),
        sum("IMPORTE_FACTURADA_DOLARES").alias("VENTA_IMPORTE_DOLARES"),
        sum("IMPORTE_DOLARES").alias("IMPORTE_DOLARES"),
        sum("IMPORTE_RECHAZADA_DOLARES").alias("IMPORTE_RECHAZADA_DOLARES"),
        sum("IMPORTE_BACKORDER_DOLARES").alias("IMPORTE_BACKORDER_DOLARES")))

# COMMAND ----------

fact_ventas_pedidos_ppto= (
fact_ventas_pedidos_ppto
    .withColumn("ANIO_MES_1", col("ANIO_MES"))  # Agregar una nueva columna "ANIO_MES_1" con los valores de "ANIO_MES" 
    .withColumn("ID_CANAL_DISTRIBUCION_1", col("ID_CANAL_DISTRIBUCION"))  # Agregar una nueva columna "ID_CANAL_DISTRIBUCION_1" con los valores de "ID_CANAL_DISTRIBUCION"
    .withColumn("ID_PRD_PPTO", concat(col("SISTEMA"), lit("-"),col("LINEA"), F.lit("-"),col("CLASE"), lit("-"),col("ID_CANAL_DISTRIBUCION")))
    .withColumn("ID_CTE", concat(col("ID_CLIENTE"), lit("-"), col("ID_CANAL_DISTRIBUCION"))))  # Agregar una nueva columna "ID_PRD" concatenando "ID_MATERIAL" y "ID_CANAL_DISTRIBUCION" separados por un guion "-"


# COMMAND ----------

dim_ppto_clientes = fact_ventas_pedidos_ppto.join(
                                      dim_ppto_clientes,
                                        on = ['ANIO_MES',
 'ID_CANAL_DISTRIBUCION',
 'ID_CLIENTE',
 'SISTEMA',
 'LINEA',
 'CLASE',
 'ANIO_MES_1',
 'ID_CANAL_DISTRIBUCION_1',
 'ID_PRD_PPTO',
 'ID_CTE'],
                                        how = 'outer')


# COMMAND ----------

dim_ppto_clientes = dim_ppto_clientes.withColumn("IMPORTE_PPTO_DOLARES",when(~(col("ID_CANAL_DISTRIBUCION")=="MI"),col("IMPORTE_PPTO")/20).otherwise(0))

# COMMAND ----------

lineas = dim_productos.select(("LINEA"),"LINEA_AGRUPADA").distinct()
# lineas.display()

# COMMAND ----------

#dim_ppto_clientes.display()

# COMMAND ----------

lineas = lineas.withColumn("LINEA",trim(col("LINEA")))

# COMMAND ----------

dim_ppto_clientes = dim_ppto_clientes.join(lineas,
                       on = ["LINEA"],
                       how = "left")

# COMMAND ----------

dim_ppto_clientes = dim_ppto_clientes.withColumn(
    "SISTEMA",
    when(~col('SISTEMA').isin(["MOTOR", "FRENOS","TREN MOTRIZ"]), "OTROS").otherwise(col("SISTEMA"))
)


# COMMAND ----------

table = "dim_ppto_clientes"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos gold
(dim_ppto_clientes.
    write.format('delta').
    partitionBy('ANIO_MES_1','ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

columns_to_transpose = ["VENTA_UNIDADES",
        "VENTA_IMPORTE",
        "VENTA_IMPORTE_DOLARES",
        "PIEZAS",
        "PIEZAS_RECHAZADA",
        "PIEZAS_BACKORDER",
        "IMPORTE",
        "IMPORTE_DOLARES",
        "IMPORTE_RECHAZADA",
        "IMPORTE_RECHAZADA_DOLARES",
        "IMPORTE_BACKORDER",
        "IMPORTE_BACKORDER_DOLARES",
        "IMPORTE_PPTO",
        "IMPORTE_PPTO_DOLARES",
        "PIEZAS_PPTO"]
        
columns_to_group_by = [
                        'ANIO_MES',
 'ID_CANAL_DISTRIBUCION',
 'ID_CLIENTE',
 'SISTEMA',
 'LINEA',
 'LINEA_AGRUPADA',
 'CLASE',
 'ID_SISTEMA',
 'FECHA_PPTO',
 'MES_FECHA_PPTO',
 'ANIO_FECHA_PPTO',
 'ANIO_MES_1',
 'ID_CANAL_DISTRIBUCION_1',
 'ID_PRD_PPTO',
 'ID_CTE',
 'INDICE'
        ]   

# COMMAND ----------

dim_ppto_clientes_trans = dim_ppto_clientes.melt(
    ids=[columns_to_group_by], values=[columns_to_transpose],
    variableColumnName="MEASURE", valueColumnName="VALUE"
)

# COMMAND ----------

measures = {"VENTA_UNIDADES":"PIEZAS",
        "VENTA_IMPORTE":"IMPORTE",
        "VENTA_IMPORTE_DOLARES":"IMPORTE",
        "PIEZAS":"PIEZAS",
        "PIEZAS_RECHAZADA":"PIEZAS",
        "PIEZAS_BACKORDER":"PIEZAS",
        "IMPORTE":"IMPORTE",
        "IMPORTE_DOLARES":"IMPORTE",
        "IMPORTE_RECHAZADA":"IMPORTE",
        "IMPORTE_RECHAZADA_DOLARES":"IMPORTE",
        "IMPORTE_BACKORDER":"IMPORTE",
        "IMPORTE_BACKORDER_DOLARES":"IMPORTE",
        "IMPORTE_PPTO":"IMPORTE",
        "IMPORTE_PPTO_DOLARES":"IMPORTE",
        "PIEZAS_PPTO":"PIEZAS"
            }

# COMMAND ----------

type = {
        "VENTA_UNIDADES":"VENTA",
        "VENTA_IMPORTE":"VENTA",
        "VENTA_IMPORTE_DOLARES":"VENTA",
        "PIEZAS":"PEDIDOS",
        "PIEZAS_RECHAZADA":"RECHAZADO",
        "PIEZAS_BACKORDER":"BACKORDER",
        "IMPORTE":"PEDIDOS",
        "IMPORTE_DOLARES":"PEDIDOS",
        "IMPORTE_RECHAZADA":"RECHAZADO",
        "IMPORTE_RECHAZADA_DOLARES":"RECHAZADO",
        "IMPORTE_BACKORDER":"BACKORDER",
        "IMPORTE_BACKORDER_DOLARES":"BACKORDER",
        "IMPORTE_PPTO":"PPTO",
        "IMPORTE_PPTO_DOLARES":"PPTO",
        "PIEZAS_PPTO":"PPTO"
            }

# COMMAND ----------

currency = {"VENTA_UNIDADES":"PIEZAS",
        "VENTA_IMPORTE":"MX",
        "VENTA_IMPORTE_DOLARES":"USD",
        "PIEZAS":"PIEZAS",
        "PIEZAS_RECHAZADA":"PIEZAS",
        "PIEZAS_BACKORDER":"PIEZAS",
        "IMPORTE":"MX",
        "IMPORTE_DOLARES":"USD",
        "IMPORTE_RECHAZADA":"MX",
        "IMPORTE_RECHAZADA_DOLARES":"USD",
        "IMPORTE_BACKORDER":"MX",
        "IMPORTE_BACKORDER_DOLARES":"USD",
        "IMPORTE_PPTO":"MX",
        "IMPORTE_PPTO_DOLARES":"USD",
        "PIEZAS_PPTO":"PIEZAS"
            }

# COMMAND ----------

measures= [(MEASURE, TYPE_1) for MEASURE, TYPE_1 in measures.items()]

# Crear el DataFrame de Spark a partir de la lista de tuplas
measures= spark.createDataFrame(measures, ['MEASURE', 'TYPE_1'])

type= [(MEASURE, TYPE_2) for MEASURE, TYPE_2 in type.items()]

# Crear el DataFrame de Spark a partir de la lista de tuplas
type= spark.createDataFrame(type, ['MEASURE', 'TYPE_2'])

currency= [(MEASURE, TYPE_3) for MEASURE, TYPE_3 in currency.items()]

# Crear el DataFrame de Spark a partir de la lista de tuplas
currency= spark.createDataFrame(currency, ['MEASURE', 'TYPE_3'])

type_measures_currency= type.join(
                                        measures,
                                        on = ['MEASURE'],
                                        how = "inner").join( currency,
                                        on = ['MEASURE'],
                                        how = "inner")


# COMMAND ----------

dim_ppto_clientes_trans= dim_ppto_clientes_trans.join(
                                        type_measures_currency,
                                        on = ['MEASURE'],
                                        how = "inner")

# COMMAND ----------

dim_ppto_clientes_trans = dim_ppto_clientes_trans.groupBy(  'ANIO_MES',
 'ID_CANAL_DISTRIBUCION',
 'ID_CLIENTE',
 'SISTEMA',
 'LINEA_AGRUPADA',
 'LINEA',
 'CLASE',
 'ID_SISTEMA',
 'FECHA_PPTO',
 'MES_FECHA_PPTO',
 'ANIO_FECHA_PPTO',
 'ANIO_MES_1',
 'ID_CANAL_DISTRIBUCION_1',
 'ID_PRD_PPTO',
 'ID_CTE',
 'INDICE',
 'TYPE_3',
"TYPE_1").pivot("TYPE_2").agg({"VALUE": "sum"})


# COMMAND ----------

# borrar genaro, tema de duplicados
dim_ppto_clientes_trans = dim_ppto_clientes_trans.dropDuplicates()

# COMMAND ----------

table = "dim_ppto_clientes_trans"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos gold
(dim_ppto_clientes_trans.
    write.format('delta').
    partitionBy('ANIO_MES_1','ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

dim_productos_pv = (dim_productos.select("ID_SISTEMA","SISTEMA","ID_LINEA_ABIERTA","LINEA","LINEA_AGRUPADA","CLASE","ID_CANAL_DISTRIBUCION").distinct())

# COMMAND ----------

dim_productos_pv.count()

# COMMAND ----------

dim_productos_pv = dim_productos_pv.withColumn(
    "SISTEMA",
    when(~col('SISTEMA').isin(["MOTOR", "FRENOS","TREN MOTRIZ"]), "OTROS").otherwise(col("SISTEMA"))
)


# COMMAND ----------

filtro_fact_ventas_pedidos = dim_ppto_clientes.select('ID_CANAL_DISTRIBUCION',
 'SISTEMA',
 'LINEA',
 'CLASE').distinct()

# COMMAND ----------

dim_productos_pv = dim_productos_pv.join(filtro_fact_ventas_pedidos,
                                         on = ['ID_CANAL_DISTRIBUCION',
 'SISTEMA',
 'LINEA',
 'CLASE'],
                                         how = 'inner')

# COMMAND ----------

dim_productos_pv = (
   dim_productos_pv
    .withColumn("ID_CANAL_DISTRIBUCION_1", col("ID_CANAL_DISTRIBUCION"))  # Agregar una nueva columna "ID_CANAL_DISTRIBUCION_1" 
    .withColumn("ID_PRD", concat(col("SISTEMA"), lit("-"),col("LINEA"), lit("-"), col("CLASE"), lit("-"),F.col("ID_CANAL_DISTRIBUCION")))
    .withColumn("ID_PRD_PPTO", concat(col("SISTEMA"), lit("-"),col("LINEA"), lit("-"),col("ID_CANAL_DISTRIBUCION")))
)

# COMMAND ----------

table = "dim_productos_pv"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos gold
(dim_productos_pv.
    write.format('delta').
    partitionBy('ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

#Llamamos la tabla 
table = "dim_clientes"
dim_clientes= (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))  

# COMMAND ----------

dim_clientes.count()

# COMMAND ----------

dim_clientes = (
   dim_clientes
.withColumn("ID_CANAL_DISTRIBUCION_1", col("ID_CANAL_DISTRIBUCION"))  # Agregar una nueva columna "ID_CANAL_DISTRIBUCION_1" 
.withColumn("ID_CTE", concat(col("ID_CLIENTE"), lit("-"), col("ID_CANAL_DISTRIBUCION"))))
            # Agregar una nueva columna "ID_PRD" concatenando "ID_MATERIAL" y "ID_CANAL_DISTRIBUCION" separados por un guion "-"

# COMMAND ----------

filtro_fact_ventas_pedidos = dim_ppto_clientes.select('ID_CTE','ID_CANAL_DISTRIBUCION',
 'ID_CLIENTE').distinct()

# COMMAND ----------

filtro_dim_ppto_clientes = dim_ppto_clientes.select('ID_CTE','ID_CANAL_DISTRIBUCION',
 'ID_CLIENTE').distinct()

# COMMAND ----------

filtro_clientes = filtro_fact_ventas_pedidos.union(filtro_dim_ppto_clientes)
filtro_clientes = filtro_clientes.distinct()

# COMMAND ----------

dim_clientes = (dim_clientes.join(filtro_clientes,
                                 on = ['ID_CLIENTE','ID_CANAL_DISTRIBUCION', 'ID_CTE'],
                                 how = 'inner'))

# COMMAND ----------

# dim_clientes = dim_clientes.unionByName(clientes_nuevos, allowMissingColumns=True)

# COMMAND ----------

dim_clientes = (
   dim_clientes
.withColumn("ID_CANAL_DISTRIBUCION_1", col("ID_CANAL_DISTRIBUCION"))  # Agregar una nueva columna "ID_CANAL_DISTRIBUCION_1" 
.withColumn("ID_CTE", concat(F.col("ID_CLIENTE"), lit("-"), col("ID_CANAL_DISTRIBUCION"))))
            # Agregar una nueva columna "ID_PRD" concatenando "ID_MATERIAL" y "ID_CANAL_DISTRIBUCION" separados por un guion "-"

# COMMAND ----------

table = "dim_clientes"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos gold
(dim_clientes.
    write.format('delta').
    partitionBy('ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

from datetime import datetime

current_month = str(datetime.now().month)
current_year = str(datetime.now().year)

if len(current_month) == 1:
    current_month = "0" + current_month
else:
    currency = current_month  

current = current_year + "-" + current_month

(
    dim_ppto_clientes
    .filter(
    (col("ANIO_MES") == current)
    )
    .dropDuplicates()
    .toPandas()    
    .to_csv("/dbfs/mnt/adls-dac-data-bi-pr-scus/diario_de_ventas/diario_de_ventas.csv", index=False)
)