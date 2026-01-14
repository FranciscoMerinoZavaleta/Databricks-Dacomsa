# Databricks notebook source
# MAGIC %md
# MAGIC # Includes

# COMMAND ----------

# MAGIC %run /Workspace/Users/fmzchskinny@gmail.com/.bundle/databricks-dacomsa/prod/files/src/WorkSpace/Pipelines/PL_010_Fase1/includes

# COMMAND ----------

# MAGIC %md
# MAGIC # Ventas_Pedidos
# MAGIC

# COMMAND ----------

#Llamamos tablas de pedidos. Dentro de BO, esta es la tabla que usa ventas y pedidos
#Importe facturado = Venta Neta
#Piezas facturada = Importe Piezas
table = "fact_pedidos_programados"
fact_ventas_pedidos = (spark.
                     read.
                     parquet(f"/mnt/adlsabastosprscus/abastos/silver/SQL/tablas_SQL/{table}.parquet/"))      

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

fact_ventas_pedidos.columns

# COMMAND ----------

#Agrupamos la tabla a nivel lineas 

fact_ventas_pedidos = (
    fact_ventas_pedidos_productos.groupBy(
        'ANIO_MES',
        'ANIO_FECHA_REGISTRO',
        'MES_FECHA_REGISTRO',
        'FECHA_REGISTRO',
        'FECHA_COMPROMISO',
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
        'FECHA_REGISTRO',
        'FECHA_COMPROMISO',
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
        'FECHA_REGISTRO',
        'FECHA_COMPROMISO',
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

table = "pedidos_programados"
target_path = f"/mnt/adlsabastosprscus/abastos/gold/planeacion/{table}.parquet/"


# COMMAND ----------

#Escritura de los archivos gold
(fact_ventas_pedidos_trans.
    write.format('delta').
    partitionBy('ANIO_MES_1','ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))