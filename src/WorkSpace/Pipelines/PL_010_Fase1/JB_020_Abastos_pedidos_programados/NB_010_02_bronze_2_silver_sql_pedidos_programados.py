# Databricks notebook source
# MAGIC %md
# MAGIC # INCLUDES

# COMMAND ----------

# MAGIC %run /Workspace/Users/fmzchskinny@gmail.com/.bundle/databricks-dacomsa/prod/files/src/WorkSpace/Pipelines/PL_010_Fase1/includes

# COMMAND ----------

# MAGIC %md
# MAGIC # Extracción Pedidos

# COMMAND ----------

#Hechos

table = "FACT_PEDIDOS_ESTATUS"
fact_pedidos = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))

fact_pedidos = fact_pedidos.withColumn("rank", rank().over(Window.partitionBy("FECHA_REGISTRO").orderBy(col("SEQ_DATE").desc()))).filter(col("rank") == 1).drop("rank")
   
fact_pedidos = (fact_pedidos.
                     select("ID_MANDANTE","FECHA_REGISTRO","PIEZAS","ID_CLIENTE_SOLICITANTE","ID_MATERIAL","ID_CLASE_DOCUMENTO_VENTA","ID_CANAL_DISTRIBUCION","FECHA_COMPROMISO","FECHA_PEDIDO","PIEZAS_FACTURADA","PIEZAS_RECHAZADA","PIEZAS_BACKORDER","IMPORTE_FACTURADA","IMPORTE_RECHAZADA","IMPORTE_BACKORDER","IMPORTE","TIPO_CAMBIO_DPRECIOS" 
                            ).
                     filter((fact_pedidos["ID_MANDANTE"] == "504") & 
                            ((fact_pedidos["ID_CANAL_DISTRIBUCION"] == "MI") | 
                             (fact_pedidos["ID_CANAL_DISTRIBUCION"] == "ME") | 
                             (fact_pedidos["ID_CANAL_DISTRIBUCION"] == "MO") 
                             ) &
                            ((fact_pedidos["ID_MATERIAL"] != "ALLOWANCES")
                              | (fact_pedidos["ID_MATERIAL"] != "VENTA_INTERCOMPAÑI" ))
                            )
                            )
fact_pedidos = fact_pedidos.withColumnRenamed("ID_CLIENTE_SOLICITANTE","ID_CLIENTE")

# COMMAND ----------

fact_pedidos = fact_pedidos \
    .withColumn("IMPORTE_FACTURADA_DOLARES",
                when(col("ID_CANAL_DISTRIBUCION") != 'MI',
                     col("IMPORTE_FACTURADA") / col("TIPO_CAMBIO_DPRECIOS")).otherwise(0)) \
    .withColumn("IMPORTE_RECHAZADA_DOLARES",
                when(col("ID_CANAL_DISTRIBUCION") != 'MI',
                     col("IMPORTE_RECHAZADA") / col("TIPO_CAMBIO_DPRECIOS")).otherwise(0)) \
    .withColumn("IMPORTE_BACKORDER_DOLARES",
                when(col("ID_CANAL_DISTRIBUCION") != 'MI',
                     col("IMPORTE_BACKORDER") / col("TIPO_CAMBIO_DPRECIOS")).otherwise(0)) \
    .withColumn("IMPORTE_DOLARES",
                when(col("ID_CANAL_DISTRIBUCION") != 'MI',
                     col("IMPORTE") / col("TIPO_CAMBIO_DPRECIOS")).otherwise(0))

# COMMAND ----------

# fact_pedidos.display()

# COMMAND ----------

# fact_pedidos.groupBy("FECHA_REGISTRO").agg(sum("IMPORTE_FACTURADA")).display()

# COMMAND ----------

fact_pedidos = (fact_pedidos.groupBy("FECHA_REGISTRO",month("FECHA_REGISTRO").alias("MES_FECHA_REGISTRO"),
    year("FECHA_REGISTRO").alias("ANIO_FECHA_REGISTRO"),
    month("FECHA_PEDIDO").alias("MES_FECHA_PEDIDO"),
    year("FECHA_PEDIDO").alias("ANIO_FECHA_PEDIDO"),
    "ID_CANAL_DISTRIBUCION",
    "ID_CLIENTE",
    "ID_MATERIAL",
    "ID_CLASE_DOCUMENTO_VENTA",
    "FECHA_COMPROMISO",
    "FECHA_REGISTRO",
    "FECHA_PEDIDO"
).agg(
    sum("PIEZAS").alias('PIEZAS'),
    sum("PIEZAS_FACTURADA").alias('PIEZAS_FACTURADA'),
    sum("PIEZAS_RECHAZADA").alias('PIEZAS_RECHAZADA'),
    sum("PIEZAS_BACKORDER").alias('PIEZAS_BACKORDER'),
    sum("IMPORTE_FACTURADA").alias('IMPORTE_FACTURADA'),
    sum("IMPORTE_RECHAZADA").alias('IMPORTE_RECHAZADA'),
    sum("IMPORTE_BACKORDER").alias('IMPORTE_BACKORDER'),
    sum("IMPORTE").alias('IMPORTE'),
    sum("IMPORTE_FACTURADA_DOLARES").alias('IMPORTE_FACTURADA_DOLARES'),
    sum("IMPORTE_RECHAZADA_DOLARES").alias('IMPORTE_RECHAZADA_DOLARES'),
    sum("IMPORTE_BACKORDER_DOLARES").alias('IMPORTE_BACKORDER_DOLARES'),
    sum("IMPORTE_DOLARES").alias('IMPORTE_DOLARES')
    
))

# COMMAND ----------

import pyspark.sql.functions as F
fact_pedidos = (fact_pedidos.withColumn("ANIO_MES", when(length("MES_FECHA_REGISTRO")==2,concat(col("ANIO_FECHA_REGISTRO"), lit("-"), col("MES_FECHA_REGISTRO"))).otherwise(concat(col("ANIO_FECHA_REGISTRO"), lit("-0"), col("MES_FECHA_REGISTRO")))))


# COMMAND ----------

fact_pedidos = fact_pedidos.withColumn("ANIO_MES_PEDIDO", when(length("MES_FECHA_PEDIDO")==2,concat(col("ANIO_FECHA_PEDIDO"), lit("-"), col("MES_FECHA_PEDIDO"))).otherwise(concat(col("ANIO_FECHA_PEDIDO"), lit("-0"), col("MES_FECHA_PEDIDO"))))


# COMMAND ----------

fact_pedidos = fact_pedidos.withColumn(
    "IMPORTE_BACKORDER",
    when(col("FECHA_COMPROMISO") > col("FECHA_REGISTRO"), 0)
    .otherwise(col("IMPORTE_BACKORDER"))
).withColumn(
    "PIEZAS_BACKORDER",
    when(col("FECHA_COMPROMISO") > col("FECHA_REGISTRO"), 0)
    .otherwise(col("PIEZAS_BACKORDER"))
).withColumn(
    "IMPORTE",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("IMPORTE"))
    .otherwise(0)
).withColumn(
    "PIEZAS",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("PIEZAS"))
    .otherwise(0)
).withColumn(
    "IMPORTE_RECHAZADA",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("IMPORTE_RECHAZADA"))
    .otherwise(0)
).withColumn(
    "PIEZAS_RECHAZADA",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("PIEZAS_RECHAZADA"))
    .otherwise(0)
).withColumn(
    "IMPORTE_BACKORDER_DOLARES",
    when(col("FECHA_COMPROMISO") > col("FECHA_REGISTRO"), 0)
    .otherwise(col("IMPORTE_BACKORDER_DOLARES"))
).withColumn(
    "IMPORTE_DOLARES",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("IMPORTE_DOLARES"))
    .otherwise(0)
).withColumn(
    "IMPORTE_RECHAZADA_DOLARES",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("IMPORTE_RECHAZADA_DOLARES"))
    .otherwise(0))


# COMMAND ----------

display(fact_pedidos.limit(5))

# COMMAND ----------

fact_pedidos = (fact_pedidos.groupBy("ANIO_MES","FECHA_REGISTRO","MES_FECHA_REGISTRO","ANIO_FECHA_REGISTRO","FECHA_COMPROMISO",
    "ID_CANAL_DISTRIBUCION",
    "ID_CLIENTE",
    "ID_MATERIAL"
).agg(
    sum("PIEZAS").alias('PIEZAS'),
    sum("PIEZAS_FACTURADA").alias('PIEZAS_FACTURADA'),
    sum("PIEZAS_RECHAZADA").alias('PIEZAS_RECHAZADA'),
    sum("PIEZAS_BACKORDER").alias('PIEZAS_BACKORDER'),
    sum("IMPORTE_FACTURADA").alias('IMPORTE_FACTURADA'),
    sum("IMPORTE_RECHAZADA").alias('IMPORTE_RECHAZADA'),
    sum("IMPORTE_BACKORDER").alias('IMPORTE_BACKORDER'),
    sum("IMPORTE").alias('IMPORTE'),
    sum("IMPORTE_FACTURADA_DOLARES").alias('IMPORTE_FACTURADA_DOLARES'),
    sum("IMPORTE_RECHAZADA_DOLARES").alias('IMPORTE_RECHAZADA_DOLARES'),
    sum("IMPORTE_BACKORDER_DOLARES").alias('IMPORTE_BACKORDER_DOLARES'),
    sum("IMPORTE_DOLARES").alias('IMPORTE_DOLARES'),
))

# COMMAND ----------

fact_pedidos = (fact_pedidos.withColumn('ID_MATERIAL',regexp_replace(col('ID_MATERIAL'), r'^[0]*', ''))
                       )

# COMMAND ----------

# fact_pedidos_dolares = (fact_pedidos_dolares.withColumn('ID_MATERIAL',regexp_replace(col('ID_MATERIAL'), r'^[0]*', ''))
#                        )

# COMMAND ----------

agrupador_fact_pedidos = fact_pedidos.groupBy("ANIO_MES").agg(max("FECHA_REGISTRO").alias("FECHA_REGISTRO"))
# agrupador_fact_pedidos.display()

# COMMAND ----------

# agrupador_fact_pedidos.display()

# COMMAND ----------

fact_pedidos = fact_pedidos.join(agrupador_fact_pedidos,
                  on = ["ANIO_MES","FECHA_REGISTRO"],
                  how = "inner")

# COMMAND ----------

table = "fact_pedidos_programados"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/SQL/tablas_SQL/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos silver
(fact_pedidos.
    write.format('delta').
    partitionBy('ANIO_MES','ID_CANAL_DISTRIBUCION').
    mode("overwrite").
    parquet(target_path))