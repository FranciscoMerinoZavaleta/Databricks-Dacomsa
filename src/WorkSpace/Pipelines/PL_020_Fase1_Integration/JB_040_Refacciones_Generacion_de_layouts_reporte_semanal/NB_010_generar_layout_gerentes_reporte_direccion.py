# Databricks notebook source
# MAGIC %run /Shared/FASE1_INTEGRACION/includes

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month

# COMMAND ----------

# MAGIC %pip install openpyxl --upgrade

# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.window import Window

# COMMAND ----------

fact_pedidos_diarios = (
    spark.read.format("parquet")
    .load(
        "/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/fact_pedidos_diarios.parquet"
    )
    .withColumn("IMPORTE_FACTURADA", col("IMPORTE_FACTURADA").cast("double"))
    .groupBy(
        "DIA_FECHA_REGISTRO",
        "MES_FECHA_REGISTRO",
        "ANIO_FECHA_REGISTRO",
        "ID_CLIENTE",
        "ANIO_MES",
    )
    .agg(
        sum("IMPORTE_FACTURADA").alias("IMPORTE_FACTURADA"),
        sum("IMPORTE_RECHAZADA").alias("IMPORTE_RECHAZADA"),
        sum("IMPORTE_BACKORDER").alias("IMPORTE_BACKORDER"),
        sum("IMPORTE").alias("IMPORTE"),
    )
    .withColumn(
        "fecha",
        concat(
            col("ANIO_FECHA_REGISTRO"),
            lit("-"),
            col("MES_FECHA_REGISTRO"),
            lit("-"),
            col("DIA_FECHA_REGISTRO"),
        ),
    )
    .withColumn("fecha", to_date(col("fecha")))
    .withColumn("week", weekofyear("fecha"))
    .filter(col("fecha") >= add_months(current_date(), -1))
)

# COMMAND ----------

# fact_pedidos_diarios = fact_pedidos_diarios.filter(col("fecha")<'2024-08-26')

# COMMAND ----------

combinacion_clientes = fact_pedidos_diarios.select('ID_CLIENTE').distinct()
combinacion_clientes = combinacion_clientes.withColumn("flag",lit(1))

# COMMAND ----------

#combinacion_clientes.where(col("ID_CLIENTE") == "000195I004").display()

# COMMAND ----------

max_weeks = (
    fact_pedidos_diarios.groupBy("week")
    .agg(max("fecha").alias("fecha"))
    .withColumn("flag", lit(1))
)
max_months = (
    fact_pedidos_diarios.groupBy( "MES_FECHA_REGISTRO")
    .agg(max("fecha").alias("fecha"))
    .withColumn("flag_month", lit(1))
)
max_weeks_clientes= combinacion_clientes.join(max_weeks, on = combinacion_clientes.flag == max_weeks.flag, how = 'inner')



# COMMAND ----------

#max_weeks_clientes.filter(((col("ID_CLIENTE") == "000195I004"))).display()

# COMMAND ----------

fact_pedidos_diarios = max_weeks_clientes.join(fact_pedidos_diarios, on = ['ID_CLIENTE','week','fecha'], how = 'left').fillna(0)

# COMMAND ----------

fact_pedidos_diarios= fact_pedidos_diarios.join(max_months, on = ['fecha','MES_FECHA_REGISTRO'], how = 'left')

# COMMAND ----------

fact_pedidos_diarios = fact_pedidos_diarios.withColumn("ANIO_FECHA_REGISTRO", year(col("fecha")))\
                                           .withColumn("MES_FECHA_REGISTRO", month(col("fecha")))


# COMMAND ----------

max_weeks = (
    fact_pedidos_diarios.groupBy( "week")
    .agg(max("fecha").alias("fecha"))
    .withColumn("flag_week", lit(1))
)


# COMMAND ----------

max_weeks = (
    fact_pedidos_diarios.groupBy( "week")
    .agg(max("fecha").alias("fecha"))
    .withColumn("flag_week", lit(1))
)


fact_pedidos_diarios = fact_pedidos_diarios.join(
    max_weeks, on=["week", "fecha"], how="inner"
)

dim_clientes = (
    spark.read.format("parquet")
    .load("/mnt/adls-dac-data-bi-pr-scus/gold/ventas/dim_clientes.parquet")
    .select("REGION", "ASESOR", "GRUPO_CLIENTE", "ID_CLIENTE","ID_GRUPO_CLIENTE","ID_ASESOR","ID_REGION")
    .filter(~(col("REGION") == "INACTIVOS"))
)

dim_ppto_clientes = (spark.read.format("parquet").load(
    "/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/dim_ppto_cliente.parquet"))
                     #.join(dim_clientes, on = ['ID_CLIENTE','ID_ASESOR','REGION'], how = "left"))

dim_ppto_clientes = (
    dim_ppto_clientes.groupBy("ANIO_MES", "ID_CLIENTE")
    .agg(sum("IMPORTE_PPTO").alias("PPTO"))
    .withColumnRenamed("ANIO_MES", "key")
    .withColumn("key", regexp_replace("key", "-0", "-"))
    #.withColumnRenamed("ID_GRUPO_CLIENTE", "ID_CLIENTE")
    .dropDuplicates()
)

responsable_linea_vtas = (
    spark.createDataFrame(
        pd.read_excel(
            "/dbfs/mnt/adls-dac-data-bi-pr-scus/powerappslandingzone_cat_reg_asesor/Catálogo Región-Asesor comercial.xlsx"
        )
    )
    .withColumnRenamed("Email Región", "correo_gerente")
    .withColumnRenamed("E-Mail", "correo_asesor")
    .withColumnRenamed("Región", "gerente")
    .drop("Mercado", "Cliente", "Línea", "Unnamed: 8")
    .dropna()
    .withColumn(
        "to_delete", concat(col("correo_gerente"), col("correo_asesor"), col("grupo"))
    )
    .dropDuplicates(["to_delete"])
    .drop("to_delete")
)

roster_gerentes = (
    responsable_linea_vtas.select("correo_gerente", "gerente")
    .withColumnRenamed("gerente", "REGION")
    .distinct()
)

roster_asesores = (
    responsable_linea_vtas.select("correo_asesor", "asesor")
    .withColumnRenamed("asesor", "ASESOR")
    .distinct()
)

# table = "DIM_CLIENTE_VENTAS"
# dim_clientes = (spark.
#                         read.
#                         parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))
# dim_clientes = (dim_clientes.
#                     select("ID_MANDANTE","ID_ORGANIZACION_VENTAS","ID_CANAL_DISTRIBUCION","ID_CLIENTE","NOMBRE_1","CIUDAD","ID_OFICINA_VENTAS","ID_ZONA_VENTAS","ID_PAIS","ID_ESTADO","ID_GRUPO_VENTAS","ID_CLIENTE_AGRUPACION","DES_CLIENTE_AGRUPACION","NUM_CLIENTE"
#                             ).
#                      filter((dim_clientes["ID_ORGANIZACION_VENTAS"] == "A717") & 
#                         (dim_clientes["SEQ_DATE"] == dim_clientes.select(max("SEQ_DATE")).collect()[0][0])).
#                      distinct())

# COMMAND ----------

#dim_ppto_clientes.display()

# COMMAND ----------

#dim_ppto_clientes.where(col("ID_CLIENTE") == "001569I002").display()

# COMMAND ----------

#fact_pedidos_diarios.filter(col('ANIO_FECHA_REGISTRO').isNull()).display()

# COMMAND ----------

#(fact_pedidos_diarios.join(fact_pedidos_diarios.groupBy("week").agg(max("fecha").alias("fecha")), on =['week','fecha'], how = 'inner')).groupBy("week").agg(sum("IMPORTE_FACTURADA")).display()

# COMMAND ----------

ww = Window.partitionBy("ID_CLIENTE").orderBy(
    col("week").asc(), col("ANIO_FECHA_REGISTRO").asc()
)

fact_pedidos_semanal = (
    fact_pedidos_diarios.filter(col("flag_week") == 1)
    .withColumnRenamed("IMPORTE_FACTURADA", "Venta")
    .withColumn(
        "Venta semana anterior",
        when(
            col("MES_FECHA_REGISTRO") > lag("MES_FECHA_REGISTRO", 1).over(ww),
            0
        ).otherwise(
            when(
                col("Venta").isNotNull() & (col("Venta") != 0),
                col("Venta") - lag("Venta", 1).over(ww)
            ).otherwise(col("Venta"))
        )
    )
    .withColumn(
        "key", concat(col("ANIO_FECHA_REGISTRO"), lit("-"), col("MES_FECHA_REGISTRO"))
    )
)

max_fecha = (
    fact_pedidos_semanal.groupBy("ID_CLIENTE")
    .agg(max("fecha").alias("fecha"))
    .withColumn("flag_max_week", lit(1))
)

fact_pedidos_semanal = fact_pedidos_semanal.join(
    max_fecha, on=["ID_CLIENTE", "fecha"], how="left"
).filter(col("flag_max_week") == 1)

fact_pedidos_semanal = fact_pedidos_semanal.drop(
    *[c for c in fact_pedidos_semanal.columns if "flag" in c]
).select("key", "ID_CLIENTE", "Venta semana anterior", "week")

# fact_pedidos_semanal.display()

# COMMAND ----------

www = Window.partitionBy("ID_CLIENTE").orderBy(col("ANIO_MES").asc())

fact_pedidos_mensuales = (
    fact_pedidos_diarios.filter(col("flag_month") == 1)
    .withColumn("ANIO_MES", concat(col("ANIO_MES"), lit("-01")))
    .withColumn("ANIO_MES", to_date("ANIO_MES"))
    .groupBy("ANIO_MES", "ID_CLIENTE")
    .agg(
        sum("IMPORTE_FACTURADA").alias("Venta cum Mes en Curso"),
        sum("IMPORTE").alias("Pedido bruto cum Mes en Curso"),
        (sum("IMPORTE") - sum("IMPORTE_RECHAZADA")).alias(
            "Pedido neto cum Mes en Curso"
        ),
        sum("IMPORTE_BACKORDER").alias("Backorder cum Mes en Curso"),
    )
    .withColumn("Prono (escalera)", lit(None))
    .withColumn("Estimado Semana Actual", lit(None))
    .withColumn("Estimado Mes Probable", lit(None))
    .withColumn("Comentarios", lit("None"))
    .withColumn("key", concat(year(col("ANIO_MES")), lit("-"), month(col("ANIO_MES"))))
    .join(fact_pedidos_semanal, on=["key", "ID_CLIENTE"], how="left")
    .filter(col("week").isNotNull())
)

# COMMAND ----------

#fact_pedidos_mensuales.filter(col("week").isNull()).display()

# COMMAND ----------

# dim_ppto_clientes.where(col("ID_CLIENTE") == "000195I004").display()

# COMMAND ----------

fact_pedidos_mensuales = (
    fact_pedidos_mensuales
    .join(dim_clientes, on="ID_CLIENTE", how="outer")
    .withColumn("key", lit(f"{current_year}-{current_month}"))
    .withColumn("ANIO_MES",lit(f"{current_year}-{current_month:02d}-01"))
    .withColumn("ANIO_MES", col("ANIO_MES").cast("date"))
    .join(dim_ppto_clientes, on=["key", "ID_CLIENTE"], how="left")
    .join(roster_gerentes, on = "REGION", how="left") ## Ojo con el join la llave es string
    .join(roster_asesores, on = "ASESOR", how="left") ## Ojo con el join la llave es string
    .withColumn("Mes", concat(year(col("ANIO_MES")), lit("-"), month(col("ANIO_MES"))))
    .withColumnRenamed("week", "Semana")
    .withColumnRenamed("REGION", "Gerente")
    .withColumnRenamed("correo_gerente", "Correo Gerente")
    .withColumnRenamed("ASESOR", "Asesor")
    .withColumnRenamed("correo_asesor", "Correo Asesor")
    .withColumnRenamed("GRUPO_CLIENTE", "Grupo")
    .withColumnRenamed("PPTO", "Ppto Mes en Curso")
    .groupBy("Mes", "Semana", "Gerente", "Correo Gerente", "Asesor", "Correo Asesor", "Grupo")
    .agg(
        sum("Venta Semana Anterior").alias("Venta Semana Anterior"),
        sum("Estimado Semana Actual").alias("Estimado Semana Actual"),
        sum("Venta cum Mes en Curso").alias("Venta cum Mes en Curso"),
        sum("Pedido bruto cum Mes en Curso").alias("Pedido bruto cum Mes en Curso"),
        sum("Pedido neto cum Mes en Curso").alias("Pedido neto cum Mes en Curso"),
        sum("Backorder cum Mes en Curso").alias("Backorder cum Mes en Curso"),
        sum("Ppto Mes en Curso").alias("Ppto Mes en Curso")
    )
    .withColumn("Pred Escalera", lit(None))
    .withColumn("Estimado Mes Probable", lit(None))
    .withColumn(
        "Ped_Net / Ppto (%)", (col("Pedido neto cum Mes en Curso") / col("Ppto Mes en Curso")) * 100
    )
    .select(
        "Mes", #a
        "Semana", #a
        "Gerente", #a
        "Correo Gerente", #a
        "Asesor", #a
        "Correo Asesor", #a
        "Grupo", #a
        "Venta Semana Anterior", #s 
        "Estimado Semana Actual", #s
        "Venta cum Mes en Curso", #s
        "Pedido bruto cum Mes en Curso", #s
        "Pedido neto cum Mes en Curso", #s
        "Backorder cum Mes en Curso", #s
        "Ppto Mes en Curso", #s
        "Pred Escalera", #s
        "Ped_Net / Ppto (%)", #s
        "Estimado Mes Probable", #s
    )
    .filter(col("Correo Gerente").isNotNull())
    .filter(col("Gerente").isNotNull())
    .filter(col("Correo Asesor").isNotNull())
    .filter(col("Asesor").isNotNull())
)

# fact_pedidos_mensuales.display()

# COMMAND ----------

# week = fact_pedidos_mensuales.agg(max('Semana')).collect()[0][0]

# COMMAND ----------

# fact_pedidos_mensuales = fact_pedidos_mensuales.withColumn("Semana",lit(week))

# COMMAND ----------

fact_pedidos_mensuales = fact_pedidos_mensuales.withColumn(
    'Venta Semana Anterior',
    when(
        (col('Venta Semana Anterior') == 0) | 
        (col('Venta Semana Anterior').isNull()) | 
        (trim(col('Venta Semana Anterior')) == ""),
        col('Venta cum Mes en Curso')
    ).otherwise(col('Venta Semana Anterior'))
)

# COMMAND ----------

fact_pedidos_mensuales = fact_pedidos_mensuales.withColumn(
    'Semana',
    when(
        col('Semana').isNull() | (trim(col('Semana')) == ""),
        first('Semana', ignorenulls=True).over(Window.partitionBy())
    ).otherwise(col('Semana'))
).withColumn(
    'Mes',
    when(
        col('Mes').isNull() | (trim(col('Mes')) == ""),
        first('Mes', ignorenulls=True).over(Window.partitionBy())
    ).otherwise(col('Mes'))
).filter(
    col("Ppto Mes en Curso").isNotNull()
)

# COMMAND ----------

fact_pedidos_mensuales = (fact_pedidos_mensuales.groupBy('Mes',
 'Semana',
 'Gerente',
 'Correo Gerente',
 'Asesor',
 'Correo Asesor',
 'Grupo',
 ).agg(
    sum("Venta Semana Anterior").alias("Venta Semana Anterior"),
    sum("Estimado Semana Actual").alias("Estimado Semana Actual"),
    sum("Venta cum Mes en Curso").alias("Venta cum Mes en Curso"),
    sum("Pedido bruto cum Mes en Curso").alias("Pedido bruto cum Mes en Curso"),
    sum("Pedido neto cum Mes en Curso").alias("Pedido neto cum Mes en Curso"),
    sum("Backorder cum Mes en Curso").alias("Backorder cum Mes en Curso"),
    sum("Ppto Mes en Curso").alias("Ppto Mes en Curso"),
    sum("Pred Escalera").alias("Pred Escalera"),
    sum("Ped_Net / Ppto (%)").alias("Ped_Net / Ppto (%)"),
    sum("Estimado Mes Probable").alias("Estimado Mes Probable"))
.where(col("Correo Asesor") != "NaN"))

# COMMAND ----------

#fact_pedidos_mensuales.display()

# COMMAND ----------

#fact_pedidos_mensuales.groupBy('Semana').agg(
#        sum("Venta Semana Anterior").alias("Venta Semana Anterior"),
#        sum("Estimado Semana Actual").alias("Estimado Semana Actual"),
#        sum("Venta cum Mes en Curso").alias("Venta cum Mes en Curso"),
#        sum("Pedido bruto cum Mes en Curso").alias("Pedido bruto cum Mes en Curso"),
#        sum("Pedido neto cum Mes en Curso").alias("Pedido neto cum Mes en Curso"),
#        sum("Backorder cum Mes en Curso").alias("Backorder cum Mes en Curso"),
#        sum("Ppto Mes en Curso").alias("Ppto Mes en Curso")).display()

# COMMAND ----------

fact_pedidos_mensuales = fact_pedidos_mensuales.withColumn(
    "Pred Escalera", 
    col("Pred Escalera").cast(StringType())
).withColumn(
    "Estimado Mes Probable",
    col("Estimado Mes Probable").cast(StringType())
)


# COMMAND ----------

table = "fact_pedidos_mensuales"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos gold
(fact_pedidos_mensuales.
    write.format('delta').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

import datetime

emails_asesores = fact_pedidos_mensuales.select("Correo Asesor").distinct().rdd.flatMap(lambda x: x).collect()
emails_gerentes = fact_pedidos_mensuales.select("Correo Gerente").distinct().rdd.flatMap(lambda x: x).collect()
semana = fact_pedidos_mensuales.select("Semana").agg(max("semana").alias("Semana")).select("Semana").rdd.flatMap(lambda x: x).collect()[0]
semana = str(semana)
today = datetime.date.today()
year = str(today.year)

# COMMAND ----------

#fact_pedidos_mensuales.filter(col("Semana").isNull()).display()

# COMMAND ----------

fact_pedidos_mensuales.display()

# COMMAND ----------

paths = []
emails = []

for mail in emails_asesores:
    
    data = (
        fact_pedidos_mensuales
        .filter(col("Correo Asesor") == mail)
        .where(~col("Correo Asesor").isin(["jose.rcarmona@kuoafmkt.com", "pedro.contreras@kuoafmkt.com"]))
    )

    email = mail.replace("@kuoafmkt.com", "")
    email = email.replace(".", "_")
    excel_path = "/tmp/reporte_semanal_" + email + "_" + year + "-" + semana + ".xlsx"
    df_pandas = data.toPandas()
    writer = pd.ExcelWriter(excel_path, engine='openpyxl')
    df_pandas.to_excel(writer, index=False)
    writer.save()
    path_ = "/mnt/adls-dac-data-bi-pr-scus/reporte_semanal/reporte_semanal_" + email + "_" + year + "-" + semana + ".xlsx"
    dbutils.fs.cp(f"file:{excel_path}", path_)
    paths.append(path_)
    emails.append(mail)

# COMMAND ----------


for mail in emails_gerentes:
    
    data = (
        fact_pedidos_mensuales
        .filter(col("Correo Gerente") == mail)
        .dropDuplicates()
    )

    email = mail.replace("@kuoafmkt.com", "")
    email = email.replace(".", "_")
    excel_path = "/tmp/reporte_semanal_" + email + "_" + year + "-" + semana + ".xlsx"
    df_pandas = data.toPandas()
    writer = pd.ExcelWriter(excel_path, engine='openpyxl')
    df_pandas.to_excel(writer, index=False)
    writer.save()
    path_ = "/mnt/adls-dac-data-bi-pr-scus/reporte_semanal/reporte_semanal_" + email + "_" + year + "-" + semana + ".xlsx"
    dbutils.fs.cp(f"file:{excel_path}", path_)
    paths.append(path_)
    emails.append(mail)

# COMMAND ----------

paths

# COMMAND ----------

paths_reporte = (
    spark
    .createDataFrame(zip(emails,paths), schema = ["mail", "path"])
    .withColumn("path", regexp_replace("path","/mnt/", ""))
)

paths_reporte.display()

# COMMAND ----------

paths_reporte.toPandas().to_json("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/dim_paths_reporte_semanal.json", orient = "records")