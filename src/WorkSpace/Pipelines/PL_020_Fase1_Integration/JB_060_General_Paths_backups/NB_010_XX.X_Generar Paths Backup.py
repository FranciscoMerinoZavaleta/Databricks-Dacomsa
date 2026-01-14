# Databricks notebook source
# MAGIC %run /Shared/FASE1_INTEGRACION/includes

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.enabled", "false")

# COMMAND ----------

backup_mimome = (
        spark.createDataFrame(dbutils.fs.ls("/mnt/adls-dac-data-bi-pr-scus/mimome_raw_backup"))
        .withColumn("Documento", lit("MIMOME"))
)

backup_prono = (
        spark.createDataFrame(dbutils.fs.ls("/mnt/adls-dac-data-bi-pr-scus/prono_raw_backup"))
        .withColumn("Documento", lit("Prono Escalera"))
)

backup_reporte_semanal = (
        spark.createDataFrame(dbutils.fs.ls("/mnt/adls-dac-data-bi-pr-scus/reporte_semanal_raw_backup"))
        .withColumn("Documento", lit("Reporte Semanal"))
)
keys = ["name","team"]
particion = (Window.partitionBy(keys).orderBy(desc('fecha_carga')))
backup = (
        backup_mimome
        .union(backup_prono)
        .union(backup_reporte_semanal)
        .select("Documento", "name", "path", "modificationTime")
        .withColumn("modificationTime", from_unixtime(col("modificationTime") / 1000).cast("timestamp"))
        .withColumnRenamed("modificationTime", "fecha_carga")
        .withColumn("path", regexp_replace("path","dbfs:/mnt/", ""))
        .withColumn(
                "team",
                when(col("path").contains("mkt_"), lit("mkt"))
                .otherwise(lit("vtas"))
        )
        .withColumn("ROW_NUM", row_number().over(particion))
        .filter((col('ROW_NUM')==1) | (col('ROW_NUM')==2))
        .drop("ROW_NUM")
        .orderBy(col("Documento"), col("fecha_carga").desc())
        .withColumn("team",when((col("Documento") == "Prono Escalera") & (col("name").contains("memo")), lit("memo")).otherwise(col("team")))
)

#backup.display()

# COMMAND ----------

backup.display()

# COMMAND ----------

schema = StructType([
    StructField("path", StringType(), True),
    StructField("name", StringType(), True),
    StructField("size", StringType(), True),
    StructField("fecha_carga", StringType(), True),  # Assuming fecha_carga is a string. Change the type if necessary.
    StructField("Documento", StringType(), True)
])

# Use the defined schema when creating the DataFrame
dummy = sqlContext.createDataFrame([], schema)

try:
    landing_zone_rs = (
        spark.createDataFrame(dbutils.fs.ls("/mnt/adls-dac-data-bi-pr-scus/powerappslandingzone_"))
        .withColumn("Documento", lit("Reporte Semanal"))
        .withColumnRenamed("modificationTime", "fecha_carga")
    )
except:
    print("no hay objetos en reporte directivo")
    landing_zone_rs = dummy

try:
    landing_zone_prono = (
        spark.createDataFrame(dbutils.fs.ls("/mnt/adls-dac-data-bi-pr-scus/powerappslandingzone"))
        .withColumn("Documento", lit("Prono"))
        .withColumnRenamed("modificationTime", "fecha_carga")
    )
except:
    print("no hay objetos en prono escalera")
    landing_zone_prono = dummy

try:
    landing_zone_mimome = (
    spark.createDataFrame(dbutils.fs.ls("/mnt/adls-dac-data-bi-pr-scus/powerappslandingzone_mimome"))
    .withColumn("Documento", lit("MIMOME"))
    .withColumnRenamed("modificationTime", "fecha_carga")
    )
except:
    print("no hay objetos en mimome")
    landing_zone_mimome = dummy

landing_zone = (
    landing_zone_mimome
    .union(landing_zone_prono)
    .union(landing_zone_rs)
    .select("Documento", "name", "path", "fecha_carga")
    .withColumn("fecha_carga", from_unixtime(col("fecha_carga") / 1000).cast("timestamp"))
    .withColumn("path", regexp_replace("path","dbfs:/mnt/", ""))
    .withColumn(
        "team",
        when(col("path").contains("mkt_"), lit("mkt"))
        .otherwise(lit("vtas"))
    )
    .orderBy(col("Documento"), col("fecha_carga").desc())
)

# COMMAND ----------

landing_zone.display()

# COMMAND ----------

backup.toPandas().to_json("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/dim_paths_backup.json", orient = "records")

# COMMAND ----------

landing_zone.toPandas().to_json("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/landing_zone_objects.json", orient = "records")

# COMMAND ----------

dbutils.notebook.exit("Proceso Concluido")