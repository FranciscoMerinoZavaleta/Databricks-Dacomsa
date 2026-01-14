



current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month
current_day = current_date.day
filter_date = f"{current_year}-{current_month}-{current_day}T00:00:00.000+00:00"




table = "EKKO"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
ekko = (spark.
       read.
       parquet(source_path).withColumnRenamed("SOCIEDAD","CENTRO")
       )




ekko = ekko.withColumn(
    "CENTRO",  # Sobrescribe la columna 'CENTRO'
    when(
        (col("ORGANIZACION_COMPRAS").isin("M010", "M020")),
        "A744"  # Caso 1: Si ORGANIZACION_COMPRAS es M010 o M020 y CENTRO es A744, asigna A744
    ).when(
        col("ORGANIZACION_COMPRAS") == "A751",
        "A751"  # Caso 2: Si ORGANIZACION_COMPRAS es A751, asigna A751
    ).otherwise(col("CENTRO"))  # Caso contrario, conserva el valor original de CENTRO
)







ekko = ekko.filter(col("CENTRO")=="A717")




ekko = ekko.select("DOCUMENTO_COMPRAS").distinct()




table = "EKPO"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
ekpo = (spark.
       read.
       parquet(source_path)
       .drop("FECHA_DOCUMENTO")
       .drop("PROVEEDOR")
       .drop("CENTRO")
       )




ekpo = ekpo.filter(
        ((col("INDICADOR_DE_BORRADO").isNull()) | 
        (col("INDICADOR_DE_BORRADO") == " ")) &
        (col("ENTREGA_COMPLETA") != "X")
    )






ekpo = ekpo.select("DOCUMENTO_COMPRAS","POSICION","MATERIAL").distinct()




ekpo = ekpo.withColumn('ID_MATERIAL', regexp_replace(col('MATERIAL'), r'^[0]*', ''))




current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month
current_day = current_date.day
filter_date = f"{current_year}-{current_month}-{current_day}T00:00:00.000+00:00"
first_day = current_date.replace(day=1).strftime("%Y-%m-%d")




table = "EKET"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
eket_foto = (spark.
       read.
       parquet(source_path)
       ).withColumn("SEQ_DATE",date_format(col("SEQ_DATE"),"yyyy-MM-dd")).withColumn("ANIO_MES_SEQ_DATE",date_format(col("SEQ_DATE"),"yyyy-MM"))


eket=eket_foto.filter(col("EBELN")=="5500002718").filter(col("EBELP").contains("0300"))






eket_foto = eket_foto.join(eket_foto.groupBy("ANIO_MES_SEQ_DATE").agg(min("SEQ_DATE").alias("SEQ_DATE")),["ANIO_MES_SEQ_DATE","SEQ_DATE"],"inner")




eket_foto = eket_foto.join(eket_foto.agg(max("SEQ_DATE").alias("SEQ_DATE")),"SEQ_DATE","inner")




eket_foto = eket_foto.select(
    col("EBELN").alias("DOCUMENTO_COMPRAS"),
    col("EBELP").alias("POSICION"),
    col("ETENR").alias("REPARTO"),
    col("UNIQUEID").alias("REPARTO_1"),
    to_date(col("EINDT"), "yyyy-MM-dd").alias("FECHA_DE_ENTREGA"),
    to_date(col("BEDAT"), "yyyy-MM-dd").alias("FECHA_DE_PEDIDO"),
    col("MENGE").cast("double").alias("CANTIDAD_DE_REPARTO"),
    col("WEMNG").cast("double").alias("CANTIDAD_ENTREGADA")
).distinct().fillna({
    "FECHA_DE_ENTREGA": "1900-01-01",
    "FECHA_DE_PEDIDO": "1900-01-01",
    "CANTIDAD_DE_REPARTO": 0,
    "CANTIDAD_ENTREGADA": 0
})





eket_foto = eket_foto.select("DOCUMENTO_COMPRAS","POSICION","REPARTO","REPARTO_1","FECHA_DE_ENTREGA","CANTIDAD_DE_REPARTO","CANTIDAD_ENTREGADA","FECHA_DE_PEDIDO").distinct()








eket_foto = (
    eket_foto.select(
        "DOCUMENTO_COMPRAS",
        "POSICION",
        "REPARTO",
        "FECHA_DE_ENTREGA",
        "CANTIDAD_DE_REPARTO",
        "CANTIDAD_ENTREGADA"
    )
    .distinct()
    .fillna(0)
    .withColumn("CANTIDAD", when(col("CANTIDAD_DE_REPARTO")>0,col("CANTIDAD_DE_REPARTO")-col("CANTIDAD_ENTREGADA")).otherwise(col("CANTIDAD_DE_REPARTO")))
    .withColumn("CANTIDAD", when(col("CANTIDAD")>=0,col("CANTIDAD")).otherwise(0))
    .withColumn("ANIO_MES", when(col("FECHA_DE_ENTREGA")<=(datetime(datetime.now().year, datetime.now().month % 12 + 1, 1) - timedelta(days=1)).strftime('%Y-%m-%d'),(datetime(datetime.now().year, datetime.now().month % 12 + 1, 1) - timedelta(days=1)).strftime('%Y-%m')).otherwise(date_format("FECHA_DE_ENTREGA", "yyyy-MM"))).groupBy("ANIO_MES", "DOCUMENTO_COMPRAS", "POSICION","FECHA_DE_ENTREGA")
    .agg(sum("CANTIDAD").alias("CANTIDAD")))








table = "EKET"
source_path = f"/mnt/adlsabastosprscus/abastos/bronze/SAP/tablas_SAP/{table}.parquet/"
eket = (spark.
       read.
       parquet(source_path)
       ).withColumn("SEQ_DATE",date_format(col("SEQ_DATE"),"yyyy-MM-dd")).withColumn("SEQ_DATE", date_sub(col("SEQ_DATE"), 1)).withColumn("ANIO_MES_SEQ_DATE",date_format(col("SEQ_DATE"),"yyyy-MM"))




eket = eket.join(eket.groupBy("ANIO_MES_SEQ_DATE").agg(max("SEQ_DATE").alias("SEQ_DATE")),["ANIO_MES_SEQ_DATE","SEQ_DATE"],"inner")




eket= eket.join(eket.agg(max("SEQ_DATE").alias("SEQ_DATE")),"SEQ_DATE","inner")




eket = eket.select(
    col("EBELN").alias("DOCUMENTO_COMPRAS"),
    col("EBELP").alias("POSICION"),
    col("ETENR").alias("REPARTO"),
    col("UNIQUEID").alias("REPARTO_1"),
    to_date(col("EINDT"), "yyyy-MM-dd").alias("FECHA_DE_ENTREGA"),
    to_date(col("BEDAT"), "yyyy-MM-dd").alias("FECHA_DE_PEDIDO"),
    col("MENGE").cast("double").alias("CANTIDAD_DE_REPARTO"),
    col("WEMNG").cast("double").alias("CANTIDAD_ENTREGADA")
).distinct().fillna({
    "FECHA_DE_ENTREGA": "1900-01-01",
    "FECHA_DE_PEDIDO": "1900-01-01",
    "CANTIDAD_DE_REPARTO": 0,
    "CANTIDAD_ENTREGADA": 0
})




eket = eket.select("DOCUMENTO_COMPRAS","POSICION","REPARTO","REPARTO_1","FECHA_DE_ENTREGA","CANTIDAD_DE_REPARTO","CANTIDAD_ENTREGADA","FECHA_DE_PEDIDO").distinct()




eket = ( 
    eket.select(
        "DOCUMENTO_COMPRAS",
        "POSICION",
        "REPARTO",
        "FECHA_DE_ENTREGA",
        "CANTIDAD_DE_REPARTO",
        "CANTIDAD_ENTREGADA"
    )
    .distinct()
    .fillna(0)
  .withColumn("CANTIDAD_FIN", when(col("CANTIDAD_DE_REPARTO")>0,col("CANTIDAD_DE_REPARTO")-col("CANTIDAD_ENTREGADA")).otherwise(col("CANTIDAD_DE_REPARTO")))
    .withColumn("ANIO_MES", when(col("FECHA_DE_ENTREGA")<=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'), (datetime.now() - timedelta(days=1)).strftime('%Y-%m')
).otherwise(date_format("FECHA_DE_ENTREGA", "yyyy-MM"))).groupBy("ANIO_MES","FECHA_DE_ENTREGA", "DOCUMENTO_COMPRAS", "POSICION")
    .agg(sum("CANTIDAD_FIN").alias("CANTIDAD_FIN")))






plan_entregas = (ekko.join(ekpo, ["DOCUMENTO_COMPRAS"], "left")
                  .dropDuplicates()
                  .join(eket_foto, ["DOCUMENTO_COMPRAS", "POSICION"], "left")).withColumn("CONCEPTO",lit("PIEZAS"))







plan_entregas = (  plan_entregas.groupBy("ANIO_MES", "ID_MATERIAL","CONCEPTO")
    .agg(sum("CANTIDAD").alias("CANTIDAD"))
)






plan_entregas_fin = (ekko.join(ekpo, ["DOCUMENTO_COMPRAS"], "left")
                     .dropDuplicates()
                     .join(eket, ["DOCUMENTO_COMPRAS", "POSICION"], "left")).withColumn("CONCEPTO",lit("PIEZAS"))




plan_entregas_fin = ( plan_entregas_fin.groupBy("ANIO_MES", "ID_MATERIAL","CONCEPTO")
    .agg(sum("CANTIDAD_FIN").alias("CANTIDAD_FIN"))
)




table = "plan_entregas"
target_path = f"/mnt/adlsabastosprscus/abastos/gold/planeacion/{table}.parquet/"


(plan_entregas.
    write.format("delta").
    partitionBy('ANIO_MES').
    mode("overwrite").
    parquet(target_path))


table = "plan_entregas_fin"
target_path = f"/mnt/adlsabastosprscus/abastos/gold/planeacion/{table}.parquet/"


(plan_entregas_fin.
    write.format("delta").
    partitionBy('ANIO_MES').
    mode("overwrite").
    parquet(target_path))