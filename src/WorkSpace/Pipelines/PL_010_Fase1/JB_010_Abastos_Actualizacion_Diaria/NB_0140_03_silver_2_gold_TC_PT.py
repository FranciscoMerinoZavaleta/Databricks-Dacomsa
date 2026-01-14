



current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month
next_year = current_year + 1




table = "T024"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
planificador = (spark
                 .read
                 .parquet(source_path)
                 .withColumn("DENOMINACION_GRPCOMP", upper(col("DENOMINACION_GRPCOMP")).alias("DENOMINACION_GRPCOMP"))
                 .select("GRUPO_DE_COMPRAS","DENOMINACION_GRPCOMP")
                )




table = "CATALOGO_DE_ARTICULOS"
cat_art_ = (spark.read.parquet(
    f"/mnt/adls-dac-data-bi-pr-scus/silver/marketing/{table}.parquet/")
    .withColumnRenamed("MATERIAL", "ID_MATERIAL")
    .join(planificador, "GRUPO_DE_COMPRAS", "left")
    .withColumnRenamed("DENOMINACION_GRPCOMP", "GRUPO_DE_COMPRAS_NOMBRE")
    .select("ID_MATERIAL","INDICADOR_ABC", "GRUPO_DE_COMPRAS", "GRUPO_DE_COMPRAS_NOMBRE", "COMPANIA" ,"SISTEMA", 
            "LINEA_AGRUPADA", "LINEA_ABIERTA", "CARACT_PLANIF_NEC", "PLANIF_NECESIDADES")
    .dropDuplicates(["ID_MATERIAL"])
    )
cat_art_.write.format("delta").mode("overwrite").parquet("/mnt/adlsabastosprscus/abastos/gold/catalogo_de_articulos_tc_pt.parquet")







table = "CATALOGO_DE_ARTICULOS"
cat_art = (spark.read.parquet(
    f"/mnt/adls-dac-data-bi-pr-scus/silver/marketing/{table}.parquet/")
    .withColumnRenamed("MATERIAL", "ID_MATERIAL")
    .join(planificador, "GRUPO_DE_COMPRAS", "left")
    .withColumnRenamed("DENOMINACION_GRPCOMP", "GRUPO_DE_COMPRAS_NOMBRE")
    )

regex_pattern = r"(\d{4}-\d{2})"

keys = ["ID_MATERIAL","ID_CANAL_DISTRIBUCION","ANIO_MES_1"]
particion = (Window.partitionBy(keys).orderBy(desc('Fecha_pronostico')))
table = "resultados_pronostico_no_parte_escalera_piezas"
aa_mkt = (spark.read.parquet(
    f"/mnt/adlsabastosprscus/abastos/gold/torre_pt/{table}.parquet/")
          .withColumn("TYPE_1", lit("PIEZAS"))
)


table = "HISTORIA_PRONO_MI"
prono_mi = (spark.read.parquet(
    f"/mnt/adls-dac-data-bi-pr-scus/gold/{table}.parquet")
            .withColumn("MES_PRONO", regexp_extract(col("datestr"), regex_pattern, 1))
            .withColumnRenamed("DATE","ANIO_MES")
            .withColumn("ANIO_MES", regexp_extract(col("ANIO_MES"), regex_pattern, 1))
            .withColumnRenamed("MATERIAL","ID_MATERIAL")
            .withColumn("CONCEPTO", when(col("CONCEPTO") == "U_PRONO", "PIEZAS").otherwise("IMPORTE"))
            .withColumnRenamed("CONCEPTO","TYPE_1")
            .filter(col("TYPE_1") == "PIEZAS")
            .drop("ID_CANAL_DISTRIBUCION")
)

keys = ["ID_MATERIAL","MES_PRONO","ANIO_MES","CONCEPTO"]
particion = (Window.partitionBy(keys).orderBy(desc('datestr')))
table = "prono_mkt_memo"
prono_memo = (spark.read.parquet(
    f"/mnt/adls-dac-data-bi-pr-scus/gold/{table}.parquet/")
            .withColumn("MES_PRONO", regexp_extract(col("datestr"), regex_pattern, 1))
            .withColumnRenamed("DATE","ANIO_MES")
            .withColumn("ANIO_MES", regexp_extract(col("ANIO_MES"), regex_pattern, 1))
            .withColumnRenamed("MATERIAL","ID_MATERIAL")
            .groupBy("ID_MATERIAL","SISTEMA","COMPANIA","LINEA_AGRUPADA","LINEA_ABIERTA", "CLASE", "SEGMENTO","CONCEPTO","MES_PRONO", "ANIO_MES","datestr")
            .agg(sum("value").alias("value"))
            .withColumn("ROW_NUM", row_number().over(particion))
            .filter(col('ROW_NUM')==1)
            .drop("ROW_NUM")
            .orderBy("ID_MATERIAL","MES_PRONO","ANIO_MES","ROW_NUM")
            .withColumn("CONCEPTO", when(col("CONCEPTO") == "U_PRONO", "PIEZAS").otherwise("IMPORTE"))
            .withColumnRenamed("CONCEPTO","TYPE_1")
            .filter(col("TYPE_1") == "PIEZAS")
)


table = "fact_pedidos_ajustados"
fact_pedidos = spark.read.parquet(
    f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"
).withColumnRenamed("LINEA", "LINEA_ABIERTA")

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
       )

table = "fact_hab_inventario_inicio"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/{table}.parquet/"
inventario_disponible =  (spark.read.parquet(source_path)
                          .withColumn("COSTO_UNITARIO", col("STOCK_VALOR") / col("STOCK_UNIDADES")))

table = "indicadores_ordenes_compra"
source_path = f"/mnt/adlsabastosprscus/abastos/gold/compras/{table}.parquet/"
ordenes_compra =  (spark.read.parquet(source_path)
             .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", "")))

table = "MATDOC"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
keys =  ["SPLIT_GUID,_PART_1", "SPLIT_GUID,_PART_2", "SPLIT_GUID,_PART_3", "SPLIT_GUID,_PART_4", "SPLIT_GUID,_PART_5", "SPLIT_GUID,_PART_6"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
costo_de_adquis =  (spark.read.parquet(source_path)
                    .withColumn("ROW_NUM", row_number().over(particion))
                    .filter(col('ROW_NUM')==1)
                    .drop("ROW_NUM")
                    .select("PROVEEDOR_DE_STOCK_ESPECIAL","MATERIAL","CENTRO","ALMACEN", "CENTRO_DE_BENEFICIO", "NOMBRE_DEL_USUARIO", "CARACTERISTICAS_DE_STOCK", "MONEDA", "COSTES_IND_ADQUIS", "CANTIDAD","FECHA_DE_DOCUMENTO","FECHA_DE_ENTRADA","CLASE_DE_MOVIMIENTO","CODIGO_TRANSACCION"
                      )
                    .where((col("CLASE_DE_MOVIMIENTO") == 101) & (col("CENTRO") == "A717"))
)

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




mbew_max = mbew.groupBy("MATERIAL").agg(min(col("PRECIO_FISCAL_1")).alias("PRECIO_FISCAL_MAX"))




inventario_disponible_unidades =  (inventario_disponible
                                   .select("ID_MATERIAL","STOCK_UNIDADES","FLAG_CS","COSTO_UNITARIO","ANIO_MES")
                                   .withColumn("ANIO_MES", col("ANIO_MES").cast("date"))
                                   .withColumnRenamed("STOCK_UNIDADES", "STOCK")
                                   .withColumn("TYPE_1", lit("PIEZAS"))
                                   .where(col("ANIO_MES") > "2023-12-31")
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
                             .where(col("ANIO_MES") > "2023-12-31")
                             .withColumnRenamed("ANIO_MES", "MES_PRONO_AUX")
                             .join(mbew.withColumnRenamed("MATERIAL","ID_MATERIAL"), ["ID_MATERIAL", "MES_PRONO_AUX"],"left")
                             .join(mbew_max.withColumnRenamed("MATERIAL","ID_MATERIAL"), ["ID_MATERIAL"],"left")
                             .withColumn("PRECIO_FISCAL_1", when(col("PRECIO_FISCAL_1").isNull(), col("PRECIO_FISCAL_MAX")).otherwise(col("PRECIO_FISCAL_1")))
                             .withColumn("COSTO_UNITARIO", when(col("FLAG_CS") == "CONSIGNADO", col("PRECIO_FISCAL_1")).otherwise(col("COSTO_UNITARIO")))
                             .withColumn("STOCK", col("STOCK") * col("COSTO_UNITARIO"))
                             .drop("PRECIO_FISCAL_1", "PRECIO_FISCAL_MAX")
                            )

inventario_disponible =  inventario_disponible_unidades.unionByName(inventario_disponible_imp).dropDuplicates()




fecha_fin = f"{current_year + 1}-{current_month-1}-01"


plan_de_entregas = (
    ordenes_compra.where((col("CENTRO") == "A717"))
    .groupBy(
        "MATERIAL",
        "CENTRO",
        "ANIO_MES",
        "TYPE_1",
    )
    .agg(sum("CANTIDAD").alias("CANTIDAD"), sum("CANTIDAD_MXN").alias("CANTIDAD_MXN"))
    .withColumn("ANIO_MES", col("ANIO_MES").cast("date"))
    .where(
        (col("ANIO_MES") <= f"{current_year + 1}-{current_month}-01")
    )
    .withColumn(
        "CANTIDAD",
        when(col("TYPE_1") == "IMPORTE", col("CANTIDAD_MXN")).otherwise(
            col("CANTIDAD")
        ),
    )
    .drop("CANTIDAD_MXN")
    .orderBy("MATERIAL", "ANIO_MES", "TYPE_1")
    .withColumnsRenamed({"CANTIDAD": "CANTIDAD_POR_LLEGAR", "MATERIAL": "ID_MATERIAL"})
)


columns_order = ['ID_MATERIAL',
                 'SISTEMA',
                 'COMPANIA',
                 'LINEA_AGRUPADA',
                 'LINEA_ABIERTA',
                 'CLASE',
                 'SEGMENTO',
                 'MES_PRONO',
                 'TYPE_1',
                 'value',
                 'ANIO_MES']




cat_art = cat_art.select("ID_MATERIAL","ST","LINEA_AGRUPADA","PLAZO_ENTREGA_PREV","TAMANO_LOTE_MINIMO","UNIDAD_DE_PESO","PESO_NETO","VOLUMEN", "UNIDAD_DE_VOLUMEN","LONGITUD","ANCHURA","ALTURA","UNIDAD_DIMENSION")






prono_memo = prono_memo.select(columns_order)
prono_mi = prono_mi.select(columns_order)

prono = prono_memo.unionByName(prono_mi)


keys = ["ID_MATERIAL","MES_PRONO_AUX","TYPE_1"]
window = Window.partitionBy(keys).orderBy("ID_MATERIAL","MES_PRONO_AUX","ANIO_MES","TYPE_1")
prono = (prono
         .filter(col("TYPE_1") == "PIEZAS")
         .withColumn("ANIO_MES", to_date(col("ANIO_MES")))
         .withColumn("MES_PRONO_AUX", to_date(col("MES_PRONO")))
         .groupBy("ID_MATERIAL", "MES_PRONO_AUX","ANIO_MES","TYPE_1")
         .agg(sum("value").alias("value"))  
         .filter(col("ANIO_MES") >= col("MES_PRONO_AUX"))
         .withColumn("ROW_NUM", row_number().over(window))
         .filter(col("ROW_NUM") < 13)
         .drop("ROW_NUM", "MES_PRONO")
         .orderBy("ID_MATERIAL","MES_PRONO_AUX","TYPE_1","ANIO_MES") 
         )

demanda_irr = prono


prono_imp = (prono.join(inventario_disponible
                .select("ID_MATERIAL","COSTO_UNITARIO","MES_PRONO_AUX"),["ID_MATERIAL","MES_PRONO_AUX"], "left")
                .withColumn("value", col("value") * col("COSTO_UNITARIO"))
                .withColumn("TYPE_1", lit("IMPORTE"))
                .drop("COSTO_UNITARIO")
             )


prono = prono.unionByName(prono_imp)


prono_dic_aux = prono.where((month(col("MES_PRONO_AUX")) == 11) & (month(col("ANIO_MES")) != 11)).withColumn("MES_PRONO_AUX", add_months(col("MES_PRONO_AUX"), 1))
prono = prono.where(month(col("MES_PRONO_AUX")) != 12)


prono = prono.unionByName(prono_dic_aux)


aa_mkt = (aa_mkt
          .orderBy("ID_MATERIAL","ANIO_MES_PRONOSTICO","TYPE_1","ANIO_MES")
          .withColumn("ANIO_MES", col("ANIO_MES").cast("date"))
          )


keys = ["ID_MATERIAL","ANIO_MES_PRONOSTICO","TYPE_1"]
window = Window.partitionBy(keys).orderBy("ID_MATERIAL","ANIO_MES_PRONOSTICO","ANIO_MES","TYPE_1")
aa_mkt = (aa_mkt
         .withColumnRenamed("Pronostico", "value")
         .groupBy("ID_MATERIAL","ANIO_MES_PRONOSTICO","ANIO_MES","TYPE_1")
         .agg(sum("value").alias("value"))
         .withColumn("value", ceil(col("value")))
         .withColumn("ANIO_MES", to_date(col("ANIO_MES")))
         .withColumn("MES_PRONO_AUX", to_date(col("ANIO_MES_PRONOSTICO")))
         .filter(col("ANIO_MES") >= col("MES_PRONO_AUX"))
         .withColumn("ROW_NUM", row_number().over(window))
         .filter(col("ROW_NUM") < 13)
         .drop("ROW_NUM", "MES_PRONO")
         .orderBy("ID_MATERIAL", "MES_PRONO_AUX","TYPE_1","ANIO_MES")    
         .drop("ANIO_MES_PRONOSTICO")
         )

demanda_irr_aa = aa_mkt


aa_mkt_imp = (aa_mkt.join(inventario_disponible
                .select("ID_MATERIAL","COSTO_UNITARIO","MES_PRONO_AUX"),["ID_MATERIAL","MES_PRONO_AUX"], "left")
                .withColumn("value", col("value") * col("COSTO_UNITARIO"))
                .withColumn("TYPE_1", lit("IMPORTE"))
                .drop("COSTO_UNITARIO"))


aa_mkt = aa_mkt.unionByName(aa_mkt_imp)




prono = (prono.groupBy("ID_MATERIAL","TYPE_1","MES_PRONO_AUX")
         .agg(mean("value").alias("DEMANDA_MENSUAL_PRONO_PROM"), stddev_samp("value").alias("DEMANDA_MENSUAL_PRONO_STD"))
         .withColumn("DEMANDA_MENSUAL_PRONO_PROM", when(col("TYPE_1") == "PIEZAS", ceil(col("DEMANDA_MENSUAL_PRONO_PROM")))
                     .otherwise(col("DEMANDA_MENSUAL_PRONO_PROM")))
         )


aa_mkt = (aa_mkt.withColumn("MES_PRONO_AUX", col("MES_PRONO_AUX").cast("date")).select("ID_MATERIAL", "MES_PRONO_AUX", "value","TYPE_1")
          .groupBy("ID_MATERIAL", "MES_PRONO_AUX","TYPE_1")
          .agg(mean("value").alias("DEMANDA_MENSUAL_PRONO_PROM"), stddev_samp("value").alias("DEMANDA_MENSUAL_PRONO_STD"))
          .withColumn("DEMANDA_MENSUAL_PRONO_PROM", when(col("TYPE_1") == "PIEZAS",ceil(col("DEMANDA_MENSUAL_PRONO_PROM")))
                     .otherwise(col("DEMANDA_MENSUAL_PRONO_PROM")))
          .orderBy("ID_MATERIAL")
    )




demanda_anterior_piezas = (fact_pedidos.where((col("ANIO_FECHA_REGISTRO") >= 2024))
                           .groupBy("ID_MATERIAL", "ANIO_MES")
                           .agg(sum("PIEZAS_FACTURADA").alias("PIEZAS"))
                           .withColumn("TYPE_1", lit("PIEZAS"))
                           .withColumnRenamed("PIEZAS", "DEMANDA_ANTERIOR")
                           .withColumn("ANIO_MES", col("ANIO_MES").cast("date"))
                           )

demanda_anterior_imp = (fact_pedidos.where((col("ANIO_FECHA_REGISTRO") >= 2024))
                           .withColumnRenamed("ANIO_MES","MES_PRONO_AUX")
                           .withColumn("MES_PRONO_AUX", col("MES_PRONO_AUX").cast("date"))
                           .join(inventario_disponible.select("ID_MATERIAL", "COSTO_UNITARIO", "MES_PRONO_AUX"), ["ID_MATERIAL", "MES_PRONO_AUX"], "left")
                           .drop("IMPORTE_FACTURADA")
                           .withColumn("IMPORTE_FACTURADA", col("PIEZAS_FACTURADA") * col("COSTO_UNITARIO"))
                           .groupBy("ID_MATERIAL", "MES_PRONO_AUX")
                           .agg(sum("IMPORTE_FACTURADA").alias("IMPORTE"))
                           .withColumn("TYPE_1", lit("IMPORTE"))
                           .withColumnRenamed("IMPORTE", "DEMANDA_ANTERIOR")
                           .withColumnRenamed("MES_PRONO_AUX", "ANIO_MES")
                           
                        )

demanda_anterior = demanda_anterior_piezas.unionByName(demanda_anterior_imp)






pedidos_productos_p = (fact_pedidos.where(col("ANIO_FECHA_REGISTRO") == current_year -1)
                .withColumnRenamed("ANIO_MES","MES_PRONO_AUX")
                .withColumn("MES_PRONO_AUX", col("MES_PRONO_AUX").cast("date"))
                .groupBy("ID_MATERIAL","MES_PRONO_AUX")
                .agg(sum("PIEZAS_FACTURADA").alias("DEMANDA_ANIO_ANT_SUM"))
                .withColumn("DEMANDA_ANIO_ANT_SUM", ceil(col("DEMANDA_ANIO_ANT_SUM")))
                .groupBy("ID_MATERIAL")
                .agg(mean("DEMANDA_ANIO_ANT_SUM").alias("DEMANDA_ANIO_ANT_PROM"),
                     var_samp("DEMANDA_ANIO_ANT_SUM").alias("DEMANDA_ANIO_ANT_VAR"))
                .withColumn("TYPE_1", lit("PIEZAS"))
                    )

pedidos_productos_imp = (fact_pedidos.where(col("ANIO_FECHA_REGISTRO") == current_year -1)
                         .withColumnRenamed("ANIO_MES","MES_PRONO_AUX")
                         .withColumn("MES_PRONO_AUX", col("MES_PRONO_AUX").cast("date"))
                         .join(inventario_disponible.select("ID_MATERIAL", "COSTO_UNITARIO", "MES_PRONO_AUX"), ["ID_MATERIAL", "MES_PRONO_AUX"], "left")
                           .drop("IMPORTE_FACTURADA")
                           .withColumn("IMPORTE_FACTURADA", col("PIEZAS_FACTURADA") * col("COSTO_UNITARIO"))
                         .groupBy("ID_MATERIAL","MES_PRONO_AUX")
                     .agg(sum("IMPORTE_FACTURADA").alias("DEMANDA_ANIO_ANT_SUM"))
                     .groupBy("ID_MATERIAL")
                     .agg(mean("DEMANDA_ANIO_ANT_SUM").alias("DEMANDA_ANIO_ANT_PROM"),
                     var_samp("DEMANDA_ANIO_ANT_SUM").alias("DEMANDA_ANIO_ANT_VAR"))
                     .withColumn("TYPE_1", lit("IMPORTE"))
                )

pedidos_productos = pedidos_productos_p.unionByName(pedidos_productos_imp)




materiales_pedibles = (cat_art.dropDuplicates().withColumn("LONGITUD", col("LONGITUD").cast("double"))
                       .withColumn("ALTURA", col("ALTURA").cast("double")).withColumn("ANCHURA", col("ANCHURA").cast("double"))
                       .withColumn("VOLUMEN", col("VOLUMEN").cast("double"))
                       )




data = [("PAC", 3700000.0, 18576000.0, 17870.0)]
columns = ["CONCEPTO", "COSTO_DE_RENTA_ANUAL", "COSTO_OPERATIVO_ANUAL", "NUMERO_DE_PALETS"]
costo_de_almacenamiento = spark.createDataFrame(data, columns)




costo_de_almacenamiento = (costo_de_almacenamiento.withColumn("COSTO_ANUAL_TOTAL", col("COSTO_DE_RENTA_ANUAL") + col("COSTO_OPERATIVO_ANUAL"))
                           .withColumn("COSTO_ANUAL_POR_PALET", col("COSTO_ANUAL_TOTAL") / col("NUMERO_DE_PALETS"))
                           .withColumn("VOLUMEN_POR_PALET", lit(1.2 * 1 * 1.3) ) #Fueron indicados por Hugo Carapia que es el encargado de almacenes
                           )


demanda_irr = (demanda_irr.select("ID_MATERIAL","ANIO_MES","MES_PRONO_AUX","value","TYPE_1")
               .withColumnRenamed("value","DEMANDA_IRR")
               .withColumn("MODELO_DE_PRONO", lit("PRONOSTICO_S&OP"))
               .orderBy("ID_MATERIAL","MES_PRONO_AUX","TYPE_1","ANIO_MES")
               )


demanda_irr_aa = (demanda_irr_aa.select("ID_MATERIAL", "ANIO_MES", "MES_PRONO_AUX", "value","TYPE_1")
                   .withColumnRenamed("value","DEMANDA_IRR")
                   .withColumn("MODELO_DE_PRONO", lit("PRONOSTICO_AA"))
                   .withColumn("MES_PRONO_AUX", col("MES_PRONO_AUX").cast("date"))
                   .withColumn("DEMANDA_IRR", col("DEMANDA_IRR").cast("double"))
                   .withColumn("ANIO_MES", col("ANIO_MES").cast("date"))
                   .orderBy("ID_MATERIAL","MES_PRONO_AUX","TYPE_1","ANIO_MES")
          )


demanda_irr = demanda_irr.unionByName(demanda_irr_aa)




demanda_irr_imp = (demanda_irr.join(inventario_disponible
                    .select("ID_MATERIAL","COSTO_UNITARIO","MES_PRONO_AUX"),["ID_MATERIAL","MES_PRONO_AUX"], "left")
                    .dropDuplicates()
                    .withColumn("DEMANDA_IRR",
                                col("DEMANDA_IRR") * col("COSTO_UNITARIO"))
                    .withColumn("TYPE_1", lit("IMPORTE"))
                    .orderBy("ID_MATERIAL","TYPE_1","MODELO_DE_PRONO","MES_PRONO_AUX","ANIO_MES")
                    .drop("COSTO_UNITARIO")
                    )


demanda_irr = demanda_irr.unionByName(demanda_irr_imp)


demanda_irr_dic_aux = demanda_irr.where((month(col("MES_PRONO_AUX")) == 11) & (month(col("ANIO_MES")) != 11) & (col("MODELO_DE_PRONO") != "PRONOSTICO_AA")).withColumn("MES_PRONO_AUX", add_months(col("MES_PRONO_AUX"), 1))
demanda_irr = demanda_irr.where(((month(col("MES_PRONO_AUX")) != 12) & (col("MODELO_DE_PRONO") == "PRONOSTICO_S&OP")) | (col("MODELO_DE_PRONO") == "PRONOSTICO_AA"))


demanda_irr = demanda_irr.unionByName(demanda_irr_dic_aux)




control_nivel_de_inv_pt = (demanda_irr.join(materiales_pedibles,"ID_MATERIAL","left")
                              .dropDuplicates()
                              .withColumn("CONCEPTO", lit("PAC"))
                              .join(costo_de_almacenamiento.select("CONCEPTO","COSTO_ANUAL_POR_PALET","VOLUMEN_POR_PALET"), "CONCEPTO","left")
                              .dropDuplicates()
                              .withColumn("VOLUMEN_POR_UNIDAD", ((col("LONGITUD")/100) * (col("ANCHURA")/100) * (col("ALTURA")/100)))
                              .withColumn("UNIDADES_APROX_POR_PALET", when(col("VOLUMEN") != 0, col("VOLUMEN_POR_PALET") / col("VOLUMEN"))
                                          .otherwise(floor(col("VOLUMEN_POR_PALET") / col("VOLUMEN_POR_UNIDAD"))))
                              .withColumn("COSTO_DE_ALM_X_UNIDAD", col("COSTO_ANUAL_POR_PALET") / col("UNIDADES_APROX_POR_PALET"))
                              .orderBy("ID_MATERIAL","TYPE_1","MODELO_DE_PRONO","MES_PRONO_AUX","ANIO_MES")                      
                              )


costo_med_x_linea_agr = control_nivel_de_inv_pt.groupBy("LINEA_AGRUPADA").agg(median(col("COSTO_DE_ALM_X_UNIDAD")).alias("COSTO_MED_ALM_X_LIN_AG"))


control_nivel_de_inv_pt =  (control_nivel_de_inv_pt
                            .join(costo_med_x_linea_agr,"LINEA_AGRUPADA","left")
                            .orderBy("ID_MATERIAL","TYPE_1","MODELO_DE_PRONO","MES_PRONO_AUX","ANIO_MES"))




modelo_inventario_opt = (demanda_irr
                         .join(inventario_disponible, ["ID_MATERIAL","MES_PRONO_AUX", "TYPE_1"], "right")
                         .dropDuplicates()
                         .join(plan_de_entregas, ["ID_MATERIAL","ANIO_MES", "TYPE_1"], "left")
                         .dropDuplicates()
                         .join(demanda_anterior, ["ID_MATERIAL", "ANIO_MES","TYPE_1"], "left")#.drop("ANIO_MES")
                         .dropDuplicates()
                         .orderBy("ID_MATERIAL","MODELO_DE_PRONO","TYPE_1","MES_PRONO_AUX","ANIO_MES")
                         .withColumn("STOCK", when(col("MES_PRONO_AUX") == col("ANIO_MES"), col("STOCK")).otherwise(lit(None)))
                         )


windowSpec = Window.partitionBy("ID_MATERIAL","MODELO_DE_PRONO","TYPE_1","MES_PRONO_AUX").orderBy("ANIO_MES").rowsBetween(Window.unboundedPreceding, Window.currentRow)

windowSpecLag = Window.partitionBy("ID_MATERIAL","MODELO_DE_PRONO","TYPE_1","MES_PRONO_AUX").orderBy("ANIO_MES")

windowSpecmean = Window.partitionBy("ID_MATERIAL","MODELO_DE_PRONO","TYPE_1","MES_PRONO_AUX").orderBy("ANIO_MES").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

modelo_inventario_opt = (modelo_inventario_opt
.withColumnRenamed(
    "STOCK","INVENTARIO"
)
.withColumn("DEMANDA_IRR_ANTERIOR", lag("DEMANDA_IRR", 1).over(windowSpecLag))
.withColumn("DEMANDA_ANTERIOR_REAL", lag("DEMANDA_ANTERIOR", 1).over(windowSpecLag))
.withColumn("DEMANDA_IRR_PROM",
            mean("DEMANDA_IRR").over(windowSpecmean))
.withColumn("DEMANDA_IRR_PROM_REAL",
            mean("DEMANDA_ANTERIOR").over(windowSpecmean))
.fillna(0)
.withColumn(
    "NIVEL_DE_INVENTARIO_REAL",
    sum(
        when(col("INVENTARIO") != 0 , col("INVENTARIO") + col("CANTIDAD_POR_LLEGAR"))
        .otherwise(when(col("CANTIDAD_POR_LLEGAR") == 0, -col("DEMANDA_ANTERIOR_REAL"))
                   .otherwise(-col("DEMANDA_ANTERIOR_REAL") + col("CANTIDAD_POR_LLEGAR")))
    ).over(windowSpec)
)
.withColumn(
    "NIVEL_DE_INVENTARIO_SIN_LLEGADAS",
    sum(
        when(col("INVENTARIO") != 0 , col("INVENTARIO"))
        .otherwise(-col("DEMANDA_IRR_ANTERIOR"))
    ).over(windowSpec)
)
.withColumn(
    "NIVEL_DE_INVENTARIO",
    sum(
        when(col("INVENTARIO") != 0 , col("INVENTARIO") + col("CANTIDAD_POR_LLEGAR"))
        .otherwise(when(col("CANTIDAD_POR_LLEGAR") == 0, -col("DEMANDA_IRR_ANTERIOR"))
                   .otherwise(-col("DEMANDA_IRR_ANTERIOR") + col("CANTIDAD_POR_LLEGAR")))
    ).over(windowSpec)
)
.withColumn("NIVEL_DE_INVENTARIO", when(col("NIVEL_DE_INVENTARIO") < 0, lit(0)).otherwise(col("NIVEL_DE_INVENTARIO")))
.withColumn("NIVEL_DE_INVENTARIO_REAL", when(col("NIVEL_DE_INVENTARIO_REAL") < 0, lit(0)).otherwise(col("NIVEL_DE_INVENTARIO_REAL")))
.withColumn("NIVEL_DE_INVENTARIO_SIN_LLEGADAS", when(col("NIVEL_DE_INVENTARIO_SIN_LLEGADAS") < 0, lit(0)).otherwise(col("NIVEL_DE_INVENTARIO_SIN_LLEGADAS")))
.withColumn(
    "COBERTURA_EN_MESES", col("NIVEL_DE_INVENTARIO") / col("DEMANDA_IRR_PROM")
    )
.withColumn("COBERTURA_EN_MESES", when(col("COBERTURA_EN_MESES") < 0, lit(0)).otherwise(col("COBERTURA_EN_MESES")))
.withColumn(
    "TIME_TO_SURVIVE", (col("NIVEL_DE_INVENTARIO_SIN_LLEGADAS") / col("DEMANDA_IRR_PROM")) * 4.345
    )
.withColumn("TIME_TO_SURVIVE", when(col("TIME_TO_SURVIVE") < 0, lit(0)).otherwise(col("TIME_TO_SURVIVE")))
.withColumn("COBERTURA_EN_MESES", when(col("COBERTURA_EN_MESES") < 0, lit(0)).otherwise(col("COBERTURA_EN_MESES")))
.withColumn("NIVEL_DE_INVENTARIO_REAL", when(col("ANIO_MES") >= f"{current_year}-{current_month}-28", col("NIVEL_DE_INVENTARIO")).otherwise(col("NIVEL_DE_INVENTARIO_REAL")))
.drop("DEMANDA_IRR_ANTERIOR","DEMANDA_IRR_PROM", "DEMANDA_ANTERIOR_REAL", "DEMANDA_ANTERIOR_REAL", "DEMANDA_IRR_PROM_REAL")
.fillna(0)
)




inv_opt = (cat_art.select("ID_MATERIAL","ST")
           .join(marc.withColumnRenamed("MATERIAL", "ID_MATERIAL").select("ID_MATERIAL", "STOCK_ALM_MAX", "PUNTO_DE_PEDIDO")
                 , "ID_MATERIAL", "left"))


inv_opt = (
    inv_opt.withColumn("STOCK_ALM_MAX", col("STOCK_ALM_MAX").cast("double"))
    .withColumn("PUNTO_DE_PEDIDO", col("PUNTO_DE_PEDIDO").cast("double"))
    .select("ID_MATERIAL", "STOCK_ALM_MAX", "PUNTO_DE_PEDIDO")
    .withColumnRenamed("STOCK_ALM_MAX", "INV_OPTIMO_DAC")
    .withColumnRenamed("PUNTO_DE_PEDIDO", "REORDER_POINT_DAC")
)




control_nivel_de_inv_pt = (control_nivel_de_inv_pt.join(inv_opt, "ID_MATERIAL", "left")
                              .join(modelo_inventario_opt, ["ID_MATERIAL","TYPE_1", "MODELO_DE_PRONO" ,"MES_PRONO_AUX", "ANIO_MES","DEMANDA_IRR"], "right")
                              .dropDuplicates()
                              .join(pedidos_productos, ["ID_MATERIAL","TYPE_1"], "left")
                              .dropDuplicates()
                              .orderBy("ID_MATERIAL","TYPE_1","MODELO_DE_PRONO","MES_PRONO_AUX","ANIO_MES")
                              .drop("CENTRO")
                              )




costo_de_adquis_ = (costo_de_adquis
.withColumn("CANTIDAD", col("CANTIDAD").cast("float"))
.withColumn("COSTES_IND_ADQUIS", col("COSTES_IND_ADQUIS").cast("float"))

.withColumn(
    "COSTO_ADQUIS_UNIDAD", col("COSTES_IND_ADQUIS") / col("CANTIDAD"))
.withColumnRenamed("MATERIAL", "ID_MATERIAL")
.groupBy("ID_MATERIAL", "CENTRO").agg(median("COSTO_ADQUIS_UNIDAD").alias("COSTO_ADQUIS_PROM_UNIDAD"))
)





varianza_lead_time  = (costo_de_adquis
                       .withColumnRenamed("MATERIAL", "ID_MATERIAL")
                       .withColumn("LEAD_TIME", (datediff(col("FECHA_DE_ENTRADA"), col("FECHA_DE_DOCUMENTO"))/30))
                       .groupBy("ID_MATERIAL").agg(var_samp(col("LEAD_TIME")).alias("LEAD_TIME_VAR"))
                       .withColumn("LEAD_TIME_VAR", when(col("LEAD_TIME_VAR") == 0, 1).otherwise(col("LEAD_TIME_VAR")))
                       )




windowSpec = Window.partitionBy("ID_MATERIAL","MODELO_DE_PRONO","TYPE_1","MES_PRONO_AUX").orderBy("ANIO_MES").rowsBetween(Window.unboundedPreceding, Window.currentRow)

windowSpecMax = Window.partitionBy("ID_MATERIAL","MODELO_DE_PRONO","TYPE_1","MES_PRONO_AUX")

control_nivel_de_inv_pt = (control_nivel_de_inv_pt.join(costo_de_adquis_,"ID_MATERIAL","left").dropDuplicates()
                              .withColumn("COSTO_ADQUIS_PROM_UNIDAD", when(col("COSTO_ADQUIS_PROM_UNIDAD") == 0, 1).otherwise(col("COSTO_ADQUIS_PROM_UNIDAD")))
                              .withColumn("ORDEN_OPTIMA", 
                                sqrt((2 * col("DEMANDA_ANIO_ANT_PROM") * col("COSTO_ADQUIS_PROM_UNIDAD")) / (col("COSTO_MED_ALM_X_LIN_AG")/12))         
                                )
                              .withColumn("TAMANO_LOTE_MINIMO", col("TAMANO_LOTE_MINIMO").cast("float"))
                              .withColumn("ORDEN_OPTIMA", when(col("ORDEN_OPTIMA") < col("TAMANO_LOTE_MINIMO"), col("TAMANO_LOTE_MINIMO")).otherwise(col("ORDEN_OPTIMA")))
                              .withColumn("ORDEN_OPTIMA", ceil(col("ORDEN_OPTIMA")))
                              .withColumn("PLAZO_ENTREGA_PREV", col("PLAZO_ENTREGA_PREV").cast("double"))
                              .join(varianza_lead_time,["ID_MATERIAL"], "left")
                              .withColumn("SIGMA_DL",
                                          sqrt(((col("PLAZO_ENTREGA_PREV")/30) * col("DEMANDA_ANIO_ANT_VAR")) + 
                                               ((col("DEMANDA_ANIO_ANT_PROM")**2) * col("LEAD_TIME_VAR")))
                                )
                              .withColumn("STOCK_DE_SEGURIDAD", ceil(1.28 * col("SIGMA_DL")))
                              .withColumn("PUNTO_DE_REORDEN", ceil((col("DEMANDA_ANIO_ANT_PROM")) * (col("PLAZO_ENTREGA_PREV")/30) + col("STOCK_DE_SEGURIDAD")))
                              .drop("LINEA_AGRUPADA", "CONCEPTO", "UNIDAD_DE_PESO", "PESO_NETO","VOLUMEN","UNIDAD_DE_VOLUMEN","UNIDADES_APROX_POR_PALLET","VOLUMEN_POR_PALETE","SIGMA_DL")
                              .orderBy("ID_MATERIAL","TYPE_1","MODELO_DE_PRONO","MES_PRONO_AUX","ANIO_MES")
                              .withColumn("NO_DE_VECES_PRO", col("NIVEL_DE_INVENTARIO")/col("PUNTO_DE_REORDEN"))
                              .withColumn("AUX", when(col("NO_DE_VECES_PRO") > 1.2, 1).otherwise(0))
                              .withColumn("MESES_HASTA_PRO_PROYECTADOS", sum(col("AUX")).over(windowSpec))
                              .withColumn("MESES_HASTA_PRO_PROYECTADOS", max(col("MESES_HASTA_PRO_PROYECTADOS")).over(windowSpecMax))
                              .withColumn("SEMAFORO", when(col("MESES_HASTA_PRO_PROYECTADOS") <= 1, "ROJO").otherwise(
                                when((col("MESES_HASTA_PRO_PROYECTADOS") > 1) & (col("MESES_HASTA_PRO_PROYECTADOS") <= 2), "AMARILLO").otherwise("VERDE")
                              ))
                              .drop("AUX","NO_DE_VECES_PRO")
                              .withColumn("COBERTURA_EN_SEMANAS", col("MESES_HASTA_PRO_PROYECTADOS") * 4.345)
                              .withColumn("INVENTARIO_EXCEDENTE", col("NIVEL_DE_INVENTARIO_REAL") - (col("PUNTO_DE_REORDEN") + col("ORDEN_OPTIMA")))
                              .withColumn("INVENTARIO_EXCEDENTE", when(col("INVENTARIO_EXCEDENTE") < 0, col("NIVEL_DE_INVENTARIO")).otherwise(col("INVENTARIO_EXCEDENTE")))
                              .withColumn("COSTO_DE_OPORTUNIDAD_PROYECTADO", col("INVENTARIO_EXCEDENTE") * 0.0066)
                              .withColumn("AGRUPADO_TIME_TO_SURVIVE", when(col("TIME_TO_SURVIVE") <= 4, "0 - 4")
                                          .otherwise(when((col("TIME_TO_SURVIVE") > 4) & (col("TIME_TO_SURVIVE") <= 12), "4 - 12")
                                                     .otherwise(when((col("TIME_TO_SURVIVE") > 12) & (col("TIME_TO_SURVIVE") <= 21), "12 - 21")
                                                                .otherwise("+21"))))
                              .withColumn("TIME_TO_SURVIVE", first(col("TIME_TO_SURVIVE")).over(windowSpecMax))
                              .withColumn("AGRUPADO_TIME_TO_SURVIVE", first(col("AGRUPADO_TIME_TO_SURVIVE")).over(windowSpecMax))
                              )




control_nivel_de_inv_pt_p = control_nivel_de_inv_pt.filter(col("TYPE_1") == "PIEZAS")
control_nivel_de_inv_pt_imp = (control_nivel_de_inv_pt_p.withColumn("INVENTARIO", col("INVENTARIO") * col("COSTO_UNITARIO"))
                               .withColumn("INV_OPTIMO_DAC", col("INV_OPTIMO_DAC") * col("COSTO_UNITARIO"))
                               .withColumn("REORDER_POINT_DAC", col("REORDER_POINT_DAC") * col("COSTO_UNITARIO"))
                               .withColumn("NIVEL_DE_INVENTARIO_REAL", col("NIVEL_DE_INVENTARIO_REAL") * col("COSTO_UNITARIO"))
                               .withColumn("NIVEL_DE_INVENTARIO", col("NIVEL_DE_INVENTARIO") * col("COSTO_UNITARIO"))
                               .withColumn("NIVEL_DE_INVENTARIO_SIN_LLEGADAS", col("NIVEL_DE_INVENTARIO_SIN_LLEGADAS") * col("COSTO_UNITARIO"))
                               .withColumn("ORDEN_OPTIMA", col("ORDEN_OPTIMA") * col("COSTO_UNITARIO"))
                               .withColumn("STOCK_DE_SEGURIDAD", col("STOCK_DE_SEGURIDAD") * col("COSTO_UNITARIO"))
                               .withColumn("PUNTO_DE_REORDEN", col("PUNTO_DE_REORDEN") * col("COSTO_UNITARIO"))
                               .withColumn("INVENTARIO_EXCEDENTE", col("INVENTARIO_EXCEDENTE") * col("COSTO_UNITARIO"))
                               .withColumn("COSTO_DE_OPORTUNIDAD_PROYECTADO", col("COSTO_DE_OPORTUNIDAD_PROYECTADO") * col("COSTO_UNITARIO"))
                               .withColumn("TYPE_1", lit("IMPORTE"))
                               )


control_nivel_de_inv_pt = control_nivel_de_inv_pt_p.unionByName(control_nivel_de_inv_pt_imp)
control_nivel_de_inv_pt = control_nivel_de_inv_pt.withColumn("PLAZO_ENTREGA_PREV", col("PLAZO_ENTREGA_PREV")/7)


inventario_optimo_aux = (control_nivel_de_inv_pt.groupBy('ID_MATERIAL','TYPE_1','INV_OPTIMO_DAC','ORDEN_OPTIMA','PUNTO_DE_REORDEN')
                         .agg(first(col("INV_OPTIMO_DAC")).alias("INV_OPTIMO_DAC"),
                              first(col("ORDEN_OPTIMA")).alias("ORDEN_OPTIMA"),
                              first(col("PUNTO_DE_REORDEN")).alias("PUNTO_DE_REORDEN")))


control_nivel_de_inv_pt.write.format("delta").mode("overwrite").parquet("/mnt/adlsabastosprscus/abastos/gold/control_nivel_de_inv_pt.parquet")






reorder = ['ID_MATERIAL',
 'TYPE_1',
 'MES_PRONO_AUX',
 'INVENTARIO',
 'FLAG_CS',
 'COSTO_UNITARIO',
 'INVENTARIO_OPT',
 'ORDEN_OPTIMA',
 'MODELO']


inventario_optimo_dacomsa  = (inventario_disponible
                              .join(control_nivel_de_inv_pt
                                    .select('ID_MATERIAL','TYPE_1','ORDEN_OPTIMA','STOCK_DE_SEGURIDAD','PUNTO_DE_REORDEN')
                                    , ["ID_MATERIAL", "TYPE_1"], "left")
                              .dropDuplicates()
                              .join(inv_opt.select("ID_MATERIAL","INV_OPTIMO_DAC"),"ID_MATERIAL", "left")
                              .dropDuplicates()
                              .withColumnRenamed("INV_OPTIMO_DAC", "INVENTARIO_OPT")
                              .withColumnRenamed("STOCK","INVENTARIO")
                              .withColumn("INVENTARIO_OPT", when(col("TYPE_1") == "IMPORTE", col("INVENTARIO_OPT") * col("COSTO_UNITARIO")).otherwise(col("INVENTARIO_OPT")))
                              .drop("MODELO_DE_PRONO", "CANTIDAD_POR_LLEGAR", "STOCK_DE_SEGURIDAD", "PUNTO_DE_REORDEN")
                              .dropDuplicates(["ID_MATERIAL","TYPE_1","MES_PRONO_AUX"])
                              .withColumn("MODELO", lit("DACOMSA"))
                              .fillna(0)
                              .select(*reorder)
                              )


inventario_optimo_aa_piezas = (inventario_disponible.where(col("TYPE_1") == "PIEZAS")
                         .join(control_nivel_de_inv_pt.select('ID_MATERIAL','TYPE_1','MES_PRONO_AUX','INV_OPTIMO_DAC',
                                                              'ORDEN_OPTIMA','STOCK_DE_SEGURIDAD','PUNTO_DE_REORDEN')
                               .dropDuplicates()
                               .where(col("TYPE_1") == "PIEZAS"), ["ID_MATERIAL", "TYPE_1", "MES_PRONO_AUX"], "left")
                         .withColumnRenamed("STOCK","INVENTARIO")
                         .withColumn("INVENTARIO_OPT", ceil(col("ORDEN_OPTIMA") + col("PUNTO_DE_REORDEN")))
                         .withColumn("INVENTARIO_OPT_IMP", col("INVENTARIO_OPT") * col("COSTO_UNITARIO"))
                         .select('ID_MATERIAL','TYPE_1', 'MES_PRONO_AUX','FLAG_CS','COSTO_UNITARIO','ORDEN_OPTIMA','INVENTARIO','INVENTARIO_OPT','INVENTARIO_OPT_IMP')
                         .withColumn("MODELO", lit("ANALITICA AVANZADA"))
                         .dropDuplicates()
 )

inventario_optimo_aa_imp = (inventario_optimo_aa_piezas
                         .drop("INVENTARIO_OPT")
                         .withColumnRenamed("INVENTARIO_OPT_IMP", "INVENTARIO_OPT")
                         .withColumn("INVENTARIO", col("INVENTARIO") * col("COSTO_UNITARIO")) 
                         .withColumn("TYPE_1",lit("IMPORTE"))
                         .select('ID_MATERIAL','TYPE_1', 'MES_PRONO_AUX','FLAG_CS','COSTO_UNITARIO','ORDEN_OPTIMA','INVENTARIO','INVENTARIO_OPT')
                         .withColumn("MODELO", lit("ANALITICA AVANZADA"))
 )

inventario_optimo_aa = inventario_optimo_aa_piezas.drop("INVENTARIO_OPT_IMP").unionByName(inventario_optimo_aa_imp)

inventario_optimo_dacomsa = inventario_optimo_dacomsa.unionByName(inventario_optimo_aa).fillna(0)




porcentaje_max = 1.255
inventario_optimo_dacomsa = (inventario_optimo_dacomsa
 .withColumn("SIN_INVENTARIO_OPTIMO", 
             when((col("INVENTARIO_OPT") == 0) | (col("INVENTARIO_OPT") == "") | (col("INVENTARIO_OPT").isNull()), col("INVENTARIO")))
 .withColumn(
    "MENOR_IGUAL_AL_INVENTARIO_OPTIMO",
    when(
        col("INVENTARIO") <= (col("INVENTARIO_OPT") * porcentaje_max), col("INVENTARIO")
    ).otherwise(col("INVENTARIO_OPT") * porcentaje_max))
 .withColumn(
     "MENOR_IGUAL_AL_INVENTARIO_OPTIMO",
     when(col("TYPE_1") == "PIEZAS", floor(col("MENOR_IGUAL_AL_INVENTARIO_OPTIMO"))
 ).otherwise(col("MENOR_IGUAL_AL_INVENTARIO_OPTIMO")))
 .withColumn(
    "CANTIDAD_EXCEDENTE_DE_INVENTARIO_OPT",
    when(col("INVENTARIO_OPT") > 0, col("INVENTARIO") - col("MENOR_IGUAL_AL_INVENTARIO_OPTIMO"))
)
)









inventario_optimo_dacomsa = (
    inventario_optimo_dacomsa.select(
        "ID_MATERIAL",
        "TYPE_1",
        "MES_PRONO_AUX",
        "INVENTARIO_OPT",
        "INVENTARIO",
        "FLAG_CS",
        "COSTO_UNITARIO",
        "MODELO",
        "MENOR_IGUAL_AL_INVENTARIO_OPTIMO",
    )
    .withColumnRenamed("MENOR_IGUAL_AL_INVENTARIO_OPTIMO", "CANTIDAD")
    .withColumn("FLAG_INVENTARIO_OPTIMO", lit("MENOR-IGUAL A INVENTARIO OPTIMO"))
    .dropDuplicates()
    .unionByName(
        inventario_optimo_dacomsa.select(
            "ID_MATERIAL",
            "TYPE_1",
            "MES_PRONO_AUX",
            "INVENTARIO_OPT",
            "INVENTARIO",
            "FLAG_CS",
            "COSTO_UNITARIO",
            "MODELO",
            "CANTIDAD_EXCEDENTE_DE_INVENTARIO_OPT",
        )
        .withColumnRenamed("CANTIDAD_EXCEDENTE_DE_INVENTARIO_OPT", "CANTIDAD")
        .withColumn("FLAG_INVENTARIO_OPTIMO", lit("EXCEDENTE"))
    )
    .unionByName(
        inventario_optimo_dacomsa.select(
            "ID_MATERIAL",
            "TYPE_1",
            "MES_PRONO_AUX",
            "INVENTARIO_OPT",
            "INVENTARIO",
            "FLAG_CS",
            "COSTO_UNITARIO",
            "MODELO",
            "SIN_INVENTARIO_OPTIMO"
        )
        .withColumnRenamed("SIN_INVENTARIO_OPTIMO", "CANTIDAD")
        .withColumn("FLAG_INVENTARIO_OPTIMO", lit("SIN INVENTARIO OPTIMO"))
        .where((col("SIN_INVENTARIO_OPTIMO").isNotNull()) & (col("INVENTARIO") != 0))
    )
    .withColumn("PORCENTAJE", (col("CANTIDAD") / col("INVENTARIO")))
    .withColumn("NO_DE_VECES_INVENTARIO_OPTIMO", col("CANTIDAD")/ col("INVENTARIO_OPT"))
    .withColumn("FLAG_SUB_O_EXCESO_DE_INV",
                when((col("NO_DE_VECES_INVENTARIO_OPTIMO") <= 0.75) & (col("FLAG_INVENTARIO_OPTIMO") == "MENOR-IGUAL A INVENTARIO OPTIMO"), "SUB-INVENTARIO")
                .otherwise(when((col("NO_DE_VECES_INVENTARIO_OPTIMO") > 0.75) & (col("NO_DE_VECES_INVENTARIO_OPTIMO") <= porcentaje_max + 0.001)
                                & (col("FLAG_INVENTARIO_OPTIMO") == "MENOR-IGUAL A INVENTARIO OPTIMO"), "INVENTARIO OPTIMO")
                           .otherwise(when((col("FLAG_INVENTARIO_OPTIMO") == "EXCEDENTE") & (col("CANTIDAD") > 0), "INVENTARIO EN EXCESO")
                           .otherwise("SIN INVENTARIO OPTIMO")))
    )
    .withColumn("SEMAFORO", 
                when(((col("FLAG_SUB_O_EXCESO_DE_INV") == "INVENTARIO EN EXCESO") |
                                 ((col("FLAG_SUB_O_EXCESO_DE_INV") == "SUB-INVENTARIO") &
                                  (col("NO_DE_VECES_INVENTARIO_OPTIMO") <= 0.75))), "ROJO")
                           .otherwise(when(((col("FLAG_SUB_O_EXCESO_DE_INV") == "INVENTARIO OPTIMO")), "VERDE")
                                      .otherwise(when(col("INVENTARIO_OPT") == 0, "GRIS"))
                           )
                                )
    .filter(
    (~(col("CANTIDAD").isNull()) |
    (col("CANTIDAD") == 0)))
    .filter(
        col("SEMAFORO").isNotNull()
    )
    .dropDuplicates()
    .orderBy("ID_MATERIAL", "MODELO", "TYPE_1", "MES_PRONO_AUX", "SEMAFORO")
)


inventario_optimo_dacomsa.write.format("delta").mode("overwrite").parquet("/mnt/adlsabastosprscus/abastos/gold/inventario_optimo_dacomsa.parquet")




