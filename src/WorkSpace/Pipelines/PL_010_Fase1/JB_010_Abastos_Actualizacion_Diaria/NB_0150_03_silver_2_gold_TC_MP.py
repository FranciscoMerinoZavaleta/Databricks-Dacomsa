



table = "MARA"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
mara = (spark.
       read.
       parquet(source_path))


table = "MBEW"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
mbew = (spark.
       read.
       parquet(source_path))

keys = ["MATERIAL", "AREA_DE_VALORACION", "CLASE_DE_VALORACION"]
window_mbew = Window.partitionBy(keys).orderBy(desc("SEQ_DATE"))

mbew = (mbew
        .withColumn("row_num", row_number().over(window_mbew))
        .filter(col("row_num")==1)
        .drop("row_num")
        .withColumn("COSTO_UNITARIO", when(col("CONTROL_DE_PRECIOS") == "S", col("PRECIO_ESTANDAR")/col("CANTIDAD_BASE")).otherwise(col("PRECIO_VARIABLE")/col("CANTIDAD_BASE")))
        )


table = "TMP_IND_ECC_MARD"
mard = (spark
        .read
        .parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/")
        .withColumn("FECCARGA", to_date(col("FECCARGA"),"yyyy-MM-dd"))
        .withColumn("ANIO_MES", to_date(col("FECCARGA"),"yyyy-MM-dd"))
        .withColumn("ANIO_MES", date_format(col("ANIO_MES"),"yyyy-MM"))
        .withColumn("ANIO_MES",col("ANIO_MES").cast("date"))
        .filter(col("FECCARGA") == col("ANIO_MES"))
)
                     

keys = ["MANDT","MATNR","WERKS","LGORT","ANIO_MES"]


window_mard = Window.partitionBy(keys).orderBy(asc("FECCARGA"))

mard = (mard
        .withColumn("row_num", row_number().over(window_mard))
        .filter(col("row_num")==1)
        .drop("row_num"))


keys = ["MATERIAL","CENTRO"]
window_marc = Window.partitionBy(keys).orderBy(desc("SEQ_DATE"))

table = "MARC"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
marc = (spark.
       read.
       parquet(source_path)
       .where(col("CENTRO").contains("A7"))
       .withColumn("row_num", row_number().over(window_marc))
       .filter(col("row_num")==1)
       .drop("row_num")
       .withColumn("PUNTO_DE_PEDIDO", col("PUNTO_DE_PEDIDO").cast("double"))
       .withColumn("STOCK_ALM_MAX", col("STOCK_ALM_MAX").cast("double"))
       )








mard = (mard
        .withColumnsRenamed({
            "MANDT": "MANDANTE",
            "MATNR": "MATERIAL",
            "WERKS": "CENTRO",
            "LGORT": "ALMACEN",
            "LABST": "LIBRE_UTILIZACION",
            "INSME": "INSPECCION_CALIDAD",
            "EINME": "STOCK_NO_LIBRE",
            "SPEME": "STOCK_BLOQUEADO",
            "RETME": "DEVOLUCIONES"
        })
        .select("MANDANTE", "MATERIAL", "CENTRO", "ALMACEN","LIBRE_UTILIZACION", "INSPECCION_CALIDAD", "STOCK_NO_LIBRE","STOCK_BLOQUEADO", "DEVOLUCIONES", "FECCARGA", "ANIO_MES"))




materiales = (mard
        .withColumn("AREA_DE_VALORACION", col("CENTRO"))
        .join(marc
              .select("MANDANTE","MATERIAL","CENTRO","STOCK_ALM_MAX","PUNTO_DE_PEDIDO"),
              on = ["MANDANTE","MATERIAL","CENTRO"],
              how = "left")
        .join(mara
            .select("MANDANTE","MATERIAL","GRUPO_DE_ARTICULOS","UNIDAD_MEDIDA_BASE"),
            on = ["MANDANTE","MATERIAL"],
            how = "left")
        .join(mbew
            .select("MANDANTE","MATERIAL","AREA_DE_VALORACION","COSTO_UNITARIO"),
            on = ["MANDANTE","MATERIAL","AREA_DE_VALORACION"],
            how = "left")
        .withColumn("UNIDAD_MEDIDA_BASE",
                    when(col("UNIDAD_MEDIDA_BASE")=="ST","PC")
                    .otherwise(col("UNIDAD_MEDIDA_BASE")))
        .withColumn("VALOR_LIBRE_UTILIZACION", col("LIBRE_UTILIZACION")*col("COSTO_UNITARIO"))
        )






materiales = (materiales
 .withColumnsRenamed({
         "LIBRE_UTILIZACION": "INVENTARIO_PZAS",
        "STOCK_ALM_MAX": "STOCK_MAX_PZAS",
        "PUNTO_DE_PEDIDO": "PUNTO_DE_PEDIDO_PZAS"
 })
 .select("MATERIAL","CENTRO","ALMACEN","ANIO_MES", "UNIDAD_MEDIDA_BASE",
         "INVENTARIO_PZAS", "STOCK_MAX_PZAS", "PUNTO_DE_PEDIDO_PZAS", "COSTO_UNITARIO")
 .withColumn("INVENTARIO_IMPORTE", col("COSTO_UNITARIO")*col("INVENTARIO_PZAS"))
 .withColumn("STOCK_MAX_IMPORTE", col("COSTO_UNITARIO")*col("STOCK_MAX_PZAS"))
 .withColumn("PUNTO_DE_PEDIDO_IMPORTE", col("COSTO_UNITARIO")*col("PUNTO_DE_PEDIDO_PZAS"))
 .drop("PRECIO_VARIABLE")
 .groupBy("MATERIAL","CENTRO","ALMACEN","ANIO_MES", "UNIDAD_MEDIDA_BASE")
 .agg(sum(col("INVENTARIO_PZAS")).alias("INVENTARIO_PZAS"),
      sum(col("STOCK_MAX_PZAS")).alias("STOCK_MAX_PZAS"),
      sum(col("PUNTO_DE_PEDIDO_PZAS")).alias("PUNTO_DE_PEDIDO_PZAS"),
      sum(col("INVENTARIO_IMPORTE")).alias("INVENTARIO_IMPORTE"),
      sum(col("STOCK_MAX_IMPORTE")).alias("STOCK_MAX_IMPORTE"),
      sum(col("PUNTO_DE_PEDIDO_IMPORTE")).alias("PUNTO_DE_PEDIDO_IMPORTE")))




pzas_df = (materiales
    .withColumnsRenamed({
    "INVENTARIO_PZAS": "INVENTARIO",
    "STOCK_MAX_PZAS": "STOCK_MAX",
    "PUNTO_DE_PEDIDO_PZAS": "PUNTO_DE_PEDIDO"
    })
    .select("MATERIAL", "CENTRO", "ALMACEN", "ANIO_MES", "UNIDAD_MEDIDA_BASE","INVENTARIO","STOCK_MAX","PUNTO_DE_PEDIDO")
    .withColumn("TYPE_1",lit("PIEZAS"))
    )


importe_df = (materiales
    .withColumnsRenamed({
    "INVENTARIO_IMPORTE": "INVENTARIO",
    "STOCK_MAX_IMPORTE": "STOCK_MAX",
    "PUNTO_DE_PEDIDO_IMPORTE": "PUNTO_DE_PEDIDO"
    })
    .select("MATERIAL", "CENTRO", "ALMACEN", "ANIO_MES", "UNIDAD_MEDIDA_BASE","INVENTARIO","STOCK_MAX","PUNTO_DE_PEDIDO")
    .withColumn("TYPE_1",lit("IMPORTE"))
    )


inventario_optimo_mp = (pzas_df.unionByName(importe_df, allowMissingColumns=True))




inventario_optimo_mp = (inventario_optimo_mp
                        .withColumnsRenamed({"STOCK_MAX":"INVENTARIO_OPT",
                                             "PUNTO_DE_PEDIDO":"PUNTO_DE_REORDEN_DAC",
                                             "MATERIAL":"ID_MATERIAL",
                                             "ANIO_MES": "MES_PRONO_AUX"})
                        .groupBy('ID_MATERIAL','TYPE_1','MES_PRONO_AUX','INVENTARIO_OPT',"CENTRO","ALMACEN", "PUNTO_DE_REORDEN_DAC","UNIDAD_MEDIDA_BASE")
                        .agg(sum(col("INVENTARIO")).alias("INVENTARIO"))
                        )




porcentaje_max = 1.255
inventario_optimo_mp = (inventario_optimo_mp
 .withColumn("SIN_INVENTARIO_OPTIMO", 
             when((col("INVENTARIO_OPT") == 0) | (col("INVENTARIO_OPT") == "") | (col("INVENTARIO_OPT").isNull()), col("INVENTARIO")))
 .withColumn(
    "MENOR_IGUAL_AL_INVENTARIO_OPTIMO",
    when(
        col("SIN_INVENTARIO_OPTIMO") != 0,lit(None))
    .otherwise(
        when(
            col("INVENTARIO") <= (col("INVENTARIO_OPT") * porcentaje_max), col("INVENTARIO")
    )
    .otherwise(
        col("INVENTARIO_OPT") * porcentaje_max
        )
    )
 )
 .withColumn(
    "CANTIDAD_EXCEDENTE_DE_INVENTARIO_OPT",
    when(
        col("SIN_INVENTARIO_OPTIMO") != 0,lit(None)
        )
    .otherwise(
        when(
            col("INVENTARIO_OPT") > 0, col("INVENTARIO") - col("MENOR_IGUAL_AL_INVENTARIO_OPTIMO")
        )
    .otherwise(
        0
        )
    )
)
)




inventario_optimo_mp = (
    inventario_optimo_mp.select(
        "ID_MATERIAL",
        "TYPE_1",
        "MES_PRONO_AUX",
        "INVENTARIO_OPT",
        "INVENTARIO",
        "CENTRO",
        "ALMACEN",
        "UNIDAD_MEDIDA_BASE",
        "MENOR_IGUAL_AL_INVENTARIO_OPTIMO",
    )
    .withColumnRenamed("MENOR_IGUAL_AL_INVENTARIO_OPTIMO", "CANTIDAD")
    .withColumn("FLAG_INVENTARIO_OPTIMO", lit("MENOR-IGUAL A INVENTARIO OPTIMO"))
    .dropDuplicates()
    .unionByName(
        inventario_optimo_mp.select(
            "ID_MATERIAL",
            "TYPE_1",
            "MES_PRONO_AUX",
            "INVENTARIO_OPT",
            "INVENTARIO",
            "CENTRO",
            "ALMACEN",
            "UNIDAD_MEDIDA_BASE",
            "CANTIDAD_EXCEDENTE_DE_INVENTARIO_OPT",
        )
        .withColumnRenamed("CANTIDAD_EXCEDENTE_DE_INVENTARIO_OPT", "CANTIDAD")
        .withColumn("FLAG_INVENTARIO_OPTIMO", lit("EXCEDENTE"))
    )
    .unionByName(
        inventario_optimo_mp.select(
            "ID_MATERIAL",
            "TYPE_1",
            "MES_PRONO_AUX",
            "INVENTARIO_OPT",
            "INVENTARIO",
            "CENTRO",
            "ALMACEN",
            "UNIDAD_MEDIDA_BASE",
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
                when(((col("FLAG_SUB_O_EXCESO_DE_INV") == "INVENTARIO EN EXCESO") &
                      ((col("NO_DE_VECES_INVENTARIO_OPTIMO") > 1.8))) |
                                 ((col("FLAG_SUB_O_EXCESO_DE_INV") == "SUB-INVENTARIO") &
                                  (col("NO_DE_VECES_INVENTARIO_OPTIMO") < 0.3)), "ROJO")
                .otherwise(when(((col("FLAG_SUB_O_EXCESO_DE_INV") == "INVENTARIO EN EXCESO") &
                                 (col("NO_DE_VECES_INVENTARIO_OPTIMO") > 1) & (col("NO_DE_VECES_INVENTARIO_OPTIMO") <= 1.8)) |
                                ((col("FLAG_SUB_O_EXCESO_DE_INV") == "SUB-INVENTARIO") & (col("NO_DE_VECES_INVENTARIO_OPTIMO") > 0.3) &
                                 (col("NO_DE_VECES_INVENTARIO_OPTIMO") < 0.75)), "AMARILLO")
                           .otherwise(when(((col("FLAG_SUB_O_EXCESO_DE_INV") == "INVENTARIO EN EXCESO") & (col("NO_DE_VECES_INVENTARIO_OPTIMO") <= 1) |
                                            (col("FLAG_SUB_O_EXCESO_DE_INV") == "SUB-INVENTARIO") &
                                            (col("NO_DE_VECES_INVENTARIO_OPTIMO") <= 1) & (col("NO_DE_VECES_INVENTARIO_OPTIMO") >= 0.75) |
                                            (col("FLAG_SUB_O_EXCESO_DE_INV") == "INVENTARIO OPTIMO")), "VERDE")
                                      .otherwise(when(col("INVENTARIO_OPT") == 0, "GRIS"))
                           )
                                )
                )
    .filter((col("CANTIDAD").isNotNull()) & (col("SEMAFORO").isNotNull()))
    .dropDuplicates()
    .orderBy("ID_MATERIAL", "TYPE_1", "MES_PRONO_AUX", "SEMAFORO")
    .withColumn("ID_MATERIAL", regexp_replace(col("ID_MATERIAL"), "^0{2,}", ""))
    .withColumn("CENTRO_1", col("CENTRO"))
    .fillna(0)
)




(inventario_optimo_mp
 .write
 .format("delta")
 .mode("overwrite")
 .partitionBy("CENTRO_1")
 .parquet("/mnt/adlsabastosprscus/abastos/gold/inventario_optimo_mp_dacomsa.parquet/"))
