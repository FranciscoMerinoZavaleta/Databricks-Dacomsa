





table = "FACT_IND_SD003_MARGEN_CLIENTE_PRODUCTO"
fact_margen_cliente_producto = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))                      
fact_margen_cliente_producto = (fact_margen_cliente_producto.
                     select('ANIO_MES',	'ID_COMPANIA',	'ID_SISTEMA',	'ID_LINEA_ABIERTA','MERCADO',	'ID_CLIENTE',	'UNIDAD_MEDIDA',	'MONEDA',	"MATNR",'TIPO_CAMBIO',	'CANTIDAD',	'VENTA TOTAL',	'COSTO_VENTA_FACT',	'MARGEN_CLIENTE',	'MARGEN_CLIENTE_PRC',	'COSTO_UNIT_FABRICACION',	'COSTO_VENTA_STD_FABR',	'COSTO_VENTA_PRECIO_STD_PLANTA',	'COSTO_VENTA_PRECIO_VAR_PLANTA',	'COSTO_VENTA_PRECIO_STD_VAR_PLANTA',	'VENTA_TOTAL_MONEDA_FACT',	'TOTAL_VENTA_CABECERO_FACT','FECCARGA')
                    )        



fact_margen_cliente_producto = fact_margen_cliente_producto.distinct()


fact_margen_cliente_producto = fact_margen_cliente_producto.na.drop(how='all')


agrupador_margen_cliente_producto = fact_margen_cliente_producto.groupBy("ANIO_MES").agg(max("FECCARGA").alias("FECCARGA"))




fact_margen_cliente_producto = fact_margen_cliente_producto.join(agrupador_margen_cliente_producto,
                         on = ["ANIO_MES","FECCARGA"],
                         how = "inner")


fact_margen_cliente_producto= (fact_margen_cliente_producto
            .withColumnRenamed("MERCADO","ID_CANAL_DISTRIBUCION")
            .withColumnRenamed("MATNR","ID_MATERIAL")
             )


fact_margen_cliente_producto= (fact_margen_cliente_producto.withColumn('ID_MATERIAL',regexp_replace(col('ID_MATERIAL'), r'^[0]*', '')))


table = 'fact_margen_cliente_producto'
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"


(fact_margen_cliente_producto.
    write.format('delta').
    partitionBy('ANIO_MES','ID_CANAL_DISTRIBUCION').
    mode("overwrite").
    parquet(target_path))




table = "FACT_IND_SD_FILLRATE"
fact_servicio = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))                  
fact_servicio = (fact_servicio.
                     select('ANIO_MES',
 'PERIODO',
 'ID_PLANTA',
 'PLANTA',
 'ID_SISTEMA',
 'SISTEMA',
 'LINEA_AGRUPADA',
 'ID_CLIENTE',
 'CLIENTE',
 'ID_INDICADOR_ABC',
 'NUM_ITEMS',
 'CANT_PEDIDO',
 'CANT_FACTURA',
 'CANT_RETENIDA',
 'CANT_FACTURA_AJUSTADA',
 'IMP_PEDIDO',
 'IMP_FACTURA',
 'IMP_RETENIDO',
 'IMP_FACTURA_AJUSTADA',
 'POS_SOLICITADAS',
 'POS_ENTREGADAS',
 'FECCARGA')
)


fact_servicio = fact_servicio.na.drop(how='all')



agrupador_servicio = fact_servicio.groupBy("ANIO_MES").agg(max("FECCARGA").alias("FECCARGA"))


fact_servicio = fact_servicio.join(agrupador_servicio,
                         on = ["ANIO_MES","FECCARGA"],
                         how = "inner")


fact_servicio= (fact_servicio.withColumn("ID_PRD",concat(col("ID_PLANTA"), lit("-"),col("ID_SISTEMA"), lit("-"),col("LINEA_AGRUPADA"), lit("-"),col("ID_INDICADOR_ABC"))))


table = "fact_servicio"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"


(fact_servicio.
    write.format('delta').
    partitionBy('ANIO_MES','ID_SISTEMA').
    mode("overwrite").
    parquet(target_path))




table = "TMP_IND_ECC_MARD"
fact_inventario = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))                  
fact_inventario = (fact_inventario.
                     select('MATNR',	'WERKS',	'LGORT',	'PSTAT',	'LVORM',	'LFGJA',	'LFMON',	'SPERR',	'LABST',	'UMLME',	'INSME',	'EINME',	'SPEME',	'RETME',	'KLABS',	'KINSM',	'KEINM',	'KSPEM','FECCARGA'
                    ).filter(col('WERKS')=='A717')
                    )


fact_inventario = (fact_inventario.withColumnRenamed("MATNR","ID_MATERIAL")
.withColumnRenamed("WERKS","ID_CENTRO")
.withColumnRenamed("LGORT","ID_ALMACEN")
.withColumnRenamed("PSTAT","ESTATUS")
.withColumnRenamed("LVORM","ELIMINACION")
.withColumnRenamed("LFGJA","ANIO_ULTIMA_ACTUALIZACION_INVENTARIO")
.withColumnRenamed("LFMON","MES_ULTIMA_ACTUALIZACION_INVENTARIO")
.withColumnRenamed("SPERR","INVENTARIO_BLOQUEO")
.withColumnRenamed("LABST","LIBRE_UTILIZACION")
.withColumnRenamed("UMLME","EN_TRASLADO")
.withColumnRenamed("INSME","INSPECCION_DE_CALIDAD")
.withColumnRenamed("EINME","STOCK_NO_LIBRE")
.withColumnRenamed("SPEME","BLOQUEADO")
.withColumnRenamed("RETME","DEVOLUCIONES")
.withColumnRenamed("KLABS","CONSIG_LIBRE_UTILIZACION")
.withColumnRenamed("KINSM","CONSIG_CONTROL_CALIDAD")
.withColumnRenamed("KEINM","CONSIG_NO_LIBRE")
.withColumnRenamed("KSPEM","CONSIG_BLOQUEADO")
             )


fact_inventario = (fact_inventario.withColumn(
    "ANIO_MES", 
    when(
        length(month(date_sub(col("FECCARGA"), 1))) == 2,
        concat(year(date_sub(col("FECCARGA"), 1)), lit("-"), month(date_sub(col("FECCARGA"), 1)))
    ).otherwise(
        concat(year(date_sub(col("FECCARGA"), 1)), lit("-0"), month(date_sub(col("FECCARGA"), 1)))
    )
)
                   .withColumn("ANIO_MESULTIMA_ACTUALIZACION_INVENTARIO", 
                    when(length(col("MES_ULTIMA_ACTUALIZACION_INVENTARIO").cast("int")) == 2, 
                         concat(col("ANIO_ULTIMA_ACTUALIZACION_INVENTARIO").cast("int"), lit("-"), col("MES_ULTIMA_ACTUALIZACION_INVENTARIO").cast("int")))
                    .otherwise(concat(col("ANIO_ULTIMA_ACTUALIZACION_INVENTARIO").cast("int"), lit("-0"), col("MES_ULTIMA_ACTUALIZACION_INVENTARIO").cast("int"))))
                   )



fact_inventario.select("ID_ALMACEN","FECCARGA").distinct().filter(col("ID_ALMACEN")=="A730").orderBy(col("FECCARGA").desc()).display()


agrupador_inventario = fact_inventario.groupBy("ANIO_MES").agg(max("FECCARGA").alias("FECCARGA"))


fact_inventario = fact_inventario.join(agrupador_inventario,
                         on = ["ANIO_MES","FECCARGA"],
                         how = "inner")


fact_inventario = fact_inventario.na.drop(how='all')


fact_inventario = (fact_inventario.withColumn('ID_MATERIAL',regexp_replace(col('ID_MATERIAL'), "^0{2,}", "")))


table = "fact_inventario"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"


(fact_inventario.
    write.format('delta').
    partitionBy('ANIO_MES','ID_ALMACEN').
    mode("overwrite").
    parquet(target_path))




table = "FACT_IND_OP006_HAB_INV"
fact_hab_inventario = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))                  
fact_hab_inventario = (fact_hab_inventario.
                     select('ANIO_MES',	'ID_COMPANIA',	'ID_SISTEMA',		'ID_MATERIAL',	'HABILIDAD_ITEM',	'STOCK_UNIDADES',	'STOCK_VALOR',	'PRONOSTICO_UNIDADES',	'PRONOSTICO_VALOR',	'VENTA_UNIDADES',	'VENTA_IMPORTE',	'VENTA_IMPORTE_REAL',	'PEDIDO_UNIDADES',	'PEDIDO_IMPORTE',	'PEDIDO_IMPORTE_REAL',	'PEDIDO_CANCELADO_UNIDADES',	'PEDIDO_CANCELADO_IMPORTE',	'PEDIDO_CANCELADO_IMPORTE_REAL',	'PEDIDO_RETENIDO_UNIDADES',	'PEDIDO_RETENIDO_IMPORTE',	'PEDIDO_RETENIDO_IMPORTE_REAL',	'IND_MATERIAL_VENTA',	'ORIGEN_PRONOSTICO', 'FECCARGA','FLAG_CS'
                    )
                    )


fact_hab_inventario= fact_hab_inventario.na.drop(how='all')


agrupador_hab_inventario = fact_hab_inventario.groupBy("ANIO_MES").agg(max("FECCARGA").alias("FECCARGA"))


fact_hab_inventario = fact_hab_inventario.join(agrupador_hab_inventario,
                         on = ["ANIO_MES","FECCARGA"],
                         how = "inner")


fact_hab_inventario = (fact_hab_inventario.withColumn('ID_MATERIAL',regexp_replace(col('ID_MATERIAL'), "^0{2,}", '')))


table = "fact_hab_inventario"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"


(fact_hab_inventario.
    write.format('delta').
    partitionBy('ANIO_MES').
    mode("overwrite").
    parquet(target_path))