





table = "FACT_IND_OP006_HAB_INV"
fact_hab_inventario = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))                  
fact_hab_inventario = (fact_hab_inventario.
                     select('ANIO_MES',	'ID_COMPANIA',	'ID_SISTEMA',		'ID_MATERIAL',	'HABILIDAD_ITEM',	'STOCK_UNIDADES',	'STOCK_VALOR',	'PRONOSTICO_UNIDADES',	'PRONOSTICO_VALOR',	'VENTA_UNIDADES',	'VENTA_IMPORTE',	'VENTA_IMPORTE_REAL',	'PEDIDO_UNIDADES',	'PEDIDO_IMPORTE',	'PEDIDO_IMPORTE_REAL',	'PEDIDO_CANCELADO_UNIDADES',	'PEDIDO_CANCELADO_IMPORTE',	'PEDIDO_CANCELADO_IMPORTE_REAL',	'PEDIDO_RETENIDO_UNIDADES',	'PEDIDO_RETENIDO_IMPORTE',	'PEDIDO_RETENIDO_IMPORTE_REAL',	'IND_MATERIAL_VENTA',	'ORIGEN_PRONOSTICO', 'FECCARGA','FLAG_CS'
                    )
                    )


fact_hab_inventario= fact_hab_inventario.na.drop(how='all')


agrupador_hab_inventario = fact_hab_inventario.groupBy("ANIO_MES").agg(min("FECCARGA").alias("FECCARGA"))


fact_hab_inventario = fact_hab_inventario.join(agrupador_hab_inventario,
                         on = ["ANIO_MES","FECCARGA"],
                         how = "inner")


fact_hab_inventario = fact_hab_inventario.select(['ANIO_MES',
 'ID_COMPANIA',
 'ID_SISTEMA',
 'ID_MATERIAL',
 'HABILIDAD_ITEM',
 'STOCK_UNIDADES',
 'STOCK_VALOR',
 'PRONOSTICO_UNIDADES',
 'PRONOSTICO_VALOR',
 'VENTA_UNIDADES',
 'VENTA_IMPORTE',
 'VENTA_IMPORTE_REAL',
 'PEDIDO_UNIDADES',
 'PEDIDO_IMPORTE',
 'PEDIDO_IMPORTE_REAL',
 'PEDIDO_CANCELADO_UNIDADES',
 'PEDIDO_CANCELADO_IMPORTE',
 'PEDIDO_CANCELADO_IMPORTE_REAL',
 'PEDIDO_RETENIDO_UNIDADES',
 'PEDIDO_RETENIDO_IMPORTE',
 'PEDIDO_RETENIDO_IMPORTE_REAL',
 'IND_MATERIAL_VENTA',
 'ORIGEN_PRONOSTICO',
 'FLAG_CS']).distinct()


fact_hab_inventario = (fact_hab_inventario.withColumn('ID_MATERIAL',regexp_replace(col('ID_MATERIAL'), "^0{2,}", '')))


table = "fact_hab_inventario_inicio"
target_path = f"/mnt/adlsabastosprscus/abastos/silver/{table}.parquet/"


(fact_hab_inventario.
    write.format('delta').
    partitionBy('ANIO_MES').
    mode("overwrite").
    parquet(target_path))