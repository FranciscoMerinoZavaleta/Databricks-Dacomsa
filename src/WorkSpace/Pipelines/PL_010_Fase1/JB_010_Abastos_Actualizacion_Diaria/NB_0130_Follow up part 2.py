

current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month
next_year = current_year + 1
current_anio_mes_follow_up = current_date.strftime("%Y-%m")




table = "follow_up"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"
follow_up= (spark
       .read
       .parquet(source_path))



ingresos = follow_up.select("ANIO_MES","CONCEPTO","ID_MATERIAL","INGRESOS","INGRESOS_FIN").distinct()


follow_up = follow_up.select("ANIO_MES","CONCEPTO","ID_MATERIAL").distinct()


table = "follow_up_final_din"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"
follow_up_din= (spark
       .read
       .parquet(source_path))




table = "follow_up_final_foto"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"
follow_up_foto= (spark
       .read
       .parquet(source_path))


follow_up_foto = follow_up_foto.select('ID_MATERIAL',
 'ANIO_MES',
 'CONCEPTO',
 'STOCK',
 'BACKORDER',
 'PEDIDOS',
 'A717',
 'A718',
 'A780,785 & 790',
 'A730',
 'TOTAL',
 'CANTIDAD',
 'INV_FINAL',
 'INV_FINAL2',
 'BO_EST',
 'RECUPERACION_BO',
 'COBERTURA_DI',
 'INV_INICIO',
 'CANTIDAD_APARTADA',
 'INV_REAL',
 'BO_EST_2')


follow_up_foto = follow_up_foto.withColumn('CANTIDAD',when(col("ANIO_MES")<current_anio_mes_follow_up, 0).otherwise(col("CANTIDAD")))




follow_up_din = follow_up_din.select('ID_MATERIAL',
 'ANIO_MES',
 'CONCEPTO',
 'STOCK_ALM_MAX',
 'STOCK_FINAL',
 'DI',
 'BACKORDER_FIN',
 'PEDIDOS_FIN',
 'VENTA_FIN',
 'PPTO',
 'A717_FIN',
 'A718_FIN',
 'A780,785 & 790_FIN',
 'A730_FIN',
 'TOTAL_FIN',
 'ID_PROVEEDOR',
 'NOMBRE_PROVEEDOR',
 'NOMBRE_PAIS',
 'NOMBRE_ESTADO',
 'PRECIO_BACKORDER',
 'PRECIO_INVENTARIO',
 'PRECIO_DEMANDA_ANUAL',
 'PRECIO_DEMANDA_MENSUAL',
 'COSTO_UNITARIO',
 'INVENTARIO_OPTIMO_VALIDADO',
 'CANTIDAD_FIN',
 'INV_INICIO_DIN',
 'INV_FINAL_DIN',
 'INV_FINAL2_DIN',
 'BO_EST_DIN',
 'RECUPERACION_BO_DIN',
 'COBERTURA_DI_DIN',
 'CANTIDAD_APARTADA_FIN',
 'INV_REAL_FIN',
 'MAX_PRECIO',
 'BO_EST_2_DIN',
 'ANIO_MES_1')


follow_up_final = follow_up.join(follow_up_din, on=["ANIO_MES","CONCEPTO","ID_MATERIAL"], how="left").join(follow_up_foto, on=["ANIO_MES","CONCEPTO","ID_MATERIAL"], how="left").join(ingresos, on=["ANIO_MES","CONCEPTO","ID_MATERIAL"], how="left").dropDuplicates()


follow_up_final = follow_up_final.select('ANIO_MES',
 'CONCEPTO',
 'ID_MATERIAL',
 'STOCK',
 'BACKORDER',
 'PEDIDOS',
 'A717',
 'A718',
 'A780,785 & 790',
 'A730',
 'TOTAL',
 'CANTIDAD',
 'INV_FINAL',
 'INV_FINAL2',
 'BO_EST',
 'RECUPERACION_BO',
 'COBERTURA_DI',
 'INV_INICIO',
 'CANTIDAD_APARTADA',
 'INV_REAL',
 'BO_EST_2',
 'STOCK_ALM_MAX',
 'STOCK_FINAL',
 'DI',
 'BACKORDER_FIN',
 'PEDIDOS_FIN',
 'VENTA_FIN',
 'PPTO',
 'A717_FIN',
 'A718_FIN',
 'A780,785 & 790_FIN',
 'A730_FIN',
 'TOTAL_FIN',
 'ID_PROVEEDOR',
 'NOMBRE_PROVEEDOR',
 'NOMBRE_PAIS',
 'NOMBRE_ESTADO',
 'PRECIO_BACKORDER',
 'PRECIO_INVENTARIO',
 'PRECIO_DEMANDA_ANUAL',
 'PRECIO_DEMANDA_MENSUAL',
 'COSTO_UNITARIO',
 'INVENTARIO_OPTIMO_VALIDADO',
 'CANTIDAD_FIN',
 'INGRESOS',
 'INGRESOS_FIN',
 'INV_INICIO_DIN',
 'INV_FINAL_DIN',
 'INV_FINAL2_DIN',
 'BO_EST_DIN',
 'RECUPERACION_BO_DIN',
 'COBERTURA_DI_DIN',
 'CANTIDAD_APARTADA_FIN',
 'INV_REAL_FIN',
 'MAX_PRECIO',
 'BO_EST_2_DIN',
 'ANIO_MES_1')


follow_up_final = follow_up_final.withColumn('CANTIDAD_FIN',when(col("ANIO_MES")<current_anio_mes_follow_up, 0).otherwise(col("CANTIDAD_FIN")))












table = "follow_up_final"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"


(follow_up_final.dropDuplicates()
    .write.format("delta").
    partitionBy('ANIO_MES_1').
    mode("overwrite").
    parquet(target_path))








