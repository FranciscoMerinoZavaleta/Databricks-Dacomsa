



keys = ["SOLICITUD_DE_PEDIDO", "POS_SOLICITUD_PEDIDO"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
table = "EBAN"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
eban = (spark
       .read
       .parquet(source_path)
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1))


eban = eban.filter(col("STATUS_TRATAMIENTO_SOLICITUD_PEDIDO")=="5")


eban = eban.filter(~col("INDICADOR_DE_BORRADO").contains("X"))


from pyspark.sql.functions import col, year, month

check = eban.filter(
    (col("CENTRO") == "A751") &
    (year(col("MODIFICADO_EL")) == 2024) &
    (month(col("MODIFICADO_EL")) == 12)
)




table = "EBKN"
keys = ["SOLICITUD_DE_PEDIDO", "POS_SOLICITUD_PEDIDO","NUM_SERIE_IMPUTACION"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
ebkn = (spark.
       read.
       parquet(source_path)).drop("INDICADOR_DE_BORRADO")


ebkn = ebkn.drop("CANTIDAD_SOLICITADA")


table = "T024D"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
t024d = (spark.
       read.
       parquet(source_path)).select("PLANIF_NECESIDADES", "CENTRO", "NOMBRE_PLANIFICADOR")



table = "LFA1"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
lfa1 = (spark.
       read.
       parquet(source_path))


table = "LFB1"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
lfb1 = (spark.
       read.
       parquet(source_path))




eban = (eban.join(t024d, ["PLANIF_NECESIDADES", "CENTRO"], "left"))


solpeds = (eban.join(ebkn,["SOLICITUD_DE_PEDIDO", "POS_SOLICITUD_PEDIDO"],"left")).select(
    "SOLICITUD_DE_PEDIDO", "POS_SOLICITUD_PEDIDO","REG_INFO_DE_COMPRAS","FECHA_DE_SOLICITUD", "MODIFICADO_EL", "CATEGORIA_MATERIAL", "MATERIAL_1", "CANTIDAD_SOLICITADA",
    "TEXTO_BREVE", "UNIDAD_DE_MEDIDA","GRUPO_DE_ARTICULOS", "GRUPO_DE_COMPRAS", "SOLICITANTE_1", "NOMBRE_PLANIFICADOR",
         "PEDIDO", "CANTIDAD_PEDIDA", "FECHA_DE_ENTREGA", col("PROVEEDOR_FIJO").alias("PROVEEDOR"), "VALOR_TOTAL_LIBERACION", "PLAZO_ENTREGA_PREV", "ORGANIZACION_COMPRAS",
         "CENTRO", "ALMACEN","STATUS_TRATAMIENTO","FECHA_ORDEN_COMPRA","FECHA_DE_LIBERACION","PRECIO_DE_VALORACION", "INDICADOR_DE_BORRADO"
          ) 


solpeds = solpeds = (solpeds.withColumn("CENTRO_1",col("CENTRO"))
           .withColumn("ALMACEN_1",col("ALMACEN")))
           


solpeds = solpeds.groupBy(['SOLICITUD_DE_PEDIDO',
 'POS_SOLICITUD_PEDIDO',
 "REG_INFO_DE_COMPRAS",
 'FECHA_DE_SOLICITUD',
 'MODIFICADO_EL',
 'CATEGORIA_MATERIAL',
 'MATERIAL_1',
 'TEXTO_BREVE',
 'UNIDAD_DE_MEDIDA',
 'GRUPO_DE_ARTICULOS',
 'GRUPO_DE_COMPRAS',
 'SOLICITANTE_1',
 "NOMBRE_PLANIFICADOR",
 'PEDIDO',
 'FECHA_DE_ENTREGA',
 'PROVEEDOR',
 'PLAZO_ENTREGA_PREV',
 'ORGANIZACION_COMPRAS',
 'CENTRO',
 'ALMACEN',
 'STATUS_TRATAMIENTO',
 'FECHA_ORDEN_COMPRA',
 'FECHA_DE_LIBERACION',
 'INDICADOR_DE_BORRADO',
 'CENTRO_1',
 'ALMACEN_1']).agg(sum("CANTIDAD_PEDIDA").alias("CANTIDAD_PEDIDA"),sum('VALOR_TOTAL_LIBERACION').alias('VALOR_TOTAL_LIBERACION'),sum('CANTIDAD_SOLICITADA').alias('CANTIDAD_SOLICITADA'),sum('PRECIO_DE_VALORACION').alias('PRECIO_DE_VALORACION'))  #se aplica una agregación sumando valores específicos y se renombran los resultados con .alias() para reflejar el tipo de datos agregados en el dataframe.


columns_to_transpose = [
    "CANTIDAD_PEDIDA",
    'VALOR_TOTAL_LIBERACION',
    'PRECIO_DE_VALORACION',
    'CANTIDAD_SOLICITADA'
]

columns_to_group_by = [
   'SOLICITUD_DE_PEDIDO',
 'POS_SOLICITUD_PEDIDO',
 "REG_INFO_DE_COMPRAS",
 'FECHA_DE_SOLICITUD',
 'MODIFICADO_EL',
 'CATEGORIA_MATERIAL',
 'MATERIAL_1',
 'TEXTO_BREVE',
 'UNIDAD_DE_MEDIDA',
 'GRUPO_DE_ARTICULOS',
 'GRUPO_DE_COMPRAS',
 'SOLICITANTE_1',
 "NOMBRE_PLANIFICADOR",
 'PEDIDO',
 'FECHA_DE_ENTREGA',
 'PROVEEDOR',
 'PLAZO_ENTREGA_PREV',
 'ORGANIZACION_COMPRAS',
 'CENTRO',
 'ALMACEN',
 'STATUS_TRATAMIENTO',
 'FECHA_ORDEN_COMPRA',
 'FECHA_DE_LIBERACION',
 'INDICADOR_DE_BORRADO',
 'CENTRO_1',
 'ALMACEN_1'
]



type = {
    "CANTIDAD_PEDIDA": 'CANTIDAD_PEDIDO',
    "VALOR_TOTAL_LIBERACION": 'CANTIDAD_PEDIDO',
    "PRECIO_DE_VALORACION": "PRECIO",
    "CANTIDAD_SOLICITADA": "CANTIDAD_SOLICITADA"
}


measures = {
    "CANTIDAD_PEDIDA": 'PIEZAS',
    "VALOR_TOTAL_LIBERACION": 'IMPORTE',
    "PRECIO_DE_VALORACION": "IMPORTE",
    "CANTIDAD_SOLICITADA": "PIEZAS"
}




measures= [(MEASURE, TYPE_1) for MEASURE, TYPE_1 in measures.items()]

measures= spark.createDataFrame(measures, ['MEASURE', 'TYPE_1'])



type= [(MEASURE, TYPE_2) for MEASURE, TYPE_2 in type.items()]
type= spark.createDataFrame(type, ['MEASURE', 'TYPE_2'])


type_measures = type.join(
                                        measures,
                                        on = ['MEASURE'],
                                        how = "inner")


solpeds = (solpeds.melt(
    ids=[columns_to_group_by], values=[columns_to_transpose],
    variableColumnName="MEASURE", valueColumnName="VALUE"))


solpeds= solpeds.join(
                                        type_measures,
                                        on = ['MEASURE'],
                                        how = "inner")


solpeds = solpeds.groupBy([
 'SOLICITUD_DE_PEDIDO',
 'POS_SOLICITUD_PEDIDO',
 "REG_INFO_DE_COMPRAS",
 'FECHA_DE_SOLICITUD',
 'MODIFICADO_EL',
 'CATEGORIA_MATERIAL',
 'MATERIAL_1',
 'TEXTO_BREVE',
 'UNIDAD_DE_MEDIDA',
 'GRUPO_DE_ARTICULOS',
 'GRUPO_DE_COMPRAS',
 'SOLICITANTE_1',
 "NOMBRE_PLANIFICADOR",
 'PEDIDO',
 'FECHA_DE_ENTREGA',
 'PROVEEDOR',
 'PLAZO_ENTREGA_PREV',
 'ORGANIZACION_COMPRAS',
 'CENTRO',
 'ALMACEN',
 'STATUS_TRATAMIENTO',
 'FECHA_ORDEN_COMPRA',
 'FECHA_DE_LIBERACION',
 'INDICADOR_DE_BORRADO',
 'CENTRO_1',
 'ALMACEN_1',
 'TYPE_1']).pivot("TYPE_2").agg({"VALUE": "sum"})




catalogo_pais = spark.createDataFrame(
    pd.read_excel(
        "/dbfs/mnt/adlsabastosprscus/abastos/catalogos/T005S.XLSX"
    )
)


catalogo_pais_region = spark.createDataFrame(
    pd.read_excel(
        "/dbfs/mnt/adlsabastosprscus/abastos/catalogos/T005U.XLSX"
    )
)


paises = spark.createDataFrame(
    pd.read_excel(
        "/dbfs/mnt/adlsabastosprscus/abastos/catalogos/PAISES.xlsx"
    )
)


paises = paises.select("Clave de país/región", "`Denomin.país/reg.`")


catalogo_pais = catalogo_pais.select("Clave de país/región", "Región", "Descripción")


pais_region = (paises.join(
    catalogo_pais_region,
    ["Clave de país/región"],
    "inner"
))


pais_region = pais_region.withColumnRenamed("Clave de país/región", "ID_PAIS" )
pais_region = pais_region.withColumnRenamed("Denomin.país/reg.", "PAIS_REG" )
pais_region = pais_region.withColumnRenamed("Región", "ID_ESTADO" )




table = "DIM_PROVEEDOR"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SQL//tablas_SQL/{table}.parquet/"
dim_proveedor = (spark.
       read.
       parquet(source_path))


dim_proveedor= (dim_proveedor.join(
    pais_region,
    ["ID_PAIS", "ID_ESTADO"],
    "inner"
))


table = "dim_proveedor"
target_path = f"/mnt/adlsabastosprscus/abastos/gold/compras/{table}.parquet/"


(dim_proveedor.
    write.format("delta").
    mode("overwrite").
    parquet(target_path))


dim_proveedor = dim_proveedor.select(
 'ID_PROVEEDOR',
 'NOMBRE_1').distinct()




table = "EINE"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
eine = (spark.
       read.
       parquet(source_path))


eine = eine.select('REG_INFO_DE_COMPRAS',
 'CENTRO',
 'ORGANIZACION_COMPRAS',
 'TP_REG_INFO_COMPRAS',
 'DATOS_ORG_COMPRAS',
 'CREADO_EL',
 'MONEDA',
 'DOCUMENTO_COMPRAS',
 'POSICION_1',
 'PRECIO_NETO',
 'UM_PRECIO_PEDIDO',
 'PRECIO_EFECTIVO',
 'INDICADOR_IMPUESTOS')


table = "EINA"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
eina = (spark.
       read.
       parquet(source_path))






eina = eina.select('REG_INFO_DE_COMPRAS',
 'MATERIAL',
 'PROVEEDOR',
 'PAIS_REG_DE_ORIGEN',
 'REGION',
 'REGTRO_INFO_COMPLETO',
 'CORRESPONDE',
 'DENOMINADOR',
 'NO_MATERIAL_DE_PROVEEDOR',
 'CRONOMARCADOR',
 'UNIDAD_MEDIDA_PEDIDO')


table = "KONP"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
konp = (spark.
       read.
       parquet(source_path))


konp = konp.select('NO_REG_CONDICION',
 'IMPORTE_DE_CONDICION',
 'MONEDA_CONDICION',
 'CLASE_DE_CONDICION')


table = "CDHDR"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
cdhdr = (spark.
       read.
       parquet(source_path))


cdhdr= cdhdr.filter(col("OBJETO_DOCUMENTO_MODIFICACION")=="INFOSATZ")


cdhdr= cdhdr.withColumnRenamed("VALOR_DE_OBJETO","REG_INFO_DE_COMPRAS")


cdhdr = cdhdr.select('REG_INFO_DE_COMPRAS').distinct()


table = "A017"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
a017 = (spark.
       read.
       parquet(source_path))


a017 = a017.filter(col("FIN_VALIDEZ")=="9999-12-31")


a017 = a017.select('PROVEEDOR',
 'MATERIAL',
 'ORGANIZACION_COMPRAS',
 'TP_REG_INFO_COMPRAS',
 'FIN_VALIDEZ',
 'FECHA_INICIO_VALIDEZ',
 'NO_REG_CONDICION',
 'APLICACION',
 'CENTRO')


table = "MARA"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
mara = (spark.
       read.
       parquet(source_path))


mara = mara.select("MATERIAL","GRUPO_DE_ARTICULOS")


from pyspark.sql.functions import col, lit, when

eina_eine = (eina
    .join(eine, on=["REG_INFO_DE_COMPRAS"], how="inner"))
    
eina_eine_mara = eina_eine.join(mara, on=["MATERIAL"], how="inner")

eina_eine_mara_a017 = eina_eine_mara.join(a017, on=['PROVEEDOR', 'MATERIAL', 'ORGANIZACION_COMPRAS', 'TP_REG_INFO_COMPRAS', 'CENTRO'], how="left")

eina_eine_mara_a017_cdhdr = eina_eine_mara_a017.join(cdhdr, on=["REG_INFO_DE_COMPRAS"], how="left")

eina_eine_mara_a017_cdhdr_konp = eina_eine_mara_a017_cdhdr.join(konp, on=["NO_REG_CONDICION"], how="left")



reginfo_prov_material = eina_eine_mara_a017_cdhdr_konp.select("REG_INFO_DE_COMPRAS",col("PROVEEDOR").alias("PROVEEDOR_REG_INFO"),col("MATERIAL").alias("MATERIAL_REG_INFO")).distinct()


zinforec = eina_eine_mara_a017_cdhdr_konp.withColumn("CLASE_DE_CONDICION", 
    when(col("CLASE_DE_CONDICION") == "PB00", lit("PRECIO_BASE"))
    .when(col("CLASE_DE_CONDICION") == "FRA1", lit("FLETE"))
    .when(col("CLASE_DE_CONDICION") == "FRA2", lit("EMPAQUE"))
    .when(col("CLASE_DE_CONDICION") == "ZOA1", lit("ADUANA"))
    .otherwise(col("CLASE_DE_CONDICION")))




inforec = zinforec


zinforec = zinforec.groupBy('NO_REG_CONDICION',
 'REG_INFO_DE_COMPRAS',
 'PROVEEDOR',
 'MATERIAL',
 'ORGANIZACION_COMPRAS',
 'TP_REG_INFO_COMPRAS',
 'CENTRO',
 'PAIS_REG_DE_ORIGEN',
 'REGION',
 'REGTRO_INFO_COMPLETO',
 'CORRESPONDE',
 'DENOMINADOR',
 'NO_MATERIAL_DE_PROVEEDOR',
 'CRONOMARCADOR',
 'UNIDAD_MEDIDA_PEDIDO',
 'DATOS_ORG_COMPRAS',
 'CREADO_EL',
 'MONEDA',
 'DOCUMENTO_COMPRAS',
 'POSICION_1',
 'PRECIO_NETO',
 'UM_PRECIO_PEDIDO',
 'PRECIO_EFECTIVO',
 'INDICADOR_IMPUESTOS',
 'GRUPO_DE_ARTICULOS',
 'FIN_VALIDEZ',
 'FECHA_INICIO_VALIDEZ',
 'APLICACION',
 'MONEDA_CONDICION').pivot("CLASE_DE_CONDICION").agg({"IMPORTE_DE_CONDICION": "first"})


from pyspark.sql.functions import col

zinforec_filtered = zinforec.select(
    'NO_REG_CONDICION',
    'REG_INFO_DE_COMPRAS',
    'PROVEEDOR',
    'MATERIAL',
    'ORGANIZACION_COMPRAS',
    'TP_REG_INFO_COMPRAS',
    'CENTRO',
    'PAIS_REG_DE_ORIGEN',
    'REGION',
    'REGTRO_INFO_COMPLETO',
    'CORRESPONDE',
    'DENOMINADOR',
    'NO_MATERIAL_DE_PROVEEDOR',
    'CRONOMARCADOR',
    'UNIDAD_MEDIDA_PEDIDO',
    'DATOS_ORG_COMPRAS',
    'CREADO_EL',
    'MONEDA',
    'DOCUMENTO_COMPRAS',
    'POSICION_1',
    'PRECIO_NETO',
    'UM_PRECIO_PEDIDO',
    'PRECIO_EFECTIVO',
    'INDICADOR_IMPUESTOS',
    'GRUPO_DE_ARTICULOS',
    'FIN_VALIDEZ',
    'FECHA_INICIO_VALIDEZ',
    'APLICACION',
    'MONEDA_CONDICION',
    'PRECIO_BASE'
).filter(col('MONEDA_CONDICION') != '%')

zinforec_condition_filtered = zinforec.filter(
    col('MONEDA_CONDICION') == '%'
).select(
    'NO_REG_CONDICION',
    'REG_INFO_DE_COMPRAS',
    'PROVEEDOR',
    'MATERIAL',
    'ORGANIZACION_COMPRAS',
    'TP_REG_INFO_COMPRAS',
    'CENTRO',
    'PAIS_REG_DE_ORIGEN',
    'REGION',
    'REGTRO_INFO_COMPLETO',
    'CORRESPONDE',
    'DENOMINADOR',
    'NO_MATERIAL_DE_PROVEEDOR',
    'CRONOMARCADOR',
    'UNIDAD_MEDIDA_PEDIDO',
    'DATOS_ORG_COMPRAS',
    'CREADO_EL',
    'MONEDA',
    'DOCUMENTO_COMPRAS',
    'POSICION_1',
    'PRECIO_NETO',
    'UM_PRECIO_PEDIDO',
    'PRECIO_EFECTIVO',
    'INDICADOR_IMPUESTOS',
    'GRUPO_DE_ARTICULOS',
    'FIN_VALIDEZ',
    'FECHA_INICIO_VALIDEZ',
    'APLICACION',
    'ADUANA',
    'EMPAQUE',
    'FLETE',
    'P000',
    'ZARA',
    'ZFIM',
    'ZFNM'
)

zinforec = zinforec_filtered.join(
    zinforec_condition_filtered,
    on=[
        'NO_REG_CONDICION',
        'REG_INFO_DE_COMPRAS',
        'PROVEEDOR',
        'MATERIAL',
        'ORGANIZACION_COMPRAS',
        'TP_REG_INFO_COMPRAS',
        'CENTRO',
        'PAIS_REG_DE_ORIGEN',
        'REGION',
        'REGTRO_INFO_COMPLETO',
        'CORRESPONDE',
        'DENOMINADOR',
        'NO_MATERIAL_DE_PROVEEDOR',
        'CRONOMARCADOR',
        'UNIDAD_MEDIDA_PEDIDO',
        'DATOS_ORG_COMPRAS',
        'CREADO_EL',
        'MONEDA',
        'DOCUMENTO_COMPRAS',
        'POSICION_1',
        'PRECIO_NETO',
        'UM_PRECIO_PEDIDO',
        'PRECIO_EFECTIVO',
        'INDICADOR_IMPUESTOS',
        'GRUPO_DE_ARTICULOS',
        'FIN_VALIDEZ',
        'FECHA_INICIO_VALIDEZ',
        'APLICACION',
    ],
    how='inner'
)



inforec = inforec.groupBy(
    'NO_REG_CONDICION',
    'REG_INFO_DE_COMPRAS',
    'PROVEEDOR',
    'PAIS_REG_DE_ORIGEN',
    'REGION',
    'MATERIAL',
    'ORGANIZACION_COMPRAS',
    'TP_REG_INFO_COMPRAS',
    'CENTRO',
    'REGTRO_INFO_COMPLETO',
    'CORRESPONDE',
    'DENOMINADOR',
    'NO_MATERIAL_DE_PROVEEDOR',
    'CRONOMARCADOR',
    'UNIDAD_MEDIDA_PEDIDO',
    'DATOS_ORG_COMPRAS',
    'CREADO_EL',
    'PRECIO_NETO',
    'PRECIO_EFECTIVO',
    'MONEDA',
    'DOCUMENTO_COMPRAS',
    'POSICION_1',
    'UM_PRECIO_PEDIDO',
    'INDICADOR_IMPUESTOS',
    'GRUPO_DE_ARTICULOS',
    'FIN_VALIDEZ',
    'FECHA_INICIO_VALIDEZ',
    'APLICACION',
).pivot("CLASE_DE_CONDICION").agg({"IMPORTE_DE_CONDICION": "first"})




inforec = inforec.join(inforec.groupBy(
 'REG_INFO_DE_COMPRAS').agg(max('FECHA_INICIO_VALIDEZ').alias('FECHA_INICIO_VALIDEZ')), on  = ['REG_INFO_DE_COMPRAS','FECHA_INICIO_VALIDEZ'], how = 'inner')


inforec = inforec.withColumn("CENTRO_1",col("CENTRO"))


inforec = inforec.groupBy([
 'REG_INFO_DE_COMPRAS',
 col('PROVEEDOR').alias('ID_PROVEEDOR'),
 'MATERIAL',
 'ORGANIZACION_COMPRAS',
 'CENTRO',
 col('PAIS_REG_DE_ORIGEN').alias('ID_PAIS'),
 col('REGION').alias('ID_ESTADO'),
 'PRECIO_NETO',
 'PRECIO_EFECTIVO',
 'MONEDA',
 'GRUPO_DE_ARTICULOS',
 'CENTRO_1']).agg(max('FECHA_INICIO_VALIDEZ').alias('FECHA_INICIO_VALIDEZ'))


inforec = inforec.join(dim_proveedor, on = ['ID_PROVEEDOR'], how = 'left')


inforec = inforec.join(
    pais_region,
    ["ID_PAIS", "ID_ESTADO"],
    "inner"
)


zinforec = zinforec.withColumn("CENTRO_1",col("CENTRO"))


table = "inforecord"
target_path = f"/mnt/adlsabastosprscus/abastos/gold/compras/{table}.parquet/"


(inforec.
    write.format("delta").
    partitionBy('CENTRO_1').
    mode("overwrite").
    parquet(target_path))


table = "condiciones_inforecord"
target_path = f"/mnt/adlsabastosprscus/abastos/gold/compras/{table}.parquet/"


(zinforec.
    write.format("delta").
    partitionBy('CENTRO_1').
    mode("overwrite").
    parquet(target_path))




table = "CATALOGO_DE_ARTICULOS"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/marketing/{table}.parquet/"
catalogo_de_articulos= (spark
       .read
       .parquet(source_path))


catalogo_de_articulos = catalogo_de_articulos.select('MATERIAL',
 'TIPO_MATERIAL',
 'TEXTO_BREVE_DE_MATERIAL',
 'UNIDAD_MEDIDA_BASE',
 'JERARQUIA_PRODUCTOS',
 'GRM',
 'GRM1',
 'GRM2',
 'GRM5',
 'GRUPO_DE_COMPRAS',
 'INDICADOR_ABC',
 'CATEGORIA_VALORACION',
 'CARACT_PLANIF_NEC',
 'TAMANO_LOTE_MINIMO',
 'CENTRO_DE_BENEFICIO',
 'PRECIO_ESTANDAR',
 'PRECIO_VARIABLE',
 'HORIZ_PLANIF_FIJO',
 'GRUPO_DE_ARTICULOS',
 'PLAZO_ENTREGA_PREV',
 'PLANIF_NECESIDADES',
 'VALOR_DE_REDONDEO',
 'SISTEMA',
 'COMPANIA',
 'LINEA_ABIERTA',
 'LINEA_AGRUPADA',
 'MARCA',
 ).distinct().dropDuplicates()


catalogo_de_articulos = catalogo_de_articulos.withColumn("TIPO_MATERIAL_1",col("TIPO_MATERIAL"))


table = "catalogo_de_articulos"
target_path = f"/mnt/adlsabastosprscus/abastos/gold/compras/{table}.parquet/"


(catalogo_de_articulos.
    write.format("delta").
    partitionBy('TIPO_MATERIAL_1').
    mode("overwrite").
    parquet(target_path))


inforec = zinforec.withColumn("MAT_PROV", concat(zinforec["MATERIAL"], zinforec["PROVEEDOR"]))




table = "MATDOC"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
keys =  ["SPLIT_GUID,_PART_1", "SPLIT_GUID,_PART_2", "SPLIT_GUID,_PART_3", "SPLIT_GUID,_PART_4", "SPLIT_GUID,_PART_5", "SPLIT_GUID,_PART_6"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))
mb51 =  (spark.read.parquet(source_path)
         .withColumn("ROW_NUM", row_number().over(particion))
         .filter(col('ROW_NUM')==1)
         .drop("ROW_NUM")
         .select(col("CANTIDAD_EN_UM_PED").alias("CANTIDAD"),  "IMPTE_MON_LOCAL", col("PEDIDO").alias("DOCUMENTO_COMPRAS"),col("POSICION_1").alias("POSICION"),"CENTRO","CLASE_DE_MOVIMIENTO","FE_CONTABILIZACION","FECHA_DE_ENTRADA", "ALMACEN"
                      )
              .filter(col("CLASE_DE_MOVIMIENTO").isin(101,102)
               )
              
        )


mb51 = mb51.withColumn("CANTIDAD", mb51["CANTIDAD"].cast("float"))
mb51 = mb51.withColumn("IMPTE_MON_LOCAL", mb51["IMPTE_MON_LOCAL"].cast("float"))


source_path = f"/mnt/adlsabastosprscus/abastos/silver/CATALOGO_DE_ARTICULOS_SC.parquet"
catalogo_articulos =  (spark.read.parquet(source_path))


mb51 = mb51.withColumn("CANTIDAD",when(col("CLASE_DE_MOVIMIENTO")==102, col("CANTIDAD")*-1).otherwise(col("CANTIDAD"))).withColumn("IMPTE_MON_LOCAL",when(col("CLASE_DE_MOVIMIENTO")==102, col("IMPTE_MON_LOCAL")*-1).otherwise(col("IMPTE_MON_LOCAL")))


mb51 = (mb51.groupBy("DOCUMENTO_COMPRAS", "POSICION", "CENTRO","FE_CONTABILIZACION") \
    .agg(
        sum("CANTIDAD").alias("CANTIDAD"),
        sum("CANTIDAD").alias("CANTIDAD2"),
        sum("IMPTE_MON_LOCAL").alias("IMPTE_MON_LOCAL")
    ).withColumn("PRECIO",col("IMPTE_MON_LOCAL")/col("CANTIDAD"))
        .withColumn("PRECIO2",col("IMPTE_MON_LOCAL")/col("CANTIDAD")))


mb51 = mb51.withColumn("ANIO_MES_ENTRADA", date_format(col("FE_CONTABILIZACION"), "yyyy-MM"))


columns_to_transpose = [
    'CANTIDAD',
    'IMPTE_MON_LOCAL',"PRECIO","PRECIO2","CANTIDAD2"]

columns_to_group_by = [
   "DOCUMENTO_COMPRAS","POSICION","CENTRO","FE_CONTABILIZACION"
   ]



type = {
    "CANTIDAD": 'INGRESOS',
    "CANTIDAD2": 'CANTIDAD_PROVEEDOR',
    'IMPTE_MON_LOCAL': 'INGRESOS',
    'PRECIO':"PRECIO_ENTRADA",
    'PRECIO2':"PRECIO_ENTRADA"
}


measures = {
    "CANTIDAD": 'PIEZAS',
    "CANTIDAD2": 'IMPORTE',
    'IMPTE_MON_LOCAL': 'IMPORTE',
    'PRECIO':'IMPORTE',
    'PRECIO2':'PIEZAS'
}



measures= [(MEASURE, TYPE_1) for MEASURE, TYPE_1 in measures.items()]

measures= spark.createDataFrame(measures, ['MEASURE', 'TYPE_1'])



type= [(MEASURE, TYPE_2) for MEASURE, TYPE_2 in type.items()]
type= spark.createDataFrame(type, ['MEASURE', 'TYPE_2'])


type_measures = type.join(
                                        measures,
                                        on = ['MEASURE'],
                                        how = "inner")


mb51 = mb51.melt(
    ids=columns_to_group_by,
    values=columns_to_transpose,
    variableColumnName="MEASURE",
    valueColumnName="VALUE"
)



mb51= mb51.join(
                                        type_measures,
                                        on = ['MEASURE'],
                                        how = "inner")


mb51 = mb51.groupBy([
"DOCUMENTO_COMPRAS","POSICION","CENTRO","FE_CONTABILIZACION",
 'TYPE_1']).pivot("TYPE_2").agg({"VALUE": "sum"})


mb51 = mb51.withColumn("ANIO_MES_ENTRADA", date_format(col("FE_CONTABILIZACION"), "yyyy-MM"))


mb51 = mb51.withColumn("ANIO_MES_ENTRADA_1",col("ANIO_MES_ENTRADA")).withColumn("CENTRO_1",col("CENTRO"))


table = "entrada"
target_path = f"/mnt/adlsabastosprscus/abastos/gold/compras/{table}.parquet/"


(mb51.
    write.format("delta").
    partitionBy(['CENTRO_1','ANIO_MES_ENTRADA_1']).
    mode("overwrite").
    parquet(target_path))


table = "indicadores_ordenes_compra"
source_path = f"/mnt/adlsabastosprscus/abastos/gold/compras/{table}.parquet/"
ordenes_compra = (spark.
       read.
       parquet(source_path))


ordenes_compra = (ordenes_compra.withColumn('MATERIAL',regexp_replace(col('MATERIAL'), r'^[0]*', '')))


ordenes_compra = ordenes_compra.withColumn("MATERIAL",when(col("MATERIAL").isNull(),lit("SERVICIO")).otherwise(col("MATERIAL"))).withColumn("TIPO_COMPRA",when((~col("MATERIAL").rlike(r'\S')),lit("SERVICIO")).otherwise(col("TIPO_COMPRA"))).withColumn("MATERIAL",when((~col("MATERIAL").rlike(r'\S')),lit("SERVICIO")).otherwise(col("MATERIAL"))).withColumn(
    "MATERIAL1",
    when(
        col("TIPO_COMPRA") == "SERVICIO",
        concat(col("MATERIAL"), lit("-"), col("GRUPO_DE_ARTICULOS"))
    ).otherwise(col("MATERIAL"))
)


ordenes_compra=ordenes_compra.withColumn("ANIO_MES_ENTRADA", date_format(col("FECHA_DE_ENTREGA"), "yyyy-MM")).withColumn("ANIO_MES_DOCUMENTO", date_format(col("FECHA_DOCUMENTO"), "yyyy-MM"))




ordenado = ordenes_compra.select([
 'DOCUMENTO_COMPRAS',
 'POSICION',
 'REPARTO',
 'FECHA_DOCUMENTO',
 "CENTRO",
 'FECHA_DE_ENTREGA',
 'CANTIDAD',
 'PRECIO',
 'TIPO_CAMBIO',
 'TYPE_1',
 'CANTIDAD_MXN',
 'ANIO_MES_ENTRADA',
 'ANIO_MES_DOCUMENTO']).withColumn("ANIO_MES_DOCUMENTO_1",col("ANIO_MES_DOCUMENTO")).withColumn("CENTRO_1",col("CENTRO"))


pivot_ordenes = ordenes_compra.groupBy(['PROVEEDOR',
 'DOCUMENTO_COMPRAS',
 'FECHA_DOCUMENTO',
 'FECHA_DE_ENTREGA',
 'POSICION',
 'SOLICITUD_DE_PEDIDO',
 'UM_PRECIO_PEDIDO',
 'CANTIDAD_BASE',
 'INDICADOR_DE_BORRADO',
 'MONEDA',
 'INCOTERMS',
 'INCOTERMS,_PARTE_2',
 'ORGANIZACION_COMPRAS',
 'CONDICIONES_DE_PAGO',
 'PAGO_EN',
 'NOMBRE_PLANIFICADOR',
 'GRUPO_DE_COMPRAS',
 'ESTADO_LIBERACION',
 'CENTRO',
 'HORIZ_PLANIF_FIJO',
 'TEXTO_BREVE',
 'GRUPO_DE_ARTICULOS',
 'TIPO_COMPRA',
 'REG_INFO_DE_COMPRAS',
 'CLAVE_DE_PAIS_REG',
 'REGION',
 'NOMBRE_1',
 'POBLACION',
 'PAIS_REG',
 'Clave de idioma',
 'Descripción',
 'ANIO_MES_DOCUMENTO']).agg(max("MATERIAL").alias("MATERIAL"),
                            max("MATERIAL1").alias("MATERIAL1"),
                            avg("PRECIO").alias("PRECIO")).withColumn("CENTRO_1",col("CENTRO")).withColumn("ANIO_MES_DOCUMENTO_1",col("ANIO_MES_DOCUMENTO"))


pivot_ordenes = (pivot_ordenes.withColumn('PROVEEDOR',regexp_replace(col('PROVEEDOR'), r'^[0]*', '')))


catalogo_articulos = catalogo_articulos.select("MATERIAL1","MATERIAL","CENTRO","TEXTO_BREVE_DE_MATERIAL",).distinct()


pivot_ordenes = pivot_ordenes.join(catalogo_articulos, on = ["MATERIAL1","MATERIAL","CENTRO"], how = "left" )


pivot_ordenes = pivot_ordenes.filter(~col("DOCUMENTO_COMPRAS").isin("4530120844", "4530120385", "4530120849","4530122245","4530122285","4530122398","4530123311"))



pivot_ordenes = pivot_ordenes.dropDuplicates(subset=["DOCUMENTO_COMPRAS","POSICION", "MATERIAL", "MATERIAL1"])




table = "pivot_ordenes"
target_path = f"/mnt/adlsabastosprscus/abastos/gold/compras/{table}.parquet/"



(pivot_ordenes.
    write.format("delta").
    partitionBy(['CENTRO_1','ANIO_MES_DOCUMENTO_1']).
    mode("overwrite").
    parquet(target_path))


table = "ordenado"
target_path = f"/mnt/adlsabastosprscus/abastos/gold/compras/{table}.parquet/"


(ordenado.
    write.format("delta").
    partitionBy(['CENTRO_1','ANIO_MES_DOCUMENTO_1']).
    mode("overwrite").
    parquet(target_path))




mb51_aux = mb51.select("DOCUMENTO_COMPRAS").distinct()
pivot_ordenes_aux = pivot_ordenes.select("DOCUMENTO_COMPRAS").distinct()

df_aux = mb51_aux.join(pivot_ordenes_aux, on = "DOCUMENTO_COMPRAS", how = "left_anti")



pivot_ordenes_aux = pivot_ordenes.select("DOCUMENTO_COMPRAS").distinct()
ordenado_aux = ordenado.select("DOCUMENTO_COMPRAS").distinct()

df_aux_1 = pivot_ordenes_aux.join(ordenado_aux, on = "DOCUMENTO_COMPRAS", how = "left_anti")





solpeds = solpeds.withColumn("UNIDAD_DE_MEDIDA", regexp_replace("UNIDAD_DE_MEDIDA", "ST", "PC"))


solpeds = (solpeds.withColumn('PROVEEDOR',regexp_replace(col('PROVEEDOR'), r'^[0]*', '')))


solpeds = (solpeds.withColumn('MATERIAL_1',regexp_replace(col('MATERIAL_1'), r'^[0]*', ''))).dropDuplicates()


table = "me5a"
target_path = f"/mnt/adlsabastosprscus/abastos/gold/compras/{table}.parquet/"

(solpeds.
    write.format("delta").
    partitionBy(['CENTRO_1','ALMACEN_1']).
    mode("overwrite").
    parquet(target_path))


