

current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month
current_day = current_date.day
filter_date = f"{current_year}-{current_month}-{current_day}T00:00:00.000+00:00"


table = "A017"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
a017 = (spark.
       read.
       parquet(source_path))


table = "EINA"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
eina = (spark.
       read.
       parquet(source_path))


table = "EKBE"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
ekbe = (spark.
       read.
       parquet(source_path))


ekbe = ekbe.filter(col("TIPO_DE_HISTORIAL_DE_PEDIDO")=="D").select("DOCUMENTO_COMPRAS","POSICION","DOCUMENTO_MATERIAL").distinct()


table = "BAPI_ENTRYSHEET_GETDETAIL"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
bapi = (spark.
       read.
       parquet(source_path))


bapi = bapi.filter(
    (col("SERVICIO").isNotNull()) & 
    (col("SERVICIO") != '') & 
    (col("SERVICIO").rlike(r'\S')))



bapi = bapi.select(col("NUMERO_DOCUMENTO").alias("DOCUMENTO_MATERIAL"),"SERVICIO").distinct()


ekbe= ekbe.join(bapi,on = "DOCUMENTO_MATERIAL", how = 'inner')


ekbe = ekbe.select("DOCUMENTO_COMPRAS","POSICION","SERVICIO").distinct()


table = "EKKO"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
ekko = (spark.
       read.
       parquet(source_path)
       .drop("POSICION").withColumnRenamed("SOCIEDAD","CENTRO")
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





table = "MATDOC"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
matdoc = (spark.
       read.
       parquet(source_path)
       )


table = "EKPO"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
ekpo = (spark.
       read.
       parquet(source_path)
       .drop("FECHA_DOCUMENTO")
       .drop("PROVEEDOR")
       .drop("CENTRO")
       .drop("INDICADOR_DE_BORRADO")
       ).filter(col("SEQ_DATE")>filter_date)


ekpo = ekpo.withColumn("MATERIAL_1", when(col("TIPO_DE_POSICION") == '9', lit("SERVICIO")).otherwise(col("MATERIAL")))


ekpo = ekpo.drop("CANTIDAD_CONFIRMADA", "INCOTERMS_LUGAR_1", "ORGANIZACION_COMPRAS", "INCOTERMS", "INCOTERMS,_PARTE_2","FECHA_DOCUMENTO")



table = "EKET"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
eket = (spark.
       read.
       parquet(source_path)
       ).filter(col("SEQ_DATE")>filter_date)


eket = eket.drop("SOLICITUD_DE_PEDIDO","FECHA_DOCUMENTO")


table = "EBAN"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
eban = (spark.
       read.
       parquet(source_path))


table = "MARC"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
marc = (spark.
       read.
       parquet(source_path))


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


catalogo_pais = spark.createDataFrame(
    pd.read_excel(
        "/dbfs/mnt/adlsabastosprscus/abastos/catalogos/T005S.XLSX"))

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


pais = paises
pais = pais.withColumnRenamed("Clave de país/región", "CLAVE_DE_PAIS_REG" )
pais = pais.withColumnRenamed("Denomin.país/reg.", "PAIS_REG" )

pais = pais.select("CLAVE_DE_PAIS_REG","PAIS_REG").distinct()

catalogo_pais = catalogo_pais.select("Clave de país/región", "Región", "Descripción")

pais_region = (paises.join(
    catalogo_pais_region,
    ["Clave de país/región"],
    "inner"
))

pais_region = pais_region.withColumnRenamed("Clave de país/región", "CLAVE_DE_PAIS_REG" )
pais_region = pais_region.withColumnRenamed("Denomin.país/reg.", "PAIS_REG" )
pais_region = pais_region.withColumnRenamed("Región", "REGION" )




marc = (marc.join(t024d, ["PLANIF_NECESIDADES", "CENTRO"], "left"))


marc_oc = marc.select("MATERIAL", "HORIZ_PLANIF_FIJO","CENTRO", "PLANIF_NECESIDADES", "NOMBRE_PLANIFICADOR")


matdoc_oc = matdoc


indicadores_oc = (ekko.join(eket,["DOCUMENTO_COMPRAS"],"left")
                  .dropDuplicates()
                   .join(ekpo,["DOCUMENTO_COMPRAS","POSICION"],"left")
                   .join(marc_oc,["MATERIAL","CENTRO"],"left")
                   .select("DOCUMENTO_COMPRAS","FECHA_DOCUMENTO","POSICION","MATERIAL","MATERIAL_1","TEXTO_BREVE","REPARTO","SOLICITUD_DE_PEDIDO","CANTIDAD_DE_PEDIDO","UNIDAD_MEDIDA_PEDIDO" ,"CANTIDAD_ENTREGADA", "CANTIDAD_DE_REPARTO", "CANTIDAD_CONFIRMADA", "PROVEEDOR","UM_PRECIO_PEDIDO","MONEDA","VALOR_NETO_DE_PEDIDO_1", "CANTIDAD_BASE","PRECIO_NETO_PEDIDO","TIPO_CAMBIO_MONEDA", 'INCOTERMS','INCOTERMS,_PARTE_2','ORGANIZACION_COMPRAS','CONDICIONES_DE_PAGO','PAGO_EN', "GRUPO_DE_ARTICULOS", "NOMBRE_PLANIFICADOR", "PLANIF_NECESIDADES","FECHA_DE_PEDIDO",
                    "GRUPO_DE_COMPRAS","ESTADO_LIBERACION","CENTRO", "HORIZ_PLANIF_FIJO","FECHA_DE_ENTREGA","INDICADOR_DE_BORRADO",'REG_INFO_DE_COMPRAS')
)      





indicadores_oc = (indicadores_oc.withColumn("ESTADO_LIBERACION", trim(col("ESTADO_LIBERACION"))).withColumn("MATERIAL",col("MATERIAL_1")).withColumn("PRECIO",col("PRECIO_NETO_PEDIDO")))




indicadores_oc = indicadores_oc.join(ekbe, on = ["DOCUMENTO_COMPRAS","POSICION"], how = 'left').withColumn("TIPO_COMPRA",when(col("MATERIAL")=='SERVICIO',"SERVICIO").otherwise("MATERIAL")).withColumn("MATERIAL",when(col("MATERIAL")=='SERVICIO',col("SERVICIO")).otherwise(col("MATERIAL")))



 columns_to_transpose = [
 'VALOR_NETO_DE_PEDIDO_1',
 'PRECIO_NETO_PEDIDO',
 'PRECIO',
 'TIPO_CAMBIO_MONEDA',
 'CANTIDAD_DE_REPARTO'
 ]
        
columns_to_group_by = [
"TEXTO_BREVE",
'DOCUMENTO_COMPRAS',
 'FECHA_DOCUMENTO',
 'POSICION',
 'MATERIAL',
 'REPARTO',
 'SOLICITUD_DE_PEDIDO',
 'PROVEEDOR',
'UM_PRECIO_PEDIDO',
'CANTIDAD_BASE',
'INDICADOR_DE_BORRADO',
 'MONEDA',
 'INCOTERMS','INCOTERMS,_PARTE_2',
 'ORGANIZACION_COMPRAS',
 'CONDICIONES_DE_PAGO',
 'PAGO_EN',
 'NOMBRE_PLANIFICADOR',
 'GRUPO_DE_COMPRAS',
 'ESTADO_LIBERACION',
 'CENTRO',
 'HORIZ_PLANIF_FIJO',
 'FECHA_DE_ENTREGA',
 "FECHA_DE_PEDIDO",
 "GRUPO_DE_ARTICULOS","TIPO_COMPRA",'REG_INFO_DE_COMPRAS']


type = {
        'VALOR_NETO_DE_PEDIDO_1': 'CANTIDAD',
 'PRECIO_NETO_PEDIDO': 'PRECIO',
 'PRECIO': 'PRECIO',
 'TIPO_CAMBIO_MONEDA': 'TIPO_CAMBIO',
 'CANTIDAD_DE_REPARTO': 'CANTIDAD'
            }


measures = {
        'VALOR_NETO_DE_PEDIDO_1': 'IMPORTE',
        'PRECIO_NETO_PEDIDO': 'IMPORTE',
        'PRECIO': 'PIEZAS',
        'TIPO_CAMBIO_MONEDA': 'IMPORTE',
        'CANTIDAD_DE_REPARTO': 'PIEZAS'
            }



measures= [(MEASURE, TYPE_1) for MEASURE, TYPE_1 in measures.items()]

measures= spark.createDataFrame(measures, ['MEASURE', 'TYPE_1'])



type= [(MEASURE, TYPE_2) for MEASURE, TYPE_2 in type.items()]
type= spark.createDataFrame(type, ['MEASURE', 'TYPE_2'])


type_measures = type.join(
                                        measures,
                                        on = ['MEASURE'],
                                        how = "inner")


indicadores_oc = (indicadores_oc.melt(
    ids=[columns_to_group_by], values=[columns_to_transpose],
    variableColumnName="MEASURE", valueColumnName="VALUE"))


indicadores_oc= indicadores_oc.join(
                                        type_measures,
                                        on = ['MEASURE'],
                                        how = "inner")


from pyspark.sql.functions import col, when, avg, sum

indicadores_oc = indicadores_oc.groupBy(
    'DOCUMENTO_COMPRAS', 'FECHA_DOCUMENTO', 'POSICION', 'MATERIAL', 'REPARTO',
    'SOLICITUD_DE_PEDIDO', 'PROVEEDOR', 'UM_PRECIO_PEDIDO', 'CANTIDAD_BASE',
    'INDICADOR_DE_BORRADO', 'MONEDA', 'INCOTERMS', 'INCOTERMS,_PARTE_2',
    'ORGANIZACION_COMPRAS', 'CONDICIONES_DE_PAGO', 'PAGO_EN',
    'NOMBRE_PLANIFICADOR', 'GRUPO_DE_COMPRAS', 'ESTADO_LIBERACION', 'CENTRO',
    'HORIZ_PLANIF_FIJO', 'FECHA_DE_ENTREGA', "FECHA_DE_PEDIDO",
    "TEXTO_BREVE", "GRUPO_DE_ARTICULOS", "TIPO_COMPRA", 'REG_INFO_DE_COMPRAS',
    'TYPE_1'
).pivot("TYPE_2").agg(
    sum(when(~col("TYPE_2").isin(["PRECIO","TIPO_CAMBIO"]), col("VALUE"))).alias("sum_value"),
    avg(when(col("TYPE_2").isin(["PRECIO","TIPO_CAMBIO"]), col("VALUE"))).alias("avg_precio")
)



indicadores_oc = indicadores_oc.select('DOCUMENTO_COMPRAS',
 'FECHA_DOCUMENTO',
 'POSICION',
 'MATERIAL',
 'REPARTO',
 'SOLICITUD_DE_PEDIDO',
 'PROVEEDOR',
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
 'FECHA_DE_ENTREGA',
 'FECHA_DE_PEDIDO',
 'TEXTO_BREVE',
 'GRUPO_DE_ARTICULOS',
 'TIPO_COMPRA',
 'REG_INFO_DE_COMPRAS',
 'TYPE_1',
 col('CANTIDAD_sum_value').alias('CANTIDAD'),
 col('PRECIO_avg_precio').alias('PRECIO'),
 col('TIPO_CAMBIO_avg_precio').alias('TIPO_CAMBIO')).fillna(0)


indicadores_oc = indicadores_oc.withColumn(
    "CANTIDAD_MXN",
    when(col("MONEDA") == "MXN", col("CANTIDAD")).otherwise(col("CANTIDAD") * col("TIPO_CAMBIO"))
)


import pyspark.sql.functions as F

indicadores_oc = (indicadores_oc.withColumn(
    "ANIO_MES", 
    F.concat(
        F.year(F.col("FECHA_DE_ENTREGA")),  # Extrae el año
        F.lit("-"),                         # Añade el guion separador
        F.format_string("%02d", F.month(F.col("FECHA_DE_ENTREGA")))  # Extrae el mes y lo formatea con dos dígitos
    )
))


indicadores_oc = indicadores_oc.withColumn("UM_PRECIO_PEDIDO", regexp_replace("UM_PRECIO_PEDIDO", "ST", "PC"))






lfa1 = lfa1.select("PROVEEDOR","NOMBRE_1","CLAVE_DE_PAIS_REG","REGION","POBLACION").distinct()


lfa1 = lfa1.join(pais,"CLAVE_DE_PAIS_REG",how='left').join(pais_region.drop("PAIS_REG"), on = ["CLAVE_DE_PAIS_REG","REGION"],how="left")


indicadores_oc = indicadores_oc.join(lfa1,on="PROVEEDOR", how ="left")


indicadores_oc.write.format("delta").mode("overwrite").parquet("/mnt/adlsabastosprscus/abastos/gold/compras/indicadores_ordenes_compra.parquet")