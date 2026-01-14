# Databricks notebook source
# MAGIC
# MAGIC
# MAGIC %run /Shared/FASE1_INTEGRACION/includes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen del Notebook hasta la Celda 16
# MAGIC
# MAGIC Este notebook está diseñado para procesar y preparar datos de tablas SAP almacenados en Azure Data Lake Storage (ADLS) para un análisis posterior. Las operaciones realizadas hasta la celda 16 son las siguientes:
# MAGIC
# MAGIC 1. **Celda 4 y Celda 5**: Estas celdas se centran en la lectura de la tabla `MAKT` desde la ruta especificada en ADLS. Los datos se filtran en función de la clave de idioma (`CLAVE_DE_IDIOMA`), procesando diferentes claves de idioma en cada celda. Los valores de la columna `MATERIAL` se estandarizan eliminando los ceros a la izquierda. Se asigna un número de fila a cada fila dentro de las particiones definidas por `keys` y ordenadas por `SEQ_DATE` en orden descendente. Solo se retiene la primera fila dentro de cada partición, lo que deduplica efectivamente los datos en función de la `SEQ_DATE` más reciente.
# MAGIC
# MAGIC 2. **Celda 6**: Similar a las celdas anteriores, esta celda procesa la tabla `MARA`. Lee los datos, estandariza la columna `MATERIAL` y deduplica los registros en función de las `keys` y `SEQ_DATE`. Las claves de partición incluyen un campo adicional en comparación con las celdas anteriores, lo que indica un proceso de deduplicación más granular.
# MAGIC
# MAGIC 3. **Celda 7**: Esta celda procesa otra tabla, `MARA_SQL`, con un enfoque similar al de la Celda 6. Indica que el notebook maneja múltiples fuentes o versiones de datos similares, posiblemente para fines de comparación o consolidación.
# MAGIC
# MAGIC 4. **Celda 8**: El enfoque cambia a la tabla `MBEW`, donde los datos se filtran por `AREA_DE_VALORACION`. Se aplica la estrategia de deduplicación con un conjunto diferente de claves, lo que sugiere que el área de valoración financiera es de interés específico en este contexto.
# MAGIC
# MAGIC El patrón general hasta la celda 16 implica la lectura de tablas SAP específicas desde ADLS, la estandarización de la columna `MATERIAL` y la aplicación de una estrategia de deduplicación basada en la fecha más reciente y otras claves relevantes. Este proceso prepara los datos para un análisis consistente y preciso en los pasos posteriores.
# MAGIC
# MAGIC

# COMMAND ----------

keys = ["MANDANTE","NO_REG_CONDICION","NO_ACTUAL_CONDICION"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
table = "KONP"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
konp = (spark.
       read.
       parquet(source_path).where(col("CLASE_DE_CONDICION") == "ZPR0")
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
       )

# COMMAND ----------

keys = ["MANDANTE","MATERIAL"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
table = "MAKT"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
makt_texto = (spark.
       read.parquet(source_path)
       .where(col("CLAVE_DE_IDIOMA") == "S")
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
       )

# COMMAND ----------

keys = ["MANDANTE","MATERIAL"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
table = "MAKT"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
makt_cfdi = (spark.
       read.
       parquet(source_path)
       .where(col("CLAVE_DE_IDIOMA") == "Z")
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
       )

# COMMAND ----------

keys = ["MANDANTE","MATERIAL"]
table = "MAKT"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
makt_cart_p = (spark.
       read.
       parquet(source_path)
       .where(col("CLAVE_DE_IDIOMA") == "Y")
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
       )

# COMMAND ----------

keys = ["MANDANTE","MATERIAL"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
table = "MARA"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
mara = (spark.
       read.
       parquet(source_path)
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
       )


# COMMAND ----------

keys = ["MANDANTE","MATERIAL"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
table = "MBEW"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
mbew = (spark.
       read.
       parquet(source_path)
       .where(col("AREA_DE_VALORACION")=="A717")
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
       )

# COMMAND ----------

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

# COMMAND ----------

keys = ["MATERIAL","NUMERO_DE_ALMACEN"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
table = "MLGN"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
mlgn = (spark.
       read.
       parquet(source_path).where(col("NUMERO_DE_ALMACEN") == "717")
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
       )

# COMMAND ----------

keys = ["MATERIAL", "DOCUMENTO_DE_VENTAS","POSICION"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
table = "VBAP"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
vbap = (spark.
       read.
       parquet(source_path)
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
       )

# COMMAND ----------

keys = ["MANDANTE","MATERIAL","CLIENTE","CDIS"]
particion = (Window.
             partitionBy(keys).orderBy(desc('IN_VALIDEZ'), desc('SEQ_DATE')))
table = "A005"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
schema = get_schema_sap(table)
a005 = (spark
       .read
       .parquet(source_path, schema = schema).where((col("ORGVT") == "A717") & (col("CLCD") == "ZPR0"))
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .withColumn("NOREGCOND", lpad(col("NOREGCOND"), 10, '0'))
       .withColumn("CLIENTE", lpad(col("CLIENTE"), 10, '0'))
)

a005  = (a005.withColumnRenamed("APL","APLICACION")
       .withColumnRenamed("NOREGCOND","NO_REG_CONDICION")
       .withColumnRenamed("CLCD","CLASE_DE_CONDICION")
       .filter(~((col("CLIENTE").startswith("0000003")) & (col("CDIS") == "ME")))
       .filter(~((col("CLIENTE").startswith("0000002")) & (col("CDIS") == "MO")))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
       )

# COMMAND ----------

keys = ["MATERIAL"]
particion = (Window.partitionBy(keys).orderBy(desc('FECHA_INICIO_VALIDEZ') , desc('SEQ_DATE')))
table = "A004"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
a004 = (spark
       .read
       .parquet(source_path).where((col("ORGANIZACION_VENTAS") == "A717") & (col("CLASE_DE_CONDICION") == "ZPR0"))
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .withColumn("NO_REG_CONDICION", lpad(col("NO_REG_CONDICION"), 10, '0'))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
       )


# COMMAND ----------

keys = ["MANDANTE","MATERIAL","ORGVT","CDIS","CE"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))

table = "MVKE"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
mvke = (spark.
       read.
       parquet(source_path).where(col("CE") == "A717")
       .withColumnRenamed("GRM","GRM6")
       .withColumnRenamed("GRM1","GRM")
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
)



# COMMAND ----------

keys = ["MANDANTE","CLIENTE"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
table = "KNA1"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
kna1 = (spark.
       read.
       parquet(source_path)
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
       )

# COMMAND ----------

table = "PRODUCTOS_FIORI"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
prod_fiori = (spark.
       read.
       parquet(source_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Carga de catálogos

# COMMAND ----------

#Catálogo ID's
catalogo_ids = spark.createDataFrame(pd.read_excel('/dbfs/mnt/adls-dac-data-bi-pr-scus/powerappslandingzone_cat_ids/catalogo_de_ids.xlsx',dtype="string"))
for c in catalogo_ids.columns:
    catalogo_ids = catalogo_ids.withColumn(c, upper(col(c)))
ids_sistema = catalogo_ids.select('GRM','SISTEMA').filter(~isnull("GrM"))
ids_compania = (catalogo_ids.select('GRM2','CIA').filter(~isnull("GrM2"))
                .withColumnRenamed("CIA","COMPANIA"))
ids_clase = catalogo_ids.select('GRM3','CLASE').filter(~isnull("GrM3"))
ids_segmento = catalogo_ids.select('GRM4','SEGMENTO').filter(~isnull("GrM4"))
ids_marca = catalogo_ids.select('GRM5','MARCA').filter(~isnull("GrM5"))
ids_linea = (catalogo_ids
             .select('GRM6','LINEA')
             .filter(~isnull("LINEA"))
             .withColumn("GRM6", when(col("GRM6").isNull(),lit("NA"))
                         .otherwise(col("GRM6")))
             .withColumnRenamed("LINEA","LINEA_ABIERTA")
             )
ids_cfdi = catalogo_ids.select('C_CLAVEPRODSERV','CFDI').filter(~isnull("C_CLAVEPRODSERV"))
ids_carta_porte = catalogo_ids.select('CLAVE_CARTA_PORTE','CARTA_PORTE').filter(~isnull("CARTA_PORTE"))
ids_linea_ab_linea_ag = (catalogo_ids.select('LINEA ABIERTA','LINEA AGRUPADA').filter(~isnull("LINEA ABIERTA"))
                         .withColumnRenamed("LINEA ABIERTA","LINEA_ABIERTA")
                         .withColumnRenamed("LINEA AGRUPADA","LINEA_AGRUPADA")
                         )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catálogo de articulos SAP

# COMMAND ----------

makt = (
    makt_texto.select("MATERIAL", "TEXTO_BREVE_DE_MATERIAL")
    .join(
        makt_cfdi.select("MATERIAL", "TEXTO_BREVE_DE_MATERIAL").withColumnRenamed(
            "TEXTO_BREVE_DE_MATERIAL", "CFDI"
        ),
        "MATERIAL",
        "left",
    )
    .join(
        makt_cart_p.select(
            "MATERIAL", "TEXTO_BREVE_DE_MATERIAL"
        ).withColumnRenamed("TEXTO_BREVE_DE_MATERIAL", "CARTA_PORTE"),
        "MATERIAL",
        "left",
    )
)

# COMMAND ----------

# Listas de columnas para el select
makt_cfdi_cat_art = ["MATERIAL", "TEXTO_BREVE_DE_MATERIAL","CFDI"] #Catálgo
makt_carta_porte_cat_art = ["MATERIAL", "TEXTO_BREVE_DE_MATERIAL","CARTA_PORTE"]
mara_cat_art = ["MATERIAL", "TIPO_MATERIAL", "UNIDAD_MEDIDA_BASE","GRUPO_DE_ARTICULOS",#Catálgo
                "LONGITUD","ANCHURA","ALTURA","UNIDAD_DIMENSION","STATMAT_TODAS_CADDIS",
                "JERARQUIA_PRODUCTOS","CODIGO_EAN_UPC","UNIDAD_DE_PESO","PESO_BRUTO","PESO_NETO",
                "VOLUMEN","UNIDAD_DE_VOLUMEN","GESTION_DE_LOTES"]
#mara_sql_cat_art = ["MATERIAL"]
marc_cat_art = ["MATERIAL", "GRUPO_DE_COMPRAS", "INDICADOR_ABC", "CARACT_PLANIF_NEC",#Catálgo
                "TAMANO_LOTE_MINIMO", "CENTRO_DE_BENEFICIO", "CLASE_APROVISIONAM", "APROVIS_ESPECIAL",
                "PRIORIDAD", "PERFIL_DE_COBERTURA", "PAIS_REGION_ORIGEN", "GRUPO_INTRASTAT",
                "PLAZO_ENTREGA_PREV", "PLANIF_NECESIDADES", "CALCULO_DE_TAMANO_DE_LOTE",
                "VALOR_DE_REDONDEO", "CALENDARIO_PLANIFIC","HORIZ_PLANIF_FIJO","PB_NIVEL_CENTRO"]
mbew_cat_art = ["MATERIAL", "CATEGORIA_VALORACION", "PRECIO_ESTANDAR", "PRECIO_VARIABLE"] #Catalogo
mlgn_cat_art = ["MATERIAL", "IND_TIP_ALM_ENT", "IND_TIPO_ALM_SAL"] 
mvke_cat_art = ["MATERIAL", "CDIS", "GRM", "GRM2", "GRM3", "GRM4", "GRM5", "GRM6", "JQUIAPROD",#Catálgo
                "PERFREDOND","ST"]
#vbap_cat_art = ["MATERIAL","CODIGO_EAN_UPC"]

fiori = ['MATERIAL','COD_EST_MERCANCIAS']

reorder = ["MATERIAL","CODIGO_EAN_UPC", "TIPO_MATERIAL", "CDIS", "TEXTO_BREVE_DE_MATERIAL", "UNIDAD_MEDIDA_BASE",
    "JERARQUIA_PRODUCTOS","GRM6", "GRM", "GRM2", "GRM3", "GRM4", "GRM5", "GESTION_DE_LOTES",
    "GRUPO_DE_COMPRAS", "INDICADOR_ABC", "CATEGORIA_VALORACION", "CARACT_PLANIF_NEC", "TAMANO_LOTE_MINIMO",
    "STATMAT_TODAS_CADDIS", "ST", "PB_NIVEL_CENTRO", "CENTRO_DE_BENEFICIO", "PRECIO_ESTANDAR", "PRECIO_VARIABLE",
    "CLASE_APROVISIONAM", "APROVIS_ESPECIAL", "HORIZ_PLANIF_FIJO", "PERFIL_DE_COBERTURA", "IND_TIP_ALM_ENT",
    "IND_TIPO_ALM_SAL", "JQUIAPROD", "GRUPO_DE_ARTICULOS", "PERFREDOND","COD_EST_MERCANCIAS", "GRUPO_INTRASTAT", "PAIS_REGION_ORIGEN","PLAZO_ENTREGA_PREV", "PLANIF_NECESIDADES", "CALCULO_DE_TAMANO_DE_LOTE", "VALOR_DE_REDONDEO", "CALENDARIO_PLANIFIC","SISTEMA", "COMPANIA", "LINEA_ABIERTA", "LINEA_AGRUPADA", "CLASE", "SEGMENTO", "MARCA", "CFDI", "CFDI_TEXTO","CARTA_PORTE","CARTA_PORTE_TEXTO","UNIDAD_DE_PESO", "PESO_BRUTO", "PESO_NETO", "VOLUMEN", "UNIDAD_DE_VOLUMEN","LONGITUD", "ANCHURA", "ALTURA", "UNIDAD_DIMENSION"]


# COMMAND ----------

# Realizar los full joins usando la columna común 'MATERIAL'
catalogo_de_art = (
    mvke.select(*mvke_cat_art)
    .join(
        mara.select(*mara_cat_art).dropDuplicates(subset=["MATERIAL"]),
        "MATERIAL",
        "left",
    )
    .join(
        marc.select(*marc_cat_art).dropDuplicates(subset=["MATERIAL"]),
        "MATERIAL",
        "left",
    )
    .join(
        mbew.select(*mbew_cat_art).dropDuplicates(subset=["MATERIAL"]),
        "MATERIAL",
        "left",
    )
    .join(
        mlgn.select(*mlgn_cat_art).dropDuplicates(subset=["MATERIAL"]),
        "MATERIAL",
        "left",
    )
    .join(
        makt.dropDuplicates(subset=["MATERIAL"]),
        "MATERIAL",
        "left",
    )
    # .join(
    #     vbap.select(*vbap_cat_art).dropDuplicates(subset=["MATERIAL"]),
    #     ["MATERIAL", "CODIGO_EAN_UPC"],
    #     "left",
    # )
    .join(ids_sistema, "GRM", "left")
    .join(ids_compania, "GRM2", "left")
    .join(ids_clase, "GRM3", "left")
    .join(ids_segmento, "GRM4", "left")
    .join(ids_marca, "GRM5", "left")
    .join(ids_linea, "GRM6", "left")
    .join(ids_linea_ab_linea_ag, "LINEA_ABIERTA", "left")
    .join(
        ids_cfdi.withColumnRenamed("CFDI", "CFDI_TEXTO").withColumnRenamed(
            "C_CLAVEPRODSERV", "CFDI"
        ),
        "CFDI",
        "left",
    )
    .join(ids_carta_porte.withColumnRenamed("CARTA_PORTE", "CARTA_PORTE_TEXTO")
          .withColumnRenamed("CLAVE_CARTA_PORTE","CARTA_PORTE"), "CARTA_PORTE", "left")
    #.where(col("INDICADOR_ABC") != " ")
    .where(col("CDIS").isin(["MI", "ME", "MO"]))
    .where(col("CARACT_PLANIF_NEC").isin(["V1", "X0", "Z1"]))
    .join(prod_fiori.select(*fiori),"MATERIAL","left")
    .select(*reorder)
    .withColumnRenamed("GRM", "GRM1")
    .withColumnRenamed("GRM6", "GRM")
    .withColumn("UNIDAD_MEDIDA_BASE", regexp_replace("UNIDAD_MEDIDA_BASE", "ST", "PC"))
    .withColumn(
        "PRECIO_ESTANDAR", format_number("PRECIO_ESTANDAR", 2)
    )
    .withColumn(
        "PRECIO_VARIABLE", format_number("PRECIO_VARIABLE", 2)
    )
    .withColumn(
        "PESO_BRUTO", format_number("PESO_BRUTO", 3)
    )
    .withColumn(
        "PESO_NETO", format_number("PESO_NETO", 3)
    )
    .withColumn(
        "VOLUMEN", col("VOLUMEN").cast("double")
    )
    .withColumn(
        "VOLUMEN", format_number("VOLUMEN", 3)
    )
    .withColumn(
        "LONGITUD", col("LONGITUD").cast("double")
    )
    .withColumn(
        "LONGITUD", format_number("LONGITUD", 3)
    )
    .withColumn(
        "ANCHURA", col("ANCHURA").cast("double")
    )
    .withColumn(
        "ANCHURA", format_number("ANCHURA", 3)
    )
    .withColumn(
        "ALTURA", col("ALTURA").cast("double")
    )
    .withColumn(
        "ALTURA", format_number("ALTURA", 3)
    )
    .withColumn(
        "UNIDAD_DE_VOLUMEN", regexp_replace("UNIDAD_DE_VOLUMEN", "CCM", "CM3")
    )
    .withColumn(
    "APROVIS_ESPECIAL", col("APROVIS_ESPECIAL").cast("integer")
    )
    .withColumn(
    "VALOR_DE_REDONDEO", col("VALOR_DE_REDONDEO").cast("integer")
    )
    .dropDuplicates()
    .withColumnRenamed("EST_MAT_ESPECIF_CE", "ST")
)

for c in catalogo_de_art.columns:
    catalogo_de_art = catalogo_de_art.withColumn(c,upper(col(c)))

# COMMAND ----------

path = "/mnt/adls-dac-data-bi-pr-scus/silver/marketing/CATALOGO_DE_ARTICULOS.parquet"
catalogo_de_art.write.format("delta").partitionBy("CDIS").mode("overwrite").parquet(path)

# COMMAND ----------

catalogo_de_articulos_sap_mi = catalogo_de_art.where(catalogo_de_art['CDIS'] == "MI")
catalogo_de_articulos_sap_me = catalogo_de_art.where(catalogo_de_art['CDIS'] == "ME")
catalogo_de_articulos_sap_mo = catalogo_de_art.where(catalogo_de_art['CDIS'] == "MO")

# COMMAND ----------

dict_exceles = {"catalogo_de_articulos_sap_mi": catalogo_de_articulos_sap_mi,
                "catalogo_de_articulos_sap_me": catalogo_de_articulos_sap_me,
                "catalogo_de_articulos_sap_mo": catalogo_de_articulos_sap_mo}

save_excel_files(dict_exceles)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lista de precios

# COMMAND ----------

catalogo_de_art = catalogo_de_art.where(catalogo_de_art['CDIS'].isin(["ME","MO"]))

# COMMAND ----------

catalogo_de_art_lista_de_precio = ["MATERIAL","CDIS","TEXTO_BREVE_DE_MATERIAL","SISTEMA","COMPANIA","CLASE","SEGMENTO","LINEA_ABIERTA","LINEA_AGRUPADA","STATMAT_TODAS_CADDIS", "ST", "PB_NIVEL_CENTRO","MARCA","PERFREDOND"]

konp_lista_de_precios = ["CLASE_DE_ESCALA","CANTIDAD_DE_ESCALA","UNIDAD_MEDIDA_ESCALA","IMPORTE_DE_CONDICION",
                         "MONEDA_CONDICION","UM_DE_PRECIO","UNIDAD_DE_MEDIDA","NO_REG_CONDICION","APLICACION",
                         "CLASE_DE_CONDICION"]

a005_lista_de_precios = ["MATERIAL","APLICACION","CLASE_DE_CONDICION","IN_VALIDEZ","VALIDEZ_A",
                         "ORGVT","NO_REG_CONDICION", "CLIENTE", "CDIS"]

kna1_lista_de_precios =["CLIENTE","NOMBRE_1","NOMBRE_2"]

reorder =["CDIS","CLIENTE","RAZON_SOCIAL","MATERIAL","CLASE_DE_CONDICION","TEXTO_BREVE_DE_MATERIAL","SISTEMA","COMPANIA","CLASE","SEGMENTO","LINEA_ABIERTA","LINEA_AGRUPADA","UNIDAD_DE_MEDIDA","IMPORTE_DE_CONDICION","MONEDA_CONDICION","UM_DE_PRECIO","IN_VALIDEZ","VALIDEZ_A","STATMAT_TODAS_CADDIS", "ST", "PB_NIVEL_CENTRO","MARCA","PERFREDOND"]

windowSpec = Window.partitionBy("CDIS","CLIENTE","RAZON_SOCIAL","MATERIAL","CLASE_DE_CONDICION").orderBy(col("IN_VALIDEZ").desc())

lista_de_precios_memo = (catalogo_de_art.select(*catalogo_de_art_lista_de_precio)
                    .join(a005.select(*a005_lista_de_precios)
                          .where(col("CDIS").isin(["ME","MO"]))
                          .withColumnRenamed("CLCD","CLASE_DE_CONDICION"),["MATERIAL", "CDIS"],"full")
                    .join(konp.select(*konp_lista_de_precios),["CLASE_DE_CONDICION","APLICACION","NO_REG_CONDICION"],
                          "left")
                    .join(kna1.select(*kna1_lista_de_precios),"CLIENTE","left")
                    .withColumnRenamed("NOMBRE_1","RAZON_SOCIAL")
                    .dropDuplicates()
                    .where(col("CDIS").isNotNull())
                    .where(col("CLASE_DE_ESCALA").isNotNull())
                    .withColumn("row_number", row_number().over(windowSpec))
                    .filter(col("row_number") == 1).drop("row_number")
                    .select(*reorder)
                    .filter(col("IN_VALIDEZ").isNotNull())
                    .withColumn("IMPORTE_DE_CONDICION",round(col("IMPORTE_DE_CONDICION").cast("double"),2))
                  #   #.withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
                    .withColumn("CLIENTE", regexp_replace(col("CLIENTE"), "^0{2,}", ""))
                    .orderBy("CLIENTE","MATERIAL")
                  #   #.where(col("IMPORTE_DE_CONDICION") != " ")
                    .withColumn("UNIDAD_DE_MEDIDA", regexp_replace("UNIDAD_DE_MEDIDA", "ST", "PC"))
                    .dropDuplicates()
                  )


# COMMAND ----------

catalogo_de_art_lista_de_precio = ["MATERIAL","TEXTO_BREVE_DE_MATERIAL","SISTEMA","COMPANIA","CLASE","SEGMENTO","LINEA_ABIERTA","LINEA_AGRUPADA","STATMAT_TODAS_CADDIS", "ST", "PB_NIVEL_CENTRO","MARCA","PERFREDOND"]

# COMMAND ----------

windowSpec = Window.partitionBy("CLIENTE","MATERIAL","CLASE_DE_CONDICION","APLICACION").orderBy(col("IN_VALIDEZ").desc())

ml_a005 = (a005.where(col("CLIENTE").contains("7999"))
                     .withColumn("row_number", row_number().over(windowSpec))
                    .filter(col("row_number") == 1).drop("row_number")
                    .withColumn("CDIS",lit("ML"))
            )

ml = (catalogo_de_articulos_sap_mi.select(*catalogo_de_art_lista_de_precio)
                    .join(ml_a005
                          .withColumnRenamed("CLCD","CLASE_DE_CONDICION"),"MATERIAL","right")
                    .join(konp.select(*konp_lista_de_precios),["CLASE_DE_CONDICION","APLICACION","NO_REG_CONDICION"],
                          "left")
                    .join(kna1.select(*kna1_lista_de_precios),"CLIENTE","left")
                    .withColumnRenamed("NOMBRE_1","RAZON_SOCIAL")
                    .dropDuplicates()
                    .where(col("CDIS").isNotNull())
                    .withColumn("row_number", row_number().over(windowSpec))
                    .filter(col("row_number") == 1).drop("row_number")
                    .select(*reorder)
                    .withColumn("CLIENTE", regexp_replace(col("CLIENTE"), "^0{2,}", ""))
                    #.where(col("IMPORTE_DE_CONDICION") !=)
                    .dropDuplicates()
                    .withColumn("UNIDAD_DE_MEDIDA", regexp_replace("UNIDAD_DE_MEDIDA", "ST", "PC"))
                    #.where(col("ST") == 99)
        )


# COMMAND ----------

lista_de_precios_memo = lista_de_precios_memo.union(ml).orderBy("CLIENTE", "MATERIAL")

# COMMAND ----------

lista_de_precios_memo = (lista_de_precios_memo.where(col("SISTEMA").isin(['TREN MOTRIZ', 'FRENOS', 'MOTOR']))
                         .dropDuplicates()
                         .orderBy("CLIENTE", "MATERIAL")
                         .withColumn("IMPORTE_DE_CONDICION", format_number("IMPORTE_DE_CONDICION", 2))
                         .withColumn("UNIDAD_DE_MEDIDA", regexp_replace("UNIDAD_DE_MEDIDA", "ST", "PC"))
                         .where(col("ST").isin(["99","97","95","93"]))
                         )

# COMMAND ----------

lista_de_precios_memo.count()

# COMMAND ----------

catalogo_de_art_lista_de_precio = ["MATERIAL","TEXTO_BREVE_DE_MATERIAL","SISTEMA","COMPANIA","CLASE","SEGMENTO","LINEA_ABIERTA","LINEA_AGRUPADA","STATMAT_TODAS_CADDIS", "ST", "PB_NIVEL_CENTRO","MARCA","PERFREDOND"]

# COMMAND ----------

windowSpec = Window.partitionBy("MATERIAL","CLASE_DE_CONDICION","APLICACION").orderBy(col("FECHA_INICIO_VALIDEZ").desc())

a004_lista_de_precios = ["MATERIAL","APLICACION","CLASE_DE_CONDICION","FECHA_INICIO_VALIDEZ","FIN_VALIDEZ",
                         "ORGANIZACION_VENTAS","NO_REG_CONDICION", "CANAL_DISTRIBUCION"]


reorder =["CDIS","MATERIAL","CLASE_DE_CONDICION","TEXTO_BREVE_DE_MATERIAL","SISTEMA","COMPANIA","CLASE","SEGMENTO","LINEA_ABIERTA","LINEA_AGRUPADA","UNIDAD_DE_MEDIDA","IMPORTE_DE_CONDICION","MONEDA_CONDICION","UM_DE_PRECIO","FECHA_INICIO_VALIDEZ","FIN_VALIDEZ","STATMAT_TODAS_CADDIS", "ST", "PB_NIVEL_CENTRO","MARCA","PERFREDOND"]

lista_de_precios_mi = (catalogo_de_articulos_sap_mi.select(*catalogo_de_art_lista_de_precio)
                    .join(a004.select(*a004_lista_de_precios)
                          .where(col("CANAL_DISTRIBUCION").isin(["MI"]))
                          .withColumnRenamed("CLCD","CLASE_DE_CONDICION"),"MATERIAL","full")
                    .join(konp.select(*konp_lista_de_precios),["CLASE_DE_CONDICION","APLICACION","NO_REG_CONDICION"],
                          "right")
                    .dropDuplicates()
                    .where(col("CANAL_DISTRIBUCION").isNotNull())
                    .withColumn("row_number", row_number().over(windowSpec))
                    .filter(col("row_number") == 1).drop("row_number")
                    .where(col("MONEDA_CONDICION") == "MXN")
                    .withColumnRenamed("CANAL_DISTRIBUCION","CDIS")
                    .withColumn("IMPORTE_DE_CONDICION", format_number("IMPORTE_DE_CONDICION",2))
                    .select(*reorder)
                    #.withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
                    .dropDuplicates()
                    .withColumn("UNIDAD_DE_MEDIDA", regexp_replace("UNIDAD_DE_MEDIDA", "ST", "PC"))
                    .orderBy("MATERIAL")
                    .where(col("ST").isin(["99","95"]))
                  )

# COMMAND ----------

lista_de_precios_mi.count()

# COMMAND ----------

dict_exceles = {"lista_de_precios_mi": lista_de_precios_mi,
                "lista_de_precios_memo": lista_de_precios_memo}

save_excel_files(dict_exceles)

# COMMAND ----------

path = "/mnt/adls-dac-data-bi-pr-scus/silver/marketing/LISTA_DE_PRECIOS_MI.parquet"
lista_de_precios_mi.write.format("delta").partitionBy("CDIS").mode("overwrite").parquet(path)

# COMMAND ----------

path = "/mnt/adls-dac-data-bi-pr-scus/silver/marketing/LISTA_DE_PRECIOS_MEMO.parquet"
lista_de_precios_memo.write.format("delta").partitionBy("CDIS").mode("overwrite").parquet(path)

# COMMAND ----------

documento = ["1","2","3","4","5"]

paths = ["adls-dac-data-bi-pr-scus/powerappsentregables/catalogo_de_articulos_sap_mi.xlsx",
         "adls-dac-data-bi-pr-scus/powerappsentregables/catalogo_de_articulos_sap_me.xlsx",
         "adls-dac-data-bi-pr-scus/powerappsentregables/catalogo_de_articulos_sap_mo.xlsx",
         "adls-dac-data-bi-pr-scus/powerappsentregables/lista_de_precios_mi.xlsx",
         "adls-dac-data-bi-pr-scus/powerappsentregables/lista_de_precios_memo.xlsx"]

dim_paths_entrg =  spark.createDataFrame(zip(documento,paths), schema = ["documento", "paths"])

dim_paths_entrg.toPandas().to_json("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/dim_paths_ent.json", orient = "records")