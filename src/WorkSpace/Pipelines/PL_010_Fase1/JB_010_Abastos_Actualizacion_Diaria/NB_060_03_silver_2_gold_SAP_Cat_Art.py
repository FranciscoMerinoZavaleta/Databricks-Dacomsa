



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



keys = ["MANDANTE","MATERIAL"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
table = "MBEW"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
mbew = (spark.
       read.
       parquet(source_path)
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
       )


keys = ["MANDANTE","MATERIAL","CENTRO"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
table = "MARC"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
marc = (spark.
       read.
       parquet(source_path)
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
       )


table = "T024D"
source_path = f"/mnt/adlsabastosprscus/abastos/silver/SAP/tablas_SAP/{table}.parquet/"
plan_necesidades = (spark
       .read
       .parquet(source_path)
       .select("PLANIF_NECESIDADES", "CENTRO", "NOMBRE_PLANIFICADOR")
       .distinct())


grupo_art = spark.createDataFrame(
    pd.read_excel(
        "/dbfs/mnt/adlsabastosprscus/abastos/catalogos/T023T.XLSX"
    )
)


grupo_art = grupo_art.select(
    col('Grupo de artículos').alias('GRUPO_DE_ARTICULOS'),
    col('`Denom.gr-artículos`').alias('DENOM_GRUPO_ART'),
    col('Descripción 2 del grupo de artículos').alias('DENOM_GRUPO_ART_2')
).distinct().filter(col('GRUPO_DE_ARTICULOS').isNotNull())



keys = ["MATERIAL","NUMERO_DE_ALMACEN"]
particion = (Window.partitionBy(keys).orderBy(desc('SEQ_DATE')))
table = "MLGN"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
mlgn = (spark.
       read.
       parquet(source_path)
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
       )


keys = ["MANDANTE","MATERIAL","ORGVT","CDIS","CE"]
particion = (Window.
             partitionBy(keys).orderBy(desc('SEQ_DATE')))

table = "MVKE"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
mvke = (spark.
       read.
       parquet(source_path)
       .withColumnRenamed("GRM","GRM6")
       .withColumnRenamed("GRM1","GRM")
       .withColumn("MATERIAL", regexp_replace(col("MATERIAL"), "^0{2,}", ""))
       .withColumn("ROW_NUM", row_number().over(particion))
       .filter(col('ROW_NUM')==1)
       .drop("ROW_NUM")
)




table = "PRODUCTOS_FIORI"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
prod_fiori = (spark.
       read.
       parquet(source_path))


table = "MAKT"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_SAP/{table}.parquet/"
makt_texto = (spark.
       read.parquet(source_path)
       )






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


mara_cat_art = ["MATERIAL", "TIPO_MATERIAL", "UNIDAD_MEDIDA_BASE","GRUPO_DE_ARTICULOS","ULTIMA_MODIFICACION","CREADO_POR"]
marc_cat_art = ["MATERIAL", "GRUPO_DE_COMPRAS", "INDICADOR_ABC", "CARACT_PLANIF_NEC","CENTRO",'PLANIF_NECESIDADES']
mbew_cat_art = ["MATERIAL", "CATEGORIA_VALORACION", "PRECIO_ESTANDAR", "PRECIO_VARIABLE","CANTIDAD_BASE","CONTROL_DE_PRECIOS"] #Catalogo



catalogo_de_art = (
        mara.select(*mara_cat_art)
    .join(
        marc.select(*marc_cat_art),
        "MATERIAL",
        "left",
    )
    .join(
        mbew.select(*mbew_cat_art),
        "MATERIAL",
        "left",
    )
    .join(
        makt.dropDuplicates(subset=["MATERIAL"]),
        "MATERIAL",
        "left",
    )
    .withColumn("UNIDAD_MEDIDA_BASE", regexp_replace("UNIDAD_MEDIDA_BASE", "ST", "PC"))
    .withColumn(
        "PRECIO_ESTANDAR", format_number("PRECIO_ESTANDAR", 2)
    )
    .withColumn(
        "PRECIO_VARIABLE", format_number("PRECIO_VARIABLE", 2)
    )
    .dropDuplicates()
)

for c in catalogo_de_art.columns:
    catalogo_de_art = catalogo_de_art.withColumn(c,upper(col(c)))


catalogo_de_art = (catalogo_de_art.join(plan_necesidades,on=["PLANIF_NECESIDADES", "CENTRO"],how='left'
    )
    .join(grupo_art, on=["GRUPO_DE_ARTICULOS"], how='left'
    )
)   






catalogo_de_art = catalogo_de_art.withColumn(
    "NOMBRE_PLANIFICADOR", upper(col("NOMBRE_PLANIFICADOR"))
).withColumn(
    "DENOM_GRUPO_ART", upper(col("DENOM_GRUPO_ART"))
).withColumn(
    "DENOM_GRUPO_ART_2", upper(col("DENOM_GRUPO_ART_2"))
)


catalogo_de_art = catalogo_de_art.withColumn('MATERIAL',regexp_replace(col('MATERIAL'), r'^[0]*', ''))




table = "indicadores_ordenes_compra"
source_path = f"/mnt/adlsabastosprscus/abastos/gold/compras/{table}.parquet/"
ordenes= (spark
       .read
       .parquet(source_path))


ordenes = ordenes.withColumn('MATERIAL',regexp_replace(col('MATERIAL'), r'^[0]*', ''))


ordenes = ordenes.withColumn("MATERIAL",when(col("MATERIAL").isNull(),lit("SERVICIO")).otherwise(col("MATERIAL")))


ordenes = ordenes.withColumn(
    "TIPO_COMPRA",
    when((~col("MATERIAL").rlike(r'\S')), lit("SERVICIO")).otherwise(col("TIPO_COMPRA"))
).withColumn(
    "MATERIAL",
    when((~col("MATERIAL").rlike(r'\S')), lit("SERVICIO")).otherwise(col("MATERIAL"))
)



servicios = (
    ordenes
    .filter(
        (col("MATERIAL").isin(["SERVICIO", "MATERIALES SERVICIO"])) |
        (col("TIPO_COMPRA") == "SERVICIO")
    )
    .withColumn(
        "MATERIAL1",
        concat(col("MATERIAL"), lit("-"), col("GRUPO_DE_ARTICULOS"))
    )
    .select(
        "MATERIAL1", "MATERIAL", "CENTRO", "GRUPO_DE_ARTICULOS", "TIPO_COMPRA"
    )
    .distinct()
)



table = "CATALOGO_DE_ARTICULOS"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/marketing/{table}.parquet/"
catalogo_de_articulos= (spark
       .read
       .parquet(source_path))


catalogo_de_articulos = catalogo_de_articulos.groupBy("MATERIAL","SISTEMA").agg(max("LINEA_AGRUPADA").alias("LINEA_AGRUPADA"))




catalogo_de_articulos = catalogo_de_art.filter(col("CENTRO")=="A717").join(catalogo_de_articulos, on = ["MATERIAL"], how = 'left')


catalogo_de_articulos = catalogo_de_articulos.unionByName(catalogo_de_art.filter(~(col("CENTRO")=="A717")), allowMissingColumns=True)


catalogo_de_articulos = catalogo_de_articulos.unionByName(servicios, allowMissingColumns=True).distinct()


ordenes = ordenes.filter(col("TIPO_COMPRA")=="MATERIAL").distinct()


catalogo_de_articulos = catalogo_de_articulos.unionByName(ordenes.select("MATERIAL", "CENTRO").distinct() \
    .join(
        catalogo_de_articulos.select("MATERIAL", "CENTRO").distinct(),
        on=["MATERIAL", "CENTRO"],
        how="left_anti"
    ),allowMissingColumns=True)


catalogo_de_articulos = catalogo_de_articulos.withColumn(
    "COMPAÑIA",
    when(col("CENTRO") == "A717", "DACOMSA")
    .when(col("CENTRO") == "A761", "FRITEC")
    .when(col("CENTRO") == "A744", "PISTONES MORESA")
    .when(col("CENTRO") == "A751", "TF VICTOR")
    .otherwise("FRITEC")  # Valor predeterminado cuando ninguna condición se cumple
)




catalogo_de_articulos = catalogo_de_articulos.withColumn(
    "CATEGORIA",
    when(col("CENTRO") == "A717", col("SISTEMA"))
    .when(col("CENTRO") == "A744", col('DENOM_GRUPO_ART'))
    .when(col("CENTRO") == "A751", col("NOMBRE_PLANIFICADOR"))
    .when(col("CENTRO") == "A761", col("DENOM_GRUPO_ART"))
    .otherwise(col("GRUPO_DE_ARTICULOS"))  # Uso de 'GRUPO_DE_ARTICULOS' como valor predeterminado
)





catalogo_de_articulos = catalogo_de_articulos.withColumn(
    "TIPO_COMPRA",
    when(col("TIPO_COMPRA").isNull(), lit("MATERIAL")).otherwise(col("TIPO_COMPRA"))
).withColumn(
    "MATERIAL1",
    when(col("MATERIAL1").isNull(), col("MATERIAL")).otherwise(col("MATERIAL1"))
).withColumn(
    "SISTEMA",
    when(col("TIPO_COMPRA") == "SERVICIO", lit("SERVICIO")).otherwise(col("SISTEMA"))
).withColumn(
    "CATEGORIA",
    when(col("TIPO_COMPRA") == "SERVICIO", lit("SERVICIO")).otherwise(col("CATEGORIA"))
).withColumn(
    "LINEA_AGRUPADA",
    when(col("TIPO_COMPRA") == "SERVICIO", lit("SERVICIO")).otherwise(col("LINEA_AGRUPADA"))
)


catalogo_de_articulos = catalogo_de_articulos.distinct()


catalogo_de_articulos = catalogo_de_articulos.drop("CATEGORIA")


categorias = spark.createDataFrame(
    pd.read_excel(
        "/dbfs/mnt/adlsabastosprscus/abastos/catalogos/catalogo_de_articulos.xlsx"
    )
)


categorias = categorias.select("MATERIAL","CENTRO","CATEGORIA").distinct()


categorias = categorias.groupBy("MATERIAL","CENTRO").agg(max("CATEGORIA").alias("CATEGORIA")).distinct()




catalogo_de_articulos = catalogo_de_articulos.join(categorias, on = ["MATERIAL","CENTRO"], how = 'left')


catalogo_de_articulos = catalogo_de_articulos.withColumn("CATEGORIA",
    when((col("CENTRO") == "A717") & (col("CATEGORIA").isNull()), col("SISTEMA"))
    .otherwise(col("CATEGORIA"))
).withColumn("CATEGORIA", upper(col("CATEGORIA")))



path = "/mnt/adlsabastosprscus/abastos/silver/CATALOGO_DE_ARTICULOS_SC.parquet"
catalogo_de_articulos.write.format("delta").mode("overwrite").parquet(path)




duplicados_df = catalogo_de_articulos.groupBy("MATERIAL1","CENTRO").agg(count(col("MATERIAL1")).alias("duplicados"))
filtered_df = duplicados_df.filter(col("duplicados") > 1)




temp_local_path = "/tmp/"
base_path = "adlsabastosprscus/abastos/"
filename = "catalogo_de_articulos.xlsx"

temp_path = f"{temp_local_path}/{filename}"
final_path = f"{base_path}/{filename}"
writer = pd.ExcelWriter(temp_path, engine='openpyxl')

catalogo_de_articulos.toPandas().to_excel(writer, index=False)
writer.save()
dbutils.fs.cp(f"file:{temp_path}", "/mnt/" + final_path)
print(f"Archivo guardado en: {final_path}")




