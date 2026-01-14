# Databricks notebook source
# MAGIC %md
# MAGIC #Includes

# COMMAND ----------

# MAGIC %run /Shared/FASE1_INTEGRACION/includes

# COMMAND ----------

# MAGIC %md
# MAGIC Imports

# COMMAND ----------

from openpyxl import load_workbook
from openpyxl.styles import PatternFill

# COMMAND ----------

current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month

# COMMAND ----------

# MAGIC %md
# MAGIC #Cargas SAP

# COMMAND ----------

table = "CATALOGO_DE_ARTICULOS"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/marketing/{table}.parquet/"
catalogo_de_articulos_sap_mi = spark.read.parquet(source_path).filter(
    col("CDIS") == "MI"
)

# COMMAND ----------

table = "CATALOGO_DE_ARTICULOS"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/marketing/{table}.parquet/"
catalogo_de_articulos_sap_memo = spark.read.parquet(source_path).filter(
    col("CDIS").isin(["ME", "MO"])
)

# COMMAND ----------

table = "LISTA_DE_PRECIOS_MI"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/marketing/{table}.parquet/"
lista_de_precios_mi = spark.read.parquet(source_path).withColumn("IMPORTE_DE_CONDICION", regexp_replace(col("IMPORTE_DE_CONDICION"), "[^0-9.]", "")).withColumn("IMPORTE_DE_CONDICION", round(col("IMPORTE_DE_CONDICION").cast("double"),2))

# COMMAND ----------

table = "LISTA_DE_PRECIOS_MEMO"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/marketing/{table}.parquet/"
lista_de_precios_memo = spark.read.parquet(source_path).withColumn("IMPORTE_DE_CONDICION", regexp_replace(col("IMPORTE_DE_CONDICION"), "[^0-9.]", "")).withColumn("IMPORTE_DE_CONDICION", round(col("IMPORTE_DE_CONDICION").cast("double"),2))

# COMMAND ----------

# MAGIC %md
# MAGIC #Carga tablas capa gold y/o silver

# COMMAND ----------

table = "fact_pedidos"
fact_pedidos = spark.read.parquet(
    f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"
)

# COMMAND ----------

table = "fact_ventas_pedidos"
fact_ventas_pedidos = spark.read.parquet(
    f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"
).withColumnRenamed("LINEA", "LINEA_ABIERTA")

table = "dim_clientes"
dim_clientes = spark.read.parquet(
    f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"
)

# COMMAND ----------

table = "fact_inventario_trans"
inventario = spark.read.parquet(
    f"/mnt/adls-dac-data-bi-pr-scus/gold/marketing/{table}.parquet/"
)

# COMMAND ----------

regex_pattern = r"(\d{4}-\d{2})"

if current_month == 1: #Cuando es enero filtra por diciembre del año anterior
    table = "HISTORIA_PRONO_MI"
    prono_mes_ant_mi = (spark.read.parquet(
        f"/mnt/adls-dac-data-bi-pr-scus/gold/{table}.parquet/")
                    .filter(col("CONCEPTO") == "U_PRONO")
                    .withColumn("MES_PRONO", regexp_extract(col("datestr"), regex_pattern, 1))
                    .filter(col("MES_PRONO") == f"{current_year-1}-12")
                    .withColumnRenamed("DATE","ANIO_MES")
                    .withColumn("ANIO_MES", regexp_extract(col("ANIO_MES"), regex_pattern, 1))
                    #.select("MATERIAL","ID_CANAL_DISTRIBUCION","ANIO_MES","value")
                    .withColumnRenamed("value", "U_PRONO")
                    .withColumnRenamed("MATERIAL","ID_MATERIAL")
                    .fillna(0)
    )

    keys = ["MATERIAL", "ID_GRUPO_CLIENTE","ID_CANAL_DISTRIBUCION","USUARIO","MES_PRONO","ANIO_MES"]
    particion = (Window.partitionBy(keys).orderBy(desc('datestr')))
    table = "prono_mkt_memo"
    prono_mes_ant_memo = (spark.read.parquet(
        f"/mnt/adls-dac-data-bi-pr-scus/gold/{table}.parquet/")
                    .filter(col("CONCEPTO") == "U_PRONO")
                    .withColumn("MES_PRONO", regexp_extract(col("datestr"), regex_pattern, 1))
                    .withColumn("datestr", col("datestr").cast("timestamp"))
                    .filter(col("MES_PRONO") == f"{current_year-1}-12")
                    .withColumnRenamed("DATE","ANIO_MES")
                    .withColumn("ANIO_MES", regexp_extract(col("ANIO_MES"), regex_pattern, 1))
                    .withColumn("ROW_NUM", row_number().over(particion))
                    .filter(col('ROW_NUM')==1)
                    .drop("ROW_NUM")
                    #.select("MATERIAL","ID_CANAL_DISTRIBUCION","ANIO_MES","value")
                    .withColumnRenamed("value", "U_PRONO")
                    .withColumnRenamed("MATERIAL","ID_MATERIAL")
                    .withColumn("ID_GRUPO_CLIENTE", lpad(col("ID_GRUPO_CLIENTE"), 10, "0"))
                    .fillna(0)
    )

elif current_month == 12: #Cuando es diciembre el pronóstico ya es de enero del próximo año en adelnate
    table = "HISTORIA_PRONO_MI"
    prono_mes_ant_mi = (spark.read.parquet(
        f"/mnt/adls-dac-data-bi-pr-scus/gold/{table}.parquet/")
                    .filter(col("CONCEPTO") == "U_PRONO")
                    .withColumn("MES_PRONO", regexp_extract(col("datestr"), regex_pattern, 1))
                    .withColumn("datestr", col("datestr").cast("timestamp"))
                    .withColumn("DATE", col("DATE").cast("date"))
                    .filter((col("MES_PRONO") == f"{current_year}-{current_month-1:02d}") & (col("DATE") >= f"{current_year +1}-01"))
                    .withColumnRenamed("DATE","ANIO_MES")
                    .withColumn("ANIO_MES", regexp_extract(col("ANIO_MES"), regex_pattern, 1))
                    #.select("MATERIAL","ID_CANAL_DISTRIBUCION","ANIO_MES","value")
                    .withColumnRenamed("value", "U_PRONO")
                    .withColumnRenamed("MATERIAL","ID_MATERIAL")
    )

    keys = ["MATERIAL", "ID_GRUPO_CLIENTE","ID_CANAL_DISTRIBUCION","USUARIO","MES_PRONO","ANIO_MES"]
    particion = (Window.partitionBy(keys).orderBy(desc('datestr')))
    table = "prono_mkt_memo"
    prono_mes_ant_memo = (spark.read.parquet(
        f"/mnt/adls-dac-data-bi-pr-scus/gold/{table}.parquet/")
                    .filter(col("CONCEPTO") == "U_PRONO")
                    .withColumn("MES_PRONO", regexp_extract(col("datestr"), regex_pattern, 1))
                    .withColumn("datestr", col("datestr").cast("timestamp"))
                    .withColumn("DATE", col("DATE").cast("date"))
                    .filter((col("MES_PRONO") == f"{current_year}-{current_month-1:02d}") & (col("DATE") >= f"{current_year + 1}-01"))
                    .withColumnRenamed("DATE","ANIO_MES")
                    .withColumn("ANIO_MES", regexp_extract(col("ANIO_MES"), regex_pattern, 1))
                    .withColumn("ROW_NUM", row_number().over(particion))
                    .filter(col('ROW_NUM')==1)
                    .drop("ROW_NUM")
                    #.select("MATERIAL","ID_CANAL_DISTRIBUCION","ANIO_MES","value")
                    .withColumnRenamed("value", "U_PRONO")
                    .withColumnRenamed("MATERIAL","ID_MATERIAL")
                    .withColumn("ID_GRUPO_CLIENTE", lpad(col("ID_GRUPO_CLIENTE"), 10, "0"))
    )
else: #Aplica para los meses de febrero a noviembre, solo toma el mes anterior
    table = "HISTORIA_PRONO_MI"
    prono_mes_ant_mi = (spark.read.parquet(
        f"/mnt/adls-dac-data-bi-pr-scus/gold/{table}.parquet/")
                    .filter(col("CONCEPTO") == "U_PRONO")
                    .withColumn("MES_PRONO", regexp_extract(col("datestr"), regex_pattern, 1))
                    .filter((col("MES_PRONO") == f"{current_year}-{current_month-1:02d}"))
                    .withColumnRenamed("DATE","ANIO_MES")
                    .withColumn("ANIO_MES", regexp_extract(col("ANIO_MES"), regex_pattern, 1))
                    #.select("MATERIAL","ID_CANAL_DISTRIBUCION","ANIO_MES","value")
                    .withColumnRenamed("value", "U_PRONO")
                    .withColumnRenamed("MATERIAL","ID_MATERIAL")
    )

    keys = ["MATERIAL", "ID_GRUPO_CLIENTE","ID_CANAL_DISTRIBUCION","USUARIO","MES_PRONO","ANIO_MES"]
    particion = (Window.partitionBy(keys).orderBy(desc('datestr')))
    table =  "prono_mkt_memo"
    prono_mes_ant_memo = (spark.read.parquet(
        f"/mnt/adls-dac-data-bi-pr-scus/gold/{table}.parquet/")
.withColumn("ID_GRUPO_CLIENTE", lit(""))
                    .filter(col("CONCEPTO") == "U_PRONO")
                    .withColumn("MES_PRONO", regexp_extract(col("datestr"), regex_pattern, 1))
                    .withColumn("datestr", col("datestr").cast("timestamp"))
                    .filter((col("MES_PRONO") == f"{current_year}-{current_month-1:02d}"))
                    .withColumnRenamed("DATE","ANIO_MES")
                    .withColumn("ANIO_MES", regexp_extract(col("ANIO_MES"), regex_pattern, 1))
                    # .withColumn("ROW_NUM", row_number().over(particion))
                    # .filter(col('ROW_NUM')==1)
                    # .drop("ROW_NUM")
                    #.select("MATERIAL","ID_CANAL_DISTRIBUCION","ANIO_MES","value")
                    .withColumnRenamed("value", "U_PRONO")
                    .withColumnRenamed("MATERIAL","ID_MATERIAL")
# Esta línea rellena el campo 'ID_GRUPO_CLIENTE' con ceros a la izquierda hasta que tenga 10 caracteres
.withColumn("ID_GRUPO_CLIENTE", lpad(col("ID_GRUPO_CLIENTE"), 10, "0"))
    )


# COMMAND ----------

display(prono_mes_ant_mi.select("LINEA_AGRUPADA").distinct())


# COMMAND ----------

table = "resultados_pronostico_no_parte_escalera" #Modelo que se usa para los layouts de mkt 
aa_mkt = (spark.read.parquet(
    f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/")
)

# COMMAND ----------

table = "resultados_pronostico_gpo_cte_escalera" #Modelo que se usa para los layouts de vtas
aa_vtas = (spark.read.parquet(
    f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Carga de catálogos
# MAGIC

# COMMAND ----------

# Catálogo ID's
catalogo_ids = spark.createDataFrame(
    pd.read_excel(
        "/dbfs/mnt/adls-dac-data-bi-pr-scus/powerappslandingzone_cat_ids/catalogo_de_ids.xlsx"
    )
)
ids_sistema = catalogo_ids.select("GrM", "SISTEMA").filter(~isnull("GrM"))
ids_compania = (
    catalogo_ids.select("GrM2", "CIA")
    .filter(~isnull("GrM2"))
    .withColumnRenamed("CIA", "COMPANIA")
)
ids_clase = catalogo_ids.select("GrM3", "CLASE").filter(~isnull("GrM3"))
ids_segmento = catalogo_ids.select("GrM4", "SEGMENTO").filter(~isnull("GrM4"))
ids_marca = catalogo_ids.select("GrM5", "MARCA").filter(~isnull("GrM5"))
ids_linea = (
    catalogo_ids.select("GrM6", "LINEA")
    .filter(~isnull("LINEA"))
    .withColumn("GrM6", when(col("GrM6").isNull(), lit("NA")).otherwise(col("GrM6")))
    .withColumnRenamed("LINEA", "LINEA_ABIERTA")
)
ids_cfdi = catalogo_ids.select("C_CLAVEPRODSERV", "CFDI").filter(
    ~isnull("C_CLAVEPRODSERV")
)
ids_carta_porte = catalogo_ids.select("CLAVE_CARTA_PORTE", "CARTA_PORTE").filter(
    ~isnull("CARTA_PORTE")
)
ids_linea_ab_linea_ag = (
    catalogo_ids.select("LINEA ABIERTA", "LINEA AGRUPADA")
    .filter(~isnull("LINEA ABIERTA"))
    .withColumnRenamed("LINEA ABIERTA", "LINEA_ABIERTA")
    .withColumnRenamed("LINEA AGRUPADA", "LINEA_AGRUPADA")
)

# COMMAND ----------

# Responasable de línea agrupada mkt
responsable_linea_mkt = (
    spark.createDataFrame(
        pd.read_excel(
            "/dbfs/mnt/adls-dac-data-bi-pr-scus/powerappslandingzone_catalogo_esp/responsable_linea_dacomsa_mkt.xlsx"
        )
    )
    .withColumnRenamed("SIS", "SISTEMA")
    .withColumnRenamed("CIA", "COMPANIA")
    .withColumnRenamed("SEG", "SEGMENTO")
    .withColumnRenamed("LINEA ABIERTA", "LINEA_ABIERTA")
    .withColumnRenamed("LINEA AGRUPADA", "LINEA_AGRUPADA")
    .select(
        "SISTEMA",
        "COMPANIA",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "CLASE",
        "SEGMENTO",
        "COORDINADOR",
        "ESPECIALISTA",
    )
)

# COMMAND ----------

# Catálogo de descuento y factor
catalogo_desc = (
    spark.createDataFrame(
        pd.read_excel(
            "/dbfs/mnt/adls-dac-data-bi-pr-scus/dummy/Descuentos - Factores para Prono.xlsx"
        )
    )
    .withColumnRenamed("SIS", "SISTEMA")
    .withColumnRenamed("CIA", "COMPANIA")
    .withColumnRenamed("SEG", "SEGMENTO")
    .withColumnRenamed("LINEA ABIERTA", "LINEA_ABIERTA")
    .withColumnRenamed("LINEA AGRUPADA", "LINEA_AGRUPADA")
    .withColumnRenamed("Desc A", "DESC_A")
    .withColumnRenamed("Factor", "FACTOR")
)

# COMMAND ----------

# Join responsable de línea con catálogo de descuento y factor
des_y_responsables_cat = responsable_linea_mkt.join(
    catalogo_desc,
    ["SISTEMA", "COMPANIA", "LINEA_ABIERTA", "LINEA_AGRUPADA", "CLASE", "SEGMENTO"],
    "left",
)

# COMMAND ----------

# Catálogo de responsable cliente-línea de ventas
responsable_linea_vtas = spark.createDataFrame(
    pd.read_excel(
        "/dbfs/mnt/adls-dac-data-bi-pr-scus/powerappslandingzone_cat_reg_asesor/Catálogo Región-Asesor comercial.xlsx"
    )
)
responsable_linea_vtas = (
    responsable_linea_vtas.withColumnRenamed("Email Región", "EMAIL")
    .withColumnRenamed("E-Mail", "EMAIL1")
    .withColumnRenamed("Región", "REGION")
    .withColumnRenamed("Asesor", "ASESOR")
    .withColumnRenamed("Grupo", "GRUPO_CLIENTE")
    .withColumnRenamed("Cliente", "CLIENTE")
    .withColumnRenamed("Línea", "LINEA_ABIERTA")
    .where(col("LINEA_ABIERTA") != "OTROS DESCUENTOS")
    .where(col("REGION") != "INACTIVOS")
)

responsable_linea_vtas_me =(
    responsable_linea_vtas.where((col("Mercado") == "ME") | (col("Mercado") == "MO"))
)
# responsable_linea_vtas.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Base escalera MI

# COMMAND ----------

current_month_dt = datetime(current_year, current_month, 1)
previous_month_dt = current_month_dt - relativedelta(months=1)
previous_month = previous_month_dt.month

# COMMAND ----------

#Inventario de libre utilización
inventario_mi = (inventario.select("ID_MATERIAL", "ID_CANAL_DISTRIBUCION", "ANIO","MES","TYPE_1","TYPE_2","STOCK")
                 .filter((col("ANIO") == current_year) & (col("MES").cast("int") == current_month) & (col("ID_CANAL_DISTRIBUCION") == "MI") & (col("TYPE_1") == "PIEZAS") & (col("TYPE_2") == "LIBRE UTILIZACION"))
                 .groupBy("ID_MATERIAL").sum("STOCK")
                 .withColumnRenamed("sum(STOCK)", "INVENTARIO")
                 )

# COMMAND ----------

#Se toma el modelo de analítica de mkt para hacer las columnas de U PRELLENADO
prellenado = (aa_mkt.drop("Fecha","Combinacion")
          .withColumn("ANIO",split(col("ANIO_MES_PRONOSTICO"),"-")[0])
          .withColumn("MES",split(col("ANIO_MES_PRONOSTICO"),"-")[1])
          .withColumnRenamed("ID_MATERIAL","MATERIAL")
          .where((col("ANIO") == current_year) & (col("MES").cast("int") == current_month))
          .join(catalogo_de_articulos_sap_mi
                    .where(col("ST") == 99)
                    .where(col("CDIS") == "MI")
                    .select(
        "MATERIAL",
        "SISTEMA",
        "COMPANIA",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "CLASE",
        "SEGMENTO",
    ), "MATERIAL", "left"
    )
    .join(
        lista_de_precios_mi.withColumnRenamed(
            "IMPORTE_DE_CONDICION", "PRECIO_DE_LISTA"
        ).select("MATERIAL", "PRECIO_DE_LISTA"),
        "MATERIAL",
        "left",
    )
    .where(col("PRECIO_DE_LISTA") != 0)
    .join(
        des_y_responsables_cat.select(
            "SISTEMA",
            "COMPANIA",
            "LINEA_ABIERTA",
            "LINEA_AGRUPADA",
            "CLASE",
            "SEGMENTO",
            "DESC_A",
            "FACTOR",
        ),
        ["SISTEMA", "COMPANIA", "LINEA_ABIERTA", "LINEA_AGRUPADA", "CLASE", "SEGMENTO"],
        "left",
    )
    .where(col("SISTEMA").isNotNull())
    .withColumn("VENTA", (col("PRECIO_DE_LISTA") * col("FACTOR")))
    .withColumn("PROMOCION", (col("DESC_A") * col("VENTA")))
    .fillna(0)
    .withColumn("unidades", when(col("VENTA") < col("PROMOCION"), col("Pronostico")/col("VENTA")).otherwise(col("Pronostico")/col("PROMOCION")))
    .withColumn("unidades",when(col("unidades").isNull(), col("Pronostico")/col("PRECIO_DE_LISTA")).otherwise(col("unidades")))
    .withColumn("U PRELLENADO", ceil(col("unidades")))
    .withColumnRenamed("Pronostico", "$ PRELLENADO")
    .withColumn("$ PRELLENADO", col("$ PRELLENADO")/1000)
    .withColumn("$ PRELLENADO", col("$ PRELLENADO").cast("double"))
    .select("MATERIAL","ID_CANAL_DISTRIBUCION","ANIO_MES","U PRELLENADO","$ PRELLENADO")
    .withColumn("U PRELLENADO",col("U PRELLENADO").cast("int"))
    .withColumn("$ PRELLENADO",col("$ PRELLENADO").cast("double"))
    )

# COMMAND ----------

prellenado.display()

# COMMAND ----------

prellenado_u = (prellenado.select("MATERIAL","ID_CANAL_DISTRIBUCION","ANIO_MES","U PRELLENADO")
                .withColumn("ANIO_MES",concat(col("ANIO_MES"),lit(" U PRELLENADO")))
                .groupBy("MATERIAL","ID_CANAL_DISTRIBUCION")
                .pivot("ANIO_MES")
                .agg(first("U PRELLENADO"))
                )

# COMMAND ----------

prellenado_imp = (prellenado.select("MATERIAL","ID_CANAL_DISTRIBUCION","ANIO_MES","$ PRELLENADO")
                .withColumn("ANIO_MES",concat(col("ANIO_MES"),lit(" $ PRELLENADO")))
                .groupBy("MATERIAL","ID_CANAL_DISTRIBUCION")
                .pivot("ANIO_MES")
                .agg(first("$ PRELLENADO"))
                )

# COMMAND ----------

#Se identifica el backorder para cad número de parte
current_month_dt = datetime(current_year, current_month, 1)
previous_month_dt = current_month_dt - relativedelta(months=1)
previous_month = previous_month_dt.month

back_order = (
    fact_pedidos.select(
        "ID_MATERIAL",
        "ID_CANAL_DISTRIBUCION",
        "MES_FECHA_REGISTRO",
        "ANIO_FECHA_REGISTRO",
        "PIEZAS_BACKORDER",
    )
    .where(
        (fact_pedidos["MES_FECHA_REGISTRO"] == previous_month)
        & (fact_pedidos["ANIO_FECHA_REGISTRO"] == current_year)
        & (fact_pedidos["ID_CANAL_DISTRIBUCION"] == "MI")
    )
    .groupBy("ID_MATERIAL")
    .sum()
    .select("ID_MATERIAL", "sum(PIEZAS_BACKORDER)")
    .withColumnRenamed("sum(PIEZAS_BACKORDER)", "BACKORDER")
)

# COMMAND ----------

# Variables necesarias
current_year = datetime.now().year #Año actual
today = datetime.now()

last_month_str = f"{current_month - 1:02d}" # Mes anterior

date_u_limit = today - timedelta(days=3 * 365) #Límite de 36 meses atrás unidades
date_u_limit_str = date_u_limit.strftime("%Y-%m") + " U REAL"

date_limit = today - timedelta(days=3 * 365) #Límite de 36 meses atrás importe
date_limit_str = date_limit.strftime("%Y-%m") + " $ REAL"

current_year_str_start_u = f"{current_year-1}-12 U REAL" #Inicio del año actual para unidades
current_year_str_end_u = f"{current_year}-{current_month} U REAL"

current_year_str_start_imp = f"{current_year}-01 $ REAL" #Inicio del año actual para importe
current_year_str_end_imp = f"{current_year}-{current_month} $ REAL"

last_month_u_str = f"{current_year}-{last_month_str} U REAL" #Mes anterior para unidades
last_month_imp_str = f"{current_year}-{last_month_str} $ REAL" #Mes anterior para importe

date_u_prono = f"{current_year}-01 U PRONO" #Inicio año actual unidades prono
date_imp_prono = f"{current_year}-01 $ PRONO" #Inicio año actual importe prono

# Demanda en unidades MI
demanda_mes_id_mat = (
    fact_pedidos
    .groupBy("ID_CANAL_DISTRIBUCION", "ANIO_MES", "ID_MATERIAL")
    .sum()
    .select("ID_CANAL_DISTRIBUCION", "ID_MATERIAL", "ANIO_MES", "sum(PIEZAS)")
    .withColumn("ANIO_MES", concat(col("ANIO_MES"), lit(" U REAL")))
)

current_year_sum_u = ( #Columna de TOTAL {current_year}
    fact_pedidos.filter(
        col("ANIO_MES").between(current_year_str_start_u, current_year_str_end_u)
    )
    .groupBy("ID_CANAL_DISTRIBUCION", "ID_MATERIAL")
    .agg(sum("PIEZAS").alias(f"TOTAL {current_year} U"))
)

# Prono unidades

if prono_mes_ant_mi.count() == 0:
    prono_unidades = (
        demanda_mes_id_mat.withColumn(
            "MES_PRONO",
            explode(
                expr(
                    f"sequence(to_date('{current_year}-01-01'), add_months(to_date('{current_year}-01-01'), 31), interval 1 month)"
                )
            ),
        )
        .withColumn("MES_PRONO", date_format(col("MES_PRONO"), "yyyy-MM"))
        .withColumn("MES_PRONO", concat(col("MES_PRONO"), lit(" U PRONO")))
        .filter((col("ID_CANAL_DISTRIBUCION") == "MI") & (col("MES_PRONO") >= date_u_prono))
        .groupBy("ID_CANAL_DISTRIBUCION", "ID_MATERIAL")
        .pivot("MES_PRONO")
        .agg(lit(0))
    )
else:
    prono_unidades = ( #Unidades pronosticadas del mes anterior
        prono_mes_ant_mi
        .withColumn("ANIO_MES", concat(col("ANIO_MES"), lit(" U PRONO")))
        .groupBy("ID_CANAL_DISTRIBUCION","ID_MATERIAL").pivot("ANIO_MES")
        .agg(first("U_PRONO"))

    )

#Pronostico de analítica avanzada

prellenado_analitica_u = prellenado_u.withColumnRenamed("MATERIAL","ID_MATERIAL")

dem_prono_cols = prono_unidades.columns[2:]

demanda_mes_id_mat = (
    demanda_mes_id_mat.filter(
        (col("ANIO_MES") >= date_u_limit_str) & (col("ID_CANAL_DISTRIBUCION") == "MI")
        & (col("ANIO_MES") <= last_month_u_str)
    )
    .groupBy("ID_CANAL_DISTRIBUCION", "ID_MATERIAL")
    .pivot("ANIO_MES")
    .sum("sum(PIEZAS)")
    .join(current_year_sum_u, ["ID_CANAL_DISTRIBUCION", "ID_MATERIAL"], "left")
    .withColumn(f"TOTAL {current_year} PRONO ACUM", lit(""))
    .join(prono_unidades, ["ID_CANAL_DISTRIBUCION", "ID_MATERIAL"], "left")
    .join(prellenado_analitica_u,["ID_CANAL_DISTRIBUCION","ID_MATERIAL"],"left")
)

dem_columns_a_u = demanda_mes_id_mat.columns
# total_anio_cal_u = current_year_sum_u.columns[-1:]
# dem_columns_b_u = demanda_mes_id_mat.columns[34:-1]

reorder_u = dem_columns_a_u

# Demanda importe MI
importe_mes_id_mat = (
    fact_pedidos.groupBy("ID_CANAL_DISTRIBUCION", "ANIO_MES", "ID_MATERIAL")
    .sum()
    .select("ID_CANAL_DISTRIBUCION", "ID_MATERIAL", "ANIO_MES", "sum(IMPORTE)")
    .withColumn("sum(IMPORTE)", col("sum(IMPORTE)")/1000)
    .withColumn("sum(IMPORTE)", col("sum(IMPORTE)").cast("double"))
    .withColumn("ANIO_MES", concat(col("ANIO_MES"), lit(" $ REAL")))
)



# Prono importe
prono_importe = (
    importe_mes_id_mat.withColumn(
        "MES_PRONO",
        explode(
            expr(
                f"sequence(to_date('{current_year}-01'), add_months(to_date('{current_year}-01'), 31), interval 1 month)"
            )
        ),
    )
    .withColumn("MES_PRONO", date_format(col("MES_PRONO"), "yyyy-MM"))
    .withColumn("MES_PRONO", concat(col("MES_PRONO"), lit(" $ PRONO")))
    .filter(
        (col("ID_CANAL_DISTRIBUCION") == "MI") & (col("MES_PRONO") >= date_imp_prono)
    )
    .groupBy("ID_CANAL_DISTRIBUCION", "ID_MATERIAL")
    .pivot("MES_PRONO")
    .agg(lit(0))
)

#Prellenado analítica avanzada importe

prellenado_analitica_imp = prellenado_imp.withColumnRenamed("MATERIAL","ID_MATERIAL")

importe_mes_id_mat = (
    importe_mes_id_mat.filter(
        (col("ANIO_MES") >= date_limit_str) & (col("ID_CANAL_DISTRIBUCION") == "MI")
        & (col("ANIO_MES") <= last_month_imp_str)
    )
    .groupBy("ID_CANAL_DISTRIBUCION", "ID_MATERIAL")
    .pivot("ANIO_MES")
    .sum("sum(IMPORTE)")
    .join(prono_importe, ["ID_CANAL_DISTRIBUCION", "ID_MATERIAL"], "left")
    .join(prellenado_analitica_imp,["ID_CANAL_DISTRIBUCION","ID_MATERIAL"],"left")
    #   .join(previous_year_sum_imp, ["ID_CANAL_DISTRIBUCION", "ID_MATERIAL"], "left")
)

dem_columns_a_imp = importe_mes_id_mat.columns[2:]
# total_anio_cal_ant_imp = previous_year_sum_imp.columns[-1:]
# dem_columns_b_imp = importe_mes_id_mat.columns[34:-1]

reorder_imp = dem_columns_a_imp

reorder = reorder_u + reorder_imp

# Join de las columnas de prc y u
base_esc_b = demanda_mes_id_mat.fillna(0).join(
    importe_mes_id_mat, ["ID_MATERIAL", "ID_CANAL_DISTRIBUCION"], "inner"
).select(*reorder)

# COMMAND ----------

reorder = [
    "MATERIAL",
    "TEXTO_BREVE_DE_MATERIAL",
    "SISTEMA",
    "COMPANIA",
    "LINEA_ABIERTA",
    "LINEA_AGRUPADA",
    "CLASE",
    "SEGMENTO",
    "INDICADOR_ABC",
    "CDIS",
    "CARACT_PLANIF_NEC",
    "STATMAT_TODAS_CADDIS",
    "ST",
    "PB_NIVEL_CENTRO",
    "PRECIO_DE_LISTA",
    "COORDINADOR",
    "ESPECIALISTA",
    "DESC_A",
    "PROMOCION",
    "VENTA",
    "FACTOR",
    "BACKORDER",
    "INVENTARIO",
    "PROMEDIO 12M",
    "PROMEDIO 6M",
    "PROMEDIO 3M",
    "VAR 12M",
    "VAR 6M",
    "VAR 3M",
]

demanda_base_mi = (
    catalogo_de_articulos_sap_mi.select(
        "MATERIAL",
        "SISTEMA",
        "COMPANIA",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "CLASE",
        "SEGMENTO",
        "INDICADOR_ABC",
        "TEXTO_BREVE_DE_MATERIAL",
        "CDIS",
        "CARACT_PLANIF_NEC",
        "STATMAT_TODAS_CADDIS",
        "ST",
        "PB_NIVEL_CENTRO",
    )
    .where(col("ST") == 99)
    .where(col("CDIS") == "MI")
    .join(
        lista_de_precios_mi.withColumnRenamed(
            "IMPORTE_DE_CONDICION", "PRECIO_DE_LISTA"
        ).select("MATERIAL", "PRECIO_DE_LISTA"),
        "MATERIAL",
        "left",
    )
    .join(
        des_y_responsables_cat.select(
            "SISTEMA",
            "COMPANIA",
            "LINEA_ABIERTA",
            "LINEA_AGRUPADA",
            "CLASE",
            "SEGMENTO",
            "COORDINADOR",
            "ESPECIALISTA",
            "DESC_A",
            "FACTOR",
        ),
        ["SISTEMA", "COMPANIA", "LINEA_ABIERTA", "LINEA_AGRUPADA", "CLASE", "SEGMENTO"],
        "left",
    )
    .join(back_order.withColumnRenamed("ID_MATERIAL", "MATERIAL"), "MATERIAL", "left")
    .join(inventario_mi.withColumnRenamed("ID_MATERIAL", "MATERIAL"), "MATERIAL", "left")
    .withColumn("DESC_A", col("DESC_A").cast("double"))
    .withColumn("FACTOR", col("FACTOR").cast("double"))
    .withColumn("VENTA", round((col("PRECIO_DE_LISTA") * col("FACTOR")),2))
    .withColumn("PROMOCION", round((col("DESC_A") * col("VENTA")),2))
    .withColumn("PROMEDIO 12M", lit(" "))
    .withColumn("PROMEDIO 6M", lit(" "))
    .withColumn("PROMEDIO 3M", lit(" "))
    .withColumn("VAR 12M", lit(" "))
    .withColumn("VAR 6M", lit(" "))
    .withColumn("VAR 3M", lit(" "))
    .select(*reorder)
    .join(base_esc_b.withColumnRenamed("ID_MATERIAL", "MATERIAL"), "MATERIAL", "right")
    .drop("ID_CANAL_DISTRIBUCION")
    .withColumnRenamed("CDIS","ID_CANAL_DISTRIBUCION")
    .withColumn("ESPECIALISTA", when(col("ESPECIALISTA").isNull(),lit("jesus.serratos@dacomsa.com")).otherwise(col("ESPECIALISTA")))
    .withColumn("COORDINADOR", when(col("COORDINADOR").isNull(),lit("rodolfo.villalba@dacomsa.com")).otherwise(col("COORDINADOR")))
    .na.fill(0)
    .dropDuplicates(["MATERIAL", "ESPECIALISTA"])
    .filter(col("ID_CANAL_DISTRIBUCION") == "MI")
)
path = "/mnt/adls-dac-data-bi-pr-scus/silver/marketing/BASE_ESCALERA_MI.parquet"
demanda_base_mi.write.format("delta").partitionBy("ESPECIALISTA").mode("overwrite").parquet(path)

# COMMAND ----------

# MAGIC %md
# MAGIC #Base USA

# COMMAND ----------

region_us = ["000016"]
region_latam = ["000015"]

# COMMAND ----------

dim_material  = (catalogo_de_articulos_sap_memo
                 .select("MATERIAL","CDIS","SISTEMA","COMPANIA","LINEA_ABIERTA","LINEA_AGRUPADA","CLASE","SEGMENTO")
                 .withColumnRenamed("MATERIAL","ID_MATERIAL")
                 .withColumnRenamed("CDIS","ID_CANAL_DISTRIBUCION")
)

# COMMAND ----------

lista_memo_join = (lista_de_precios_memo
                   .select("CDIS","CLIENTE",'RAZON_SOCIAL')
                #    .withColumn("NO_CLTE",concat(lit("000000"),col("NO_CLTE")))
                   .withColumnRenamed("CLIENTE","ID_GRUPO_CLIENTE")
                   #.withColumnRenamed("MATERIAL","ID_MATERIAL")
                   .withColumnRenamed("CDIS","ID_CANAL_DISTRIBUCION")
                   .dropDuplicates()
)

# COMMAND ----------

inventario_memo = (inventario.select("ID_MATERIAL", "ID_CANAL_DISTRIBUCION", "ANIO","MES","TYPE_1","TYPE_2","STOCK")
                 .filter((col("ANIO") == current_year) & (col("MES").cast("int") == current_month) & (col("ID_CANAL_DISTRIBUCION") != "MI") & (col("TYPE_1") == "PIEZAS") & (col("TYPE_2") == "LIBRE UTILIZACION"))
                 .groupBy("ID_MATERIAL").sum("STOCK")
                 .withColumnRenamed("sum(STOCK)", "INVENTARIO")
                 )

# COMMAND ----------

# Assuming you have defined current_month and current_year variables
current_month_dt = datetime(current_year, current_month, 1)
previous_month_dt = current_month_dt - relativedelta(months=1)
previous_month = previous_month_dt.month

back_order = (
    fact_pedidos.join(dim_clientes, ["ID_CLIENTE", "ID_CANAL_DISTRIBUCION"], "left")
    .where(col("ID_REGION").isin(region_us))
    .join(dim_material, ["ID_MATERIAL", "ID_CANAL_DISTRIBUCION"], "left")
    .select(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "ID_MATERIAL",
        "ID_CANAL_DISTRIBUCION",
        "PIEZAS_BACKORDER",
        "ANIO_MES",
    )
    .withColumnRenamed("ID_MATERIAL", "MATERIAL")
    .where(
        (fact_pedidos["MES_FECHA_REGISTRO"] == previous_month)
        & (fact_pedidos["ANIO_FECHA_REGISTRO"] == current_year)
        & (fact_pedidos["ID_CANAL_DISTRIBUCION"] != "MI")
    )
    .groupBy("MATERIAL","REGION","ASESOR","ID_SHIP_TO")
    .sum()
    .select("MATERIAL", "sum(PIEZAS_BACKORDER)")
    .withColumnRenamed("sum(PIEZAS_BACKORDER)", "BACKORDER")
)

# COMMAND ----------

current_year = datetime.now().year
today = datetime.now()

last_month_str = f"{current_month - 1:02d}"

date_u_limit = today - timedelta(days=3 * 365)
date_u_limit_str = date_u_limit.strftime("%Y-%m") + " U REAL"

date_limit = today - timedelta(days=3 * 365)
date_limit_str = date_limit.strftime("%Y-%m") + " $ REAL"

current_year_str_start_u = f"{current_year-1}-12 U REAL"
current_year_str_end_u = f"{current_year}-{current_month} U REAL"

current_year_str_start_imp = f"{current_year}-01 $ REAL"
current_year_str_end_imp = f"{current_year}-{current_month} $ REAL"

last_month_u_str = f"{current_year}-{last_month_str} U REAL"
last_month_imp_str = f"{current_year}-{last_month_str} $ REAL"

date_u_prono = f"{current_year}-01 U PRONO"
date_imp_prono = f"{current_year}-01 $ PRONO"

base_usa = (
    fact_pedidos.join(dim_clientes, ["ID_CLIENTE", "ID_CANAL_DISTRIBUCION"], "left")
    .where(col("ID_REGION").isin(region_us))
    .join(dim_material, ["ID_MATERIAL", "ID_CANAL_DISTRIBUCION"], "left")
    .select(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "ID_MATERIAL",
        "ID_CANAL_DISTRIBUCION",
        "PIEZAS",
        "IMPORTE",
        "PIEZAS_BACKORDER",
        "ANIO_MES",
    )
    .withColumnRenamed("ID_MATERIAL", "MATERIAL")
    .withColumn("IMPORTE",(col("IMPORTE")/1000).cast("double"))
    .withColumn("MATERIAL", when(col("MATERIAL").isin(["5789M","5809M", "5980M1"]), concat(lit("0"),col("MATERIAL"))).otherwise(col("MATERIAL"))) #Se hace para que hagan match con catálogo MEMO
)

# Demanda en piezas MEMO US
base_usa_id_material = (
    base_usa.groupBy(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "MATERIAL",
        "ID_CANAL_DISTRIBUCION",
        "ANIO_MES",
    )
    .sum()
    .select(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "MATERIAL",
        "ID_CANAL_DISTRIBUCION",
        "ANIO_MES",
        "sum(PIEZAS)",
    )
    .filter(
        (col("ANIO_MES") >= date_limit_str)
        & (col("ID_CANAL_DISTRIBUCION").isin(["ME", "MO"]))
        & (col("ANIO_MES") <= last_month_u_str)
    )
    .withColumn("ANIO_MES", concat(col("ANIO_MES"), lit(" U REAL")))
    .groupBy(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "MATERIAL",
        "ID_CANAL_DISTRIBUCION",
    )
    .pivot("ANIO_MES")
    .agg(sum("sum(PIEZAS)").alias("PIEZAS")) 
    .where(col("REGION") != "INACTIVOS")
)

current_year_sum_u = (
    base_usa.filter(
        col("ANIO_MES").between(current_year_str_start_u, current_year_str_end_u)
    )
    .groupBy("ID_CANAL_DISTRIBUCION", "MATERIAL","ID_GRUPO_CLIENTE","ID_SHIP_TO")
    .agg(sum("PIEZAS").alias(f"TOTAL {current_year} U"))
)



# COMMAND ----------

base_usa_id_material.display()

# COMMAND ----------


# Prono unidades

if prono_mes_ant_memo.count() == 0:
    prono_unidades = (
        base_usa_id_material.withColumn(
            "MES_PRONO",
            explode(
                expr(
                    f"sequence(to_date('{current_year}-01-01'), add_months(to_date('{current_year + 1}-01-01'), 31), interval 1 month)"
                )
            ),
        )
        .withColumn("MES_PRONO", date_format(col("MES_PRONO"), "yyyy-MM"))
        .withColumn("MES_PRONO", concat(col("MES_PRONO"), lit(" U PRONO")))
        .filter((col("ID_CANAL_DISTRIBUCION") == "MI") & (col("MES_PRONO") >= date_u_prono))
        .groupBy("ID_CANAL_DISTRIBUCION", "MATERIAL","ID_GRUPO_CLIENTE")
        .pivot("MES_PRONO")
        .agg(lit(0))
    )
else:
    prono_unidades = ( #Unidades pronosticadas del mes anterior
        prono_mes_ant_memo
        .withColumn("ANIO_MES", concat(col("ANIO_MES"), lit(" U PRONO")))
        .withColumnsRenamed({"ID_MATERIAL":"MATERIAL",
                             "ID_GRUPO_CLIENTE":"ID_SHIP_TO"})
        .groupBy("ID_CANAL_DISTRIBUCION","MATERIAL","ID_SHIP_TO").pivot("ANIO_MES")
        .agg(first("U_PRONO"))
        .withColumn("ID_GRUPO_CLIENTE", regexp_extract(col("ID_SHIP_TO"),   r"0*(\d{4})(?=\D|$)", 1))
        .withColumn("ID_GRUPO_CLIENTE", lpad(col("ID_GRUPO_CLIENTE"), 10, "0"))
    )

dem_prono_cols = prono_unidades.columns[2:]

base_usa_id_material = (
    base_usa_id_material
    .join(current_year_sum_u, ["ID_CANAL_DISTRIBUCION", "MATERIAL","ID_GRUPO_CLIENTE","ID_SHIP_TO"], "left")
    .withColumn(f"TOTAL {current_year} PRONO ACUM", lit(""))
    .join(prono_unidades, ["ID_CANAL_DISTRIBUCION", "MATERIAL","ID_GRUPO_CLIENTE","ID_SHIP_TO"], "left")
)

dem_columns_a_u = base_usa_id_material.columns
reorder_u = dem_columns_a_u

# # Demanda en importe MEMO US

base_usa_importe = (
    base_usa.groupBy(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "MATERIAL",
        "ID_CANAL_DISTRIBUCION",
        "ANIO_MES",
    )
    .sum()
    .select(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "MATERIAL",
        "ID_CANAL_DISTRIBUCION",
        "ANIO_MES",
        "sum(IMPORTE)",
    )
    .filter(
        (col("ANIO_MES") >= date_limit_str) & (col("ANIO_MES") <= last_month_imp_str)
    )
    .withColumn("ANIO_MES", concat(col("ANIO_MES"), lit(" $ REAL")))
    #.withColumn("sum(IMPORTE)", col)
    .groupBy(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "MATERIAL",
        "ID_CANAL_DISTRIBUCION",
    )
    .pivot("ANIO_MES")
    .sum("sum(IMPORTE)")
    .where(col("REGION") != "INACTIVOS")
)

# # PRONO IMPORTE

prono_importe = (
    base_usa_importe.withColumn(
        "MES_PRONO",
        explode(
            expr(
                f"sequence(to_date('{current_year}-01-01'), add_months(to_date('{current_year}-01-01'), 31), interval 1 month)"
            )
        ),
    )
    .withColumn("MES_PRONO", date_format(col("MES_PRONO"), "yyyy-MM"))
    .withColumn("MES_PRONO", concat(col("MES_PRONO"), lit(" $ PRONO")))
    .filter((col("MES_PRONO") >= date_imp_prono))
    .groupBy("ID_CANAL_DISTRIBUCION", "MATERIAL")
    .pivot("MES_PRONO")
    .agg(lit(0))
)

# # PRONO IMPORTE

base_usa_importe = base_usa_importe.join(
    prono_importe, ["ID_CANAL_DISTRIBUCION", "MATERIAL"], "left"
)

dem_columns_a_imp = base_usa_importe.columns[11:]

reorder_imp = dem_columns_a_imp
reorder = reorder_u + reorder_imp

base_esc_b = (base_usa_id_material.join(
    base_usa_importe,
    [
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "MATERIAL",
        "ID_CANAL_DISTRIBUCION"
    ],
    "left",
).fillna(0)
.select(*reorder)
)

# COMMAND ----------

reorder = [
    "ID_ASESOR",
    "ASESOR",
    "ID_PAIS",
    "ID_REGION",
    "REGION",
    "ID_GRUPO_CLIENTE",
    "RAZON_SOCIAL",
    "ID_SHIP_TO",
    "ESTADO",
    "SISTEMA",
    "COMPANIA",
    "LINEA_ABIERTA",
    "LINEA_AGRUPADA",
    "CLASE",
    "SEGMENTO",
    "MATERIAL",
    "ID_CANAL_DISTRIBUCION",
    "STATMAT_TODAS_CADDIS",
    "ST",
    "PB_NIVEL_CENTRO",
    "TEXTO_BREVE_DE_MATERIAL",
    "PRECIO_DE_LISTA",
    "BACKORDER",
    "INVENTARIO",
    "BUSCAR",
    "PRECIO",
    "RESUMEN"
]

reorder_us = reorder + reorder_u[11:] + reorder_imp


demanda_base_us = (
    catalogo_de_articulos_sap_memo.select(
        "MATERIAL",
        "SISTEMA",
        "COMPANIA",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "CLASE",
        "SEGMENTO",
        "INDICADOR_ABC",
        "TEXTO_BREVE_DE_MATERIAL",
        "CDIS",
        "CARACT_PLANIF_NEC",
        "STATMAT_TODAS_CADDIS",
        "ST",
        "PB_NIVEL_CENTRO",
    )
    .where((col("ST") == 99) | (col("ST") == 93) | (col("ST") == 97))
    .where(col("CDIS") != "MI")
    .join(back_order,"MATERIAL","left")
    .join(inventario_memo.withColumnRenamed("ID_MATERIAL","MATERIAL"),"MATERIAL","left")
    .join(base_esc_b.withColumnRenamed("ID_MATERIAL", "MATERIAL"), "MATERIAL", "left")
    .join(responsable_linea_vtas.select("EMAIL", "EMAIL1", "REGION", "ASESOR"),
    ["REGION", "ASESOR"],
    "inner",
    )
    .drop("REGION","ASESOR")
    .withColumnRenamed("EMAIL","REGION")
    .withColumnRenamed("EMAIL1","ASESOR")
    .dropDuplicates(["ASESOR","REGION","ESTADO","MATERIAL","ID_GRUPO_CLIENTE","ID_SHIP_TO"])
    .drop("ID_CANAL_DISTRIBUCION")
    .withColumnRenamed("CDIS","ID_CANAL_DISTRIBUCION")
    .na.fill(0)
    .withColumn("ID_GRUPO_CLIENTE", regexp_replace(col("ID_GRUPO_CLIENTE"), "^0+", ""))
    .join(
        lista_de_precios_memo.withColumnRenamed("IMPORTE_DE_CONDICION","PRECIO_DE_LISTA") 
            .withColumnRenamed("CLIENTE","ID_GRUPO_CLIENTE")
        .select("MATERIAL", "PRECIO_DE_LISTA", "MONEDA_CONDICION","ID_GRUPO_CLIENTE")
        .withColumn("PRECIO_DE_LISTA", col("PRECIO_DE_LISTA").cast("double"))
        # CONTROL DEL TIPO DE CAMBIO
        # CONTROL DEL TIPO DE CAMBIO
        .withColumn("PRECIO_DE_LISTA", when(col("MONEDA_CONDICION") == "USD", col("PRECIO_DE_LISTA") * 19.5).otherwise(col("PRECIO_DE_LISTA"))),
        ["ID_GRUPO_CLIENTE","MATERIAL"],
        "left"
    )
    .where(col("PRECIO_DE_LISTA") != 0)
    .withColumn("PRECIO_DE_LISTA",(col("PRECIO_DE_LISTA").cast("double")))
    .withColumn("BUSCAR",concat(col("RAZON_SOCIAL"),col("MATERIAL"),col("ID_CANAL_DISTRIBUCION")))
    .withColumn("PRECIO",concat(col("ID_GRUPO_CLIENTE"),col("MATERIAL")))
    .withColumn("RESUMEN",concat(col("LINEA_AGRUPADA"),col("ID_CANAL_DISTRIBUCION")))
    .withColumn("ID_SHIP_TO", regexp_replace(col("ID_SHIP_TO"), "^0+", ""))
    .withColumn("ID_CANAL_DISTRIBUCION", when(col("ID_GRUPO_CLIENTE").startswith("3"), "MO").otherwise("ME"))
    .drop("RAZON_SOCIAL")
)

demanda_base_us = (demanda_base_us.join(lista_memo_join.select("ID_CANAL_DISTRIBUCION", "ID_GRUPO_CLIENTE","RAZON_SOCIAL"),
                   ["ID_CANAL_DISTRIBUCION","ID_GRUPO_CLIENTE"], "left")
                    .select(*reorder_us)
                   )

#demanda_base_us = demanda_base_us.where(col("ID_GRUPO_CLIENTE") == col("ID_SHIP_TO"))

path = "/mnt/adls-dac-data-bi-pr-scus/silver/marketing/BASE_ESCALERA_US.parquet"
demanda_base_us.write.format("delta").mode("overwrite").parquet(path)

# COMMAND ----------

demanda_base_us.groupBy("ID_REGION").agg(sum("2025-01 U PRONO")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Base LATAM

# COMMAND ----------

current_month_dt = datetime(current_year, current_month, 1)
previous_month_dt = current_month_dt - relativedelta(months=1)
previous_month = previous_month_dt.month

back_order = (
    fact_pedidos.join(dim_clientes, ["ID_CLIENTE", "ID_CANAL_DISTRIBUCION"], "left")
    .where((~col("ID_PAIS").isin(region_us)))
    .join(dim_material, ["ID_MATERIAL", "ID_CANAL_DISTRIBUCION"], "left")
    .select(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "ID_MATERIAL",
        "ID_CANAL_DISTRIBUCION",
        "PIEZAS_BACKORDER",
        "ANIO_MES"
    )
    .withColumnRenamed("ID_MATERIAL", "MATERIAL")
    .where(
        (fact_pedidos["MES_FECHA_REGISTRO"] == previous_month)
        & (fact_pedidos["ANIO_FECHA_REGISTRO"] == current_year)
        & (fact_pedidos["ID_CANAL_DISTRIBUCION"] != "MI")
    )
    .groupBy("MATERIAL","REGION","ASESOR","ID_SHIP_TO")
    .sum()
    .select("MATERIAL", "sum(PIEZAS_BACKORDER)")
    .withColumnRenamed("sum(PIEZAS_BACKORDER)", "BACKORDER")
)

# COMMAND ----------

current_year = datetime.now().year
today = datetime.now()

last_month_str = f"{current_month - 1:02d}"

date_u_limit = today - timedelta(days=3 * 365)
date_u_limit_str = date_u_limit.strftime("%Y-%m") + " U REAL"

date_limit = today - timedelta(days=3 * 365)
date_limit_str = date_limit.strftime("%Y-%m") + " $ REAL"

current_year_str_start_u = f"{current_year-1}-12 U REAL"
current_year_str_end_u = f"{current_year}-{current_month} U REAL"

current_year_str_start_imp = f"{current_year}-01 $ REAL"
current_year_str_end_imp = f"{current_year}-{current_month} $ REAL"

last_month_u_str = f"{current_year}-{last_month_str} U REAL"
last_month_imp_str = f"{current_year}-{last_month_str} $ REAL"

date_u_prono = f"{current_year}-01 U PRONO"
date_imp_prono = f"{current_year}-01 $ PRONO"

base_latam = (
    fact_pedidos.join(dim_clientes, ["ID_CLIENTE", "ID_CANAL_DISTRIBUCION"], "left")
    .where((col("ID_REGION").isin(region_latam)))
    .join(dim_material, ["ID_MATERIAL", "ID_CANAL_DISTRIBUCION"], "left")
    .select(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "ID_MATERIAL",
        "ID_CANAL_DISTRIBUCION",
        "PIEZAS",
        "IMPORTE",
        "PIEZAS_BACKORDER",
        "ANIO_MES",
    )
    .withColumnRenamed("ID_MATERIAL", "MATERIAL")
    .withColumn("IMPORTE",(col("IMPORTE")/1000).cast("double"))
    .withColumn("MATERIAL", when(col("MATERIAL").isin(["5789M","5809M", "5980M1"]), concat(lit("0"),col("MATERIAL"))).otherwise(col("MATERIAL"))) #Se hace para que hagan match con catálogo MEMO
)

# Demanda en piezas MEMO US
base_latam_id_material = (
    base_latam.groupBy(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "MATERIAL",
        "ID_CANAL_DISTRIBUCION",
        "ANIO_MES",
    )
    .sum()
    .select(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "MATERIAL",
        "ID_CANAL_DISTRIBUCION",
        "ANIO_MES",
        "sum(PIEZAS)",
    )
    .filter(
        (col("ANIO_MES") >= date_limit_str)
        & (col("ID_CANAL_DISTRIBUCION").isin(["ME", "MO"]))
        & (col("ANIO_MES") <= last_month_u_str)
    )
    .withColumn("ANIO_MES", concat(col("ANIO_MES"), lit(" U REAL")))
    .groupBy(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "MATERIAL",
        "ID_CANAL_DISTRIBUCION",
    )
    .pivot("ANIO_MES")
    .agg(sum("sum(PIEZAS)").alias("PIEZAS")) 
    .where(col("REGION") != "INACTIVOS")
)

current_year_sum_u = (
    base_latam.filter(
        col("ANIO_MES").between(current_year_str_start_u, current_year_str_end_u)
    )
    .groupBy("ID_CANAL_DISTRIBUCION", "MATERIAL","ID_GRUPO_CLIENTE","ID_SHIP_TO")
    .agg(sum("PIEZAS").alias(f"TOTAL {current_year} U"))
)


# PRONO UNIDADES
if prono_mes_ant_memo.count() == 0:
    prono_unidades = (
        base_latam_id_material.withColumn(
            "MES_PRONO",
            explode(
                expr(
                    f"sequence(to_date('{current_year}-01-01'), add_months(to_date('{current_year + 1}-01-01'), 31), interval 1 month)"
                )
            ),
        )
        .withColumn("MES_PRONO", date_format(col("MES_PRONO"), "yyyy-MM"))
        .withColumn("MES_PRONO", concat(col("MES_PRONO"), lit(" U PRONO")))
        .filter((col("ID_CANAL_DISTRIBUCION") == "MI") & (col("MES_PRONO") >= date_u_prono))
        .groupBy("ID_CANAL_DISTRIBUCION", "ID_MATERIAL","ID_GRUPO_CLIENTE")
        .pivot("MES_PRONO")
        .agg(lit(0))
    )
else:
    prono_unidades = ( #Unidades pronosticadas del mes anterior
        prono_mes_ant_memo
        .withColumn("ANIO_MES", concat(col("ANIO_MES"), lit(" U PRONO")))
        .withColumnsRenamed({"ID_MATERIAL":"MATERIAL",
                             "ID_GRUPO_CLIENTE":"ID_SHIP_TO"})
        .groupBy("ID_CANAL_DISTRIBUCION","MATERIAL","ID_SHIP_TO").pivot("ANIO_MES")
        .agg(first("U_PRONO"))
        .withColumn("ID_GRUPO_CLIENTE", regexp_extract(col("ID_SHIP_TO"),   r"0*(\d{4})(?=\D|$)", 1))
        .withColumn("ID_GRUPO_CLIENTE", lpad(col("ID_GRUPO_CLIENTE"), 10, "0"))
    )

dem_prono_cols = prono_unidades.columns[2:]

base_latam_id_material = (
    base_latam_id_material
    .join(current_year_sum_u, ["ID_CANAL_DISTRIBUCION", "MATERIAL","ID_GRUPO_CLIENTE","ID_SHIP_TO"], "left")
    .withColumn(f"TOTAL {current_year} PRONO ACUM", lit(""))
    .join(prono_unidades, ["ID_CANAL_DISTRIBUCION", "MATERIAL","ID_GRUPO_CLIENTE","ID_SHIP_TO"], "left")
)

dem_columns_a_u = base_latam_id_material.columns
reorder_u = dem_columns_a_u

# # Demanda en importe MEMO US

base_latam_importe = (
    base_latam.groupBy(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "MATERIAL",
        "ID_CANAL_DISTRIBUCION",
        "ANIO_MES",
    )
    .sum()
    .select(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "MATERIAL",
        "ID_CANAL_DISTRIBUCION",
        "ANIO_MES",
        "sum(IMPORTE)",
    )
    .filter(
        (col("ANIO_MES") >= date_limit_str) & (col("ANIO_MES") <= last_month_imp_str)
    )
    .withColumn("ANIO_MES", concat(col("ANIO_MES"), lit(" $ REAL")))
    .groupBy(
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "MATERIAL",
        "ID_CANAL_DISTRIBUCION",
    )
    .pivot("ANIO_MES")
    .sum("sum(IMPORTE)")
    .where(col("REGION") != "INACTIVOS")
)

# # PRONO IMPORTE

prono_importe = (
    base_latam_importe.withColumn(
        "MES_PRONO",
        explode(
            expr(
                f"sequence(to_date('{current_year}-01-01'), add_months(to_date('{current_year}-01-01'), 31), interval 1 month)"
            )
        ),
    )
    .withColumn("MES_PRONO", date_format(col("MES_PRONO"), "yyyy-MM"))
    .withColumn("MES_PRONO", concat(col("MES_PRONO"), lit(" $ PRONO")))
    .filter((col("MES_PRONO") >= date_imp_prono))
    .groupBy("ID_CANAL_DISTRIBUCION", "MATERIAL")
    .pivot("MES_PRONO")
    .agg(lit(0))
)

# # PRONO IMPORTE

base_latam_importe = base_latam_importe.join(
    prono_importe, ["ID_CANAL_DISTRIBUCION", "MATERIAL"], "left"
)

dem_columns_a_imp = base_latam_importe.columns[11:]

reorder_imp = dem_columns_a_imp
reorder = reorder_u + reorder_imp

base_esc_b = (base_latam_id_material.join(
    base_latam_importe,
    [
        "ID_ASESOR",
        "ASESOR",
        "ID_PAIS",
        "ID_REGION",
        "REGION",
        "ESTADO",
        "ID_GRUPO_CLIENTE",
        "RAZON_SOCIAL",
        "ID_SHIP_TO",
        "MATERIAL",
        "ID_CANAL_DISTRIBUCION"
    ],
    "inner",
).fillna(0)
 .select(*reorder)
 )

# COMMAND ----------

reorder = [
    "ID_ASESOR",
    "ASESOR",
    "ID_PAIS",
    "ID_REGION",
    "REGION",
    "ID_GRUPO_CLIENTE",
    "RAZON_SOCIAL",
    "ID_SHIP_TO",
    "ESTADO",
    "SISTEMA",
    "COMPANIA",
    "LINEA_ABIERTA",
    "LINEA_AGRUPADA",
    "CLASE",
    "SEGMENTO",
    "MATERIAL",
    "ID_CANAL_DISTRIBUCION",
    "STATMAT_TODAS_CADDIS",
    "ST",
    "PB_NIVEL_CENTRO",
    "TEXTO_BREVE_DE_MATERIAL",
    "PRECIO_DE_LISTA",
    "BACKORDER",
    "INVENTARIO",
    "BUSCAR",
    "PRECIO",
    "RESUMEN"
]

reorder_latam = reorder + reorder_u[11:] + reorder_imp


demanda_base_latam = (
    catalogo_de_articulos_sap_memo.select(
        "MATERIAL",
        "SISTEMA",
        "COMPANIA",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "CLASE",
        "SEGMENTO",
        "INDICADOR_ABC",
        "TEXTO_BREVE_DE_MATERIAL",
        "CDIS",
        "CARACT_PLANIF_NEC",
        "STATMAT_TODAS_CADDIS",
        "ST",
        "PB_NIVEL_CENTRO",
    )
    .where((col("ST") == 99) | (col("ST") == 93) | (col("ST") == 97))
    .where(col("CDIS") != "MI")
    .join(back_order,"MATERIAL","left")
    .join(inventario_memo.withColumnRenamed("ID_MATERIAL","MATERIAL"),"MATERIAL","left")
    .join(base_esc_b.withColumnRenamed("ID_MATERIAL", "MATERIAL"), "MATERIAL", "left")
    .join(responsable_linea_vtas.select("EMAIL", "EMAIL1", "REGION", "ASESOR"),
    ["REGION", "ASESOR"],
    "inner",
    )
    .drop("REGION","ASESOR")
    .withColumnRenamed("EMAIL","REGION")
    .withColumnRenamed("EMAIL1","ASESOR")
    .dropDuplicates(["ASESOR","REGION","ESTADO","MATERIAL","ID_GRUPO_CLIENTE","ID_SHIP_TO"])
    .drop("CDIS")
    .na.fill(0)
    .withColumn("ID_GRUPO_CLIENTE", regexp_replace(col("ID_GRUPO_CLIENTE"), "^0+", ""))
    .join(
        lista_de_precios_memo.withColumnRenamed("IMPORTE_DE_CONDICION","PRECIO_DE_LISTA") 
            .withColumnRenamed("CLIENTE","ID_GRUPO_CLIENTE")
        .select("MATERIAL", "PRECIO_DE_LISTA", "MONEDA_CONDICION","ID_GRUPO_CLIENTE")
        .withColumn("PRECIO_DE_LISTA", col("PRECIO_DE_LISTA").cast("double"))
        .withColumn("PRECIO_DE_LISTA", when(col("MONEDA_CONDICION") == "USD", col("PRECIO_DE_LISTA") * 19.5).otherwise(col("PRECIO_DE_LISTA"))),
        ["ID_GRUPO_CLIENTE","MATERIAL"],
        "left"
    )
    .where(col("PRECIO_DE_LISTA") != 0)
    .withColumn("BUSCAR",concat(col("RAZON_SOCIAL"),col("MATERIAL"),col("ID_CANAL_DISTRIBUCION")))
    .withColumn("PRECIO",concat(col("ID_GRUPO_CLIENTE"),col("MATERIAL")))
    .withColumn("RESUMEN",concat(col("LINEA_AGRUPADA"),col("ID_CANAL_DISTRIBUCION")))
    .withColumn("ID_SHIP_TO", regexp_replace(col("ID_SHIP_TO"), "^0+", ""))
    .withColumn("ID_CANAL_DISTRIBUCION", when(col("ID_GRUPO_CLIENTE").startswith("3"), "MO").otherwise("ME"))
    .drop("RAZON_SOCIAL")
)

demanda_base_latam = (demanda_base_latam.join(lista_memo_join.select("ID_CANAL_DISTRIBUCION", "ID_GRUPO_CLIENTE","RAZON_SOCIAL"),
                   ["ID_CANAL_DISTRIBUCION","ID_GRUPO_CLIENTE"], "left")
                    .where(col("RAZON_SOCIAL") != " ")
                    .select(*reorder_latam)
                   )

#demanda_base_latam_me = demanda_base_latam.where((col("ID_CANAL_DISTRIBUCION") == "ME"))

#demanda_base_latam_mo = demanda_base_latam.where((col("ID_CANAL_DISTRIBUCION") == "MO") & (col("ID_GRUPO_CLIENTE") == col("ID_SHIP_TO")))

#demanda_base_latam = demanda_base_latam_me.union(demanda_base_latam_mo)

path = "/mnt/adls-dac-data-bi-pr-scus/silver/marketing/BASE_ESCALERA_LATAM.parquet"
demanda_base_latam.write.format("delta").mode("overwrite").parquet(path)

# COMMAND ----------

demanda_base_latam.groupBy("ID_REGION").agg(sum("2025-01 U PRONO")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Escalera Ventas

# COMMAND ----------

w = Window.orderBy("ANIO_MES")

# Crear un DataFrame con la columna MES_PRONO
prono = spark.range(1)  # Inicia un DataFrame con una fila para usar explode
prono = (
    prono.withColumn(
        "ANIO_MES",
        explode(
            expr(
                "sequence(add_months(current_date(), 1), add_months(current_date(), 19), interval 1 month)"
            )
        ),
    )
    .withColumn("ANIO_MES", date_format(col("ANIO_MES"), "yyyy-MM"))
    .withColumn("ANIO_MES", to_date(col("ANIO_MES")))
    .withColumn("aux", lit(1))
    .withColumn("aux", sum(lit(0.01)).over(w).cast("double"))
    .withColumn("aux", lit(1) + col("aux") - lit(0.01))
    .withColumn("aux", col("aux").cast("double"))  # Limit the decimal places to 2
    .drop("id")
)

# COMMAND ----------

last_month_date = add_months(to_date(lit(current_date.strftime("%Y-%m-%d"))), -1)

# COMMAND ----------

venta_mes_ant = (fact_ventas_pedidos.where(col("ID_CANAL_DISTRIBUCION") == "MI").join(dim_clientes, "ID_CTE", "left")
    .select(
        "REGION",
        "ASESOR",
        "GRUPO_CLIENTE",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "VENTA_IMPORTE",
        "ANIO_MES",
    )
    .groupBy(
        "REGION",
        "ASESOR",
        "GRUPO_CLIENTE",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "ANIO_MES",
    )
    .agg(sum("VENTA_IMPORTE").alias("VENTA_IMPORTE"))
    .withColumn("ANIO_MES", to_date(col("ANIO_MES")))
    .join(
        responsable_linea_vtas.select(
            "EMAIL", "EMAIL1", "REGION", "ASESOR", "GRUPO_CLIENTE", "LINEA_ABIERTA"
        ),
        ["REGION", "ASESOR", "GRUPO_CLIENTE", "LINEA_ABIERTA"],
        "inner",
    )
    .select(
        "EMAIL",
        "EMAIL1",
        "GRUPO_CLIENTE",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "ANIO_MES",
        "VENTA_IMPORTE",
    )
    .withColumnRenamed("EMAIL", "REGION")
    .withColumnRenamed("EMAIL1", "ASESOR")
    .filter(col("ANIO_MES") == date_format(add_months(last_month_date, 0), "yyyy-MM-01"))
    .withColumn("VENTA_IMPORTE",col("VENTA_IMPORTE")/1000)
    .withColumnRenamed("VENTA_IMPORTE","VENTA_IMPORTE_MES_ANTERIOR")
    .withColumn("VENTA_IMPORTE_MES_ANTERIOR", 
                when(col("VENTA_IMPORTE_MES_ANTERIOR") < 0, 0)
                .otherwise(col("VENTA_IMPORTE_MES_ANTERIOR"))
    )
)

# COMMAND ----------

escalera_vtas = (
    fact_ventas_pedidos.where(col("ID_CANAL_DISTRIBUCION") == "MI").join(dim_clientes, "ID_CTE", "left")
    .select(
        "REGION",
        "ASESOR",
        "GRUPO_CLIENTE",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "VENTA_IMPORTE",
        "ANIO_MES",
    )
    .groupBy(
        "REGION",
        "ASESOR",
        "GRUPO_CLIENTE",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "ANIO_MES",
    )
    .agg(sum("VENTA_IMPORTE").alias("VENTA_IMPORTE"))
    .withColumn("ANIO_MES", to_date(col("ANIO_MES")))
    .withColumn("MES_CORRIENTE", lit(f"{current_year}-{current_month}"))
    .withColumn("MES_CORRIENTE", date_format(col("MES_CORRIENTE"), "yyyy-MM"))
    .withColumn("MES_CORRIENTE", concat(col("MES_CORRIENTE"), lit("-01")))
    .join(
        responsable_linea_vtas.select(
            "EMAIL", "EMAIL1", "REGION", "ASESOR", "GRUPO_CLIENTE", "LINEA_ABIERTA"
        ),
        ["REGION", "ASESOR", "GRUPO_CLIENTE", "LINEA_ABIERTA"],
        "inner",
    )
    .select(
        "EMAIL",
        "EMAIL1",
        "GRUPO_CLIENTE",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "MES_CORRIENTE",
        "ANIO_MES",
        "VENTA_IMPORTE",
    )
    .withColumnRenamed("EMAIL", "REGION")
    .withColumnRenamed("EMAIL1", "ASESOR")
    # .drop("ANIO_FECHA_MES_FACTURA")
)

window_spec = Window.partitionBy(
    "REGION", "ASESOR", "GRUPO_CLIENTE", "LINEA_ABIERTA"
).orderBy("ANIO_MES")

escalera_vtas_prono = (
    escalera_vtas.select(
        "REGION",
        "ASESOR",
        "GRUPO_CLIENTE",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "VENTA_IMPORTE",
    )
    .dropDuplicates()
    .withColumn("MES_CORRIENTE", lit(f"{current_year}-{current_month}"))
    .withColumn("MES_CORRIENTE", to_date(col("MES_CORRIENTE")))
    .withColumn(
        "ANIO_MES",
        explode(
            expr(
                "sequence(add_months(current_date(),1), add_months(current_date(), 19), interval 1 month)"
            )
        ),
    )
    .withColumn("ANIO_MES", date_format(col("ANIO_MES"), "yyyy-MM"))
    .withColumn("ANIO_MES", concat(col("ANIO_MES"), lit("-01")))
    .withColumn("ANIO_MES", to_date(col("ANIO_MES")))
    .withColumn("VENTA_IMPORTE", lit(0))
    .filter(col("ASESOR").isNotNull())
    .select(
        "REGION",
        "ASESOR",
        "GRUPO_CLIENTE",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "MES_CORRIENTE",
        "ANIO_MES",
        "VENTA_IMPORTE",
    )
)

esc = escalera_vtas.union(escalera_vtas_prono)

# COMMAND ----------

from scipy.stats import t
t_value = t.ppf((1 + .95) / 2, 33) 

# COMMAND ----------

# Assuming fact_ventas_pedidos and dim_clientes are defined DataFrames and t is imported from scipy.stats
# Your initial join and selection of columns
escalera_stats = fact_ventas_pedidos.where(col("ID_CANAL_DISTRIBUCION") == "MI").join(dim_clientes, "ID_CTE", "full_outer").select(
    "REGION",
    "ASESOR",
    "GRUPO_CLIENTE",
    "LINEA_ABIERTA",
    "LINEA_AGRUPADA",
    "ANIO_MES",
    "IMPORTE_BACKORDER",
    "VENTA_IMPORTE",
)

# Your window definition and other transformations remain unchanged
w = Window.partitionBy(
    "REGION", "ASESOR", "GRUPO_CLIENTE", "LINEA_ABIERTA", "LINEA_AGRUPADA"
).orderBy(col("ANIO_MES").asc())

stats_df = (
    escalera_stats#.filter(col("REGION").isNotNull())
    .withColumn(
        "VENTA_IMPORTE",
        when(col("VENTA_IMPORTE") == 0, col("IMPORTE_BACKORDER")).otherwise(
            col("VENTA_IMPORTE")
        ),
    )
    .withColumn("ANIO_MES", concat(col("ANIO_MES"), lit("-01")))
    .filter(col("VENTA_IMPORTE") != 0)
    .withColumn("ANIO_MES", to_date(col("ANIO_MES")))
    .filter(col("ANIO_MES") >= add_months(now(), -36))
    .groupBy(
        "REGION",
        "ASESOR",
        "GRUPO_CLIENTE",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "ANIO_MES",
    )
    .agg(sum("VENTA_IMPORTE").alias("VENTA_IMPORTE"))
    .withColumn(
        "crecimiento",
        (col("VENTA_IMPORTE") - lead("VENTA_IMPORTE", 1).over(w))
        / col("VENTA_IMPORTE"),
    )
)

stats_ = (
    escalera_stats#.filter(col("REGION").isNotNull())
    #.dropDuplicates()
    .withColumn("ANIO_MES", concat(col("ANIO_MES"), lit("-01")))
    #.filter(col("VENTA_IMPORTE") != 0)
    .withColumn("ANIO_MES", to_date(col("ANIO_MES")))
    .filter((col("ANIO_MES") >= f"{current_year-1}-{current_month:02d}-01") &
            (col("ANIO_MES") < f"{current_year}-{current_month:02d}-01"))
    .groupBy(
        "REGION",
        "ASESOR",
        "GRUPO_CLIENTE",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
    )
    .agg(sum("VENTA_IMPORTE").alias("PROMEDIO_IMPORTE_12_MESES"))
    .join(
        responsable_linea_vtas
        .select("EMAIL","EMAIL1","REGION","ASESOR","GRUPO_CLIENTE", "LINEA_ABIERTA"),
        ["REGION","ASESOR","GRUPO_CLIENTE","LINEA_ABIERTA"],
        "left"
        )
    .dropDuplicates()
    .drop("REGION","ASESOR")
    .withColumnRenamed("EMAIL","REGION")
    .withColumnRenamed("EMAIL1","ASESOR")
    .withColumn("PROMEDIO_IMPORTE_12_MESES",(col("PROMEDIO_IMPORTE_12_MESES")/1000)/12)
)

#stats_.agg(sum("PROMEDIO_IMPORTE_12_MESES").alias("SUMA_PROMEDIO")).display()
#

# COMMAND ----------

# Perform aggregation without orderBy
stats_df = (
    stats_df
    .groupBy("REGION", "ASESOR", "GRUPO_CLIENTE", "LINEA_ABIERTA", "LINEA_AGRUPADA")
    .agg(
        median("crecimiento").alias("Median"),
        stddev_pop("crecimiento").alias("sd"),
        count("crecimiento").alias("n")
    )
    .join(
        responsable_linea_vtas
        .select("EMAIL","EMAIL1","REGION","ASESOR","GRUPO_CLIENTE", "LINEA_ABIERTA"),
        ["REGION","ASESOR","GRUPO_CLIENTE","LINEA_ABIERTA"],
        "inner"
    )
    .select("EMAIL","EMAIL1","GRUPO_CLIENTE","LINEA_ABIERTA","LINEA_AGRUPADA","Median","sd","n")
    .withColumnRenamed("EMAIL","REGION")
    .withColumnRenamed("EMAIL1","ASESOR")
    .filter(col("REGION").isNotNull())
    # Now apply orderBy on the result DataFrame
    .orderBy("REGION", "ASESOR", "GRUPO_CLIENTE", "LINEA_ABIERTA", "LINEA_AGRUPADA")
    .withColumn("t_value", lit(t_value))
    .filter(col("Median").isNotNull())
    .withColumn("sd", when(col("sd").isNull(), lit(0)).otherwise(col("sd")))
)

#stats_df.display()

# COMMAND ----------

w = Window.partitionBy(
    "REGION", "ASESOR", "GRUPO_CLIENTE", "LINEA_ABIERTA", "LINEA_AGRUPADA"
).orderBy(col("ANIO_MES").asc())

ww = (
    Window.partitionBy(
        "REGION", "ASESOR", "GRUPO_CLIENTE", "LINEA_ABIERTA", "LINEA_AGRUPADA"
    )
    .orderBy("ANIO_MES")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

x = Window.partitionBy(
    "REGION", "ASESOR", "GRUPO_CLIENTE", "LINEA_ABIERTA", "LINEA_AGRUPADA"
).orderBy(col("t").asc())

window = Window.partitionBy("REGION", "ASESOR", "GRUPO_CLIENTE", "LINEA_ABIERTA", "LINEA_AGRUPADA").orderBy(prono.aux)

escalera_vtas = (
    esc
    .drop("MES_CORRIENTE")
    .dropDuplicates()
    .withColumn("VENTA_LAG", lag("VENTA_IMPORTE", 1).over(w))
    .join(
        stats_df,
        ["REGION", "ASESOR", "GRUPO_CLIENTE", "LINEA_ABIERTA", "LINEA_AGRUPADA"],
        how="left",
    )
    .join(
        stats_,
        ["REGION", "ASESOR", "GRUPO_CLIENTE", "LINEA_ABIERTA", "LINEA_AGRUPADA"],
        how="left",
    )
    .join(prono, "ANIO_MES", "left")
    .join(venta_mes_ant.select("REGION","ASESOR","GRUPO_CLIENTE","LINEA_ABIERTA","LINEA_AGRUPADA","VENTA_IMPORTE_MES_ANTERIOR"),
           ["REGION","ASESOR","GRUPO_CLIENTE","LINEA_ABIERTA","LINEA_AGRUPADA"],
          "left"
    )
    .orderBy(col("ANIO_MES").asc())
    .withColumn("VENTA_IMPORTE", col("VENTA_IMPORTE").cast("double"))
    .withColumn("VENTA_LAG", (col("VENTA_LAG")/1000).cast("double"))
    .withColumn("PROMEDIO_IMPORTE_12_MESES", when(((col("PROMEDIO_IMPORTE_12_MESES") < 0) | (col("PROMEDIO_IMPORTE_12_MESES").isNull())), lit(0)).otherwise(col("PROMEDIO_IMPORTE_12_MESES")))
    .withColumn(
        "VENTA_IMPORTE_MES_ANTERIOR", when(col("VENTA_IMPORTE_MES_ANTERIOR") != 0, col("VENTA_IMPORTE_MES_ANTERIOR")).otherwise(lit(0))
    )
    .withColumn(
        "VENTA_IMPORTE_MES_ANTERIOR",
        when(col("VENTA_IMPORTE_MES_ANTERIOR").isNotNull(), col("VENTA_IMPORTE_MES_ANTERIOR")).otherwise(lit(0)),
    )
    .withColumn("ANIO_MES", to_date(col("ANIO_MES")))
    .withColumn("MES_CORRIENTE", lit(f"{current_year}-{current_month}"))
    .withColumn("MES_CORRIENTE", date_format(col("MES_CORRIENTE"), "yyyy-MM"))
    .withColumn("MES_CORRIENTE", concat(col("MES_CORRIENTE"), lit("-01")))
    .filter(col("ANIO_MES") >= current_date)
    .dropDuplicates()
    .withColumn("t", row_number().over(window))
    #.withColumn("t",)
    .withColumn(
        "VENTA_IMPORTE_MES_ANTERIOR",
        when(col("VENTA_IMPORTE_MES_ANTERIOR") < 0, lit(0))
        .otherwise(col("VENTA_IMPORTE_MES_ANTERIOR"))
    )
    .withColumn(
        "Median",
        when(col("Median") < -0.99, lit(-1))
        .otherwise(col("Median"))
    )
    .withColumn(
        "VENTA_LAG",
        when(((col("VENTA_LAG").isNotNull()) & (col("VENTA_LAG") != 0)), col("VENTA_LAG")).otherwise(
            col("PROMEDIO_IMPORTE_12_MESES")
        )
    )
    .withColumn(
        "PRELLENADO_INTELIGENTE", col("PROMEDIO_IMPORTE_12_MESES") * (1 + (col("Median") ** col("n")))
    )
    .withColumn(
        "LIM_SUP",
        col("PROMEDIO_IMPORTE_12_MESES")*(1 + col("Median") + col("t_value")*col("aux")*sqrt(1 + col("sd")/col("n")))
    )
    .withColumn(
        "LIM_INF",
        col("PROMEDIO_IMPORTE_12_MESES")*(1-col("Median")-col("t_value")*col("aux")*sqrt(1 + col("sd")/col("n")))
    )
    .withColumn("PRONOSTICO_REVISADO", lit(0))
    .withColumn("COMENTARIO", lit("na"))
    .withColumnRenamed("ANIO_MES", "MES_PRONO")
    .select(
        "REGION",
        "ASESOR",
        "GRUPO_CLIENTE",
        "LINEA_ABIERTA",
        "LINEA_AGRUPADA",
        "MES_CORRIENTE",
        "MES_PRONO",
        "Median",
        "sd",
        "n",
        "t",
        "aux",
        "VENTA_LAG",
        "VENTA_IMPORTE_MES_ANTERIOR",
        "PROMEDIO_IMPORTE_12_MESES",
        "LIM_INF",
        "PRELLENADO_INTELIGENTE",
        "LIM_SUP",
        "PRONOSTICO_REVISADO",
        "COMENTARIO",
    )
    .withColumn(
        "LIM_INF",
        when(col("LIM_INF") < 0, 0)
        .otherwise(col("LIM_INF"))
    )
    .withColumn(
        "LIM_INF",
        round(col("LIM_INF"), 2)
    )
    .withColumn(
        "PRELLENADO_INTELIGENTE",
        when(col("PRELLENADO_INTELIGENTE") < 0, 0)
        .otherwise(col("PRELLENADO_INTELIGENTE"))
    )
    .withColumn(
        "PRELLENADO_INTELIGENTE",
        when(col("n") < 7, lit(0)) 
        .otherwise(col("PRELLENADO_INTELIGENTE"))
    )#Esta regla se agrego por que si hay una venta muy alta en el mes anterior y en general el producto no tiene venta el prellenado se vuelve mayor que el límite superior, que si bien puede ser cierto que la venta sea mayor al límite superior al ser casos que tienen venta aprox cada 6 meses el prellenado se vuelve cero
    .withColumn(
        "PRELLENADO_INTELIGENTE",
        round(col("PRELLENADO_INTELIGENTE"), 2)
    )
    .withColumn(
        "LIM_SUP",
        round(col("LIM_SUP"), 2)
    )
    .fillna(0)
    .select("REGION","ASESOR","GRUPO_CLIENTE","LINEA_ABIERTA","LINEA_AGRUPADA","MES_CORRIENTE","MES_PRONO","VENTA_IMPORTE_MES_ANTERIOR", "PROMEDIO_IMPORTE_12_MESES","LIM_INF","PRELLENADO_INTELIGENTE","LIM_SUP","PRONOSTICO_REVISADO","COMENTARIO")
    .withColumn("VENTA_IMPORTE_MES_ANTERIOR", round(col("VENTA_IMPORTE_MES_ANTERIOR"), 2))
    .withColumn("PROMEDIO_IMPORTE_12_MESES", round(col("PROMEDIO_IMPORTE_12_MESES"), 2))
    .dropDuplicates()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Envio de archivos .csv a nube

# COMMAND ----------

# Define paths for output
paths = []
base_path = "adls-dac-data-bi-pr-scus/csvpowerapps/marketing"
temp_local_path = "/tmp/"


# Obtén los correos de especialistas y coordinadores MI
print("Obteniendo los correos de especialistas...")
#volver a descomentar
mails_mkt_esp = [row.ESPECIALISTA for row in demanda_base_mi.select("ESPECIALISTA").distinct().collect()]
print(mails_mkt_esp)
print("Obteniendo los correos de coordinadores...")
mails_mkt_coord = [row.COORDINADOR for row in demanda_base_mi.select("COORDINADOR").distinct().collect()]
print(mails_mkt_coord)
print("Layouts full access...")
mails_mkt_full = ["jesus.serratos@dacomsa.com","rodolfo.villaba@dacomsa.com"]

# Procesar cada especialista
print("Procesando base escalera especialistas...")
for mail in mails_mkt_esp:
     process_user(mail, "esp", demanda_base_mi)

# Procesar cada coordinador
print("Procesando base escalera coordinadores...")
for mail in mails_mkt_coord:
     process_user(mail, "coord", demanda_base_mi)

print("Procesando base escalera full access...")
for mail in mails_mkt_full:
     process_user(mail, "full", demanda_base_mi)

# COMMAND ----------

# Obtén los correos de especialistas y coordinadores US
print("Obteniendo los correos de USA...")
mails_mkt_us = [row.ASESOR for row in demanda_base_us.select("ASESOR")
                  #.where((col("ASESOR")!="pedro.contreras@dacomsa.com") & (col("ASESOR")!="jose.rcarmona@dacomsa.com"))
                  .distinct().collect()]
print(mails_mkt_us)
# Procesar cada especialista
print("Procesando base USA...")
for mail in mails_mkt_us:
    process_user_me(mail, "ases", demanda_base_us)


# Obtén los correos de especialistas y coordinadores LATAM
print("Obteniendo los correos de LATAM...")
mails_mkt_latam = [row.ASESOR for row in demanda_base_latam.select("ASESOR")
                  .where((col("ASESOR")!="pedro.contreras@dacomsa.com") & (col("ASESOR")!='juan.villicana@dacomsa.com'))
                #          & (col("ASESOR")!="jorge.estrada@dacomsa.com"))
                  .distinct().collect()]

# Procesar cada especialista
print("Procesando base LATAM...")
for mail in mails_mkt_latam:
    process_user_me(mail, "ases",demanda_base_latam)

# Procesar cada coordinador
#for mail in mails_mkt_reg:
#     process_user_me(mail, "reg", demanda_base_latam)


# COMMAND ----------

paths_mkt = (
    spark
    .createDataFrame(zip(mails_mkt_esp + mails_mkt_coord + mails_mkt_full + mails_mkt_us + mails_mkt_latam, paths), schema = ["mail", "path"])
    .withColumn("path", regexp_replace("path","/dbfs/mnt/", ""))
)

paths_mkt.display()


# COMMAND ----------

emails_vtas_asesor = (escalera_vtas.select("ASESOR")
         .distinct()
         .rdd.flatMap(lambda x: x)
         .collect())

emails_vtas_asesor = [i for i in emails_vtas_asesor if i is not None]

emails_vtas_region = (escalera_vtas.select("REGION")
         .distinct()
         .rdd.flatMap(lambda x: x)
         .collect())

emails_vtas_region = [i for i in emails_vtas_region if i is not None]

# COMMAND ----------

paths = []

temp_local_path = "/tmp/"
base_path = "adls-dac-data-bi-pr-scus/csvpowerapps/ventas"

for mail in emails_vtas_asesor:
    
    id = mail.replace("@dacomsa.com", "")
    id = id.replace(".", "_")
    filename = f"baseescalera_{id}_{current_year}_{current_month}.xlsx"
    temp_path = f"{temp_local_path}/{filename}"
    final_path = f"{base_path}/{filename}"

    paths.append(final_path)

    df = escalera_vtas.filter(col("ASESOR") == mail).toPandas()

    # Crear un writer de Pandas usando XlsxWriter
    writer = pd.ExcelWriter(temp_path, engine='openpyxl')
    
    # Convertir el DataFrame a Excel y guardar localmente
    df.to_excel(writer, index=False)
    writer.save()
    #writer.close()
    
    # Mover el archivo al punto de montaje en DBFS (si estás en Databricks)
    dbutils.fs.cp(f"file:{temp_path}", "/mnt/" + final_path)
    print(f"Archivo guardado en: {final_path}")
    

for mail in emails_vtas_region:
    
    id = mail.replace("@dacomsa.com", "")
    id = id.replace(".", "_")
    filename = f"baseescalera_{id}_{current_year}_{current_month}.xlsx"
    temp_path = f"{temp_local_path}/{filename}"
    final_path = f"{base_path}/{filename}"

    paths.append(final_path)

    df = escalera_vtas.filter(col("REGION") == mail).toPandas()

    # Crear un writer de Pandas usando XlsxWriter
    writer = pd.ExcelWriter(temp_path, engine='openpyxl')
    
    # Convertir el DataFrame a Excel y guardar localmente
    df.to_excel(writer,sheet_name="BASE", startrow=0, index=False,header=True)
    writer.save()
    #writer.close()
    
    # Mover el archivo al punto de montaje en DBFS (si estás en Databricks)
    dbutils.fs.cp(f"file:{temp_path}", "/mnt/" + final_path)
    print(f"Archivo guardado en: {final_path}")


mail_full_access = ["miguel.laguna@dacomsa.com","stephanie.molina@dacomsa.com","jorge.estradad@dacomsa.com"]

for mail in mail_full_access:
    
    id = mail.replace("@dacomsa.com", "")
    id = id.replace(".", "_")
    filename = f"baseescalera_{id}_{current_year}_{current_month}.xlsx"
    temp_path = f"{temp_local_path}/{filename}"
    final_path = f"{base_path}/{filename}"

    paths.append(final_path)

    df = escalera_vtas.toPandas()

    # Crear un writer de Pandas usando XlsxWriter
    writer = pd.ExcelWriter(temp_path, engine='openpyxl')
    
    # Convertir el DataFrame a Excel y guardar localmente
    df.to_excel(writer,sheet_name="BASE", startrow=0, index=False,header=True)
    writer.save()
    #writer.close()
    
    # Mover el archivo al punto de montaje en DBFS (si estás en Databricks)
    dbutils.fs.cp(f"file:{temp_path}", "/mnt/" + final_path)
    print(f"Archivo guardado en: {final_path}")

# COMMAND ----------

paths_vtas = (
    spark
    .createDataFrame(zip(emails_vtas_asesor + emails_vtas_region + mail_full_access, paths), schema = ["mail", "path"])
    .withColumn("path", regexp_replace("path","/dbfs/mnt/", ""))
)


# COMMAND ----------

paths = paths_mkt.union(paths_vtas)
#paths.display()

# COMMAND ----------

paths.toPandas().to_json("/dbfs/mnt/adls-dac-data-bi-pr-scus/catalogos/dim_paths.json", orient = "records")

# COMMAND ----------

paths.display()

# COMMAND ----------

import smtplib
from email.mime.text import MIMEText
import re

# COMMAND ----------

smtp_server = "smtp-mail.outlook.com"
smtp_port = 587
smtp_username = "power.support@desc.com"
smtp_password = "15.Que-1346."
sender = "power.support@desc.com"
receiver = ["jesus.serratos@dacomsa.com", "stephanie.molina@dacomsa.com", "jorge.estradad@dacomsa.com", "alejandra.rocha@desc.com"]
subject = "Layouts | Notificación"

# Crear el mensaje MIME
email_body = "Ya están disponibles los layouts para el pronóstico de Escalera"
msg = MIMEText(email_body)
msg['Subject'] = subject
msg['From'] = sender
# Enviar el correo electrónico
for r in receiver:
    msg['To'] = r
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        #server.sendmail(sender, r, msg.as_string())