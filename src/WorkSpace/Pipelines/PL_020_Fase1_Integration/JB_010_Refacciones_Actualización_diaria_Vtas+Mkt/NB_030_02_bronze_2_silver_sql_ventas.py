# Databricks notebook source
# MAGIC %md
# MAGIC # INCLUDES

# COMMAND ----------

# MAGIC %run /Shared/FASE1_INTEGRACION/includes

# COMMAND ----------

# MAGIC %md
# MAGIC # DIMENSIONES

# COMMAND ----------

# MAGIC %md
# MAGIC ## CLIENTES

# COMMAND ----------

#Dimension
table = "DIM_CLIENTE_VENTAS"
dim_clientes = (spark.
                        read.
                        parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))
dim_clientes = (dim_clientes.
                    select("ID_MANDANTE","ID_ORGANIZACION_VENTAS","ID_CANAL_DISTRIBUCION","ID_CLIENTE","NOMBRE_1","CIUDAD","ID_OFICINA_VENTAS","ID_ZONA_VENTAS","ID_PAIS","ID_ESTADO","ID_GRUPO_VENTAS","ID_CLIENTE_AGRUPACION","DES_CLIENTE_AGRUPACION","NUM_CLIENTE"
                            ).
                     filter((dim_clientes["ID_ORGANIZACION_VENTAS"] == "A717") & 
                        (dim_clientes["SEQ_DATE"] == dim_clientes.select(max("SEQ_DATE")).collect()[0][0])).
                     distinct())


# COMMAND ----------

# MAGIC %md
# MAGIC ##REGIONES

# COMMAND ----------

#Dimension
table = "DIM_GRUPO_VENTAS"
dim_grupo_ventas = (spark.
                        read.
                        parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))
dim_grupo_ventas = (dim_grupo_ventas.
                    select("ID_MANDANTE","ID_OFICINA_VENTAS","ID_GRUPO_VENTAS","DES_GRUPO_VENTAS"
                            ).filter((dim_grupo_ventas["SEQ_DATE"] == dim_grupo_ventas.select(max("SEQ_DATE")).collect()[0][0])).
                     distinct())


# COMMAND ----------

# MAGIC %md
# MAGIC ##ESTADOS

# COMMAND ----------

#Dimension
table = "DIM_ESTADOS"
dim_estados = (spark.
                        read.
                        parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))
dim_estados = (dim_estados.
                    select("ID_MANDANTE","ID_PAIS","ID_ESTADO","DES_ESTADO"
                            ).
                    filter((dim_estados["ID_MANDANTE"]=="504") & 
                        (dim_estados["SEQ_DATE"] == dim_estados.select(max("SEQ_DATE")).collect()[0][0])).
                     distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC ##ASESORES

# COMMAND ----------

#Dimension
table = "DIM_ZONA_VENTAS"
dim_zona_ventas = (spark.
                        read.
                        parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))
dim_zona_ventas = (dim_zona_ventas.
                    select("ID_MANDANTE","ID_ZONA_VENTAS","DES_ZONA_VENTAS"
                            ).filter((dim_zona_ventas["SEQ_DATE"] == dim_zona_ventas.select(max("SEQ_DATE")).collect()[0][0])).
                     distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC ##CANAL_DISTRIBUCION

# COMMAND ----------

#Dimension
table = "DIM_CANAL_DISTRIBUCION"
dim_canal_distribucion = (spark.
                        read.
                        parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))
dim_canal_distribucion = (dim_canal_distribucion.
                    select("ID_MANDANTE","ID_CANAL_DISTRIBUCION","DES_CANAL_DISTRIBUCION"
                            ).filter((dim_canal_distribucion["SEQ_DATE"] == dim_canal_distribucion.select(max("SEQ_DATE")).collect()[0][0])).
                     distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC ##PAISES

# COMMAND ----------

#####Catálogo sugerido
paises = {
    'PL': 'Polonia',
    'MX': 'México',
    'VG': 'Islas Vírgenes Británicas',
    'CN': 'China',
    'AT': 'Austria',
    'RU': 'Rusia',
    'SV': 'El Salvador',
    'CL': 'Chile',
    'AU': 'Australia',
    'CA': 'Canadá',
    'BR': 'Brasil',
    'HN': 'Honduras',
    'GT': 'Guatemala',
    'DE': 'Alemania',
    'EC': 'Ecuador',
    'ES': 'España',
    'VE': 'Venezuela',
    'TR': 'Turquía',
    'CR': 'Costa Rica',
    'KR': 'Corea del Sur',
    '@@@': 'Código no válido',
    'US': 'Estados Unidos',
    'FR': 'Francia',
    'PA': 'Panamá',
    'SG': 'Singapur',
    'IT': 'Italia',
    'PE': 'Perú',
    'HU': 'Hungría',
    'BE': 'Bélgica',
    'NI': 'Nicaragua',
    'BO': 'Bolivia',
    'CO': 'Colombia',
    'UY': 'Uruguay',
    'AR': 'Argentina'
}


# COMMAND ----------

dim_paises = [(ID_PAIS, PAIS) for ID_PAIS, PAIS in paises.items()]

# Crear el DataFrame de Spark a partir de la lista de tuplas
dim_paises = spark.createDataFrame(dim_paises, ['ID_PAIS', 'PAIS'])


# COMMAND ----------

# MAGIC %md
# MAGIC ##PPTO

# COMMAND ----------

# CODIGO ORIGINAL 
table = "DIM_PPT_CLIENTE"
dim_ppto_cliente = (spark.
                        read.
                        parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/").distinct())
dim_ppto_cliente = (dim_ppto_cliente.select('SISTEMA',
'GRUPO',
'SHIP_TO',
'ID_CLIENTE',
'DESC_CLIENTE',
'ESTADO',
'ID_ASESOR',
'DESC_REGIONAL',
'DESC_ASESOR',
'LINEA MOD',
'MERCADO',
'FECHA',
'IMPORTE',
'INDICE',
'PIEZAS'
).filter((dim_ppto_cliente["SEQ_DATE"] == dim_ppto_cliente.select(max("SEQ_DATE")).collect()[0][0])).distinct())

# COMMAND ----------

import pyspark.sql.functions as F

dim_ppto_cliente = dim_ppto_cliente.withColumn(
    "MES_FECHA_PPTO", 
    month("FECHA")
).withColumn(
    "ANIO_FECHA_PPTO", 
    year("FECHA")
)

# COMMAND ----------

import pyspark.sql.functions as F
dim_ppto_cliente = (dim_ppto_cliente.withColumn("ANIO_MES", when(length("MES_FECHA_PPTO")==2,concat(col("ANIO_FECHA_PPTO"), lit("-"), col("MES_FECHA_PPTO"))).otherwise(concat(col("ANIO_FECHA_PPTO"), lit("-0"), col("MES_FECHA_PPTO")))))

# COMMAND ----------

dim_ppto_cliente = (dim_ppto_cliente
            .withColumnRenamed("MERCADO","ID_CANAL_DISTRIBUCION")
                  .withColumnRenamed("GRUPO","GRUPO_CLIENTE")
             .withColumnRenamed("IMPORTE","IMPORTE_PPTO")
             .withColumnRenamed("LINEA MOD","LINEA_AGRUPADA")
             .withColumnRenamed("INDICE","INDICE_PPTO")
             .withColumnRenamed("PIEZAS","PIEZAS_PPTO")
             .withColumnRenamed("FECHA","FECHA_PPTO")
             .withColumnRenamed("DESC_CLIENTE","RAZON_SOCIAL")
             .withColumnRenamed("DESC_REGIONAL","REGION")
             .withColumnRenamed("DESC_ASESOR","ASESOR")
             )

# COMMAND ----------

dim_ppto_cliente = dim_ppto_cliente.groupBy('SISTEMA',
 'GRUPO_CLIENTE',
 'ID_CLIENTE',
 'RAZON_SOCIAL',
 'ESTADO',
 'ID_ASESOR',
 'REGION',
 'ASESOR',
 'LINEA_AGRUPADA',
 'ID_CANAL_DISTRIBUCION',
 'FECHA_PPTO',
 'MES_FECHA_PPTO',
 'ANIO_FECHA_PPTO',
 'ANIO_MES',
).agg(
    sum("IMPORTE_PPTO").alias("IMPORTE_PPTO"),
    sum("PIEZAS_PPTO").alias("PIEZAS_PPTO"),
    max('INDICE_PPTO').alias("INDICE")
)

# COMMAND ----------

current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month
next_year = current_year + 1
 

# COMMAND ----------

dim_clientes_region_asesor = dim_ppto_cliente.select("ID_CLIENTE","ID_CANAL_DISTRIBUCION","REGION","ASESOR").filter(col("ANIO_FECHA_PPTO")== current_year).distinct().withColumnRenamed("REGION","REGION_PPTO").withColumnRenamed("ASESOR","ASESOR_PPTO")

# COMMAND ----------

table="dim_ppto_cliente"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos silver
(dim_ppto_cliente.
    write.format('delta').
    partitionBy('ANIO_MES','SISTEMA').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ##JOIN CLIENTES CON DIMENSIONES

# COMMAND ----------

dim_clientes_estados = (dim_clientes.
    join(dim_estados,
         on = ["ID_MANDANTE","ID_PAIS","ID_ESTADO"],
         how = "inner"))

# COMMAND ----------

dim_clientes_estados_grupos = (dim_clientes_estados.
    join(dim_grupo_ventas,
         on = ["ID_MANDANTE","ID_OFICINA_VENTAS","ID_GRUPO_VENTAS"],
         how = "inner"))

# COMMAND ----------

dim_clientes_estados_grupos_zona = (dim_clientes_estados_grupos.
 join(dim_zona_ventas, 
     on = ["ID_MANDANTE","ID_ZONA_VENTAS"],
     how = "inner"))

# COMMAND ----------

dim_clientes_estados_grupos_zona_canal = (dim_clientes_estados_grupos_zona.
 join(dim_canal_distribucion, 
     on = ["ID_MANDANTE","ID_CANAL_DISTRIBUCION"],
     how = "inner"))

# COMMAND ----------

dim_clientes_estados_grupos_zona_canal_pais = (dim_clientes_estados_grupos_zona_canal.
 join(dim_paises, 
     on = ["ID_PAIS"],
     how = "inner"))

# COMMAND ----------

dim_clientes = dim_clientes_estados_grupos_zona_canal_pais

# COMMAND ----------

dim_clientes = (dim_clientes
             .withColumnRenamed("NOMBRE_1", "RAZON_SOCIAL")
             .withColumnRenamed("ID_CLIENTE_AGRUPACION", "ID_GRUPO_CLIENTE")
             .withColumnRenamed("DES_CLIENTE_AGRUPACION", "GRUPO_CLIENTE")
             .withColumnRenamed("DES_ESTADO", "ESTADO")
             .withColumnRenamed("DES_GRUPO_VENTAS", "ASESOR")
             .withColumnRenamed("DES_ZONA_VENTAS", "REGION")
             .withColumnRenamed("ID_ZONA_VENTAS", "ID_REGION")
             .withColumnRenamed("ID_GRUPO_VENTAS", "ID_ASESOR")
             .withColumnRenamed("DES_CANAL_DISTRIBUCION", "CANAL_DISTRIBUCION")
             )

# COMMAND ----------

dim_clientes = (dim_clientes
                .withColumn("ID_SHIP_TO", dim_clientes["NUM_CLIENTE"])
                .withColumn("SHIP_TO", dim_clientes["CIUDAD"]))


# COMMAND ----------

dim_clientes = (dim_clientes.select('ID_PAIS','PAIS','ID_ESTADO', 'ESTADO', 'ID_CANAL_DISTRIBUCION','CANAL_DISTRIBUCION','ID_REGION','REGION','ID_ASESOR','ASESOR','ID_GRUPO_CLIENTE','GRUPO_CLIENTE','ID_CLIENTE','RAZON_SOCIAL','CIUDAD','NUM_CLIENTE','ID_SHIP_TO','SHIP_TO')).distinct()

# COMMAND ----------

#dim_clientes.display()

# COMMAND ----------

diferencias_clientes_region_asesor_sap_ppto = dim_clientes.select("ID_CLIENTE",'RAZON_SOCIAL',"GRUPO_CLIENTE","REGION","ASESOR","ID_CANAL_DISTRIBUCION").join(dim_clientes_region_asesor, on = ['ID_CLIENTE','ID_CANAL_DISTRIBUCION'], how = 'inner').withColumnRenamed("REGION","REGION_SAP").withColumnRenamed("ASESOR","ASESOR_SAP").withColumn("CHECK_REGION",when(col("REGION_SAP")==col("REGION_PPTO"),True).otherwise(False)).withColumn("CHECK_ASESOR",when(col("ASESOR_SAP")==col("ASESOR_PPTO"),True).otherwise(False)).filter((col("CHECK_REGION")==False) | (col("CHECK_ASESOR")==False))

# COMMAND ----------

dim_clientes = dim_clientes.join(dim_clientes_region_asesor, on = ['ID_CLIENTE','ID_CANAL_DISTRIBUCION'], how = 'outer')

# COMMAND ----------

#dim_clientes.where(col("ID_CLIENTE") == "0000000271").display()

# COMMAND ----------

from pyspark.sql.functions import col, when

# Primero, actualizamos las columnas "REGION" y "ASESOR" con las condiciones dadas
dim_clientes = dim_clientes \
    .withColumn("REGION", when(col("REGION_PPTO").isNull(), col("REGION")).otherwise(col("REGION_PPTO"))) \
    .withColumn("ASESOR", when(col("ASESOR_PPTO").isNull(), col("ASESOR")).otherwise(col("ASESOR_PPTO")))

# Luego, eliminamos las columnas "REGION_PPTO" y "ASESOR_PPTO"
dim_clientes = dim_clientes.drop("REGION_PPTO", "ASESOR_PPTO")


# COMMAND ----------

dim_clientes.select("id_cliente").distinct().count()

# COMMAND ----------

table = "dim_clientes"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"

# COMMAND ----------

(dim_clientes.
    dropDuplicates().
    write.format('delta').
    partitionBy('ID_PAIS','ID_CANAL_DISTRIBUCION').
    mode("overwrite").
    parquet(target_path))


# COMMAND ----------

# Convertir el DataFrame de Spark a un DataFrame de Pandas
df_pandas = dim_clientes.toPandas()

# Crear el directorio si no existe
dbutils.fs.mkdirs("/mnt/adls-dac-data-bi-pr-scus/silver/validacion")

# Exportar a un archivo Excel en una ruta local
local_file_path = "/tmp/dim_clientes.xlsx"
df_pandas.to_excel(local_file_path, index=False)

# Copiar el archivo a DBFS
dbutils.fs.cp(f"file://{local_file_path}", 
              "/mnt/adls-dac-data-bi-pr-scus/silver/validacion/dim_clientes.xlsx")

# COMMAND ----------

table = "diferencias_clientes"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"

# COMMAND ----------

(diferencias_clientes_region_asesor_sap_ppto.
    write.format('delta').
    partitionBy('ID_CANAL_DISTRIBUCION').
    mode("overwrite").
    parquet(target_path))


# COMMAND ----------

# MAGIC %md
# MAGIC ##FECHAS

# COMMAND ----------

#Dimension
dim_fechas = spark.read.csv('/mnt/adls-dac-data-bi-pr-scus/raw/catalogos/FECHAS.csv',header=True, inferSchema=True)

# COMMAND ----------

dim_fechas = (dim_fechas
            .withColumnRenamed("A�O","ANIO"))

# COMMAND ----------

dim_fechas = (dim_fechas.na.drop())

# COMMAND ----------

table = "dim_fechas"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"

# COMMAND ----------

(dim_fechas.
    write.format('delta').
    partitionBy('ANIO').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ##PRODUCTOS

# COMMAND ----------

#Dimension
table = "CATALOGO_DE_ARTICULOS"
dim_productos = (spark.
                        read.
                        parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/marketing/{table}.parquet/"))



# COMMAND ----------

dim_productos = dim_productos.select('MATERIAL',
 'CODIGO_EAN_UPC',
 'TIPO_MATERIAL',
 'TEXTO_BREVE_DE_MATERIAL',
 'GRM',
 'GRM1',
 'GRM2',
 'INDICADOR_ABC',
 'STATMAT_TODAS_CADDIS',
 'ST',
 'SISTEMA',
 'COMPANIA',
 'LINEA_ABIERTA',
 'LINEA_AGRUPADA',
 'CLASE',
 'SEGMENTO',
 'MARCA',
 'CFDI',
 'CFDI_TEXTO',
 'CARTA_PORTE',
 'CARTA_PORTE_TEXTO',
 'UNIDAD_DE_PESO',
 'PESO_BRUTO',
 'PESO_NETO',
 'VOLUMEN',
 'LONGITUD',
 'ANCHURA',
 'ALTURA',
 'CDIS')

# COMMAND ----------

dim_productos = (dim_productos
            .withColumnRenamed("MATERIAL","ID_MATERIAL")
             .withColumnRenamed('CODIGO_EAN_UPC','EAN')
             .withColumnRenamed('TEXTO_BREVE_DE_MATERIAL','Número de material')
             .withColumnRenamed("CDIS", "ID_CANAL_DISTRIBUCION")
             .withColumnRenamed("GRM", "ID_LINEA_ABIERTA")
             .withColumnRenamed("GRM1", "ID_SISTEMA")
             .withColumnRenamed("GRM2", "ID_COMPANIA")
             .withColumnRenamed('INDICADOR_ABC', "IND_ABC")
             .withColumnRenamed("ST", "ESTATUS_PRODUCTOS")
             .withColumnRenamed('STATMAT_TODAS_CADDIS', "ESTATUS_MAESTRO_MATERIALES")
             .withColumnRenamed('COMPANIA', "COMPAÑIA")
             .withColumnRenamed('LINEA_ABIERTA', "LINEA")
             .withColumnRenamed('ANCHURA', "ANCHO")
             )

# COMMAND ----------

dim_productos = dim_productos.filter(dim_productos["Número de material"] != "PISTONES GASOLINA NA 6 CIL. DT 466" )

# COMMAND ----------

dim_productos= dim_productos.withColumn('ID_MATERIAL',regexp_replace(col('ID_MATERIAL'), r'^[0]*', ''))

# COMMAND ----------

dim_productos = dim_productos.select(['ID_MATERIAL',
 'EAN',
 'Número de Material',
 'ID_LINEA_ABIERTA',
 'ID_SISTEMA',
 'ID_COMPANIA',
 'ID_CANAL_DISTRIBUCION',
 'IND_ABC',
 'ESTATUS_PRODUCTOS',
 'ESTATUS_MAESTRO_MATERIALES',
 'SISTEMA',
 'COMPAÑIA',
 'LINEA',
 'LINEA_AGRUPADA',
 'CLASE',
 'SEGMENTO',
 'PESO_BRUTO',
 'PESO_NETO',
 'VOLUMEN',
 'LONGITUD',
 'ANCHO',
 'ALTURA'])

# COMMAND ----------

table = "dim_productos"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"

# COMMAND ----------

(dim_productos.
    write.format('delta').
    partitionBy('ID_CANAL_DISTRIBUCION').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

#Dimension
table = "DIM_MATERIAL_VENTAS"
dim_material = (spark.
                        read.
                        parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))
from pyspark.sql import functions as F

dim_material = (dim_material
                .select("ID_MATERIAL", "ID_CANAL_DISTRIBUCION", "PESO_BRUTO", "PESO_NETO", "VOLUMEN",
                         "DES_MATERIAL", "ID_GRUPO_MATERIAL","DES_GRUPO_MATERIAL", "DES_GRUPO_MATERIAL_1",
                        "ID_GRUPO_MATERIAL_1", "DES_GRUPO_MATERIAL_2", "ID_GRUPO_MATERIAL_2", 
                        "ID_GRUPO_MATERIAL_3","DES_GRUPO_MATERIAL_3", "ID_GRUPO_MATERIAL_4","DES_GRUPO_MATERIAL_4","ID_ESTATUS_VENTAS", "ID_INDICADOR_ABC", "NUM_MATERIAL")
                .filter((dim_material["ID_MANDANTE"] == "504") & 
                        (dim_material["ID_CENTRO"] == "A717") &
                         ((dim_material["ID_CANAL_DISTRIBUCION"] == "MI") | 
                             (dim_material["ID_CANAL_DISTRIBUCION"] == "ME") | 
                             (dim_material["ID_CANAL_DISTRIBUCION"] == "MO") ) &
                        (~dim_material["ID_MATERIAL"].isin(["ALLOWANCES", "VENTA_INTERCOMPAÑI"])) &                        
                         (dim_material["DES_GRUPO_MATERIAL_1"].isin(['FRENOS','LUBRICANTES','TREN MOTRIZ','MOTOR','OTROS DESCUENTOS','OTROS FINANZAS' ])) & 
                        (dim_material["SEQ_DATE"] == dim_material.select(max("SEQ_DATE")).collect()[0][0])
                )
               )
                     

# COMMAND ----------

dim_material = (dim_material
            .withColumnRenamed("MATERIAL","ID_MATERIAL")
             .withColumnRenamed("ID_GRUPO_MATERIAL", "ID_LINEA_ABIERTA")
             .withColumnRenamed("ID_GRUPO_MATERIAL_1", "ID_SISTEMA")
             .withColumnRenamed("ID_GRUPO_MATERIAL_2", "ID_COMPANIA")
                .withColumnRenamed("ID_GRUPO_MATERIAL_3", "ID_CLASE")
             .withColumnRenamed("ID_GRUPO_MATERIAL_4", "ID_SEGMENTO")
             .withColumnRenamed("ID_INDICADOR_ABC", "IND_ABC")
             .withColumnRenamed("ID_ESTATUS_VENTAS", "ESTATUS_PRODUCTOS")
             .withColumnRenamed("DES_GRUPO_MATERIAL_1", "SISTEMA")
             .withColumnRenamed("DES_GRUPO_MATERIAL_2", "COMPAÑIA")
             .withColumnRenamed("DES_GRUPO_MATERIAL", "LINEA")
              .withColumnRenamed("DES_GRUPO_MATERIAL_3", "CLASE")
             .withColumnRenamed("DES_GRUPO_MATERIAL_4", "SEGMENTO")
             )

# COMMAND ----------

dim_material = (dim_material
             .withColumn("EAN",dim_material["NUM_MATERIAL"])
             .withColumn("ESTATUS_MAESTRO_MATERIALES", dim_material["ESTATUS_PRODUCTOS"])
             .withColumn("LONGITUD", dim_material["VOLUMEN"])
            .withColumn("ANCHO", dim_material["VOLUMEN"])
            .withColumn("ALTURA", dim_material["VOLUMEN"])
             )

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Crear una SparkSession
spark = SparkSession.builder \
    .appName("Crear DataFrame en PySpark") \
    .getOrCreate()

# Definir los datos de las columnas
linea_linea_agrupada = [
    ("VELCON", "FH"),
    ("VEHYCO", "PV"),
    ("VARIOS", "VA"),
    ("VALVULAS", "VD"),
    ("VALVULAS", "VI"),
    ("VALVULAS", "VV"),
    ("TSP TRANSMISIONES", "TO"),
    ("TSP EMBRAGUES", "TN"),
    ("TRANS PTES TSP", "TP"),
    ("TRANS PTES TSP", "TT"),
    ("TRANS PTES TREMEC", "PT"),
    ("TRANS PTES TREMEC", "TR"),
    ("Trans Ptes Autopar", "TA"),
    ("Trans Ptes Autopar", "TC"),
    ("TF VICTOR", "LB"),
    ("TF VICTOR", "RD"),
    ("TF VICTOR", "RE"),
    ("TF VICTOR", "TD"),
    ("TF VICTOR", "TF"),
    ("SUSPENSION", "TQ"),
    ("SOLENOIDES", "SO"),
    ("SELLO V", "SV"),
    ("SEGMENTOS", "ND"),
    ("ROTORES HD", "RH"),
    ("RACE MAZAS RUEDA", "MA"),
    ("RACE", "HR"),
    ("PUNTERIAS", "PI"),
    ("PUNTERIAS", "PU"),
    ("PLASTIGAGE", "PL"),
    ("PISTONES MORESA", "DL"),
    ("PISTONES MORESA", "PD"),
    ("PISTONES MORESA", "PM"),
    ("MISCELANEOS", "MO"),
    ("METALES", "CI"),
    ("METALES", "MG"),
    ("METALES", "MH"),
    ("MEDIAS REP DIESEL", "MP"),
    ("LIQUIDO DE FRENOS", "LF"),
    ("KITS MODELO 80", "E8"),
    ("FRITEC ROTORES", "RT"),
    ("FRITEC BLOCKS", "BS"),
    ("FILTROS", "FI"),
    ("EMBRAGUES EATON", "EP"),
    ("EJE DIF. SPICER", "ED"),
    ("CRUCETAS SPICER", "CS"),
    ("CONJUNTOS DE MOTOR", "CM"),
    ("CARDANES SPICER", "CP"),
    ("CARDANES AUTOPAR", "PC"),
    ("CAMISAS", "CU"),
    ("CABEZAS DE MOTOR", "CD"),
    ("BOMBAS ELECTRICAS", "BM"),
    ("BOMBAS DE AGUA", "AD"),
    ("BOMBAS DE AGUA", "AG"),
    ("BOMBAS DE AGUA", "BL"),
    ("BOMBAS DE ACEITE", "BB"),
    ("BOMBAS DE ACEITE", "BU"),
    ("BOBINAS HUMEDAS", "BH"),
    ("BOBINAS DE N CERRADO", "BO"),
    ("BLOCKS FRENO DE AIRE", "NC"),
    ("BLOCKS F.A. PREMIUM", "NG"),
    ("BIOCERAMIC FT", "FT"),
    ("BANDAS", "BG"),
    ("BALATAS FRASLE", "BF"),
    ("BALATAS BIOCERAMIC", "BC"),
    ("BALATA INDUSTRIAL", "BE"),
    ("BALATA FRENO TAMBOR", "NB"),
    ("BALATA FRENO DISCO", "NA"),
    ("BALATA FRENO DISCO", "NK"),
    ("BALATA FD FREINER", "BJ"),
    ("BALATA FD CARTEK", "BI"),
    ("BALATA F DISCO OP", "NJ"),
    ("AUTOPAR TECHNIK", "BA"),
    ("AUTOPAR SUPERSTOP", "BD"),
    ("AUTOPAR EMBRAGUES", "EA"),
    ("AUTOPAR DIFERENCIAL", "P8"),
    ("AUTOPAR CRUCETAS", "CA"),
    ("AUTOPAR CILINDROS", "CF"),
    ("AUTOPAR BLOCKS SUPER", "AB"),
    ("ARBOLES DE LEVAS", "AL"),
    ("ARBOLES DE LEVAS", "AS"),
    ("ANILLOS MORESA", "AC"),
    ("ANILLOS MORESA", "AI"),
    ("ACEITES", "AT"),
    ("ACCESORIOS BALATA", "AE"),
    ("AAM DIFERENCIAL", "AM"),
    ("FREMAX ROTORES","FX")### ⇦ se agrega esta linea nueva por motivo del ticket 602075 y 596756 de Jesus Emilio.
]

# Definir el esquema de las columnas
schema = StructType([
    StructField("LINEA_AGRUPADA", StringType(), True),
    StructField("ID_LINEA_ABIERTA", StringType(), True)
])

# Crear el DataFrame
linea_linea_agrupada = spark.createDataFrame(linea_linea_agrupada, schema)

# COMMAND ----------

dim_material = dim_material.join(
    linea_linea_agrupada,
    on = ["ID_LINEA_ABIERTA"],
    how = "left"
)


# COMMAND ----------

from pyspark.sql.functions import when, col

# Crear la nueva columna "LINEA_AGRUPADA" basada en condiciones
dim_material = (dim_material
                .withColumn("LINEA_AGRUPADA_PPTO",
                            when(col("LINEA_AGRUPADA").isNull(), col("LINEA")).otherwise(col("LINEA_AGRUPADA")))
               )

# COMMAND ----------

dim_material = dim_material.withColumn('ID_MATERIAL',regexp_replace(col('ID_MATERIAL'), r'^[0]*', ''))

# COMMAND ----------

table = "dim_material"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"

# COMMAND ----------

(dim_material.
    write.format('delta').
    partitionBy('ID_CANAL_DISTRIBUCION').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

dim_material.columns

# COMMAND ----------

dim_material_select = dim_material.select(
 'ID_MATERIAL',
 'ID_CANAL_DISTRIBUCION',
 'DES_MATERIAL',
 )

# COMMAND ----------

dim_productos_select = dim_productos.select('ID_MATERIAL',
 'EAN',
 'ID_LINEA_ABIERTA',
 'ID_SISTEMA',
 'ID_COMPANIA',
 'ID_CANAL_DISTRIBUCION',
 'IND_ABC',
 'ESTATUS_PRODUCTOS',
 'ESTATUS_MAESTRO_MATERIALES',
 'SISTEMA',
 'COMPAÑIA',
 'LINEA',
 'LINEA_AGRUPADA',
 'CLASE',
 'SEGMENTO',
 'PESO_BRUTO',
 'PESO_NETO',
 'VOLUMEN',
 'LONGITUD',
 'ANCHO',
 'ALTURA')

# COMMAND ----------

dim_material_check=dim_material.select('ID_MATERIAL','ID_CANAL_DISTRIBUCION').distinct()

# COMMAND ----------

dim_productos_check=dim_productos.select('ID_MATERIAL','ID_CANAL_DISTRIBUCION').distinct()

# COMMAND ----------

dim_productos_material = dim_material_select.join( dim_productos_select,
                                          on = ['ID_MATERIAL','ID_CANAL_DISTRIBUCION'],
                                          how = 'left'

)

# COMMAND ----------

dim_productos_material = dim_productos_material.withColumn(
    "SISTEMA",
    when(
        col("SISTEMA").isNull() | col("SISTEMA").isin(["002", "EMPAQUE", "LUBRICANTES"]),
        "OTROS"
    ).otherwise(col("SISTEMA"))
)

# COMMAND ----------

dim_productos_material = dim_productos_material.withColumn(
    "LINEA",
    when(
        col("LINEA").isNull(),
        "OTROS"
    ).otherwise(col("LINEA"))
)

# COMMAND ----------

dim_productos_material = dim_productos_material.withColumn(
    "IND_ABC",
    when(
        col("IND_ABC").isNull(), 
        "OTROS").otherwise(when(col("IND_ABC")=="A12","A"
    ).otherwise(col("IND_ABC")))
).dropDuplicates()

# COMMAND ----------

table = "dim_productos_material"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"

# COMMAND ----------

(dim_productos_material.
    write.format('delta').
    partitionBy('ID_CANAL_DISTRIBUCION').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Extracción Pedidos

# COMMAND ----------

#Hechos
table = "FACT_PEDIDOS_ESTATUS"
fact_pedidos = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))

fact_pedidos = fact_pedidos.withColumn("rank", rank().over(Window.partitionBy("FECHA_REGISTRO").orderBy(col("SEQ_DATE").desc()))).filter(col("rank") == 1).drop("rank")
   
fact_pedidos = (fact_pedidos.
                     select("ID_MANDANTE","FECHA_REGISTRO","PIEZAS","ID_CLIENTE_SOLICITANTE","ID_MATERIAL","ID_CLASE_DOCUMENTO_VENTA","ID_CANAL_DISTRIBUCION","FECHA_COMPROMISO","FECHA_PEDIDO","PIEZAS_FACTURADA","PIEZAS_RECHAZADA","PIEZAS_BACKORDER","IMPORTE_FACTURADA","IMPORTE_RECHAZADA","IMPORTE_BACKORDER","IMPORTE","TIPO_CAMBIO_DPRECIOS" 
                            ).
                     filter((fact_pedidos["ID_MANDANTE"] == "504") & 
                            ((fact_pedidos["ID_CANAL_DISTRIBUCION"] == "MI") | 
                             (fact_pedidos["ID_CANAL_DISTRIBUCION"] == "ME") | 
                             (fact_pedidos["ID_CANAL_DISTRIBUCION"] == "MO") 
                             ) &
                            ((fact_pedidos["ID_MATERIAL"] != "ALLOWANCES")
                              | (fact_pedidos["ID_MATERIAL"] != "VENTA_INTERCOMPAÑI" ))
                            )
                            )
fact_pedidos = fact_pedidos.withColumnRenamed("ID_CLIENTE_SOLICITANTE","ID_CLIENTE")

# COMMAND ----------

table = "FACT_PEDIDOS_ESTATUS"
fact_pedidos_gln = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))
                     
fact_pedidos_gln.display()

# COMMAND ----------

(fact_pedidos
    .groupBy("FECHA_REGISTRO")
    .agg(
        sum("PIEZAS").alias("SUM_PIEZAS"),
        sum("PIEZAS_FACTURADA").alias("SUM_PIEZAS_FACTURADA"),
        sum("PIEZAS_RECHAZADA").alias("SUM_PIEZAS_RECHAZADA"),
        sum("PIEZAS_BACKORDER").alias("SUM_PIEZAS_BACKORDER"),
        sum("IMPORTE_FACTURADA").alias("SUM_IMPORTE_FACTURADA"),
        sum("IMPORTE_RECHAZADA").alias("SUM_IMPORTE_RECHAZADA"),
        sum("IMPORTE_BACKORDER").alias("SUM_IMPORTE_BACKORDER"),
        sum("IMPORTE").alias("SUM_IMPORTE"),
        sum("TIPO_CAMBIO_DPRECIOS").alias("SUM_TIPO_CAMBIO_DPRECIOS")
    )
    .orderBy(col("FECHA_REGISTRO").desc())
    .display()
)

# COMMAND ----------

fact_pedidos = fact_pedidos \
    .withColumn("IMPORTE_FACTURADA_DOLARES",
                when(col("ID_CANAL_DISTRIBUCION") != 'MI',
                     col("IMPORTE_FACTURADA") / col("TIPO_CAMBIO_DPRECIOS")).otherwise(0)) \
    .withColumn("IMPORTE_RECHAZADA_DOLARES",
                when(col("ID_CANAL_DISTRIBUCION") != 'MI',
                     col("IMPORTE_RECHAZADA") / col("TIPO_CAMBIO_DPRECIOS")).otherwise(0)) \
    .withColumn("IMPORTE_BACKORDER_DOLARES",
                when(col("ID_CANAL_DISTRIBUCION") != 'MI',
                     col("IMPORTE_BACKORDER") / col("TIPO_CAMBIO_DPRECIOS")).otherwise(0)) \
    .withColumn("IMPORTE_DOLARES",
                when(col("ID_CANAL_DISTRIBUCION") != 'MI',
                     col("IMPORTE") / col("TIPO_CAMBIO_DPRECIOS")).otherwise(0))

# COMMAND ----------

fact_pedidos.groupBy("FECHA_REGISTRO").agg(sum("IMPORTE_FACTURADA")).orderBy(col("FECHA_REGISTRO").desc()).display()

fact_pedidos.groupBy(year("FECHA_REGISTRO").alias("ANIO"), month("FECHA_REGISTRO").alias("MES")).agg(sum("IMPORTE_FACTURADA").alias("SUM_IMPORTE_FACTURADA")).orderBy(col("ANIO").desc(), col("MES").desc()).display()

# COMMAND ----------

fact_pedidos = (fact_pedidos.groupBy("FECHA_REGISTRO",month("FECHA_REGISTRO").alias("MES_FECHA_REGISTRO"),
    year("FECHA_REGISTRO").alias("ANIO_FECHA_REGISTRO"),
    month("FECHA_PEDIDO").alias("MES_FECHA_PEDIDO"),
    year("FECHA_PEDIDO").alias("ANIO_FECHA_PEDIDO"),
    "ID_CANAL_DISTRIBUCION",
    "ID_CLIENTE",
    "ID_MATERIAL",
    "ID_CLASE_DOCUMENTO_VENTA",
    "FECHA_COMPROMISO",
    "FECHA_REGISTRO",
    "FECHA_PEDIDO"
).agg(
    sum("PIEZAS").alias('PIEZAS'),
    sum("PIEZAS_FACTURADA").alias('PIEZAS_FACTURADA'),
    sum("PIEZAS_RECHAZADA").alias('PIEZAS_RECHAZADA'),
    sum("PIEZAS_BACKORDER").alias('PIEZAS_BACKORDER'),
    sum("IMPORTE_FACTURADA").alias('IMPORTE_FACTURADA'),
    sum("IMPORTE_RECHAZADA").alias('IMPORTE_RECHAZADA'),
    sum("IMPORTE_BACKORDER").alias('IMPORTE_BACKORDER'),
    sum("IMPORTE").alias('IMPORTE'),
    sum("IMPORTE_FACTURADA_DOLARES").alias('IMPORTE_FACTURADA_DOLARES'),
    sum("IMPORTE_RECHAZADA_DOLARES").alias('IMPORTE_RECHAZADA_DOLARES'),
    sum("IMPORTE_BACKORDER_DOLARES").alias('IMPORTE_BACKORDER_DOLARES'),
    sum("IMPORTE_DOLARES").alias('IMPORTE_DOLARES')
    
))

# COMMAND ----------

import pyspark.sql.functions as F
fact_pedidos = (fact_pedidos.withColumn("ANIO_MES", when(length("MES_FECHA_REGISTRO")==2,concat(col("ANIO_FECHA_REGISTRO"), lit("-"), col("MES_FECHA_REGISTRO"))).otherwise(concat(col("ANIO_FECHA_REGISTRO"), lit("-0"), col("MES_FECHA_REGISTRO")))))


# COMMAND ----------

fact_pedidos = fact_pedidos.withColumn("ANIO_MES_PEDIDO", when(length("MES_FECHA_PEDIDO")==2,concat(col("ANIO_FECHA_PEDIDO"), lit("-"), col("MES_FECHA_PEDIDO"))).otherwise(concat(col("ANIO_FECHA_PEDIDO"), lit("-0"), col("MES_FECHA_PEDIDO"))))


# COMMAND ----------

fact_pedidos = fact_pedidos.withColumn(
    "IMPORTE_BACKORDER",
    when(col("FECHA_COMPROMISO") > col("FECHA_REGISTRO"), 0)
    .otherwise(col("IMPORTE_BACKORDER"))
).withColumn(
    "PIEZAS_BACKORDER",
    when(col("FECHA_COMPROMISO") > col("FECHA_REGISTRO"), 0)
    .otherwise(col("PIEZAS_BACKORDER"))
).withColumn(
    "IMPORTE",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("IMPORTE"))
    .otherwise(0)
).withColumn(
    "PIEZAS",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("PIEZAS"))
    .otherwise(0)
).withColumn(
    "IMPORTE_RECHAZADA",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("IMPORTE_RECHAZADA"))
    .otherwise(0)
).withColumn(
    "PIEZAS_RECHAZADA",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("PIEZAS_RECHAZADA"))
    .otherwise(0)
).withColumn(
    "IMPORTE_BACKORDER_DOLARES",
    when(col("FECHA_COMPROMISO") > col("FECHA_REGISTRO"), 0)
    .otherwise(col("IMPORTE_BACKORDER_DOLARES"))
).withColumn(
    "IMPORTE_DOLARES",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("IMPORTE_DOLARES"))
    .otherwise(0)
).withColumn(
    "IMPORTE_RECHAZADA_DOLARES",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("IMPORTE_RECHAZADA_DOLARES"))
    .otherwise(0))


# COMMAND ----------

fact_pedidos = (fact_pedidos.groupBy("ANIO_MES","FECHA_REGISTRO","MES_FECHA_REGISTRO","ANIO_FECHA_REGISTRO",
    "ID_CANAL_DISTRIBUCION",
    "ID_CLIENTE",
    "ID_MATERIAL"
).agg(
    sum("PIEZAS").alias('PIEZAS'),
    sum("PIEZAS_FACTURADA").alias('PIEZAS_FACTURADA'),
    sum("PIEZAS_RECHAZADA").alias('PIEZAS_RECHAZADA'),
    sum("PIEZAS_BACKORDER").alias('PIEZAS_BACKORDER'),
    sum("IMPORTE_FACTURADA").alias('IMPORTE_FACTURADA'),
    sum("IMPORTE_RECHAZADA").alias('IMPORTE_RECHAZADA'),
    sum("IMPORTE_BACKORDER").alias('IMPORTE_BACKORDER'),
    sum("IMPORTE").alias('IMPORTE'),
    sum("IMPORTE_FACTURADA_DOLARES").alias('IMPORTE_FACTURADA_DOLARES'),
    sum("IMPORTE_RECHAZADA_DOLARES").alias('IMPORTE_RECHAZADA_DOLARES'),
    sum("IMPORTE_BACKORDER_DOLARES").alias('IMPORTE_BACKORDER_DOLARES'),
    sum("IMPORTE_DOLARES").alias('IMPORTE_DOLARES'),
))

# COMMAND ----------

fact_pedidos = (fact_pedidos.withColumn('ID_MATERIAL',regexp_replace(col('ID_MATERIAL'), r'^[0]*', ''))
                       )

# COMMAND ----------

# fact_pedidos_dolares = (fact_pedidos_dolares.withColumn('ID_MATERIAL',regexp_replace(col('ID_MATERIAL'), r'^[0]*', ''))
#                        )

# COMMAND ----------

fact_pedidos.groupBy("FECHA_REGISTRO").agg(sum("IMPORTE_FACTURADA")).orderBy(col("FECHA_REGISTRO").desc()).display()

fact_pedidos.groupBy(year("FECHA_REGISTRO").alias("ANIO"), month("FECHA_REGISTRO").alias("MES")).agg(sum("IMPORTE_FACTURADA").alias("SUM_IMPORTE_FACTURADA")).orderBy(col("ANIO").desc(), col("MES").desc()).display()

# COMMAND ----------

agrupador_fact_pedidos = fact_pedidos.groupBy("ANIO_MES").agg(max("FECHA_REGISTRO").alias("FECHA_REGISTRO"))
# agrupador_fact_pedidos.display()

# COMMAND ----------

agrupador_fact_pedidos.display()

# COMMAND ----------

fact_pedidos = fact_pedidos.join(agrupador_fact_pedidos,
                  on = ["ANIO_MES","FECHA_REGISTRO"],
                  how = "inner")

# COMMAND ----------

table = "fact_pedidos"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos silver
(fact_pedidos.
    write.format('delta').
    partitionBy('ANIO_MES','ID_CANAL_DISTRIBUCION').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC #Pedidos diarios

# COMMAND ----------

#Hechos
table = "FACT_PEDIDOS_ESTATUS_ULTIMO_REGISTRO"
fact_pedidos_diarios = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/"))
fact_pedidos_diarios = fact_pedidos_diarios.withColumn("rank", rank().over(Window.partitionBy("FECHA_REGISTRO").orderBy(col("SEQ_DATE").desc()))).filter(col("rank") == 1).drop("rank")                   
fact_pedidos_diarios = (fact_pedidos_diarios.
                     select("ID_MANDANTE","FECHA_REGISTRO","PIEZAS","ID_CLIENTE_SOLICITANTE","ID_MATERIAL","ID_CLASE_DOCUMENTO_VENTA","ID_CANAL_DISTRIBUCION","FECHA_COMPROMISO","PIEZAS_FACTURADA","PIEZAS_RECHAZADA","PIEZAS_BACKORDER","IMPORTE_FACTURADA","IMPORTE_RECHAZADA","IMPORTE_BACKORDER","IMPORTE","FECHA_PEDIDO","TIPO_CAMBIO_DPRECIOS"
                            ).
                     filter((fact_pedidos_diarios["ID_MANDANTE"] == "504") & 
                            ((fact_pedidos_diarios["ID_CANAL_DISTRIBUCION"] == "MI") | 
                             (fact_pedidos_diarios["ID_CANAL_DISTRIBUCION"] == "ME") | 
                             (fact_pedidos_diarios["ID_CANAL_DISTRIBUCION"] == "MO") 
                             ) &
                            ((fact_pedidos_diarios["ID_MATERIAL"] != "ALLOWANCES")
                              | (fact_pedidos_diarios["ID_MATERIAL"] != "VENTA_INTERCOMPAÑI" ))
                            )
                            )
fact_pedidos_diarios = fact_pedidos_diarios.withColumnRenamed("ID_CLIENTE_SOLICITANTE","ID_CLIENTE")

# COMMAND ----------

fact_pedidos_diarios = fact_pedidos_diarios \
    .withColumn("IMPORTE_FACTURADA_DOLARES",
                when(col("ID_CANAL_DISTRIBUCION") != 'MI',
                     col("IMPORTE_FACTURADA") / col("TIPO_CAMBIO_DPRECIOS")).otherwise(0)) \
    .withColumn("IMPORTE_RECHAZADA_DOLARES",
                when(col("ID_CANAL_DISTRIBUCION") != 'MI',
                     col("IMPORTE_RECHAZADA") / col("TIPO_CAMBIO_DPRECIOS")).otherwise(0)) \
    .withColumn("IMPORTE_BACKORDER_DOLARES",
                when(col("ID_CANAL_DISTRIBUCION") != 'MI',
                     col("IMPORTE_BACKORDER") / col("TIPO_CAMBIO_DPRECIOS")).otherwise(0)) \
    .withColumn("IMPORTE_DOLARES",
                when(col("ID_CANAL_DISTRIBUCION") != 'MI',
                     col("IMPORTE") / col("TIPO_CAMBIO_DPRECIOS")).otherwise(0))

# COMMAND ----------

fact_pedidos_diarios = (fact_pedidos_diarios.groupBy("FECHA_REGISTRO",month("FECHA_REGISTRO").alias("MES_FECHA_REGISTRO"),
    year("FECHA_REGISTRO").alias("ANIO_FECHA_REGISTRO"),
    day("FECHA_REGISTRO").alias("DIA_FECHA_REGISTRO"),
    month("FECHA_PEDIDO").alias("MES_FECHA_PEDIDO"),
    year("FECHA_PEDIDO").alias("ANIO_FECHA_PEDIDO"),
    "ID_CANAL_DISTRIBUCION",
    "ID_CLIENTE",
    "ID_MATERIAL",
    "ID_CLASE_DOCUMENTO_VENTA",
    "FECHA_COMPROMISO",
    "FECHA_REGISTRO",
    "FECHA_PEDIDO"
).agg(
    sum("PIEZAS").alias('PIEZAS'),
    sum("PIEZAS_FACTURADA").alias('PIEZAS_FACTURADA'),
    sum("PIEZAS_RECHAZADA").alias('PIEZAS_RECHAZADA'),
    sum("PIEZAS_BACKORDER").alias('PIEZAS_BACKORDER'),
    sum("IMPORTE_FACTURADA").alias('IMPORTE_FACTURADA'),
    sum("IMPORTE_RECHAZADA").alias('IMPORTE_RECHAZADA'),
    sum("IMPORTE_BACKORDER").alias('IMPORTE_BACKORDER'),
    sum("IMPORTE").alias('IMPORTE'),
    sum("IMPORTE_FACTURADA_DOLARES").alias('IMPORTE_FACTURADA_DOLARES'),
    sum("IMPORTE_RECHAZADA_DOLARES").alias('IMPORTE_RECHAZADA_DOLARES'),
    sum("IMPORTE_BACKORDER_DOLARES").alias('IMPORTE_BACKORDER_DOLARES'),
    sum("IMPORTE_DOLARES").alias('IMPORTE_DOLARES')
))

# COMMAND ----------

fact_pedidos_diarios = fact_pedidos_diarios.withColumn(
    "IMPORTE_BACKORDER",
    when(col("FECHA_COMPROMISO") > col("FECHA_REGISTRO"), 0)
    .otherwise(col("IMPORTE_BACKORDER"))
).withColumn(
    "PIEZAS_BACKORDER",
    when(col("FECHA_COMPROMISO") > col("FECHA_REGISTRO"), 0)
    .otherwise(col("PIEZAS_BACKORDER"))
).withColumn(
    "IMPORTE",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("IMPORTE"))
    .otherwise(0)
).withColumn(
    "PIEZAS",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("PIEZAS"))
    .otherwise(0)
).withColumn(
    "IMPORTE_RECHAZADA",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("IMPORTE_RECHAZADA"))
    .otherwise(0)
).withColumn(
    "PIEZAS_RECHAZADA",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("PIEZAS_RECHAZADA"))
    .otherwise(0)
).withColumn(
    "IMPORTE_BACKORDER_DOLARES",
    when(col("FECHA_COMPROMISO") > col("FECHA_REGISTRO"), 0)
    .otherwise(col("IMPORTE_BACKORDER_DOLARES"))
).withColumn(
    "IMPORTE_DOLARES",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("IMPORTE_DOLARES"))
    .otherwise(0)
).withColumn(
    "IMPORTE_RECHAZADA_DOLARES",
    when(col("ID_CLASE_DOCUMENTO_VENTA").isin(["ZOMI", "ZOME", "ZOMO"]), col("IMPORTE_RECHAZADA_DOLARES"))
    .otherwise(0))


                       

# COMMAND ----------

fact_pedidos_diarios = (fact_pedidos_diarios.withColumn("ANIO_MES", when(length("MES_FECHA_REGISTRO")==2,concat(col("ANIO_FECHA_REGISTRO"), lit("-"), col("MES_FECHA_REGISTRO"))).otherwise(concat(col("ANIO_FECHA_REGISTRO"), lit("-0"), col("MES_FECHA_REGISTRO")))))


# COMMAND ----------

fact_pedidos_diarios = (fact_pedidos_diarios.groupBy("ANIO_MES","FECHA_REGISTRO","MES_FECHA_REGISTRO","ANIO_FECHA_REGISTRO","DIA_FECHA_REGISTRO",
    "ID_CANAL_DISTRIBUCION",
    "ID_CLIENTE",
    "ID_MATERIAL"
).agg(
    sum("PIEZAS").alias('PIEZAS'),
    sum("PIEZAS_FACTURADA").alias('PIEZAS_FACTURADA'),
    sum("PIEZAS_RECHAZADA").alias('PIEZAS_RECHAZADA'),
    sum("PIEZAS_BACKORDER").alias('PIEZAS_BACKORDER'),
    sum("IMPORTE_FACTURADA").alias('IMPORTE_FACTURADA'),
    sum("IMPORTE_RECHAZADA").alias('IMPORTE_RECHAZADA'),
    sum("IMPORTE_BACKORDER").alias('IMPORTE_BACKORDER'),
    sum("IMPORTE").alias('IMPORTE'),
    sum("IMPORTE_FACTURADA_DOLARES").alias('IMPORTE_FACTURADA_DOLARES'),
    sum("IMPORTE_RECHAZADA_DOLARES").alias('IMPORTE_RECHAZADA_DOLARES'),
    sum("IMPORTE_BACKORDER_DOLARES").alias('IMPORTE_BACKORDER_DOLARES'),
    sum("IMPORTE_DOLARES").alias('IMPORTE_DOLARES'),
))

# COMMAND ----------

fact_pedidos_diarios = (fact_pedidos_diarios.withColumn('ID_MATERIAL',regexp_replace(col('ID_MATERIAL'), r'^[0]*', ''))
                       )

# COMMAND ----------

table = "fact_pedidos_diarios"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos silver
(fact_pedidos_diarios.
    write.format('delta').
    partitionBy('ANIO_MES','ID_CANAL_DISTRIBUCION').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# MAGIC %md
# MAGIC #REBATE

# COMMAND ----------

table = "CTL_REBATE"
rebate= (spark.
                        read.
                        parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/").distinct())

# COMMAND ----------

table="rebate"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos silver
(rebate.
    write.format('delta').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------



# COMMAND ----------

table = "CTL_REBATE_CLIENTE"
rebate_cliente = (spark.
                        read.
                        parquet(f"/mnt/adls-dac-data-bi-pr-scus/bronze/tablas_sql/{table}.parquet/").distinct())

# COMMAND ----------

table="rebate_cliente"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos silver
(rebate_cliente.
    write.format('delta').
    mode("overwrite").
    parquet(target_path))