# Databricks notebook source
# MAGIC %run /Shared/FASE1_INTEGRACION/includes
# MAGIC

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC #Cliente - Linea

# COMMAND ----------

#Hechos
table = "fact_pedidos"
fact_pedidos = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))    

# COMMAND ----------

# Realizar el agrupamiento y la agregación
pedidos_clientes_productos = (
    fact_pedidos
    .groupBy(
        'MES_FECHA_REGISTRO',
        'ANIO_FECHA_REGISTRO',
        'ID_MATERIAL',
        'ID_CLIENTE',
        'ANIO_MES',
        'ID_CANAL_DISTRIBUCION',
        'FECHA_REGISTRO'
    )
    .agg(
        sum("PIEZAS").alias('PIEZAS'),
        sum("PIEZAS_FACTURADA").alias('VENTA_UNIDADES'),
        sum("PIEZAS_RECHAZADA").alias('PIEZAS_RECHAZADA'),
        sum("PIEZAS_BACKORDER").alias('PIEZAS_BACKORDER'),
        sum("IMPORTE_FACTURADA").alias('VENTA_IMPORTE'),
        sum("IMPORTE_RECHAZADA").alias('IMPORTE_RECHAZADA'),
        sum("IMPORTE_BACKORDER").alias('IMPORTE_BACKORDER'),
        sum("IMPORTE").alias('IMPORTE')
    )
    .filter(
        (col("ANIO_FECHA_REGISTRO") >= 2020) & (col("FECHA_REGISTRO") < date_format(current_date(), "yyyy-MM-01"))
    )
)




# COMMAND ----------

#Hechos
table = "dim_material"
dim_productos_materiales = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))    

# COMMAND ----------

pedidos_clientes_productos = pedidos_clientes_productos.join(dim_productos_materiales,
                                                   on = ["ID_MATERIAL","ID_CANAL_DISTRIBUCION"],
                                                   how = "inner")

# COMMAND ----------

pedidos_clientes_productos = (pedidos_clientes_productos.groupBy(
 'ID_SISTEMA',
 'SISTEMA',
 'ID_LINEA_ABIERTA',
 'LINEA',
 'LINEA_AGRUPADA',
 'ID_CLIENTE',
 'ANIO_MES',
 'ID_CANAL_DISTRIBUCION').agg(  
    sum("PIEZAS").alias('PIEZAS'),
    sum('VENTA_UNIDADES').alias('VENTA_UNIDADES'),
    sum("PIEZAS_RECHAZADA").alias('PIEZAS_RECHAZADA'),
    sum("PIEZAS_BACKORDER").alias('PIEZAS_BACKORDER'),
    sum('VENTA_IMPORTE').alias('VENTA_IMPORTE'),
    sum("IMPORTE_RECHAZADA").alias('IMPORTE_RECHAZADA'),
    sum("IMPORTE_BACKORDER").alias('IMPORTE_BACKORDER'),
    sum("IMPORTE").alias('IMPORTE')))

# COMMAND ----------

#Hechos
table = "dim_clientes"
dim_clientes = (spark.
                     read.
                     parquet(f"/mnt/adls-dac-data-bi-pr-scus/silver/tablas_sql/{table}.parquet/"))   

# COMMAND ----------

pedidos_clientes_productos = pedidos_clientes_productos.join(dim_clientes,
                                                   on = ["ID_CLIENTE","ID_CANAL_DISTRIBUCION"],
                                                   how = "inner")

# COMMAND ----------

pedidos_clientes_productos = (pedidos_clientes_productos.groupBy(
 'ID_SISTEMA',
 'SISTEMA',
 'ID_LINEA_ABIERTA',
 'LINEA',
 'LINEA_AGRUPADA',
 'GRUPO_CLIENTE',
 'ANIO_MES',
 'ID_CANAL_DISTRIBUCION').agg(  
    sum("PIEZAS").alias('PIEZAS'),
    sum('VENTA_UNIDADES').alias('VENTA_UNIDADES'),
    sum("PIEZAS_RECHAZADA").alias('PIEZAS_RECHAZADA'),
    sum("PIEZAS_BACKORDER").alias('PIEZAS_BACKORDER'),
    sum('VENTA_IMPORTE').alias('VENTA_IMPORTE'),
    sum("IMPORTE_RECHAZADA").alias('IMPORTE_RECHAZADA'),
    sum("IMPORTE_BACKORDER").alias('IMPORTE_BACKORDER'),
    sum("IMPORTE").alias('IMPORTE'),
    countDistinct("ID_CLIENTE").alias("CLIENTES_ACTIVOS")))

# COMMAND ----------

pedidos_clientes_productos = pedidos_clientes_productos.withColumn("ANIO_MES_1", F.col("ANIO_MES")) \
                                   .withColumn("ID_CANAL_DISTRIBUCION_1", F.col("ID_CANAL_DISTRIBUCION"))

# COMMAND ----------

pedidos_clientes_productos = pedidos_clientes_productos.withColumn("ID_PRD", F.concat(F.col("LINEA"), F.lit("-"),F.col("GRUPO_CLIENTE"), F.lit("-"), F.col("ID_CANAL_DISTRIBUCION")))

# COMMAND ----------

fechas = pedidos_clientes_productos.select("ANIO_MES").distinct()
# fechas.display()

# COMMAND ----------

fechas = fechas.withColumn("ID",lit(1))

# COMMAND ----------

combinaciones = pedidos_clientes_productos.select("ID_PRD").distinct()

# COMMAND ----------

combinaciones_gpo_linea = pedidos_clientes_productos.select("ID_PRD","LINEA","GRUPO_CLIENTE","ID_CANAL_DISTRIBUCION").distinct()

# COMMAND ----------

combinaciones_gpo_linea = combinaciones_gpo_linea.withColumnRenamed("ID_PRD","Combinacion")

# COMMAND ----------

# combinaciones.count()

# COMMAND ----------

combinaciones = combinaciones.withColumn("ID",lit(1))

# COMMAND ----------

combinaciones_fechas = combinaciones.join(fechas,
                       on = ["ID"],
                       how = 'inner')

# COMMAND ----------

pedidos_clientes_productos = combinaciones_fechas.join(pedidos_clientes_productos,
                          on =  ["ANIO_MES","ID_PRD"],
                          how = "left")

# COMMAND ----------

df_modelo = pedidos_clientes_productos

# COMMAND ----------

df_modelo = df_modelo.withColumn("FECHA", to_date(concat(col("ANIO_MES"), F.lit("-01"))))

# COMMAND ----------

# # Crear una columna con el último día del mes
df_modelo = df_modelo.withColumn('FECHA', last_day(df_modelo['FECHA']))


# COMMAND ----------

df_modelo = df_modelo.drop("ID")

# COMMAND ----------

combinaciones = combinaciones.drop("ID")

# COMMAND ----------

schema = StructType([StructField('ID_PRD', StringType(), True), StructField('FECHA', DateType(), True), StructField('ANIO_MES', StringType(), True), StructField('ID_SISTEMA', StringType(), True), StructField('SISTEMA', StringType(), True), StructField('ID_LINEA_ABIERTA', StringType(), True), StructField('LINEA', StringType(), True), StructField('LINEA_AGRUPADA', StringType(), True), StructField('GRUPO_CLIENTE', StringType(), True), StructField('ID_CANAL_DISTRIBUCION', StringType(), True), StructField('PIEZAS', DoubleType(), True), StructField('VENTA_UNIDADES', DoubleType(), True), StructField('PIEZAS_RECHAZADA', DoubleType(), True), StructField('PIEZAS_BACKORDER', DoubleType(), True), StructField('VENTA_IMPORTE', DoubleType(), True), StructField('IMPORTE_RECHAZADA', DoubleType(), True), StructField('IMPORTE_BACKORDER', DoubleType(), True), StructField('IMPORTE', DoubleType(), False), StructField('CLIENTES_ACTIVOS', DoubleType(), True), StructField('ANIO_MES_1', StringType(), True), StructField('ID_CANAL_DISTRIBUCION_1', StringType(), True), StructField('promedio_6_meses', DoubleType(), True), StructField('IMPORTE_SUAVIZADO', DoubleType(), True)])


# COMMAND ----------

df_modelo = df_modelo.fillna({'IMPORTE': 0})

# COMMAND ----------

esquema_suavizado = spark.createDataFrame([],schema)

# COMMAND ----------

# Initialize an empty DataFrame to accumulate results
df_modelo_suavizado_total = spark.createDataFrame([], esquema_suavizado.schema)

# Asegurarse que la columna 'FECHA' es de tipo date
df_modelo = df_modelo.withColumn("FECHA", to_date(col("FECHA")))

#Preparar una ventana de tiempo para calcular el promedio de los últimos 6 meses
windowSpec = Window.partitionBy("ID_PRD").orderBy("FECHA").rowsBetween(-6, -1)


for valor in combinaciones:
    # Filter df_modelo by the current identifier
    df_modelo_suavizado = df_modelo.filter(col("ID_PRD") == valor)
    
    # Calculate the moving average over the last 6 months
    df_modelo_suavizado = df_modelo_suavizado.withColumn(
    "promedio_6_meses", 
    mean(col("IMPORTE")).over(windowSpec))

    #Calculate quartiles and IQR
    quantiles = df_modelo_suavizado.stat.approxQuantile("IMPORTE", [0.25, 0.75], 0)
    Q1, Q3 = quantiles
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    #Filter to find values outside the quartiles
df_fuera_de_rango = df_modelo_suavizado.filter((col("IMPORTE") < lower_bound) | (col("IMPORTE") > upper_bound)
    )

df_fuera_de_rango = df_fuera_de_rango.withColumnRenamed("promedio_6_meses","IMPORTE_SUAVIZADO")
df_modelo_suavizado = df_modelo_suavizado.join(
df_fuera_de_rango.select("ID_PRD", "FECHA","IMPORTE_SUAVIZADO"),on=["ID_PRD", "FECHA"],how="left")

# Replace the 'IMPORTE' with the smoothed 'promedio_6_meses' where applicable
df_modelo_suavizado = df_modelo_suavizado.withColumn("IMPORTE_SUAVIZADO", when(col("IMPORTE_SUAVIZADO").isNull(), col("IMPORTE")).otherwise(col("IMPORTE_SUAVIZADO")))
    
# Accumulate in the total DataFrame if necessary
df_modelo_suavizado_total = df_modelo_suavizado_total.unionByName(df_modelo_suavizado, allowMissingColumns=True)

    # # Now you can work with df_modelo_suavizado_total which contains all the processed data




# COMMAND ----------

df_modelo_suavizado_total = df_modelo_suavizado_total.filter(col("FECHA")>'2020-12-31')

# COMMAND ----------

table = "pedidos_clientes_productos"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/"

# COMMAND ----------

#Escritura de los archivos gold
(df_modelo_suavizado_total.
    write.format('delta').
    partitionBy('ANIO_MES_1','ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##Combinaciones

# COMMAND ----------

# Get the current date
current_date = datetime.now()

# Set the end_date to the last day of the month before the current month
end_date = datetime(current_date.year, current_date.month, 1) - timedelta(days=1)

# Set the start_date to 24 months before the end_date
start_date = end_date.replace(year=end_date.year - 2)

combinaciones_fechas = df_modelo_suavizado_total.filter((col("FECHA") >= lit(start_date)) & (col("FECHA") <= lit(end_date)))





# COMMAND ----------

combinaciones_fechas = combinaciones_fechas.groupBy("ID_PRD").agg(count("ANIO_MES").alias("Conteo_Fechas"),
                                                                sum(when(col("IMPORTE") < 1, 1).otherwise(0)).alias("Conteo de Ceros"))


# COMMAND ----------

combinaciones_a_modelar = combinaciones_fechas.filter(
    (col("Conteo_Fechas") == 25) & (col("Conteo de ceros") <= 12)
)

# COMMAND ----------

combinaciones_no_modelables = combinaciones_fechas.filter(
    (col("Conteo_Fechas") == 25) & (col("Conteo de ceros") > 12)
)

# COMMAND ----------

# combinaciones_gpo_cte_a_modelar.count()

# COMMAND ----------

combinaciones_a_modelar = combinaciones_a_modelar.select("ID_PRD")

# COMMAND ----------

combinaciones_no_modelables = combinaciones_no_modelables.select("ID_PRD")

# COMMAND ----------

df_modelo = df_modelo_suavizado_total.join(combinaciones_a_modelar,
                                            on = ["ID_PRD"],
                                            how = "inner")

# COMMAND ----------

df_modelo_no_modelables = df_modelo_suavizado_total.join(combinaciones_no_modelables,
                                            on = ["ID_PRD"],
                                            how = "inner")


# COMMAND ----------

# Convertir la columna VENTA_IMPORTE a tipo Double (número)
df_modelo = df_modelo.withColumn("IMPORTE", df_modelo["IMPORTE_SUAVIZADO"].cast(DoubleType()))

# COMMAND ----------

# Convertir la columna VENTA_IMPORTE a tipo Double (número)
df_modelo_no_modelables = df_modelo_no_modelables.withColumn("IMPORTE", df_modelo["IMPORTE_SUAVIZADO"].cast(DoubleType()))

# COMMAND ----------

df_modelo_total_combinaciones = df_modelo.groupBy("ID_PRD","FECHA","ANIO_MES").agg(sum('IMPORTE').alias('IMPORTE'))
# Supongamos que 'df_spark' es tu DataFrame de PySpark
df_modelo_total_combinaciones= df_modelo_total_combinaciones.toPandas()
combinaciones_a_modelar = combinaciones_a_modelar.toPandas()

# COMMAND ----------

df_modelo_total_combinaciones_no_modelables = df_modelo_no_modelables.groupBy("ID_PRD","FECHA","ANIO_MES").agg(sum('IMPORTE').alias('IMPORTE'))
# Supongamos que 'df_spark' es tu DataFrame de PySpark
df_modelo_total_combinaciones_no_modelables= df_modelo_total_combinaciones_no_modelables.toPandas()
combinaciones_no_modelables = combinaciones_no_modelables.toPandas()

# COMMAND ----------

df_modelo_total_combinaciones = df_modelo_total_combinaciones.rename(columns={'IMPORTE': 'y', 'FECHA': 'ds'})

# COMMAND ----------

df_modelo_total_combinaciones_no_modelables = df_modelo_total_combinaciones_no_modelables.rename(columns={'IMPORTE': 'y', 'FECHA': 'ds'})

# COMMAND ----------

# Inicialización de DataFrames
parametros_total = pd.DataFrame()
fcst_df_total = pd.DataFrame()
mape_value_total = pd.DataFrame()

parametros = pd.DataFrame()
fcst_df = pd.DataFrame()
mape_value = pd.DataFrame()

# COMMAND ----------

# Supongo que df_modelo_total_combinaciones y combinaciones_a_modelar están definidos previamente
import pandas as pd
import numpy as np
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.arima.model import ARIMA
from itertools import product
import matplotlib.pyplot as plt
from prophet import Prophet
print("Combinaciones totales: ", len(combinaciones_a_modelar))
for valor in range(len(combinaciones_a_modelar)):
    print("valor: ", valor)
    #Configura el nivel de registro global a WARNING
    logging.getLogger('cmdstanpy').setLevel(logging.WARNING)
    logging.getLogger('prophet').setLevel(logging.WARNING)

    df_modelo_total = df_modelo_total_combinaciones[df_modelo_total_combinaciones["ID_PRD"] == combinaciones_a_modelar.iloc[valor]['ID_PRD']]
    df_modelo_total = df_modelo_total.sort_values(by='ds', ascending=True)
    df_modelo_total = df_modelo_total.reset_index(drop=True)
    df_modelo_total = df_modelo_total.fillna(0)

    df_modelo_total['ds'] = pd.to_datetime(df_modelo_total['ds'])
    df_modelo_total.set_index('ds', inplace=True)

    # Función para calcular el MAPE
    def mean_absolute_percentage_error(y_true, y_pred):
        return np.mean(np.abs((y_true - y_pred) / y_true)) * 100

    # Modelo Holt-Winters
    hw_model = ExponentialSmoothing(df_modelo_total['y'], seasonal='add', trend='add', seasonal_periods=12)
    hw_fitted_model = hw_model.fit()
    hw_fitted_values = hw_fitted_model.fittedvalues
    hw_forecast_12_months = hw_fitted_model.forecast(steps=12)
    hw_mape = mean_absolute_percentage_error(df_modelo_total['y'], hw_fitted_values)
    hw_mae = mean_absolute_error(df_modelo_total['y'], hw_fitted_values)

    # Modelo ARIMA
    p = d = q = range(0, 3)
    pdq = list(product(p, d, q))
    best_aic = float('inf')
    best_pdq = None
    for param in pdq:
        try:
            arima_model = ARIMA(df_modelo_total['y'], order=param)
            arima_fitted_model = arima_model.fit()
            if arima_fitted_model.aic < best_aic:
                best_aic = arima_fitted_model.aic
                best_pdq = param
        except:
            continue
    arima_model = ARIMA(df_modelo_total['y'], order=best_pdq)
    arima_fitted_model = arima_model.fit()
    arima_fitted_values = arima_fitted_model.fittedvalues
    arima_forecast_12_months = arima_fitted_model.forecast(steps=12)
    arima_mape = mean_absolute_percentage_error(df_modelo_total['y'], arima_fitted_values)
    arima_mae = mean_absolute_error(df_modelo_total['y'], arima_fitted_values)

    # Modelo Prophet
    df_prophet = df_modelo_total.reset_index().rename(columns={'ds': 'ds', 'y': 'y'})
    prophet_model = Prophet()
    prophet_model.fit(df_prophet)
    prophet_forecast = prophet_model.predict(df_prophet[['ds']])
    prophet_fitted_values = prophet_forecast['yhat']
    prophet_mape = mean_absolute_percentage_error(df_modelo_total['y'].reset_index(drop=True), prophet_fitted_values)
    prophet_mae = mean_absolute_error(df_modelo_total['y'].reset_index(drop=True), prophet_fitted_values)

    future = prophet_model.make_future_dataframe(periods=12, freq='M')
    prophet_forecast_12_months = prophet_model.predict(future)['yhat'].tail(12)

    mapes = [('Holt-Winters', hw_mape), ('ARIMA', arima_mape), ('Prophet', prophet_mape)]
    best_model = sorted(mapes, key=lambda x: x[1])[0]

    maes = [('Holt-Winters', hw_mae), ('ARIMA', arima_mae), ('Prophet', prophet_mae)]
    best_model_0 = sorted(maes, key=lambda x: x[1])[0]


    # Crear un DataFrame con los valores reales, ajustados y pronósticos
    future_dates = pd.date_range(start=df_modelo_total.index[-1], periods=13, freq='M')[1:]

    resultados = pd.DataFrame({
        'Fecha': df_modelo_total.index.append(future_dates),
        'Valores Reales': pd.concat([df_modelo_total['y'], pd.Series([np.nan]*12, index=future_dates)]),
        'Holt-Winters Ajustados': hw_fitted_values.append(hw_forecast_12_months),
        'ARIMA Ajustados': arima_fitted_values.append(arima_forecast_12_months),
        'Prophet Ajustados': pd.Series(prophet_fitted_values.append(prophet_forecast_12_months).reset_index(drop=True).values, index=df_modelo_total.index.append(future_dates)),
        'Combinacion': df_modelo_total.iloc[0]['ID_PRD'],
        'ID': valor

    })
    
    mapes = pd.DataFrame({
        'Modelo': ['Holt-Winters', 'ARIMA', 'Prophet'],
        'MAPE': [hw_mape, arima_mape, prophet_mape],
        'MAE': [hw_mae, arima_mae, prophet_mae],
        'Combinacion': df_modelo_total.iloc[0]['ID_PRD'],
        'ID': valor
    })

    mapes.replace([np.inf, -np.inf], 'serie discontinua', inplace=True)
      

    if (df_modelo_total == 0).any().any():

        if best_model_0[0] == 'Prophet':
            fcst_df_total = pd.concat([fcst_df_total, resultados[['Fecha','Valores Reales','Prophet Ajustados','Combinacion',"ID"]].rename(columns={'Prophet Ajustados': 'Valores Ajustados'})])
            mape_value_total = pd.concat([mape_value_total, mapes[mapes['Modelo']=='Prophet']])
        elif best_model_0[0] == 'ARIMA':
            fcst_df_total = pd.concat([fcst_df_total,resultados[['Fecha','Valores Reales','ARIMA Ajustados','Combinacion',"ID"]].rename(columns={'ARIMA Ajustados': 'Valores Ajustados'})])
            mape_value_total = pd.concat([mape_value_total, mapes[mapes['Modelo']=='ARIMA']])
        elif best_model_0[0] == 'Holt-Winters':
            fcst_df_total = pd.concat([fcst_df_total, resultados[['Fecha','Valores Reales','Holt-Winters Ajustados','Combinacion',"ID"]].rename(columns={'Holt-Winters Ajustados': 'Valores Ajustados'})])
            mape_value_total = pd.concat([mape_value_total, mapes[mapes['Modelo']=='Holt-Winters']])

    else:

        if best_model[0] == 'Prophet':
            fcst_df_total = pd.concat([fcst_df_total, resultados[['Fecha','Valores Reales','Prophet Ajustados','Combinacion',"ID"]].rename(columns={'Prophet Ajustados': 'Valores Ajustados'})])
            mape_value_total = pd.concat([mape_value_total, mapes[mapes['Modelo']=='Prophet']])
        elif best_model[0] == 'ARIMA':
            fcst_df_total = pd.concat([fcst_df_total,resultados[['Fecha','Valores Reales','ARIMA Ajustados','Combinacion',"ID"]].rename(columns={'ARIMA Ajustados': 'Valores Ajustados'})])
            mape_value_total = pd.concat([mape_value_total, mapes[mapes['Modelo']=='ARIMA']])
        elif best_model[0] == 'Holt-Winters':
            fcst_df_total = pd.concat([fcst_df_total, resultados[['Fecha','Valores Reales','Holt-Winters Ajustados','Combinacion',"ID"]].rename(columns={'Holt-Winters Ajustados': 'Valores Ajustados'})])
            mape_value_total = pd.concat([mape_value_total, mapes[mapes['Modelo']=='Holt-Winters']])


mape_value_aph_total = spark.createDataFrame(mape_value_total)
table = "mape_value_total_aph_gpo_cte_total"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/"
#Escritura de los archivos gold
(mape_value_aph_total.
    write.format('delta ').
    #  partitionBy('ANIO_MES_1','ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

resultados_aph_total = spark.createDataFrame(fcst_df_total)
# resultados_ap_total= resultados_ap_total.withColumn("ANIO_MES_1",col("ANIO_MES"))
table = "resultados_aph_gpo_cte_total"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/"
#Escritura de los archivos gold
(resultados_aph_total.
    write.format('delta ').
    #  partitionBy('ANIO_MES_1').
    mode("overwrite").
    parquet(target_path))


     








   

# COMMAND ----------

print(valor)

# COMMAND ----------

# mape_value_gpo_cte_ap_total = spark.createDataFrame(mape_value_gpo_cte_total)
# table = "mape_value_gpo_cte_total_aph_total_3105_bu"
# target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/"
# #Escritura de los archivos gold
# (mape_value_gpo_cte_ap_total.
#     write.format('delta ').
#     #  partitionBy('ANIO_MES_1','ID_CANAL_DISTRIBUCION_1').
#     mode("overwrite").
#     parquet(target_path))

# resultados_gpo_cte_ap_total = spark.createDataFrame(fcst_df_gpo_cte_total)
# # resultados_gpo_cte_ap_total= resultados_gpo_cte_ap_total.withColumn("ANIO_MES_1",col("ANIO_MES"))
# table = "resultados_gpo_cte_aph_total_3105_bu"
# target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/"
# #Escritura de los archivos gold
# (resultados_gpo_cte_ap_total.
#     write.format('delta ').
#     #  partitionBy('ANIO_MES_1').
#     mode("overwrite").
#     parquet(target_path))

# COMMAND ----------

# df_modelo_total_combinaciones_no_modelables[df_modelo_total_combinaciones_no_modelables["ID_PRD"] == combinaciones_no_modelables.iloc[0]['ID_PRD']]

# COMMAND ----------

resultado_modelos_no_modelables_total = pd.DataFrame()
modelos_no_modelables_total = pd.DataFrame()

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

for valor in range(len(combinaciones_no_modelables)):
        df_modelo_no_modelables = df_modelo_total_combinaciones_no_modelables[df_modelo_total_combinaciones_no_modelables["ID_PRD"] == combinaciones_no_modelables.iloc[valor]['ID_PRD']]
        df_modelo_no_modelables  = df_modelo_no_modelables .fillna(0)
        df_modelo_no_modelables  = df_modelo_no_modelables .sort_values(by='ds', ascending=True)
        df_modelo_no_modelables['type'] = 'real'  # Agregar una columna para indicar que estos son datos reales

        # Verificar si la serie de tiempo no tuvo cambios
        no_changes = (df_modelo_no_modelables['y'].diff().dropna() == 0).all()

        # Agregar columna de cambios
        df_modelo_no_modelables['no_changes'] = no_changes

        if no_changes:
            # Obtener el último valor de la serie original
            ultimo_valor = df_modelo_no_modelables['y'].iloc[-1]
            
            # Crear un DataFrame con los próximos 12 meses
            last_date = df_modelo_no_modelables['ds'].iloc[-1]
            new_dates = [(last_date + pd.DateOffset(months=i+1)).strftime('%Y-%m-%d') for i in range(12)]
            new_values = [ultimo_valor] * 12
            ID_PRD = df_modelo_no_modelables['ID_PRD'].iloc[0]
            
            new_df = pd.DataFrame({
                'ID_PRD': [ID_PRD] * 12,
                'ds': new_dates,
                'y': new_values,
                'type': 'forecast',
                'ID': valor
            })

            modelos = pd.DataFrame({
                'Combinacion': [ID_PRD],
                'Type': 'sin cambios',
                'ID': valor,
                'Modelo':'Carry Forward'
            })

            new_df['ds'] = pd.to_datetime(new_df['ds'])
            
            # Concatenar los DataFrames
            result_df = pd.concat([df_modelo_no_modelables, new_df], ignore_index=True)
        else:
            # Calcular la tendencia del último año
            df_last_year = df_modelo_no_modelables[df_modelo_no_modelables['ds'] >= (df_modelo_no_modelables['ds'].max() - pd.DateOffset(years=1))]
            avg_change_per_month = df_last_year['y'].diff().dropna().mean()
            
            # Obtener el último valor de la serie original
            last_date = df_modelo_no_modelables['ds'].iloc[-1]
            last_value = df_modelo_no_modelables['y'].iloc[-1]
            ID_PRD = df_modelo_no_modelables['ID_PRD'].iloc[0]
            
            # Crear un DataFrame con los próximos 12 meses basado en la tendencia
            new_dates = [(last_date + pd.DateOffset(months=i+1)).strftime('%Y-%m-%d') for i in range(12)]
            new_values = [last_value + (i+1) * avg_change_per_month for i in range(12)]
            
            new_df = pd.DataFrame({
                'ID_PRD': [ID_PRD] * 12,
                'ds': new_dates,
                'y': new_values,
                'type': 'forecast',
                'ID': valor
            })

            modelos = pd.DataFrame({
                'Combinacion': [ID_PRD],
                'Type': 'con cambios',
                'ID': valor,
                'Modelo':'Proyección Tendencia'
            })

            new_df['ds'] = pd.to_datetime(new_df['ds'])

            df_modelo_no_modelables = df_modelo_no_modelables.reset_index(drop=True)
            
            # Concatenar los DataFrames
            result_df = pd.concat([df_modelo_no_modelables, new_df], ignore_index=True)
            resultado_modelos_no_modelables = pd.DataFrame({
                'Fecha' : result_df['ds'],
                'Combinacion': [ID_PRD]*len(result_df['ds']),
                'Valores Reales': df_modelo_no_modelables['y'],
                'Valores Ajustados': result_df['y'],
                'ID': valor
            })
            resultado_modelos_no_modelables_total = pd.concat([resultado_modelos_no_modelables_total,resultado_modelos_no_modelables])
            modelos_no_modelables_total = pd.concat([modelos_no_modelables_total,modelos])

resultado_modelos_no_modelables_total = spark.createDataFrame(resultado_modelos_no_modelables_total)
table = "resultado_modelos_no_modelables_gpo_cte_total"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/"
#Escritura de los archivos gold
(resultado_modelos_no_modelables_total.
    write.format('delta ').
    #  partitionBy('ANIO_MES_1','ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

modelos_no_modelables_total= spark.createDataFrame(modelos_no_modelables_total)
# resultados_ap_total= resultados_ap_total.withColumn("ANIO_MES_1",col("ANIO_MES"))
table = "modelos_no_modelables_total"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/"
#Escritura de los archivos gold
(modelos_no_modelables_total.
    write.format('delta ').
    #  partitionBy('ANIO_MES_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# #Hechos
# table = "mape_value_gpo_cte_total_aph_total_3105"
# mape = (spark.
#                      read.
#                      parquet(f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/"))    

# COMMAND ----------

# #Hechos
# table = "modelos_no_modelables_total"
# modelos_no_modelables_total = (spark.
#                      read.
#                      parquet(f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/"))   

# COMMAND ----------

mape = mape_value_aph_total

# COMMAND ----------

from pyspark.sql.functions import col, when

# Suponiendo que 'df' es tu DataFrame original y 'MAPE' es una columna existente en él.
mape = mape.withColumn("Type", when(col("MAPE") == 'serie discontinua', 'serie discontinua').otherwise('serie continua'))\
         .withColumn("MAPE", when(col("MAPE") != 'serie discontinua', col("MAPE")).otherwise(0))


# COMMAND ----------

filtro_mape=mape.select('Combinacion').distinct()

# COMMAND ----------

mape.count()

# COMMAND ----------

modelos_no_modelables_total.count()

# COMMAND ----------

modelos_no_modelables_total= modelos_no_modelables_total.filter(
    ~col("Combinacion").isin([row.Combinacion for row in filtro_mape.select('Combinacion').collect()])
)

# COMMAND ----------

modelos = mape.unionByName(modelos_no_modelables_total, allowMissingColumns=True)

# COMMAND ----------

table = "modelos_gpo_cte"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/"
#Escritura de los archivos gold
(modelos.
    write.format('delta ').
    #  partitionBy('ANIO_MES_1','ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------

# #Hechos
# table = "resultados_gpo_cte_aph_total_3105"
# resultados = (spark.
#                      read.
#                      parquet(f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/"))    

# COMMAND ----------

# #Hechos
# table = "resultado_modelos_no_modelables_total"
# resultado_modelos_no_modelables_total = (spark.
#                      read.
#                      parquet(f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/"))  

# COMMAND ----------

resultados = resultados_aph_total 

# COMMAND ----------

filtro_resultados = resultados.select('Combinacion').distinct()

# COMMAND ----------

resultado_modelos_no_modelables_total= resultado_modelos_no_modelables_total.filter(
    ~col("Combinacion").isin([row.Combinacion for row in filtro_resultados.select('Combinacion').collect()])
)

# COMMAND ----------

resultados_pronostico = resultados.unionByName(resultado_modelos_no_modelables_total, allowMissingColumns=True)

# COMMAND ----------

from pyspark.sql.functions import col, date_format, current_date

resultados_pronostico = resultados_pronostico \
    .withColumn("ANIO_MES", date_format(col("Fecha"), "yyyy-MM")) \
    .withColumn("ANIO_MES_PRONOSTICO", date_format(current_date(), "yyyy-MM")) \
    .withColumn("Fecha_pronostico", date_format(current_date(), "yyyy-MM-dd")) \
    .withColumn("ANIO_MES_1", col("ANIO_MES"))

# COMMAND ----------

dim_productos_materiales = dim_productos_materiales.select("LINEA","LINEA_AGRUPADA","ID_CANAL_DISTRIBUCION","SISTEMA").distinct()

# COMMAND ----------

resultados_pronostico = resultados_pronostico.join(combinaciones_gpo_linea, on = 'Combinacion', how = 'inner')

# COMMAND ----------

resultados_pronostico = resultados_pronostico.join(dim_productos_materiales,
                           on = ["LINEA","ID_CANAL_DISTRIBUCION"],
                           how= "inner")

# COMMAND ----------

resultados_pronostico = resultados_pronostico.withColumn("Valores Ajustados",when(col("Valores Ajustados")<=0,0).otherwise(col("Valores Ajustados")))

# COMMAND ----------

table = "resultados_pronostico_gpo_cte"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/"
#Escritura de los archivos gold
(resultados_pronostico.
    write.format('delta ').
    partitionBy('ANIO_MES').
    mode("overwrite").
    parquet(target_path))

# COMMAND ----------



resultados_pronostico = resultados_pronostico.filter(col("Valores Reales").isNull())



# COMMAND ----------


resultados_pronostico = resultados_pronostico.withColumn("ANIO_MES", date_format(col("Fecha"), "yyyy-MM")) \
    .withColumn("ANIO_MES_PRONOSTICO", date_format(current_date(), "yyyy-MM")) \
    .withColumn("Fecha_pronostico", date_format(current_date(), "yyyy-MM-dd"))



# COMMAND ----------

resultados_pronostico =  resultados_pronostico.drop("ID","Valores Reales")

# COMMAND ----------

resultados_pronostico = resultados_pronostico.withColumnRenamed("Valores Ajustados", "Pronostico")

# COMMAND ----------

resultados_pronostico = resultados_pronostico.withColumn("Pronostico",when(col("Pronostico")<=0,0).otherwise(col("Pronostico")))

# COMMAND ----------

table = "resultados_pronostico_gpo_cte_escalera"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/analitica_avanzada/{table}.parquet/"
#Escritura de los archivos gold
(resultados_pronostico.
    write.format('delta ').
    #  partitionBy('ANIO_MES_1','ID_CANAL_DISTRIBUCION_1').
    mode("overwrite").
    parquet(target_path))