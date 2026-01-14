

current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month
next_year = current_year + 1
current_anio_mes_follow_up = current_date.strftime("%Y-%m")


if current_date.day == 1:
    print("ok")
else:
    dbutils.notebook.exit("Proceso Concluido")





table = "follow_up"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"
follow_up= (spark
       .read
       .parquet(source_path))


current_anio_mes_follow_up


follow_up = follow_up.filter(col("ANIO_MES")>= current_anio_mes_follow_up)


current_anio_mes_follow_up


follow_up = follow_up.fillna(0)




follow_up_piezas = follow_up.filter(col("CONCEPTO") == 'PIEZAS')


follow_up_piezas = (
    follow_up_piezas.withColumn("INV_REAL", col("TOTAL")).withColumn("INV_FINAL", col("CANTIDAD") + col("INV_REAL") - col("DI") - col("BACKORDER"))
    .withColumn("INV_FINAL2", when(col("INV_FINAL") <= 0, 0).otherwise(col("INV_FINAL")))
    .withColumn("BO_EST", when(col("INV_FINAL") > 0, 0).otherwise(-col("INV_FINAL")))
    .withColumn(
        "RECUPERACION_BO",
        when(
            col("BACKORDER") == 0,
            0
        ).otherwise(
            when(
                (col("INV_REAL") + col("CANTIDAD")) > col("BACKORDER"),
                col("BACKORDER")
            ).otherwise(col("INV_REAL") + col("CANTIDAD"))
        )
    )
    .withColumn(
        "COBERTURA_DI",
        when(
            (col("CANTIDAD") + col("INV_REAL") - col("BACKORDER")) > col("DI"),
            col("DI")
        ).when(
            (col("CANTIDAD") + col("INV_REAL") - col("BACKORDER")) < 0,
            0
        ).otherwise(
            col("CANTIDAD") + col("INV_REAL") - col("BACKORDER")
        )
    ).withColumn("INV_REAL_FIN", col("TOTAL_FIN")).withColumn("INV_FINAL_DIN", col("CANTIDAD_FIN") + col("INV_REAL_FIN") - col("DI") - col("VENTA_FIN") - col("BACKORDER_FIN"))
    .withColumn("INV_FINAL2_DIN", when(col("INV_FINAL_DIN") <= 0, 0).otherwise(col("INV_FINAL_DIN")))
    .withColumn("BO_EST_DIN", when(col("INV_FINAL_DIN") > 0, 0).otherwise(-col("INV_FINAL_DIN")))
    .withColumn(
        "RECUPERACION_BO_DIN",
        when(
            col("BACKORDER_FIN") == 0,
            0
        ).otherwise(
            when(
                (col("INV_REAL_FIN") + col("CANTIDAD_FIN")) > col("BACKORDER_FIN"),
                col("BACKORDER_FIN")
            ).otherwise(col("INV_REAL_FIN") + col("CANTIDAD_FIN"))
        )
    )
    .withColumn(
        "COBERTURA_DI_DIN",
        when(
            (col("CANTIDAD_FIN") + col("INV_REAL_FIN") - col("BACKORDER_FIN")) > col("DI"),
            col("DI")
        ).when(
            (col("CANTIDAD_FIN") + col("INV_REAL_FIN") - col("BACKORDER_FIN")) < 0,
            0
        ).otherwise(
            col("CANTIDAD_FIN") + col("INV_REAL_FIN") - col("BACKORDER_FIN")
        )
    )
    )
    








from datetime import datetime, timedelta

current_anio_mes = (current_date.replace(day=28) + timedelta(days=4)).replace(day=1).replace(day=1).strftime("%Y-%m")

print(current_anio_mes)



window_spec = Window.partitionBy("ID_MATERIAL").orderBy("ANIO_MES")



follow_up_piezas = follow_up_piezas.withColumn("INV_INICIO", lag("INV_FINAL2").over(window_spec)).withColumn("BO_EST_2", lag("BO_EST").over(window_spec)).withColumn("INV_INICIO_DIN", lag("INV_FINAL2_DIN").over(window_spec)).withColumn("BO_EST_2_DIN", lag("BO_EST_DIN").over(window_spec)).fillna(0)


current_anio_mes


follow_up_piezas_1 = (
        follow_up_piezas.filter(col("ANIO_MES") == current_anio_mes).withColumn("INV_FINAL", col("CANTIDAD") + col("INV_INICIO") - col("DI") - col("BO_EST_2"))
        .withColumn("INV_FINAL2", when(col("INV_FINAL") <= 0, 0).otherwise(col("INV_FINAL")))
        .withColumn("BO_EST", when(col("INV_FINAL") > 0, 0).otherwise(-col("INV_FINAL")))
        .withColumn(
            "RECUPERACION_BO",
            when(
                col("BO_EST") == 0,
                0
            ).otherwise(
                when(
                    (col("INV_INICIO") + col("CANTIDAD")) > col("BO_EST_2"),
                    col("BO_EST")
                ).otherwise(col("INV_INICIO") + col("CANTIDAD"))
            )
        )
        .withColumn(
            "COBERTURA_DI",
            when(
                (col("CANTIDAD") + col("INV_INICIO") - col("BO_EST_2")) > col("DI"),
                col("DI")
            ).when(
                (col("CANTIDAD") + col("INV_INICIO") - col("BO_EST_2")) < 0,
                0
            ).otherwise(
                col("CANTIDAD") + col("INV_INICIO") - col("BO_EST_2")
            )
        ).withColumn("INV_FINAL_DIN", col("CANTIDAD_FIN") + col("INV_INICIO_DIN") - col("DI") - col("BO_EST_2_DIN"))
        .withColumn("INV_FINAL2_DIN", when(col("INV_FINAL_DIN") <= 0, 0).otherwise(col("INV_FINAL_DIN")))
        .withColumn("BO_EST_DIN", when(col("INV_FINAL_DIN") > 0, 0).otherwise(-col("INV_FINAL_DIN")))
        .withColumn(
            "RECUPERACION_BO_DIN",
            when(
                col("BO_EST_DIN") == 0,
                0
            ).otherwise(
                when(
                    (col("INV_INICIO_DIN") + col("CANTIDAD_FIN")) > col("BO_EST_2_DIN"),
                    col("BO_EST_DIN")
                ).otherwise(col("INV_INICIO_DIN") + col("CANTIDAD_FIN"))
            )
        )
        .withColumn(
            "COBERTURA_DI_DIN",
            when(
                (col("CANTIDAD_FIN") + col("INV_INICIO_DIN") - col("BO_EST_2_DIN")) > col("DI"),
                col("DI")
            ).when(
                (col("CANTIDAD_FIN") + col("INV_INICIO_DIN") - col("BO_EST_2_DIN")) < 0,
                0
            ).otherwise(
                col("CANTIDAD_FIN") + col("INV_INICIO_DIN") - col("BO_EST_2_DIN")
            )
        ))


follow_up_piezas_2 = follow_up_piezas.filter(col("ANIO_MES")!=follow_up_piezas_1.select("ANIO_MES").collect()[0][0])


follow_up_piezas = follow_up_piezas_1.unionByName(follow_up_piezas_2, allowMissingColumns=True).fillna(0)


current_anio_mes  = (datetime.strptime(current_anio_mes, "%Y-%m").replace(day=28) + timedelta(days=4)).replace(day=1).strftime("%Y-%m")


current_anio_mes


window_spec = Window.partitionBy("ID_MATERIAL").orderBy("ANIO_MES")


follow_up_piezas = follow_up_piezas.withColumn("INV_INICIO", lag("INV_FINAL2").over(window_spec)).withColumn("BO_EST_2", lag("BO_EST").over(window_spec)).withColumn("INV_INICIO_DIN", lag("INV_FINAL2_DIN").over(window_spec)).withColumn("BO_EST_2_DIN", lag("BO_EST_DIN").over(window_spec)).fillna(0)


follow_up_piezas_1 = (
        follow_up_piezas.filter(col("ANIO_MES") == current_anio_mes).withColumn("INV_FINAL", col("CANTIDAD") + col("INV_INICIO") - col("DI") - col("BO_EST_2"))
        .withColumn("INV_FINAL2", when(col("INV_FINAL") <= 0, 0).otherwise(col("INV_FINAL")))
        .withColumn("BO_EST", when(col("INV_FINAL") > 0, 0).otherwise(-col("INV_FINAL")))
        .withColumn(
            "RECUPERACION_BO",
            when(
                col("BO_EST") == 0,
                0
            ).otherwise(
                when(
                    (col("INV_INICIO") + col("CANTIDAD")) > col("BO_EST_2"),
                    col("BO_EST")
                ).otherwise(col("INV_INICIO") + col("CANTIDAD"))
            )
        )
        .withColumn(
            "COBERTURA_DI",
            when(
                (col("CANTIDAD") + col("INV_INICIO") - col("BO_EST_2")) > col("DI"),
                col("DI")
            ).when(
                (col("CANTIDAD") + col("INV_INICIO") - col("BO_EST_2")) < 0,
                0
            ).otherwise(
                col("CANTIDAD") + col("INV_INICIO") - col("BO_EST_2")
            )
        ).withColumn("INV_FINAL_DIN", col("CANTIDAD_FIN") + col("INV_INICIO_DIN") - col("DI") - col("BO_EST_2_DIN"))
        .withColumn("INV_FINAL2_DIN", when(col("INV_FINAL_DIN") <= 0, 0).otherwise(col("INV_FINAL_DIN")))
        .withColumn("BO_EST_DIN", when(col("INV_FINAL_DIN") > 0, 0).otherwise(-col("INV_FINAL_DIN")))
        .withColumn(
            "RECUPERACION_BO_DIN",
            when(
                col("BO_EST_DIN") == 0,
                0
            ).otherwise(
                when(
                    (col("INV_INICIO_DIN") + col("CANTIDAD_FIN")) > col("BO_EST_2_DIN"),
                    col("BO_EST_DIN")
                ).otherwise(col("INV_INICIO_DIN") + col("CANTIDAD_FIN"))
            )
        )
        .withColumn(
            "COBERTURA_DI_DIN",
            when(
                (col("CANTIDAD_FIN") + col("INV_INICIO_DIN") - col("BO_EST_2_DIN")) > col("DI"),
                col("DI")
            ).when(
                (col("CANTIDAD_FIN") + col("INV_INICIO_DIN") - col("BO_EST_2_DIN")) < 0,
                0
            ).otherwise(
                col("CANTIDAD_FIN") + col("INV_INICIO_DIN") - col("BO_EST_2_DIN")
            )
        ))





follow_up_piezas_2 = follow_up_piezas.filter(col("ANIO_MES")!=follow_up_piezas_1.select("ANIO_MES").collect()[0][0])


follow_up_piezas = follow_up_piezas_1.unionByName(follow_up_piezas_2, allowMissingColumns=True)


current_anio_mes  = (datetime.strptime(current_anio_mes, "%Y-%m").replace(day=28) + timedelta(days=4)).replace(day=1).strftime("%Y-%m")


current_anio_mes


window_spec = Window.partitionBy("ID_MATERIAL").orderBy("ANIO_MES")


follow_up_piezas = follow_up_piezas.withColumn("INV_INICIO", lag("INV_FINAL2").over(window_spec)).withColumn("BO_EST_2", lag("BO_EST").over(window_spec)).withColumn("INV_INICIO_DIN", lag("INV_FINAL2_DIN").over(window_spec)).withColumn("BO_EST_2_DIN", lag("BO_EST_DIN").over(window_spec)).fillna(0)


follow_up_piezas_1 = (
        follow_up_piezas.filter(col("ANIO_MES") == current_anio_mes).withColumn("INV_FINAL", col("CANTIDAD") + col("INV_INICIO") - col("DI") - col("BO_EST_2"))
        .withColumn("INV_FINAL2", when(col("INV_FINAL") <= 0, 0).otherwise(col("INV_FINAL")))
        .withColumn("BO_EST", when(col("INV_FINAL") > 0, 0).otherwise(-col("INV_FINAL")))
        .withColumn(
            "RECUPERACION_BO",
            when(
                col("BO_EST") == 0,
                0
            ).otherwise(
                when(
                    (col("INV_INICIO") + col("CANTIDAD")) > col("BO_EST_2"),
                    col("BO_EST")
                ).otherwise(col("INV_INICIO") + col("CANTIDAD"))
            )
        )
        .withColumn(
            "COBERTURA_DI",
            when(
                (col("CANTIDAD") + col("INV_INICIO") - col("BO_EST_2")) > col("DI"),
                col("DI")
            ).when(
                (col("CANTIDAD") + col("INV_INICIO") - col("BO_EST_2")) < 0,
                0
            ).otherwise(
                col("CANTIDAD") + col("INV_INICIO") - col("BO_EST_2")
            )
        ).withColumn("INV_FINAL_DIN", col("CANTIDAD_FIN") + col("INV_INICIO_DIN") - col("DI") - col("BO_EST_2_DIN"))
        .withColumn("INV_FINAL2_DIN", when(col("INV_FINAL_DIN") <= 0, 0).otherwise(col("INV_FINAL_DIN")))
        .withColumn("BO_EST_DIN", when(col("INV_FINAL_DIN") > 0, 0).otherwise(-col("INV_FINAL_DIN")))
        .withColumn(
            "RECUPERACION_BO_DIN",
            when(
                col("BO_EST_DIN") == 0,
                0
            ).otherwise(
                when(
                    (col("INV_INICIO_DIN") + col("CANTIDAD_FIN")) > col("BO_EST_2_DIN"),
                    col("BO_EST_DIN")
                ).otherwise(col("INV_INICIO_DIN") + col("CANTIDAD_FIN"))
            )
        )
        .withColumn(
            "COBERTURA_DI_DIN",
            when(
                (col("CANTIDAD_FIN") + col("INV_INICIO_DIN") - col("BO_EST_2_DIN")) > col("DI"),
                col("DI")
            ).when(
                (col("CANTIDAD_FIN") + col("INV_INICIO_DIN") - col("BO_EST_2_DIN")) < 0,
                0
            ).otherwise(
                col("CANTIDAD_FIN") + col("INV_INICIO_DIN") - col("BO_EST_2_DIN")
            )
        ))


follow_up_piezas_2 = follow_up_piezas.filter(col("ANIO_MES")!=follow_up_piezas_1.select("ANIO_MES").collect()[0][0])


follow_up_piezas = follow_up_piezas_1.unionByName(follow_up_piezas_2, allowMissingColumns=True)


    

    
    





follow_up_piezas = follow_up_piezas.withColumn(
    "MAX_PRECIO",
    when(col("PRECIO_INVENTARIO") >= col("COSTO_UNITARIO"), col("PRECIO_INVENTARIO"))
    .otherwise(col("COSTO_UNITARIO"))
)


follow_up_importe = (
    follow_up_piezas
    .withColumn('CANTIDAD', col('CANTIDAD') * col('MAX_PRECIO'))
    .withColumn('CANTIDAD_FIN', col('CANTIDAD_FIN') * col('MAX_PRECIO'))
    .withColumn('INGRESOS', col('INGRESOS') * col('MAX_PRECIO'))
    .withColumn('INGRESOS_FIN', col('INGRESOS_FIN') * col('MAX_PRECIO'))
    .withColumn('INV_FINAL', col('INV_FINAL') * col('MAX_PRECIO'))
    .withColumn('INV_FINAL2', col('INV_FINAL2') * col('MAX_PRECIO'))
    .withColumn('BO_EST', col('BO_EST') * col('PRECIO_DEMANDA_MENSUAL'))
    .withColumn('RECUPERACION_BO', col('RECUPERACION_BO') * col('PRECIO_BACKORDER'))
    .withColumn('COBERTURA_DI', col('COBERTURA_DI') * col('PRECIO_DEMANDA_MENSUAL'))
    .withColumn('INV_FINAL_DIN', col('INV_FINAL_DIN') * col('MAX_PRECIO'))
    .withColumn('INV_FINAL2_DIN', col('INV_FINAL2_DIN') * col('MAX_PRECIO'))
    .withColumn('BO_EST_DIN', col('BO_EST_DIN') * col('PRECIO_DEMANDA_MENSUAL'))
    .withColumn('RECUPERACION_BO_DIN', col('RECUPERACION_BO_DIN') * col('PRECIO_BACKORDER'))
    .withColumn('COBERTURA_DI_DIN', col('COBERTURA_DI_DIN') * col('PRECIO_DEMANDA_MENSUAL'))
    .withColumn('CANTIDAD_APARTADA', col('CANTIDAD_APARTADA') * col('MAX_PRECIO'))
    .withColumn('CANTIDAD_APARTADA_FIN', col('CANTIDAD_APARTADA_FIN') * col('MAX_PRECIO'))
    .withColumn('INV_INICIO', col('INV_INICIO') * col('MAX_PRECIO'))
      .withColumn('INV_INICIO_DIN', col('INV_INICIO_DIN') * col('MAX_PRECIO'))
       .withColumn('INV_REAL', col('INV_REAL') * col('MAX_PRECIO'))
      .withColumn('INV_REAL_FIN', col('INV_REAL_FIN') * col('MAX_PRECIO'))
).withColumn("CONCEPTO",lit("IMPORTE"))



from pyspark.sql.functions import col

follow_up_importe = follow_up.filter(col("CONCEPTO") == "IMPORTE").select(
    'ID_MATERIAL', 'ANIO_MES', 'CONCEPTO', 'STOCK_ALM_MAX', 'STOCK', 'STOCK_FINAL',
    'DI', 'BACKORDER', 'PEDIDOS', 'VENTA', 'BACKORDER_FIN', 'PEDIDOS_FIN', 'VENTA_FIN',
    'PPTO', 'A717', 'A718', 'A780,785 & 790', 'A730', 'TOTAL', 'A717_FIN', 'A718_FIN',
    'A780,785 & 790_FIN', 'A730_FIN', 'TOTAL_FIN', 'ID_PROVEEDOR', 'NOMBRE_PROVEEDOR',
    'NOMBRE_PAIS', 'NOMBRE_ESTADO', 'PRECIO_BACKORDER', 'PRECIO_INVENTARIO',
    'PRECIO_DEMANDA_ANUAL', 'PRECIO_DEMANDA_MENSUAL', 'COSTO_UNITARIO', 'INVENTARIO_OPTIMO_VALIDADO','ANIO_MES_1'
).join(
    follow_up_importe.select(
        'ID_MATERIAL', 'ANIO_MES', 'CONCEPTO', 'CANTIDAD', 'CANTIDAD_FIN', 'INGRESOS',
        'INGRESOS_FIN', 'INV_FINAL', 'INV_FINAL2', 'BO_EST', 'RECUPERACION_BO', 'COBERTURA_DI', 'INV_INICIO','INV_INICIO_DIN','INV_FINAL_DIN', 'INV_FINAL2_DIN', 'BO_EST_DIN', 'RECUPERACION_BO_DIN', 'COBERTURA_DI_DIN', 'CANTIDAD_APARTADA','CANTIDAD_APARTADA_FIN','INV_REAL','INV_REAL_FIN',
        'MAX_PRECIO'
    ), on=['ID_MATERIAL', 'ANIO_MES', 'CONCEPTO'], how='left'
)









follow_up = follow_up_importe.unionByName(follow_up_piezas, allowMissingColumns=True)


follow_up = follow_up.fillna(0)




















table = "follow_up_v2_foto"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"


(follow_up.
    write.format("delta").
    partitionBy('ANIO_MES_1').
    mode("overwrite").
    parquet(target_path))


table = "follow_up_final"
source_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"
follow_up_completo= (spark
       .read
       .parquet(source_path))





follow_up_completo = follow_up_completo.filter(col("ANIO_MES")<current_anio_mes_follow_up)










follow_up_final = follow_up_completo.unionByName(follow_up, allowMissingColumns=True)


current_anio_mes


from pyspark.sql.functions import col, when, lit

variables_originales = [
    "INV_FINAL", "INV_FINAL2", "BO_EST", "COBERTURA_DI",
    "RECUPERACION_BO", "INV_FINAL_DIN", "INV_FINAL2_DIN",
    "BO_EST_DIN", "COBERTURA_DI_DIN", "RECUPERACION_BO_DIN", "INV_INICIO",
    "INV_INICIO_DIN"
]

for var in variables_originales:
    follow_up_final = follow_up_final.withColumn(
        var,
        when(col("ANIO_MES") > lit(current_anio_mes), 0).otherwise(col(var))
    )



current_anio_mes_follow_up


follow_up_final = follow_up_final.withColumn("BO_EST",when(col("ANIO_MES").isin(["2025-01","2025-02"]),col("BACKORDER")).otherwise(col("BO_EST")))


from pyspark.sql.functions import col, when, lit

variables_originales = [
    "STOCK", "STOCK_FINAL", "BACKORDER", "PEDIDOS",
    "BACKORDER_FIN", "PEDIDOS_FIN", "VENTA_FIN",
    "A717", "A718", "A780,785 & 790", "A730", "TOTAL",
    "A717_FIN", "A718_FIN", "A780,785 & 790_FIN", "A730_FIN",
    "TOTAL_FIN", "INGRESOS","INGRESOS_FIN","INV_REAL","INV_REAL_FIN",
    "CANTIDAD_APARTADA",
    "CANTIDAD_APARTADA_FIN"
]

for var in variables_originales:
    follow_up_final = follow_up_final.withColumn(
        var,
        when(col("ANIO_MES") > lit(current_anio_mes_follow_up), 0).otherwise(col(var))
    )





follow_up_final = follow_up_final.withColumn(
    "INV_INICIO",
    when(
        col("ANIO_MES") <= lit(current_anio_mes_follow_up),
        col("STOCK")
    ).otherwise(col("INV_INICIO"))
).withColumn(
    "INV_INICIO_DIN",
    when(
         col("ANIO_MES") <= lit(current_anio_mes_follow_up),  # Agregar enero 2025
        col("STOCK_FINAL")
    ).otherwise(col("INV_INICIO_DIN"))
)


follow_up_final = follow_up_final.withColumn("BO_EST",when(col("ANIO_MES")>=lit(current_anio_mes_follow_up),col("BO_EST")*0.25).otherwise(col("BO_EST")))


current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month
next_year = current_year + 1
current_anio_mes_follow_up = current_date.strftime("%Y-%m")




table = "follow_up_final_foto"
target_path = f"/mnt/adls-dac-data-bi-pr-scus/gold/ventas/{table}.parquet/"


(follow_up_final.
    write.format("delta").
    partitionBy('ANIO_MES_1').
    mode("overwrite").
    parquet(target_path))








