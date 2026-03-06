#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para consultar datos de MongoDB - Detección de Diabetes
Proyecto: topicos-deteccion-diabetes
"""

from pyspark.sql import SparkSession
import pandas as pd
from tabulate import tabulate

# Configuración de MongoDB (USA LA MISMA IP DE TU SCRIPT)
MONGO_IP = "172.28.112.1"  # ← Usa la misma IP que en export_gold_to_mongo.py
MONGO_DATABASE = "medallondiabetes"
MONGO_COLLECTION = "gold_analytics"

print("=" * 80)
print("🔍 CONSULTANDO DATOS DESDE MONGODB")
print("=" * 80)
print(f"🌐 Conectando a MongoDB en: {MONGO_IP}:27017")
print(f"📦 Base de datos: {MONGO_DATABASE}")
print(f"📋 Colección: {MONGO_COLLECTION}")

# Crear sesión Spark con configuración de MongoDB
spark = SparkSession.builder \
    .appName("Consulta_MongoDB_Diabetes") \
    .config("spark.mongodb.connection.uri", f"mongodb://{MONGO_IP}:27017") \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .config("spark.mongodb.collection", MONGO_COLLECTION) \
    .getOrCreate()

try:
    # 1. LEER TODOS LOS DATOS
    print("\n📖 Leyendo datos desde MongoDB...")
    df = spark.read \
        .format("mongodb") \
        .option("spark.mongodb.connection.uri", f"mongodb://{MONGO_IP}:27017") \
        .option("spark.mongodb.database", MONGO_DATABASE) \
        .option("spark.mongodb.collection", MONGO_COLLECTION) \
        .load()
    
    # Mostrar información general
    count = df.count()
    print(f"\n📊 Total de documentos en MongoDB: {count}")
    
    print("\n📋 Esquema de los documentos:")
    df.printSchema()
    
    print("\n🔍 Primeros 5 registros:")
    df.show(5, truncate=False)
    
    # 2. CONSULTAS ANALÍTICAS
    print("\n" + "=" * 80)
    print("📊 CONSULTAS ANALÍTICAS")
    print("=" * 80)
    
    # 2.1 Distribución por nivel de riesgo
    print("\n📊 Distribución por nivel de riesgo:")
    riesgo_df = df.groupBy("risk_level").count().orderBy("risk_level")
    riesgo_df.show(truncate=False)
    
    # Convertir a pandas para mejor visualización
    riesgo_pd = riesgo_df.toPandas()
    print("\n📊 Tabla de distribución por riesgo:")
    print(tabulate(riesgo_pd, headers='keys', tablefmt='grid', showindex=False))
    
    # 2.2 Estadísticas por outcome
    print("\n📊 Estadísticas por resultado (outcome):")
    stats_df = df.groupBy("outcome").agg(
        {"glucose": "avg", "bmi": "avg", "age": "avg"}
    ).withColumnRenamed("avg(glucose)", "glucose_prom") \
     .withColumnRenamed("avg(bmi)", "bmi_prom") \
     .withColumnRenamed("avg(age)", "edad_prom")
    
    stats_df.show(truncate=False)
    
    # 2.3 Correlación entre riesgo y outcome
    print("\n📊 Matriz riesgo vs outcome:")
    matriz_df = df.groupBy("risk_level", "outcome").count().orderBy("risk_level", "outcome")
    matriz_df.show(truncate=False)
    
    # 2.4 Si existe la tabla de métricas, consultarla también
    try:
        print("\n" + "=" * 80)
        print("📊 CONSULTANDO TABLA DE MÉTRICAS")
        print("=" * 80)
        
        df_metricas = spark.read \
            .format("mongodb") \
            .option("spark.mongodb.connection.uri", f"mongodb://{MONGO_IP}:27017") \
            .option("spark.mongodb.database", MONGO_DATABASE) \
            .option("spark.mongodb.collection", "metricas_por_riesgo") \
            .load()
        
        print("\n📋 Métricas por nivel de riesgo:")
        df_metricas.show(truncate=False)
        
    except Exception as e:
        print(f"\n⚠️ No se pudo leer la colección 'metricas_por_riesgo': {str(e)}")
    
    # 3. CONSULTAS ESPECÍFICAS CON FILTROS
    print("\n" + "=" * 80)
    print("🔍 CONSULTAS CON FILTROS")
    print("=" * 80)
    
    # 3.1 Pacientes con riesgo ALTO
    print("\n🔍 Pacientes con riesgo ALTO (máximo 5):")
    alto_riesgo = df.filter(df.risk_level == "ALTO").limit(5)
    alto_riesgo.show(truncate=False)
    
    # 3.2 Pacientes con glucosa > 150
    print("\n🔍 Pacientes con glucosa > 150 (máximo 5):")
    glucosa_alta = df.filter(df.glucose > 150).limit(5)
    glucosa_alta.show(truncate=False)
    
    # 3.3 Pacientes diabéticos (outcome=1) con IMC > 30
    print("\n🔍 Pacientes diabéticos con IMC > 30 (máximo 5):")
    diabeticos_obesos = df.filter((df.outcome == 1) & (df.bmi > 30)).limit(5)
    diabeticos_obesos.show(truncate=False)
    
    # 4. EXPORTAR RESULTADOS A CSV (opcional)
    print("\n" + "=" * 80)
    print("💾 EXPORTANDO RESULTADOS")
    print("=" * 80)
    
    # Guardar distribución por riesgo
    output_dir = "/home/hadoop/topicos-deteccion-diabetes/reports/consultas"
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    riesgo_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"file:{output_dir}/distribucion_riesgo")
    
    print(f"\n✅ Resultados guardados en: {output_dir}")
    
except Exception as e:
    print(f"\n❌ Error durante la consulta: {str(e)}")
    import traceback
    traceback.print_exc()
    
finally:
    spark.stop()
    print("\n🛑 Sesión Spark cerrada")
    print("=" * 80)