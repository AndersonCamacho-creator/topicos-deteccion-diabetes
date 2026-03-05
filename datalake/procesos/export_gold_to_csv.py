#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para exportar capa Functional a CSV (Gold) - Detección de Diabetes
Proyecto: topicos-deteccion-diabetes
"""

from pyspark.sql import SparkSession
import os
import subprocess
import glob
from datetime import datetime

# Crear sesión Spark con soporte Hive
spark = SparkSession.builder \
    .appName("Export-Diabetes-Gold-To-CSV") \
    .enableHiveSupport() \
    .getOrCreate()

# Configuración
database = "topicosunc_functional"
tabla_principal = "DIABETES_ANALYTICS"
tabla_metricas = "metricas_por_riesgo"

BASE_DIR = "/home/hadoop/topicos-deteccion-diabetes"
REPORTS_DIR = f"{BASE_DIR}/reports"
TEMP_DIR = f"{REPORTS_DIR}/temp"

print("=" * 80)
print("🚀 EXPORTANDO CAPA GOLD A CSV")
print("=" * 80)
print(f"📂 Base de datos: {database}")
print(f"📋 Tabla principal: {tabla_principal}")
print(f"📊 Tabla de métricas: {tabla_metricas}")
print(f"📁 Directorio reports: {REPORTS_DIR}")
print(f"📁 Directorio temporal: {TEMP_DIR}")
print(f"🕒 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def exportar_tabla(df, nombre_tabla):
    """Función auxiliar para exportar una tabla a CSV dentro de temp"""
    print(f"\n📥 Exportando tabla: {nombre_tabla}")
    
    count = df.count()
    print(f"   📊 Registros: {count}")
    
    # Ruta dentro de temp
    output_path = f"file:{TEMP_DIR}/{nombre_tabla.lower()}"
    
    # Exportar a CSV en temp
    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("header", "true") \
      .option("sep", ",") \
      .csv(output_path)
    
    print(f"   ✅ Archivos guardados en: {TEMP_DIR}/{nombre_tabla.lower()}/")
    
    # Mostrar nombre del archivo generado
    csv_files = glob.glob(f"{TEMP_DIR}/{nombre_tabla.lower()}/part-*.csv")
    if csv_files:
        print(f"   📄 Archivo: {os.path.basename(csv_files[0])}")

try:
    # Crear directorios si no existen
    os.makedirs(REPORTS_DIR, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)
    
    print(f"\n📁 Directorios preparados:")
    print(f"   • {REPORTS_DIR}")
    print(f"   • {TEMP_DIR}")
    
    # 1. EXPORTAR TABLA PRINCIPAL
    print("\n" + "-" * 40)
    print("📋 TABLA PRINCIPAL: DIABETES_ANALYTICS")
    print("-" * 40)
    
    df_principal = spark.table(f"{database}.{tabla_principal}")
    exportar_tabla(df_principal, "diabetes_analytics")
    
    # 2. EXPORTAR TABLA DE MÉTRICAS
    print("\n" + "-" * 40)
    print("📊 TABLA DE MÉTRICAS: metricas_por_riesgo")
    print("-" * 40)
    
    try:
        df_metricas = spark.table(f"{database}.{tabla_metricas}")
        exportar_tabla(df_metricas, "metricas_por_riesgo")
    except Exception as e:
        print(f"⚠️ La tabla de métricas no existe o no se pudo leer: {str(e)}")
    
    # 3. RESUMEN FINAL
    print("\n" + "=" * 80)
    print("🎉 EXPORTACIÓN COMPLETADA EXITOSAMENTE!")
    print("=" * 80)
    print(f"📁 Archivos generados en: {TEMP_DIR}")
    
    print("\n📋 Estructura de archivos:")
    subprocess.run(["find", TEMP_DIR, "-type", "f", "-name", "*.csv", "-exec", "ls", "-lh", "{}", ";"])
    
    print("\n📊 Estadísticas:")
    print(f"   • diabetes_analytics/ : {df_principal.count()} registros")
    if 'df_metricas' in locals():
        print(f"   • metricas_por_riesgo/ : {df_metricas.count()} registros")
    
except Exception as e:
    print(f"\n❌ Error durante la exportación: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
    
finally:
    spark.stop()
    print("\n🛑 Sesión Spark cerrada")
    print("=" * 80)