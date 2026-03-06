#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para exportar capa Functional a CSV (Gold) - Detección de Diabetes
Proyecto: topicos-deteccion-diabetes
"""

import sys

from pyspark.sql import SparkSession
import os
import subprocess
import glob
import shutil
from datetime import datetime

# Crear sesión Spark con soporte Hive
spark = SparkSession.builder \
    .appName("Diabetes_Gold_to_CSV") \
    .enableHiveSupport() \
    .getOrCreate()

# Configuración
database = "topicosunc_functional"
tabla_principal = "DIABETES_ANALYTICS"
tabla_metricas = "metricas_por_riesgo"

# Directorios actualizados
BASE_DIR = "/home/hadoop/topicos-deteccion-diabetes"
REPORTS_DIR = f"{BASE_DIR}/reports"
TEMP_DIR = f"{REPORTS_DIR}/temp"  # ← NUEVA RUTA: reports/temp

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
    """Función auxiliar para exportar una tabla a CSV"""
    print(f"\n📥 Exportando tabla: {nombre_tabla}")
    
    count = df.count()
    print(f"   📊 Registros: {count}")
    
    # Ruta temporal dentro de reports/temp
    temp_path = f"file:{TEMP_DIR}/temp_{nombre_tabla.lower()}"
    
    # Exportar a CSV
    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("header", "true") \
      .option("sep", ",") \
      .csv(temp_path)
    
    # Buscar y renombrar el archivo
    local_temp = f"{TEMP_DIR}/temp_{nombre_tabla.lower()}"
    csv_files = glob.glob(f"{local_temp}/part-*.csv")
    
    if csv_files:
        dest_file = f"{REPORTS_DIR}/{nombre_tabla.lower()}.csv"
        shutil.copy2(csv_files[0], dest_file)
        print(f"   ✅ Archivo guardado: {dest_file}")
        
        # Mostrar primeras líneas
        print(f"   🔍 Primeros 2 registros:")
        result = subprocess.run(["head", "-2", dest_file], capture_output=True, text=True)
        for line in result.stdout.split('\n')[:2]:
            if line:
                print(f"      {line[:100]}...")
        
        return dest_file
    else:
        print(f"   ⚠️ No se encontró archivo para {nombre_tabla}")
        return None

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
    archivo_principal = exportar_tabla(df_principal, "diabetes_analytics")
    
    # 2. EXPORTAR TABLA DE MÉTRICAS
    print("\n" + "-" * 40)
    print("📊 TABLA DE MÉTRICAS: metricas_por_riesgo")
    print("-" * 40)
    
    try:
        df_metricas = spark.table(f"{database}.{tabla_metricas}")
        archivo_metricas = exportar_tabla(df_metricas, "metricas_por_riesgo")
    except Exception as e:
        print(f"⚠️ La tabla de métricas no existe o no se pudo leer: {str(e)}")
        archivo_metricas = None
    
    # 3. CREAR ARCHIVO CONSOLIDADO
    print("\n" + "-" * 40)
    print("📦 CREANDO ARCHIVO CONSOLIDADO")
    print("-" * 40)
    
    # Leer ambos CSVs y crear un resumen
    if archivo_principal and archivo_metricas:
        df_resumen = spark.createDataFrame([
            ("Tabla Principal", "diabetes_analytics", df_principal.count(), archivo_principal),
            ("Tabla Métricas", "metricas_por_riesgo", df_metricas.count() if 'df_metricas' in locals() else 0, archivo_metricas)
        ], ["tipo", "nombre", "registros", "archivo"])
        
        resumen_path = f"{REPORTS_DIR}/resumen_exportacion.csv"
        df_resumen.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"file:{TEMP_DIR}/resumen")
        
        csv_resumen = glob.glob(f"{TEMP_DIR}/resumen/part-*.csv")
        if csv_resumen:
            shutil.copy2(csv_resumen[0], resumen_path)
            print(f"✅ Resumen guardado: {resumen_path}")
    
    # 4. LIMPIAR ARCHIVOS TEMPORALES
    print("\n" + "-" * 40)
    print("🧹 LIMPIANDO ARCHIVOS TEMPORALES")
    print("-" * 40)
    
    # Eliminar directorios temporales (opcional)
    # shutil.rmtree(TEMP_DIR, ignore_errors=True)
    # print(f"✅ Directorio temporal eliminado: {TEMP_DIR}")
    
    # 5. RESUMEN FINAL
    print("\n" + "=" * 80)
    print("🎉 EXPORTACIÓN COMPLETADA EXITOSAMENTE!")
    print("=" * 80)
    print(f"📁 Archivos generados en: {REPORTS_DIR}")
    print("\n📋 Lista de archivos:")
    subprocess.run(["ls", "-lh", REPORTS_DIR])
    
    print("\n📊 Estadísticas finales:")
    print(f"   • diabetes_analytics.csv: {df_principal.count()} registros")
    if 'df_metricas' in locals():
        print(f"   • metricas_por_riesgo.csv: {df_metricas.count()} registros")
    print(f"   • resumen_exportacion.csv: 2 registros")
    
except Exception as e:
    print(f"\n❌ Error durante la exportación: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
    
finally:
    spark.stop()
    print("\n🛑 Sesión Spark cerrada")
    print("=" * 80)