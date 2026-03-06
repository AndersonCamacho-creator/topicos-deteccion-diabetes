#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para exportar datos Gold a MongoDB - Detección de Diabetes
Proyecto: topicos-deteccion-diabetes
"""

from pyspark.sql import SparkSession
import os
import glob
import subprocess

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

MONGO_IP = "172.28.112.1"
MONGO_PORT = "27017"
MONGO_DATABASE = "medallondiabetes"
MONGO_COLLECTION_PRINCIPAL = "gold_diabetes_analytics"
MONGO_COLLECTION_METRICAS = "gold_metricas_riesgo"

BASE_DIR = "/home/hadoop/topicos-deteccion-diabetes"
REPORTS_DIR = f"{BASE_DIR}/reports"
TEMP_DIR = f"{REPORTS_DIR}/temp"

print("=" * 80)
print("🔍 CONFIGURACIÓN DE CONEXIÓN MONGODB")
print("=" * 80)
print(f"🌐 Conectando a MongoDB en: {MONGO_IP}:{MONGO_PORT}")
print(f"📦 Base de datos: {MONGO_DATABASE}")
print(f"📋 Colección principal: {MONGO_COLLECTION_PRINCIPAL}")
print(f"📋 Colección métricas: {MONGO_COLLECTION_METRICAS}")
print(f"📁 Directorio reports: {REPORTS_DIR}")
print(f"📁 Directorio temporal: {TEMP_DIR}")

# =============================================================================
# VERIFICAR CONECTIVIDAD CON MONGODB
# =============================================================================

def verificar_conexion_mongodb():
    """Verifica que MongoDB sea accesible desde WSL"""
    print("\n🔍 Verificando conectividad con MongoDB...")
    
    result = subprocess.run(
        ["nc", "-zv", MONGO_IP, MONGO_PORT], 
        capture_output=True, 
        text=True,
        timeout=5
    )
    
    if result.returncode == 0:
        print(f"   ✅ MongoDB es accesible en {MONGO_IP}:{MONGO_PORT}")
        return True
    else:
        print(f"   ❌ No se puede conectar a MongoDB en {MONGO_IP}:{MONGO_PORT}")
        print(f"   📝 Verifica que el firewall de Windows permita el puerto 27017")
        print(f"   📝 Ejecuta en PowerShell como ADMIN: New-NetFirewallRule -DisplayName 'MongoDB' -Direction Inbound -LocalPort 27017 -Protocol TCP -Action Allow")
        return False

# =============================================================================
# FUNCIÓN PARA EXPORTAR A MONGODB
# =============================================================================

def exportar_csv_a_mongodb(spark, csv_path, nombre_tabla, collection_name):
    """
    Exporta un archivo CSV a una colección de MongoDB
    """
    print(f"\n{'=' * 60}")
    print(f"📤 EXPORTANDO: {nombre_tabla}")
    print(f"{'=' * 60}")
    print(f"📁 Archivo: {csv_path}")
    print(f"🎯 Colección: {collection_name}")
    
    try:
        
        print("\n📖 Leyendo archivo CSV...")
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "PERMISSIVE") \
            .option("samplingRatio", "1.0") \
            .csv(csv_path)
        
        count = df.count()
        print(f"\n📊 Total de registros: {count}")
        
        if count == 0:
            print(f"   ⚠️ El archivo está vacío")
            return False
        
        print(f"\n🔍 Primeros 3 registros:")
        df.show(3, truncate=False)
        
        # Mostrar el conteo total 
        print(f"\n📊 Esperado: 768 registros | Actual: {count} registros")
        
        if count < 768:
            print(f"   ⚠️ Solo se cargaron {count} de 768 registros")
            print(f"   📝 Verificando si hay múltiples archivos parte...")
        
        # Escribir en MongoDB
        print(f"\n💾 Escribiendo en MongoDB...")
        print(f"   • URI: mongodb://{MONGO_IP}:{MONGO_PORT}")
        print(f"   • Base de datos: {MONGO_DATABASE}")
        print(f"   • Colección: {collection_name}")
        
        df.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("spark.mongodb.connection.uri", f"mongodb://{MONGO_IP}:{MONGO_PORT}") \
            .option("spark.mongodb.database", MONGO_DATABASE) \
            .option("spark.mongodb.collection", collection_name) \
            .save()
        
        print(f"   ✅ {nombre_tabla} exportada exitosamente!")
        
        # Verificar
        df_check = spark.read \
            .format("mongodb") \
            .option("spark.mongodb.connection.uri", f"mongodb://{MONGO_IP}:{MONGO_PORT}") \
            .option("spark.mongodb.database", MONGO_DATABASE) \
            .option("spark.mongodb.collection", collection_name) \
            .load()
        
        check_count = df_check.count()
        print(f"\n🔍 Verificación en MongoDB: {check_count} registros")
        
        if check_count == count:
            print(f"   ✅ Coincide con el CSV original")
        else:
            print(f"   ⚠️ Diferencia: CSV={count}, MongoDB={check_count}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error exportando {nombre_tabla}: {str(e)}")
        return False
    
    
# =============================================================================
# PROGRAMA PRINCIPAL
# =============================================================================

def main():
    # Crear sesión Spark
    spark = SparkSession.builder \
        .appName("Diabetes_Gold_to_MongoDB") \
        .config("spark.mongodb.connection.uri", f"mongodb://{MONGO_IP}:{MONGO_PORT}") \
        .config("spark.mongodb.database", MONGO_DATABASE) \
        .getOrCreate()
    
    try:
        # 1. Verificar conectividad con MongoDB
        if not verificar_conexion_mongodb():
            print("\n❌ No se puede continuar sin conexión a MongoDB")
            return
        
        # 2. Buscar archivos CSV
        print("\n" + "=" * 80)
        print("🔍 BUSCANDO ARCHIVOS CSV")
        print("=" * 80)
        print(f"📁 Directorio: {TEMP_DIR}")
        
        csv_files = glob.glob(f"{TEMP_DIR}/**/part-*.csv", recursive=True)
        print(f"📊 Total archivos encontrados: {len(csv_files)}")
        
        # Mostrar todos los archivos encontrados
        for i, file in enumerate(csv_files):
            size = os.path.getsize(file)
            print(f"   {i+1}. {os.path.basename(file)} - {size} bytes - {os.path.dirname(file)}")
        
        if not csv_files:
            print("❌ No se encontraron archivos CSV")
            return
        
        # 3. Identificar archivos por tipo
        archivo_principal = None
        archivo_metricas = None
        
        for file in csv_files:
            if "diabetes_analytics" in file.lower():
                archivo_principal = f"file:{file}"
                print(f"\n✅ Encontrado archivo principal: {file}")
            elif "metricas_por_riesgo" in file.lower():
                archivo_metricas = f"file:{file}"
                print(f"✅ Encontrado archivo de métricas: {file}")
        
        # 4. Exportar archivo principal 
        if archivo_principal:
            exportar_csv_a_mongodb(
                spark, 
                archivo_principal, 
                "DIABETES ANALYTICS", 
                MONGO_COLLECTION_PRINCIPAL
            )
        else:
            print("\n❌ No se encontró el archivo principal (diabetes_analytics)")
            print("   Ejecuta primero export_gold_to_csv.py")
        
        # 5. Exportar archivo de métricas
        if archivo_metricas:
            exportar_csv_a_mongodb(
                spark, 
                archivo_metricas, 
                "MÉTRICAS POR RIESGO", 
                MONGO_COLLECTION_METRICAS
            )
        
        # 6. Resumen final
        print("\n" + "=" * 80)
        print("🎉 PROCESO COMPLETADO")
        print("=" * 80)
        print(f"📊 Base de datos MongoDB: {MONGO_DATABASE}")
        print(f"   • Colección: {MONGO_COLLECTION_PRINCIPAL}")
        print(f"   • Colección: {MONGO_COLLECTION_METRICAS}")
        print("\n📝 Para verificar en MongoDB Compass:")
        print(f"   mongodb://{MONGO_IP}:{MONGO_PORT}")
        print(f"   Base de datos: {MONGO_DATABASE}")
        
    except Exception as e:
        print(f"\n❌ Error general: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()
        print("\n🛑 Sesión Spark cerrada")
        print("=" * 80)

if __name__ == "__main__":
    main()