#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark para despliegue de capa Curated (Parquet) - Detección de Diabetes
Proyecto: topicos-deteccion-diabetes
Entorno: TopicosUNC
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, when, avg, round

# =============================================================================
# @section 1. Configuración de parámetros
# =============================================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description='Proceso de carga - Capa Curated Diabetes')
    parser.add_argument('--env', type=str, default='TopicosUNC', help='Entorno: TopicosUNC, etc.')
    parser.add_argument('--username', type=str, default='hadoop', help='Usuario HDFS')
    parser.add_argument('--base_path', type=str, default='/user', help='Ruta base en HDFS')
    parser.add_argument('--source_db', type=str, default='landing', help='Base de datos origen')
    parser.add_argument('--enable-validation', action='store_true', default=True, help='Activar validaciones')
    return parser.parse_args()

# =============================================================================
# @section 2. Inicialización de SparkSession
# =============================================================================

def create_spark_session(app_name="ProcesoCurated-Diabetes-TopicosUNC"):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

# =============================================================================
# @section 3. Funciones auxiliares
# =============================================================================

def crear_database(spark, env, username, base_path):
    """Crea la base de datos Curated si no existe"""
    db_name = f"{env}_curated".lower()
    db_location = f"{base_path}/{username}/datalake/{db_name.upper()}"
    
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_location}'")
    print(f"✅ Database '{db_name}' creada en: {db_location}")
    return db_name

def imputar_valores_nulos(df):
    """
    Limpieza de datos: Reemplazar valores 0 por la media del grupo (Outcome)
    En el dataset original, los valores 0 en ciertas columnas indican datos faltantes
    """
    print("   🔧 Aplicando limpieza de datos...")
    
    # Convertir columnas a tipo numérico para operaciones
    df_numeric = df.select(
        col("pregnancies").cast(IntegerType()).alias("pregnancies"),
        col("glucose").cast(IntegerType()).alias("glucose"),
        col("bloodpressure").cast(IntegerType()).alias("bloodpressure"),
        col("skinthickness").cast(IntegerType()).alias("skinthickness"),
        col("insulin").cast(IntegerType()).alias("insulin"),
        col("bmi").cast(DoubleType()).alias("bmi"),
        col("diabetespedigreefunction").cast(DoubleType()).alias("diabetespedigreefunction"),
        col("age").cast(IntegerType()).alias("age"),
        col("outcome").cast(IntegerType()).alias("outcome")
    )
    
    # Calcular medias por outcome para columnas críticas
    print("   📊 Calculando medias por grupo (Outcome)...")
    
    # Columnas que necesitan imputación (valores 0 son inválidos)
    columnas_a_imputar = ["glucose", "bloodpressure", "skinthickness", "insulin", "bmi"]
    
    # Crear un DataFrame con las medias por outcome
    medias_por_outcome = df_numeric.groupBy("outcome").agg(
        *[round(avg(when(col(c) != 0, col(c))), 1).alias(f"media_{c}") for c in columnas_a_imputar]
    )
    
    print("   📊 Medias calculadas:")
    medias_por_outcome.show()
    
    # Hacer join para tener las medias disponibles
    df_con_medias = df_numeric.join(medias_por_outcome, on="outcome", how="left")
    
    # Imputar valores 0 con las medias correspondientes
    for c in columnas_a_imputar:
        df_con_medias = df_con_medias.withColumn(
            c,
            when(col(c) == 0, col(f"media_{c}")).otherwise(col(c))
        )
    
    # Eliminar columnas de medias temporales
    columnas_media = [f"media_{c}" for c in columnas_a_imputar]
    df_limpio = df_con_medias.drop(*columnas_media)
    
    # Verificar que no quedan valores 0 en las columnas críticas
    print("   ✅ Verificación después de imputación:")
    for c in columnas_a_imputar:
        count_zeros = df_limpio.filter(col(c) == 0).count()
        print(f"      - {c}: {count_zeros} valores cero")
    
    return df_limpio

def aplicar_reglas_calidad(df, enable_validation=True):
    """
    Aplica reglas de calidad de datos adicionales
    """
    if enable_validation:
        print("   🔍 Aplicando validaciones de calidad...")
        
        # Validar rangos lógicos para variables médicas
        reglas_validacion = [
            (col("glucose").between(50, 200), "glucose fuera de rango (50-200)"),
            (col("bloodpressure").between(40, 130), "bloodpressure fuera de rango (40-130)"),
            (col("bmi").between(15, 70), "bmi fuera de rango (15-70)"),
            (col("age").between(18, 100), "age fuera de rango (18-100)"),
            (col("outcome").isin(0, 1), "outcome debe ser 0 o 1")
        ]
        
        for condicion, mensaje in reglas_validacion:
            registros_invalidos = df.filter(~condicion).count()
            if registros_invalidos > 0:
                print(f"   ⚠️  {mensaje}: {registros_invalidos} registros")
    
    return df

def insertar_datos_parquet(spark, db_name, table_name, df_transformed):
    """
    Inserta datos en tabla Parquet (particionada por outcome)
    """
    table_full_name = f"{db_name}.{table_name}"
    
    print(f"🚀 Insertando datos en {table_full_name}...")
    
    # Escribir en formato Parquet particionado por outcome
    df_transformed.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("outcome") \
        .saveAsTable(table_full_name)
    
    # Refrescar metadatos
    spark.sql(f"MSCK REPAIR TABLE {table_full_name}")
    
    print(f"✅ Datos insertados en '{table_full_name}'")

# =============================================================================
# @section 4. Configuración de tablas
# =============================================================================

TABLAS_CONFIG = [
    {
        "nombre": "DIABETES",
        "partitioned_by": ["outcome"]
    }
]

# =============================================================================
# @section 5. Proceso principal
# =============================================================================

def main():
    args = parse_arguments()
    spark = create_spark_session()
    
    try:
        env_lower = args.env.lower()
        db_curated = f"{env_lower}_curated"
        db_source = f"{env_lower}_{args.source_db}"
        
        print(f"🌍 Entorno: {args.env}")
        print(f"🗄️  Base origen: {db_source}")
        print(f"🎯 Base destino: {db_curated}")
        
        # 1. Crear database Curated
        crear_database(spark, env_lower, args.username, args.base_path)
        
        # 2. Procesar cada tabla
        for config in TABLAS_CONFIG:
            table_name = config["nombre"]
            print(f"\n📥 Procesando tabla Curated: {table_name}")
            
            # Leer datos desde Landing (formato AVRO)
            print(f"   📖 Leyendo datos desde {db_source}.{table_name}")
            df_source = spark.table(f"{db_source}.{table_name}")
            
            # Mostrar información del origen
            count_origen = df_source.count()
            print(f"   📊 Registros en origen: {count_origen}")
            print(f"   📋 Esquema origen:")
            df_source.printSchema()
            
            # Mostrar valores nulos/cero antes de limpieza
            print("   📊 Valores cero antes de limpieza:")
            columnas_criticas = ["glucose", "bloodpressure", "skinthickness", "insulin", "bmi"]
            for c in columnas_criticas:
                count_zeros = df_source.filter(col(c) == "0").count()
                print(f"      - {c}: {count_zeros} valores cero")
            
            # 3. Aplicar transformaciones (imputación de valores nulos)
            df_limpio = imputar_valores_nulos(df_source)
            
            # 4. Aplicar reglas de calidad
            df_final = aplicar_reglas_calidad(df_limpio, args.enable_validation)
            
            # 5. Insertar en Curated (formato Parquet)
            insertar_datos_parquet(
                spark, 
                db_curated, 
                table_name, 
                df_final
            )
            
            # 6. Validación final
            print(f"   🔍 Validando carga en {db_curated}.{table_name}:")
            spark.sql(f"SELECT * FROM {db_curated}.{table_name} LIMIT 5").show(truncate=False)
            
            # 7. Estadísticas por outcome después de limpieza
            print(f"   📊 Estadísticas por Outcome (después de limpieza):")
            spark.sql(f"""
                SELECT outcome, 
                       COUNT(*) as cantidad,
                       ROUND(AVG(glucose), 1) as glucose_prom,
                       ROUND(AVG(bmi), 1) as bmi_prom,
                       ROUND(AVG(age), 1) as edad_prom
                FROM {db_curated}.{table_name} 
                GROUP BY outcome 
                ORDER BY outcome
            """).show()
            
            # 8. Mostrar particiones
            print(f"   📂 Particiones disponibles:")
            spark.sql(f"SHOW PARTITIONS {db_curated}.{table_name}").show(truncate=False)
        
        print("\n🎉 Proceso Curated completado exitosamente!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()