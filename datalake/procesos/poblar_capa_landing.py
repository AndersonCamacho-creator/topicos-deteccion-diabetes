#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark para despliegue de capa Landing (AVRO) - Detección de Diabetes
Proyecto: topicos-deteccion-diabetes
Entorno: TopicosUNC
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# =============================================================================
# @section 1. Configuración de parámetros
# =============================================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description='Proceso de carga - Capa Landing Diabetes')
    parser.add_argument('--env', type=str, default='TopicosUNC', help='Entorno: TopicosUNC, etc.')
    parser.add_argument('--username', type=str, default='hadoop', help='Usuario HDFS')
    parser.add_argument('--base_path', type=str, default='/user', help='Ruta base en HDFS')
    parser.add_argument('--schema_path', type=str, 
                       default='/user/hadoop/datalake/schema', 
                       help='Ruta de esquemas AVRO en HDFS')
    parser.add_argument('--source_db', type=str, default='workload', 
                       help='Base de datos origen (workload)')
    return parser.parse_args()

# =============================================================================
# @section 2. Inicialización de SparkSession
# =============================================================================

def create_spark_session(app_name="Proceso_Carga_Landing_Diabetes"):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.avro.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

# =============================================================================
# @section 3. Funciones auxiliares
# =============================================================================

def crear_database(spark, env, username, base_path):
    """Crea la base de datos Landing si no existe"""
    db_name = f"{env}_landing".lower()
    db_location = f"{base_path}/{username}/datalake/{db_name.upper()}"
    
    # Eliminar si existe y crear nueva
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_location}'")
    print(f"✅ Database '{db_name}' creada en: {db_location}")
    return db_name

def crear_tabla_avro_hive(spark, db_name, table_name, location, schema_avsc_url):
    """
    Crea tabla AVRO en Hive usando el esquema definido
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
    STORED AS AVRO
    LOCATION '{location}'
    TBLPROPERTIES (
        'avro.schema.url'='{schema_avsc_url}',
        'avro.output.codec'='snappy'
    )
    """
    spark.sql(create_sql)
    print(f"✅ Tabla AVRO '{db_name}.{table_name}' registrada en Hive Metastore")

def insertar_datos_avro(spark, db_name, table_name, df_source, partition_col=None):
    """
    Inserta datos en tabla AVRO desde DataFrame origen
    """
    table_full_name = f"{db_name}.{table_name}"
    
    print(f"🚀 Iniciando inserción en {table_full_name}...")
    print(f"   📊 Registros a insertar: {df_source.count()}")
    print(f"   📋 Columnas: {df_source.columns}")
    
    # Asegurar nombres de columnas en minúscula (recomendación Avro)
    df_source = df_source.toDF(*[c.lower() for c in df_source.columns])
    
    # Escribir datos en formato AVRO con partición si existe
    if partition_col:
        print(f"   📂 Particionando por: {partition_col}")
        df_source.write \
            .format("avro") \
            .mode("overwrite") \
            .partitionBy(partition_col) \
            .saveAsTable(table_full_name)
        
        # Refrescar metadatos solo si es tabla particionada
        try:
            spark.sql(f"MSCK REPAIR TABLE {table_full_name}")
            print(f"   ✅ Metadatos refrescados para tabla particionada")
        except Exception as e:
            print(f"   ⚠️ No se pudo refrescar particiones: {str(e)}")
    else:
        print(f"   📂 Sin particiones")
        df_source.write \
            .format("avro") \
            .mode("overwrite") \
            .saveAsTable(table_full_name)
    
    print(f"✅ Datos insertados en '{table_full_name}'")
    
# =============================================================================
# @section 4. Configuración de tablas
# =============================================================================

TABLAS_CONFIG = [
    {
        "nombre": "DIABETES",
        "archivo_avsc": "diabetes.avsc",
        # Particionar por Outcome (0/1) para mejorar consultas
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
        db_landing = f"{env_lower}_landing"
        db_source = f"{env_lower}_workload"
        
        print(f"🌍 Entorno: {args.env}")
        print(f"🗄️  Base origen: {db_source}")
        print(f"🎯 Base destino: {db_landing}")
        
        # 1. Crear database Landing
        crear_database(spark, env_lower, args.username, args.base_path)
        
        # 2. Procesar cada tabla
        for config in TABLAS_CONFIG:
            table_name = config["nombre"]
            print(f"\n📥 Procesando tabla Landing: {table_name}")
            
            # Definir ubicación física de la tabla
            location = f"{args.base_path}/{args.username}/datalake/{db_landing.upper()}/{table_name.lower()}"
            
            # URL del esquema AVRO en HDFS
            schema_url = f"hdfs://localhost:9000{args.schema_path}/{db_landing.upper()}/{config['archivo_avsc']}"
            
            print(f"   📍 Ubicación HDFS: {location}")
            print(f"   📋 Esquema AVRO: {schema_url}")
            
            # A. Crear estructura AVRO en Hive
            crear_tabla_avro_hive(
                spark, 
                db_landing, 
                table_name, 
                location, 
                schema_url
            )
            
            # B. Leer datos desde Workload
            print(f"   📖 Leyendo datos desde {db_source}.{table_name}")
            df_source = spark.table(f"{db_source}.{table_name}")
            
            # Mostrar conteo de registros origen
            count_origen = df_source.count()
            print(f"   📊 Registros en origen: {count_origen}")
            
            # C. Insertar datos en formato AVRO
            print(f"   💾 Insertando datos en formato AVRO...")
            insertar_datos_avro(
                spark, 
                db_landing, 
                table_name, 
                df_source,
                config["partitioned_by"][0] if config["partitioned_by"] else None
            )
                        
            # D. Validación
            print(f"   🔍 Validando carga en {db_landing}.{table_name}:")
            spark.sql(f"SELECT * FROM {db_landing}.{table_name} LIMIT 5").show(truncate=False)
            
            # E. Mostrar estadísticas por partición
            print(f"   📊 Estadísticas por Outcome:")
            spark.sql(f"""
                SELECT outcome, COUNT(*) as cantidad 
                FROM {db_landing}.{table_name} 
                GROUP BY outcome 
                ORDER BY outcome
            """).show()
            
            # F. Mostrar particiones
            print(f"   📂 Particiones disponibles:")
            spark.sql(f"SHOW PARTITIONS {db_landing}.{table_name}").show(truncate=False)
        
        print("\n🎉 Proceso de Landing completado exitosamente!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()