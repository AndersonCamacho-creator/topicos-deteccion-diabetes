#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark para despliegue de capa Workload - Detección de Diabetes
Proyecto: topicos-deteccion-diabetes
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# =============================================================================
# @section 1. Configuración de parámetros
# =============================================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description='Proceso de carga - Capa Workload Diabetes')
    parser.add_argument('--env', type=str, default='TopicosA', help='Entorno: DEV, QA, PROD')
    parser.add_argument('--username', type=str, default='hadoop', help='Usuario HDFS')
    parser.add_argument('--base_path', type=str, default='/user', help='Ruta base en HDFS')
    parser.add_argument('--local_data_path', type=str, 
                       default='/user/hadoop/dataset', 
                       help='Ruta de datos en HDFS (no local)')
    return parser.parse_args()

# =============================================================================
# @section 2. Inicialización de SparkSession con soporte Hive
# =============================================================================

def create_spark_session(app_name="Proceso_Carga_Workload_Diabetes"):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false") \
        .config("spark.sql.legacy.charVarcharCodegen", "true") \
        .getOrCreate()

# =============================================================================
# @section 3. Funciones auxiliares
# =============================================================================

def crear_database(spark, env, username, base_path):
    """Crea la base de datos si no existe"""
    db_name = f"{env}_workload".lower()  # Convertir a minúsculas
    db_location = f"{base_path}/{username}/datalake/{db_name.upper()}"
    
    # Eliminar si existe y crear nueva
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_location}'")
    print(f"✅ Database '{db_name}' creada en: {db_location}")
    return db_name

def crear_tabla_external(spark, db_name, table_name, df, location, spark_schema):
    """
    Crea tabla externa en Hive con formato TEXTFILE y propiedades específicas
    """
    # Registrar DataFrame como vista temporal
    df.createOrReplaceTempView(f"tmp_{table_name}")
    
    # Crear tabla externa usando SQL
    columnas_sql = ', '.join([f'{field.name} STRING' for field in spark_schema.fields])
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
        {columnas_sql}
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\\n'
    STORED AS TEXTFILE
    LOCATION '{location}'
    TBLPROPERTIES(
        'skip.header.line.count'='1'
    )
    """
    spark.sql(create_table_sql)
    
    # Insertar datos desde la vista temporal
    spark.sql(f"""
        INSERT OVERWRITE TABLE {db_name}.{table_name}
        SELECT * FROM tmp_{table_name}
    """)
    
    print(f"✅ Tabla '{db_name}.{table_name}' desplegada en: {location}")

# =============================================================================
# @section 4. Definición de esquema para DIABETES
# =============================================================================

SCHEMA_DIABETES = StructType([
    StructField("Pregnancies", StringType(), True),
    StructField("Glucose", StringType(), True),
    StructField("BloodPressure", StringType(), True),
    StructField("SkinThickness", StringType(), True),
    StructField("Insulin", StringType(), True),
    StructField("BMI", StringType(), True),
    StructField("DiabetesPedigreeFunction", StringType(), True),
    StructField("Age", StringType(), True),
    StructField("Outcome", StringType(), True)
])

# =============================================================================
# @section 5. Proceso principal de carga
# =============================================================================

def main():
    args = parse_arguments()
    spark = create_spark_session()
    
    try:
        # 1. Crear database
        db_name = crear_database(spark, args.env, args.username, args.base_path)
        
        # 2. Configuración de la tabla
        table_name = "DIABETES"
        archivo_datos = "diabetes.data"  # Tu archivo con separador pipe
        esquema = SCHEMA_DIABETES
        
        # 3. Definir rutas
        # Importante: local_data_path es la ruta en HDFS donde están los datos
        ruta_hdfs_datos = f"{args.local_data_path}/{archivo_datos}"
        ruta_table_hive = f"{args.base_path}/{args.username}/datalake/{db_name.upper()}/{table_name.lower()}"
        
        print(f"📥 Procesando tabla: {table_name}")
        print(f"   📂 Fuente HDFS: {ruta_hdfs_datos}")
        print(f"   🎯 Destino Hive: {ruta_table_hive}")
        
        # 4. Leer datos desde HDFS con esquema explícito
        df = spark.read.csv(
            ruta_hdfs_datos,
            schema=esquema,
            sep='|',
            header=True,
            nullValue='\\N',
            emptyValue=''
        )
        
        # 5. Verificar que se leyó correctamente
        print(f"📊 Registros leídos: {df.count()}")
        print("🔍 Primeros 5 registros:")
        df.show(5, truncate=False)
        
        # 6. Crear tabla externa y cargar datos
        crear_tabla_external(spark, db_name, table_name, df, ruta_table_hive, esquema)
        
        # 7. Validación final
        print(f"🔍 Validando carga en {db_name}.{table_name}:")
        spark.sql(f"SELECT * FROM {db_name}.{table_name} LIMIT 5").show(truncate=False)
        
        # 8. Contar registros totales
        count = spark.sql(f"SELECT COUNT(*) FROM {db_name}.{table_name}").collect()[0][0]
        print(f"📊 Total registros en tabla: {count}")
        
        print("\n🎉 Proceso Workload completado exitosamente!")
        
    except Exception as e:
        print(f"❌ Error en el proceso: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()