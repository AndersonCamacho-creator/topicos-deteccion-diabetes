#!/usr/bin/env python3
# -*- coding: utf-8 -*- 
"""
Script PySpark para despliegue de capa Functional (Parquet) - Detección de Diabetes
Proyecto: topicos-deteccion-diabetes
Entorno: TopicosUNC
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, when, lit, round, concat, udf, avg, count
from pyspark.sql.functions import year, month, dayofmonth, datediff, current_date

# =============================================================================
# @section 1. Configuración de parámetros
# =============================================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description='Proceso de carga - Capa Functional Diabetes')
    parser.add_argument('--env', type=str, default='TopicosUNC', help='Entorno: TopicosUNC, etc.')
    parser.add_argument('--username', type=str, default='hadoop', help='Usuario HDFS')
    parser.add_argument('--base_path', type=str, default='/user', help='Ruta base en HDFS')
    parser.add_argument('--source_db', type=str, default='curated', help='Base de datos origen')
    return parser.parse_args()

# =============================================================================
# @section 2. Inicialización de SparkSession
# =============================================================================

def create_spark_session(app_name="Proceso_Carga_Functional_Diabetes"):
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
    """Crea la base de datos Functional si no existe"""
    db_name = f"{env}_functional".lower()
    db_location = f"{base_path}/{username}/datalake/{db_name.upper()}"
    
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_location}'")
    print(f"✅ Database '{db_name}' creada en: {db_location}")
    return db_name

def crear_tabla_functional(spark, db_name, table_name, schema, location):
    """
    Crea tabla en Hive con formato Parquet usando el esquema definido
    """
    # Construir definición de columnas (EXCLUYENDO risk_level porque es partición)
    columns_def = []
    for field in schema.fields:
        if field.name != "risk_level":  # ← ¡EXCLUIR risk_level de las columnas!
            tipo = field.dataType.simpleString().upper()
            columns_def.append(f"{field.name} {tipo}")
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
        {', '.join(columns_def)}
    )
    PARTITIONED BY (risk_level STRING)
    STORED AS PARQUET
    LOCATION '{location}'
    TBLPROPERTIES (
        'parquet.compression'='SNAPPY'
    )
    """
    spark.sql(create_sql)
    print(f"✅ Tabla Functional '{db_name}.{table_name}' registrada en Hive Metastore")
    
def enriquecer_datos_diabetes(df):
    """
    Enriquecimiento de datos: Crear nuevas columnas para análisis
    """
    print("   🔧 Aplicando enriquecimiento de datos...")
    
    df_enriched = df \
        .withColumn("pregnancies_cat", 
                    when(col("pregnancies") == 0, "0")
                    .when(col("pregnancies").between(1, 3), "1-3")
                    .when(col("pregnancies").between(4, 6), "4-6")
                    .otherwise("7+")) \
        .withColumn("glucose_level",
                    when(col("glucose") < 100, "Normal")
                    .when(col("glucose").between(100, 125), "Prediabetes")
                    .when(col("glucose") > 125, "Diabetes")
                    .otherwise("Desconocido")) \
        .withColumn("bmi_category",
                    when(col("bmi") < 18.5, "Bajo peso")
                    .when(col("bmi").between(18.5, 24.9), "Normal")
                    .when(col("bmi").between(25, 29.9), "Sobrepeso")
                    .when(col("bmi") >= 30, "Obesidad")
                    .otherwise("Desconocido")) \
        .withColumn("age_group",
                    when(col("age") < 30, "Joven (18-29)")
                    .when(col("age").between(30, 44), "Adulto joven (30-44)")
                    .when(col("age").between(45, 59), "Adulto (45-59)")
                    .when(col("age") >= 60, "Adulto mayor (60+)")
                    .otherwise("Desconocido")) \
        .withColumn("pressure_level",
                    when(col("bloodpressure") < 80, "Normal")
                    .when(col("bloodpressure").between(80, 89), "Prehipertensión")
                    .when(col("bloodpressure") >= 90, "Hipertensión")
                    .otherwise("Desconocido")) \
        .withColumn("risk_score",
                    round(
                        when(col("glucose") > 125, lit(3)).otherwise(lit(0)) +
                        when(col("bmi") >= 30, lit(3)).otherwise(lit(0)) +
                        when(col("age") >= 45, lit(2)).otherwise(lit(0)) +
                        when(col("bloodpressure") >= 90, lit(2)).otherwise(lit(0)) +
                        when(col("pregnancies") >= 4, lit(1)).otherwise(lit(0))
                    , 0)) \
        .withColumn("risk_level",
                    when(col("risk_score") >= 8, "ALTO")
                    .when(col("risk_score").between(4, 7), "MEDIO")
                    .when(col("risk_score").between(1, 3), "BAJO")
                    .when(col("risk_score") == 0, "MUY BAJO")
                    .otherwise("DESCONOCIDO")) \
        .withColumn("dpf_level",
                    when(col("diabetespedigreefunction") < 0.3, "Bajo")
                    .when(col("diabetespedigreefunction").between(0.3, 0.7), "Medio")
                    .when(col("diabetespedigreefunction") > 0.7, "Alto")
                    .otherwise("Desconocido")) \
        .withColumn("insulin_sensitivity",
                    when(col("insulin") < 100, "Sensible")
                    .when(col("insulin").between(100, 300), "Normal")
                    .when(col("insulin") > 300, "Resistente")
                    .otherwise("Desconocido")) \
        .withColumn("skin_thickness_cat",
                    when(col("skinthickness") < 10, "Delgado")
                    .when(col("skinthickness").between(10, 30), "Normal")
                    .when(col("skinthickness") > 30, "Grueso")
                    .otherwise("Desconocido")) \
        .withColumn("diabetes_diagnosis",
                    when(col("outcome") == 1, "DIABÉTICO")
                    .otherwise("NO DIABÉTICO"))
    
    return df_enriched

def calcular_metricas_agregadas(spark, db_functional, table_name):
    """
    Calcula métricas agregadas y las guarda en una tabla de resumen
    """
    print("   📊 Calculando métricas agregadas...")
    
    # Leer datos funcionales
    df = spark.table(f"{db_functional}.{table_name}")
    
    # Calcular métricas por grupo
    metricas_por_riesgo = df.groupBy("risk_level", "outcome").agg(
        round(avg("glucose"), 1).alias("glucose_prom"),
        round(avg("bmi"), 1).alias("bmi_prom"),
        round(avg("age"), 1).alias("edad_prom"),
        round(avg("bloodpressure"), 1).alias("presion_prom"),
        count("*").alias("total_pacientes")
    ).orderBy("risk_level", "outcome")
    
    # Guardar métricas como tabla separada
    metricas_por_riesgo.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{db_functional}.metricas_por_riesgo")
    
    print(f"   ✅ Métricas guardadas en {db_functional}.metricas_por_riesgo")
    
    return metricas_por_riesgo

# =============================================================================
# @section 4. Definición de esquema para Functional
# =============================================================================

SCHEMA_FUNCTIONAL = StructType([
    # Columnas originales (limpias)
    StructField("pregnancies", IntegerType(), True),
    StructField("glucose", IntegerType(), True),
    StructField("bloodpressure", IntegerType(), True),
    StructField("skinthickness", IntegerType(), True),
    StructField("insulin", IntegerType(), True),
    StructField("bmi", DoubleType(), True),
    StructField("diabetespedigreefunction", DoubleType(), True),
    StructField("age", IntegerType(), True),
    StructField("outcome", IntegerType(), True),
    
    # Nuevas columnas enriquecidas
    StructField("pregnancies_cat", StringType(), True),
    StructField("glucose_level", StringType(), True),
    StructField("bmi_category", StringType(), True),
    StructField("age_group", StringType(), True),
    StructField("pressure_level", StringType(), True),
    StructField("risk_score", IntegerType(), True),
    StructField("risk_level", StringType(), True),
    StructField("dpf_level", StringType(), True),
    StructField("insulin_sensitivity", StringType(), True),
    StructField("skin_thickness_cat", StringType(), True),
    StructField("diabetes_diagnosis", StringType(), True)
])

# =============================================================================
# @section 5. Configuración de tablas
# =============================================================================

# Nombre de la tabla de origen en Curated
SOURCE_TABLE_NAME = "DIABETES"

# Nombre de la tabla de destino en Functional
TARGET_TABLE_NAME = "DIABETES_ANALYTICS"

# =============================================================================
# @section 6. Proceso principal
# =============================================================================

def main():
    args = parse_arguments()
    spark = create_spark_session()
    
    try:
        env_lower = args.env.lower()
        db_functional = f"{env_lower}_functional"
        db_source = f"{env_lower}_{args.source_db}"
        
        print(f"🌍 Entorno: {args.env}")
        print(f"🗄️  Base origen: {db_source}")
        print(f"🎯 Base destino: {db_functional}")
        print(f"📋 Tabla origen: {SOURCE_TABLE_NAME}")
        print(f"📋 Tabla destino: {TARGET_TABLE_NAME}")
        
        # 1. Crear database Functional
        crear_database(spark, env_lower, args.username, args.base_path)
        
        # 2. Definir ubicación física de la tabla de destino
        location = f"{args.base_path}/{args.username}/datalake/{db_functional.upper()}/{TARGET_TABLE_NAME.lower()}"
        
        # 3. Crear estructura de tabla Functional 
        crear_tabla_functional(
            spark=spark,
            db_name=db_functional,
            table_name=TARGET_TABLE_NAME,
            schema=SCHEMA_FUNCTIONAL,
            location=location
        )
        
        # 4. Leer datos desde Curated
        print(f"   📖 Leyendo datos desde {db_source}.{SOURCE_TABLE_NAME}")
        df_source = spark.table(f"{db_source}.{SOURCE_TABLE_NAME}")
        
        # Mostrar información del origen
        count_origen = df_source.count()
        print(f"   📊 Registros en origen: {count_origen}")
        
        # 5. Aplicar enriquecimiento
        df_enriched = enriquecer_datos_diabetes(df_source)
        
        # 6. Insertar en Functional
        print(f"   💾 Insertando datos enriquecidos en {TARGET_TABLE_NAME}...")
        df_enriched.write \
            .mode("overwrite") \
            .format("parquet") \
            .option("compression", "snappy") \
            .partitionBy("risk_level") \
            .saveAsTable(f"{db_functional}.{TARGET_TABLE_NAME}")
        
        # Refrescar metadatos
        spark.sql(f"MSCK REPAIR TABLE {db_functional}.{TARGET_TABLE_NAME}")
        
        # 7. Validación
        print(f"   🔍 Validando carga en {db_functional}.{TARGET_TABLE_NAME}:")
        spark.sql(f"SELECT * FROM {db_functional}.{TARGET_TABLE_NAME} LIMIT 5").show(truncate=False)
        
        # 8. Estadísticas por nivel de riesgo
        print(f"   📊 Distribución por nivel de riesgo:")
        spark.sql(f"""
            SELECT risk_level, 
                   COUNT(*) as cantidad,
                   ROUND(AVG(glucose), 1) as glucose_prom,
                   ROUND(AVG(bmi), 1) as bmi_prom
            FROM {db_functional}.{TARGET_TABLE_NAME} 
            GROUP BY risk_level 
            ORDER BY risk_level
        """).show()
        
        # 9. Mostrar particiones
        print(f"   📂 Particiones disponibles:")
        spark.sql(f"SHOW PARTITIONS {db_functional}.{TARGET_TABLE_NAME}").show(truncate=False)
        
        # 10. Calcular métricas agregadas
        metricas = calcular_metricas_agregadas(spark, db_functional, TARGET_TABLE_NAME)
        metricas.show()
        
        print("\n🎉 Proceso Functional completado exitosamente!")
        print(f"📊 Tablas disponibles en {db_functional}:")
        spark.sql(f"SHOW TABLES IN {db_functional}").show(truncate=False)
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()