from pyspark.sql import SparkSession
import os
import glob

MONGO_IP = "172.28.112.1"
MONGO_DATABASE = "medallondiabetes"
MONGO_COLLECTION = "gold_analytics"

print("=" * 80)
print("🔍 CONFIGURACIÓN DE CONEXIÓN")
print("=" * 80)
print(f"🌐 Conectando a MongoDB en: {MONGO_IP}:27017")
print(f"📦 Base de datos: {MONGO_DATABASE}")
print(f"📋 Colección: {MONGO_COLLECTION}")

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("Diabetes_CSV_to_MongoDB_Gold") \
    .config("spark.mongodb.connection.uri", f"mongodb://{MONGO_IP}:27017") \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .config("spark.mongodb.collection", MONGO_COLLECTION) \
    .getOrCreate()

# Rutas de tus archivos
BASE_DIR = "/home/hadoop/topicos-deteccion-diabetes"
REPORTS_DIR = f"{BASE_DIR}/reports"
TEMP_DIR = f"{REPORTS_DIR}/temp"

print("=" * 80)
print("🚀 EXPORTANDO CSV GOLD A MONGODB")
print("=" * 80)
print(f"📁 Buscando archivos CSV en: {TEMP_DIR}")

# Buscar archivos CSV generados
csv_files = glob.glob(f"{TEMP_DIR}/**/part-*.csv", recursive=True)
print(f"📊 Archivos encontrados: {len(csv_files)}")

if not csv_files:
    print("❌ No se encontraron archivos CSV para procesar")
    spark.stop()
    exit(1)

# Mostrar archivos encontrados
for i, file in enumerate(csv_files):
    print(f"   {i+1}. {file}")

# Elegir el primer archivo
csv_path = f"file:{csv_files[0]}"
print(f"\n📥 Procesando: {csv_path}")

try:
    # Leer CSV
    print("\n📖 Leyendo archivo CSV...")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv_path)
    
    # Mostrar información del DataFrame
    print("\n📋 Esquema del DataFrame:")
    df.printSchema()
    
    print("\n🔍 Primeros 5 registros:")
    df.show(5, truncate=False)
    
    count = df.count()
    print(f"\n📊 Total de registros: {count}")
    
    # Escribir en MongoDB
    print(f"\n💾 Escribiendo en MongoDB...")
    print(f"   • URI: mongodb://{MONGO_IP}:27017")
    print(f"   • Base de datos: {MONGO_DATABASE}")
    print(f"   • Colección: {MONGO_COLLECTION}")
    
    df.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("spark.mongodb.connection.uri", f"mongodb://{MONGO_IP}:27017") \
        .option("spark.mongodb.database", MONGO_DATABASE) \
        .option("spark.mongodb.collection", MONGO_COLLECTION) \
        .save()
    
    print("\n✅ ¡Exportación a MongoDB completada exitosamente!")
    
    # Verificar
    print("\n🔍 Verificando datos en MongoDB...")
    df_check = spark.read \
        .format("mongodb") \
        .option("spark.mongodb.connection.uri", f"mongodb://{MONGO_IP}:27017") \
        .option("spark.mongodb.database", MONGO_DATABASE) \
        .option("spark.mongodb.collection", MONGO_COLLECTION) \
        .load()
    
    check_count = df_check.count()
    print(f"   ✅ Registros en MongoDB: {check_count}")
    
    if check_count == count:
        print("   ✅ Coincide con el CSV original")
    else:
        print(f"   ⚠️ Diferencia: CSV={count}, MongoDB={check_count}")
    
    print("\n🔍 Primeros 5 registros desde MongoDB:")
    df_check.show(5, truncate=False)
    
except Exception as e:
    print(f"\n❌ Error durante la exportación: {str(e)}")
    import traceback
    traceback.print_exc()
    
finally:
    spark.stop()
    print("\n🛑 Sesión Spark cerrada")
    print("=" * 80)