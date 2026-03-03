# 🏆PROYECTO FINAL DE TOPICOS BI -DETECCIÓN DE DIABETES

Proyecto educativo de ingeniería de datos que implementa un pipeline ETL con Apache Spark siguiendo el patrón de arquitectura Medallón (Bronze → Silver → Gold), adaptado a capas: Workload → Landing → Curated → Functional, con integración completa: Hive → Parquet → CSV → MongoDB.
📋 Tabla de Contenidos

- 🏗️ Arquitectura Medallón Explicada
```table
┌───────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                         FLUJO DE DATOS                                            │
├───────────────────────────────────────────────────────────────────────────────────────────────────┤ 
│                                                                         
│📥 Fuentes → 🥉 Workload → 🥈 Landing → 🥇 Curated → ⚡ Functional → 📄 gold.csv → 🗄️ MongoDB
│              (HDFS)         (Avro)        (Parquet)    (Parquet)     (Export)    (NoSQL)
│           (Bronze)      (Silver)     (Gold)      (Analytics)         
│
└───────────────────────────────────────────────────────────────────────────────────────────────────┘
```
- 📁 Estructura del Repositorio
```
topicos-deteccion-diabetes/
│
├── 📁 datalake/                    # Datos fuente de ejemplo
│    ├── 📁 data/                    # Datos fuente de ejemplo
│    │  └── diabetes.csv              # Catálogo de empresas (pipe-delimited)
│    │
│    ├── 📁 dataset/                    # Datos fuente de ejemplo
│    │   └── diabetes.data              # Catálogo de empresas (pipe-delimited)
│    │
│    ├── 📁 procesos/                   # Scripts PySpark del pipeline
│    │   ├── poblar_capa_workload.py   # ▶️ Ingesta: CSV → Hive TEXTFILE
│    │   ├── poblar_capa_landing.py    # ▶️ Estandarización: → Avro + partición
│    │   ├── poblar_capa_curated.py    # ▶️ Limpieza y validación de calidad
│    │   ├── poblar_capa_functional.py # ▶️ Enriquecimiento con JOINs
│    │   ├── export_gold_to_csv.py     # 🆕 Extracción: Hive Functional → CSV
│    │   └── export_gold_to_mongo.py   # 🆕 Carga: gold.csv → MongoDB
│    │
│    └── 📁 schema/                     # Esquemas Avro para validación
│       └── diabetes.avsc
│
├── 📁 documentation/                   # Rutas HDFS generadas (no versionadas)
│       └── gold.csv                  # Archivo final consolidado
├── 📁 reports/                   # Rutas HDFS generadas (no versionadas)
│       ├── temp/                     # Archivos temporales de exportación
│       └── gold.csv                  # Archivo final consolidado
├── 📁 scripts/                   # Rutas HDFS generadas (no versionadas)
│       └── consulta_mongodb.py                  # Archivo final consolidado
│
├── 📄.gitignore                   # Rutas HDFS generadas (no versionadas)
└── 📄 README.md                  # Documentación
```
- ⚙️ Tecnologías Utilizadas
- 🚀 Guía de Ejecución Paso a Paso (9 Pasos)
- 🔍 Detalle de Cada Capa
- 📊 Esquema de Datos
- 📤 Exportación: Hive → CSV → MongoDB
- 💡 Mejores Prácticas Implementadas
- 🔧 Solución de Problemas Comunes
- 📚 Recursos de Aprendizaje

🏷️ Licencia: MIT - Libre uso para fines educativos y de investigación

👨‍💻 Autores:
- [Camacho Zamora Anderson David](https://github.com/AndersonCamacho-creator)
- [Coronado Rodriguez Takeshi](https://github.com/TakeCoRo123)
- [Vásquez Acosta Evelin Yudith](https://github.com/evelin-vasquez-a)

👨‍💻 CoAutor: 
- [Jaime Llanos](https://github.com/jllanosb)

📅 Última actualización: Marzo 2026

## ✨ "La calidad de los datos no es un paso, es un viaje a través de capas de refinamiento" ✨
