from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
import os

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("LoadAndMergeFLDashboard") \
    .getOrCreate()

# Teste de la connexion à Hadoop HDFS
try:
    print("Test de la connexion à HDFS...")
    test_path = "hdfs://localhost:9000/student_loan_data"
    spark.read.text(test_path).show(1)
    print("Connexion réussie")
except Exception as e:
    print(f"ECHEC : {e}")
    spark.stop()
    exit()

# Répertoire qui contient les dataset sur HDFS
hdfs_directory = "hdfs://localhost:9000/student_loan_data"

# Chargement des datasets disponibles sur HDFS
file_paths = [
    os.path.join(hdfs_directory, file)
    for file in os.listdir("/home/stephanelam/DataInteg/student_loan_project/raw_data")
    if file.startswith("FL_Dashboard") and file.endswith(".xls")
]

dataframes = []
for path in file_paths:
    print(f"Loading file: {path}")
    df = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("dataAddress", "'Quarterly Activity'!A1") \
        .load(path)
    
    # Ici on nettoie les colonnes qui contiennent des caractères spéciaux pour une meilleure lecture
    columns_to_clean = ["$ of Loans Originated", "$ of Disbursements"]
    for col_name in columns_to_clean:
        if col_name in df.columns:
            df = df.withColumn(col_name, regexp_replace(col(col_name), "[$,]", "").cast("double"))
    
    dataframes.append(df)

# Ici on fait une jointure de table
if len(dataframes) > 0:
    merged_df = dataframes[0]
    for df in dataframes[1:]:
        merged_df = merged_df.unionByName(df)
else:
    print("No files to process.")
    spark.stop()
    exit()

# On affiche tout
merged_df.show()

# Stocke le résultat fusionné sur HDFS
output_path = f"{hdfs_directory}/merged_FL_Dashboard"
print(f"On écrit les données fussionnés ici: {output_path}...")
merged_df.write.parquet(output_path, mode="overwrite")
print(f"Les données fusionnées sont ici : {output_path}")

spark.stop()
