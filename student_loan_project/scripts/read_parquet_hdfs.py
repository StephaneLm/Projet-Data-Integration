from pyspark.sql import SparkSession

# Initialiser Spark
spark = SparkSession.builder \
    .appName("ReadParquet") \
    .getOrCreate()


# Lire les fichiers de format parquet
merged_data = spark.read.parquet("hdfs://localhost:9000/student_loan_data/merged_FL_Dashboard")

# On renomme les colonnes compliqués à lire et problématique
cleaned_columns = {col: col.strip().replace("$", "").replace(" ", "_") for col in merged_data.columns}
for old_name, new_name in cleaned_columns.items():
    merged_data = merged_data.withColumnRenamed(old_name, new_name)

# On affiche les nouvelles colonnes fusionnées
print(merged_data.columns)


# Afficher les données
merged_data.show()

print(merged_data.columns)


# Métrique pour calculer le total des prêts par État
total_loans_by_state = merged_data.groupBy("State").agg({"of_Loans_Originated": "sum"})
total_loans_by_state.show()


# Ici on calcule les écoles qui recoivent beaucoup (voir le plus) de prêts
top_schools = merged_data.groupBy("School").agg({"of Loans Originated": "sum"}) \
    .orderBy("sum(of Loans Originated)", ascending=False) \
    .limit(10)
top_schools.show()


# Sauvegarder les fichiers en CSV
merged_data.write.csv("hdfs://localhost:9000/student_loan_data/merged_FL_Dashboard_csv", header=True)
