import pandas as pd
from sqlalchemy import create_engine

# ✅ Chemin vers ton CSV
CSV_PATH = "/Users/ibtissamnabet/Desktop/M2/MEGADATA/projet/data_clean/statements_test.csv"

# ✅ Connexion PostgreSQL
engine = create_engine("postgresql+psycopg2://localhost/dsa_project")

print("➡️ Lecture du CSV...")
df = pd.read_csv(CSV_PATH)

print("✅ Colonnes AVANT correction :")
print(df.columns.tolist())

# =============================
# ✅ CORRECTION DES COLONNES *_m100000
# =============================

df.columns = df.columns.str.replace(r"_m[0-9]+$", "", regex=True)

print("✅ Colonnes APRÈS correction :")
print(df.columns.tolist())

print("✅ Nombre de lignes :", len(df))

# =============================
# ✅ IMPORT
# =============================

# Define the batch size
BATCH_SIZE = 1000

print("➡️ Import dans PostgreSQL...")
for i in range(0, len(df), BATCH_SIZE):
    batch = df.iloc[i:i + BATCH_SIZE]
    batch.to_sql(
        "statements_test",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )
    print(f"✅ Batch {i // BATCH_SIZE + 1} imported successfully")

print("✅ IMPORT TERMINÉ AVEC SUCCÈS")
