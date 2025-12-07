import pandas as pd
import sqlite3

# Chargement du CSV 
df = pd.read_csv("data_clean/clean/dsa_clean.csv")

# Connexion à la base
conn = sqlite3.connect("dsa.db")

# Insertion dans la table
df.to_sql("dsa_decisions", conn, if_exists="replace", index=False)

conn.close()

print(" Import des données terminé dans dsa.db")
