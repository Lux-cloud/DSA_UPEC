import sqlite3

# Création / connexion à la base
conn = sqlite3.connect("dsa.db")
cursor = conn.cursor()

# Création de la table principale
cursor.execute("""
CREATE TABLE IF NOT EXISTS dsa_decisions (
    uuid TEXT PRIMARY KEY,
    platform_name TEXT,
    decision_visibility TEXT,
    decision_ground TEXT,
    category TEXT,
    content_type TEXT,
    content_language TEXT,
    content_date TEXT,
    application_date TEXT,
    territorial_scope TEXT,
    automated_detection BOOLEAN,
    automated_decision TEXT,
    created_at TEXT
)
""")

conn.commit()
conn.close()

print("Base dsa.db et table dsa_decisions créées")
