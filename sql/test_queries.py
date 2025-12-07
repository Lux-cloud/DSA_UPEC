import sqlite3

conn = sqlite3.connect("dsa.db")
cursor = conn.cursor()

# Nombre total de décisions
cursor.execute("SELECT COUNT(*) FROM dsa_decisions")
print(" Nombre total de décisions :", cursor.fetchone()[0])

# Afficher 5 lignes
cursor.execute("SELECT platform_name, decision_visibility, automated_decision FROM dsa_decisions LIMIT 5")
for row in cursor.fetchall():
    print(row)

conn.close()
