import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os

import time
start_time = time.time()



conn = sqlite3.connect("dsa.db")

os.makedirs("output", exist_ok=True)


# 0. ANALYSE 3V (VOLUME & VARIÉTÉ) ---- avec beaucoup de data on le respectera 


df_volume = pd.read_sql_query("SELECT COUNT(*) AS total FROM dsa_decisions;", conn)
df_variete = pd.read_sql_query("SELECT COUNT(DISTINCT platform_name) AS total FROM dsa_decisions;", conn)

print("📊 VOLUME TOTAL :", df_volume.iloc[0, 0])
print("📊 VARIÉTÉ (plateformes) :", df_variete.iloc[0, 0])


# 1. Décisions par plateforme


df_platform = pd.read_sql_query("""
SELECT platform_name, COUNT(*) AS total
FROM dsa_decisions
GROUP BY platform_name
ORDER BY total DESC;
""", conn)

plt.figure(figsize=(12, 6))  
plt.bar(df_platform["platform_name"], df_platform["total"])
plt.title("Nombre de décisions par plateforme")
plt.xlabel("Plateforme")
plt.ylabel("Nombre de décisions")
plt.xticks(rotation=45, ha="right")  
plt.tight_layout()
plt.savefig("output/decisions_par_plateforme.png")
plt.close()


# 2. Automatisation des décisions (%)


df_auto = pd.read_sql_query("""
SELECT automated_decision, COUNT(*) AS total
FROM dsa_decisions
GROUP BY automated_decision;
""", conn)

plt.figure()
plt.pie(df_auto["total"], labels=df_auto["automated_decision"], autopct="%1.1f%%")
plt.title("Part des décisions automatisées")
plt.savefig("output/decisions_automatisees.png")
plt.close()


# 3. Décisions dans le temps


df_time = pd.read_sql_query("""
SELECT application_date, COUNT(*) AS total
FROM dsa_decisions
GROUP BY application_date
ORDER BY application_date;
""", conn)

plt.figure()
plt.plot(df_time["application_date"], df_time["total"], marker="o")
plt.title("Évolution du nombre de décisions dans le temps")
plt.xlabel("Date")
plt.ylabel("Nombre de décisions")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("output/decisions_temps.png")
plt.close()




# 3bis. Analyse de croissance mensuelle

df_growth = pd.read_sql_query("""
SELECT substr(application_date, 4, 7) AS mois, COUNT(*) AS total
FROM dsa_decisions
GROUP BY mois
ORDER BY mois;
""", conn)

df_growth.to_csv("output/croissance_mensuelle.csv", index=False)

plt.figure()
plt.plot(df_growth["mois"], df_growth["total"], marker="o")
plt.title("Croissance mensuelle des décisions")
plt.xlabel("Mois")
plt.ylabel("Nombre de décisions")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("output/croissance_mensuelle.png")
plt.close()




# 3ter. Tendances par plateforme dans le temps

df_trends = pd.read_sql_query("""
SELECT platform_name, application_date, COUNT(*) AS total
FROM dsa_decisions
GROUP BY platform_name, application_date
ORDER BY application_date;
""", conn)

df_trends.to_csv("output/tendances_plateformes_temps.csv", index=False)


# 4. Types de contenu modérés


df_content = pd.read_sql_query("""
SELECT content_type, COUNT(*) AS total
FROM dsa_decisions
GROUP BY content_type
ORDER BY total DESC;
""", conn)

plt.figure(figsize=(12, 6))  # Increase figure size
plt.bar(df_content["content_type"], df_content["total"])
plt.title("Types de contenus modérés")
plt.xlabel("Type de contenu")
plt.ylabel("Nombre")
plt.xticks(rotation=45, ha="right")  # Rotate labels and align to the right
plt.tight_layout()
plt.savefig("output/types_contenu.png")
plt.close()


# 5. Catégories juridiques


df_cat = pd.read_sql_query("""
SELECT category, COUNT(*) AS total
FROM dsa_decisions
GROUP BY category
ORDER BY total DESC;
""", conn)

plt.figure(figsize=(12, 6))  # Increase figure size
plt.bar(df_cat["category"], df_cat["total"])
plt.title("Catégories juridiques")
plt.xlabel("Catégorie")
plt.ylabel("Nombre")
plt.xticks(rotation=45, ha="right")  # Rotate labels and align to the right
plt.tight_layout()
plt.savefig("output/categories_juridiques.png")
plt.close()

# 6. Langues des contenus (CORRIGÉ)


df_lang = pd.read_sql_query("""
SELECT content_language, COUNT(*) AS total
FROM dsa_decisions
GROUP BY content_language
ORDER BY total DESC;
""", conn)

#  SUPPRESSION DES LANGUES NULL
df_lang = df_lang.dropna(subset=["content_language"])

#  Conversion en chaîne (sécurité)
df_lang["content_language"] = df_lang["content_language"].astype(str)

plt.figure(figsize=(12, 6)) 
plt.bar(df_lang["content_language"], df_lang["total"])
plt.title("Langues des contenus modérés")
plt.xlabel("Langue")
plt.ylabel("Nombre")
plt.xticks(rotation=45, ha="right")  
plt.tight_layout()
plt.savefig("output/langues.png")
plt.close()





# 6bis. Analyse géographique (PAR PAYS)

df_geo_raw = pd.read_sql_query("""
SELECT territorial_scope
FROM dsa_decisions;
""", conn)

# Explosion des pays
geo_counts = {}

for scope in df_geo_raw["territorial_scope"].dropna():
    try:
        countries = eval(scope) if isinstance(scope, str) else []
        for c in countries:
            geo_counts[c] = geo_counts.get(c, 0) + 1
    except:
        continue

df_geo = pd.DataFrame(geo_counts.items(), columns=["country", "total"])
df_geo = df_geo.sort_values("total", ascending=False)

df_geo.to_csv("output/decisions_par_pays.csv", index=False)

# (Graphique  pays)
plt.figure(figsize=(12, 6))  # Increase figure size
plt.bar(df_geo["country"].head(15), df_geo["total"].head(15))
plt.title("Top 15 pays concernés par les décisions")
plt.xlabel("Pays")
plt.ylabel("Nombre de décisions")
plt.xticks(rotation=45, ha="right")  
plt.tight_layout()
plt.savefig("output/decisions_par_pays.png")
plt.close()



# 6ter. Modération automatique par pays

df_auto_geo_raw = pd.read_sql_query("""
SELECT territorial_scope, automated_decision
FROM dsa_decisions;
""", conn)

auto_geo = {}

for _, row in df_auto_geo_raw.iterrows():
    scope = row["territorial_scope"]
    auto = row["automated_decision"]

    try:
        countries = eval(scope) if isinstance(scope, str) else []
        for c in countries:
            if c not in auto_geo:
                auto_geo[c] = {"FULLY": 0, "OTHER": 0}
            if auto == "FULLY":
                auto_geo[c]["FULLY"] += 1
            else:
                auto_geo[c]["OTHER"] += 1
    except:
        continue

df_auto_geo = pd.DataFrame([
    [k, v["FULLY"], v["OTHER"]] for k, v in auto_geo.items()
], columns=["country", "automated", "non_automated"])

df_auto_geo.to_csv("output/moderation_auto_par_pays.csv", index=False)




# 6qutr. Type de contenu par pays

df_type_geo = pd.read_sql_query("""
SELECT territorial_scope, content_type
FROM dsa_decisions;
""", conn)

type_geo = {}

for _, row in df_type_geo.iterrows():
    scope = row["territorial_scope"]
    ctype = row["content_type"]

    try:
        countries = eval(scope) if isinstance(scope, str) else []
        for c in countries:
            key = (c, ctype)
            type_geo[key] = type_geo.get(key, 0) + 1
    except:
        continue

df_type_geo_final = pd.DataFrame([
    [k[0], k[1], v] for k, v in type_geo.items()
], columns=["country", "content_type", "total"])

df_type_geo_final.to_csv("output/type_contenu_par_pays.csv", index=False)

# 7. Comparaison Google / Shopify


df_compare = pd.read_sql_query("""
SELECT platform_name, COUNT(*) AS total
FROM dsa_decisions
WHERE platform_name IN ('Google Shopping', 'Shopify')
GROUP BY platform_name;
""", conn)

plt.figure()
plt.bar(df_compare["platform_name"], df_compare["total"])
plt.title("Google Shopping vs Shopify")
plt.xlabel("Plateforme")
plt.ylabel("Nombre de décisions")
plt.tight_layout()
plt.savefig("output/google_vs_shopify.png")
plt.close()


# 8. Types de décisions (REMOVED / DISABLED)


df_decision_type = pd.read_sql_query("""
SELECT decision_visibility, COUNT(*) AS total
FROM dsa_decisions
GROUP BY decision_visibility;
""", conn)

df_decision_type = df_decision_type.dropna(subset=["decision_visibility"])

df_decision_type["decision_visibility"] = df_decision_type["decision_visibility"].astype(str)

plt.figure(figsize=(12, 6))  
plt.bar(df_decision_type["decision_visibility"], df_decision_type["total"])
plt.title("Types de décisions")
plt.xlabel("Type de décision")
plt.ylabel("Nombre")
plt.xticks(rotation=45, ha="right")  
plt.tight_layout()
plt.savefig("output/types_decisions.png")
plt.close()


# 9. Activité par plateforme et par jour


df_activity = pd.read_sql_query("""
SELECT platform_name, application_date, COUNT(*) AS total
FROM dsa_decisions
GROUP BY platform_name, application_date
ORDER BY application_date;
""", conn)

df_activity.to_csv("output/activite_plateforme_jour.csv", index=False)


# 10. Synthèse globale par plateforme


df_synthese = pd.read_sql_query("""
SELECT
  platform_name,
  COUNT(*) AS total_decisions,
  SUM(CASE WHEN automated_decision = 'FULLY' THEN 1 ELSE 0 END) AS decisions_automatiques,
  SUM(CASE WHEN decision_visibility = 'REMOVED' THEN 1 ELSE 0 END) AS contenus_supprimes
FROM dsa_decisions
GROUP BY platform_name;
""", conn)

df_synthese.to_csv("output/synthese_plateformes.csv", index=False)


# 11. Décisions les plus récentes


df_recent = pd.read_sql_query("""
SELECT *
FROM dsa_decisions
ORDER BY created_at DESC
LIMIT 10;
""", conn)

df_recent.to_csv("output/decisions_recentes.csv", index=False)

conn.close()

print(" TOUS les graphiques + fichiers d’analyse ont été générés dans le dossier output/")
