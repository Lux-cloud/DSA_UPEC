import pandas as pd
import json


# 1. Chargement du fichier brut


df = pd.read_csv(
    "data/sor-global-2025-12-06-full-00000-00000.csv", 
    sep=",",                                         
    low_memory=False
)

#  Nettoyage des noms de colonnes 
df.columns = df.columns.str.strip()

print(" Colonnes détectées :")
print(df.columns.tolist())

# 
# 2. Sélection des colonnes utiles
# 

cols = [
    "uuid",
    "platform_name",
    "decision_visibility",
    "decision_ground",
    "category",
    "content_type",
    "content_language",
    "content_date",
    "application_date",
    "territorial_scope",
    "automated_detection",
    "automated_decision",
    "created_at"
]

#  Sélection sécurisée 
cols_existantes = [c for c in cols if c in df.columns]
df = df[cols_existantes]

print("Colonnes conservées :")
print(df.columns.tolist())


# 3. Nettoyage des listes JSON sous forme de texte


def clean_array(val):
    if pd.isna(val):
        return None
    try:
        data = json.loads(val)
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return None
    except:
        return val

if "decision_visibility" in df.columns:
    df["decision_visibility"] = df["decision_visibility"].apply(clean_array)

if "content_type" in df.columns:
    df["content_type"] = df["content_type"].apply(clean_array)


# 4. Simplification des libellés techniques


if "decision_visibility" in df.columns:
    df["decision_visibility"] = df["decision_visibility"].str.replace(
        "DECISION_VISIBILITY_CONTENT_", "", regex=False
    )

if "decision_ground" in df.columns:
    df["decision_ground"] = df["decision_ground"].str.replace(
        "DECISION_GROUND_", "", regex=False
    )

if "content_type" in df.columns:
    df["content_type"] = df["content_type"].str.replace(
        "CONTENT_TYPE_", "", regex=False
    )

if "category" in df.columns:
    df["category"] = df["category"].str.replace(
        "STATEMENT_CATEGORY_", "", regex=False
    )


# 5. Nettoyage des booléens


if "automated_detection" in df.columns:
    df["automated_detection"] = df["automated_detection"].map({
        "Yes": True,
        "No": False,
        True: True,
        False: False
    })

if "automated_decision" in df.columns:
    df["automated_decision"] = df["automated_decision"].str.replace(
        "AUTOMATED_DECISION_", "", regex=False
    )





# 6. Conversion des dates au format dd/mm/yyyy


date_cols = ["content_date", "application_date", "created_at"]

for col in date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%d/%m/%Y")

# 7. Nettoyage du champ pays (JSON)

def clean_countries(val):
    if pd.isna(val):
        return None
    try:
        return json.loads(val)
    except:
        return None

if "territorial_scope" in df.columns:
    df["territorial_scope"] = df["territorial_scope"].apply(clean_countries)


# 8. Export 


df.to_csv("data_clean/clean/dsa_clean.csv", index=False)

print("\n Nettoyage terminé avec succès")
print(df.head())
