import pandas as pd
import json


# 1. Chargement du fichier brut


df = pd.read_csv(
    "./data/sor-global-2025-12-01-full-00000-00000.csv", 
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
    "uuid", #ok 
    "decision_visibility", #ok
    "decision_visibility_other", #ok
    "platform_name", #ok 
    "platform_uid", #ok
    "decision_ground",#ok 
    "category",  #ok 
    "application_date",
    "territorial_scope",#ok
    "automated_detection", #ok 
    "automated_decision", #ok 
    "created_at",
    "source_type",
    "decision_provision",#ok
    "decision_monetary",#ok 
    "decision_account", #ok 
    "decision_facts", #ok
    "end_date_account_restriction", #ok
    "illegal_content_legal_ground",#ok
    "illegal_content_explanation",#ok
    "incompatible_content_ground",#ok
    "incompatible_content_explanation",#ok
    "incompatible_content_illegal",#ok
    "content_type", #ok
    "content_type_other" #ok,
    "content_language" , #ok
    "content_date", #ok







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
    ).str.replace(
        "DECISION VISIBILITY ", "", regex=False
    ).str.replace("_", " ")

if "decision_ground" in df.columns:
    df["decision_ground"] = df["decision_ground"].str.replace(
        "DECISION_GROUND_", "", regex=False
    ).str.replace("_", " ")

if "content_type" in df.columns:
    df["content_type"] = df["content_type"].apply(clean_array).str.replace(
        "CONTENT_TYPE_", "", regex=False
    ).str.replace("_", " ")


if "source_type" in df.columns:
    df["source_type"] = df["source_type"].str.replace(
        "SOURCE_TYPE_", "", regex=False
    ).str.replace("_", " ")

    
if "category" in df.columns:
    df["category"] = df["category"].str.replace(
        "STATEMENT_CATEGORY_", "", regex=False
    ).str.replace("_", " ")

if "automated_decision" in df.columns:
    df["automated_decision"] = df["automated_decision"].str.replace(
        "AUTOMATED_DECISION_", "", regex=False
    ).str.replace("_", " ")

if "decision_account" in df.columns:
    df["decision_account"] = df["decision_account"].str.replace(
        "DECISION_ACCOUNT_", "", regex=False
    ).str.replace("_", " ")

if "decision_provision" in df.columns:
    df["decision_provision"] = df["decision_provision"].str.replace(
        "DECISION_PROVISION_", "", regex=False
    ).str.replace("_", " ")


if "decision_monetary" in df.columns:
    df["decision_monetary"] = df["decision_monetary"].str.replace(
        "DECISION_MONETARY_", "", regex=False
    ).str.replace("_", " ")
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

if "decision_facts" in df.columns:
    df["decision_facts"] = df["decision_facts"].str.normalize("NFKD").str.encode("ascii", errors="ignore").str.decode("utf-8")

columns_to_clean = [
    "decision_facts",
    "illegal_content_legal_ground",
    "illegal_content_explanation",
    "incompatible_content_ground",
    "incompatible_content_explanation"
]

for col in columns_to_clean:
    if col in df.columns:
        df[col] = df[col].str.normalize("NFKD").str.encode("ascii", errors="ignore").str.decode("utf-8")

for col in ["uuid", "platform_uid"]:
    if col in df.columns:
        df[col] = df[col].astype(str)


# 6. Conversion des dates au format dd/mm/yyyy


date_cols = ["content_date", "application_date", "created_at","end_date_account_restriction"]

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
df.to_parquet("data_clean/clean/dsa_clean.parquet", index=False)

print("\n Nettoyage terminé avec succès")
print(df.head())
