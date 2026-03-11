import requests
import pandas as pd
import json
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

# =========================
# PARAMÈTRES
# =========================
MAX_AGE_DAYS = 3

BELGIQUE_URLS = {
    "Hainaut": "https://carbu.com/france//index.php/meilleurs-prix/Hainaut/BE_ht/1",
    "Flandre Occidentale": "https://carbu.com/france//index.php/meilleurs-prix/Flandre%20Occidentale/BE_foc/1",
}

PLEIN_LITRES = 40
CONSO_L_100 = 6

OUTPUT_PATH = Path("data/prix_carburants.json")

# =========================
# 1) FRANCE
# =========================
BASE_URL_FR = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2/records"

all_results = []
offset = 0
page_size = 100

while True:
    params = {"limit": page_size, "offset": offset}
    r = requests.get(BASE_URL_FR, params=params, timeout=60)
    r.raise_for_status()
    payload = r.json()
    results = payload.get("results", [])

    if not results:
        break

    all_results.extend(results)

    if len(results) < page_size:
        break

    offset += page_size

df = pd.DataFrame(all_results)
df["code_departement"] = df["code_departement"].astype(str)
df = df[df["code_departement"].isin(["59", "62"])].copy()

for col in ["gazole_maj", "sp95_maj", "e10_maj"]:
    df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=MAX_AGE_DAYS)

df_gazole = df[
    df["gazole_prix"].notna() &
    df["gazole_maj"].notna() &
    (df["gazole_maj"] >= cutoff)
].copy()

prix_fr_gazole = round(df_gazole["gazole_prix"].astype(float).mean(), 3)
nb_fr_gazole = int(df_gazole.shape[0])

df_e10 = df[
    df["e10_prix"].notna() &
    df["e10_maj"].notna() &
    (df["e10_maj"] >= cutoff)
].copy()

df_sp95 = df[
    df["sp95_prix"].notna() &
    df["sp95_maj"].notna() &
    (df["sp95_maj"] >= cutoff)
].copy()

if len(df_e10) > 0:
    prix_fr_sp95 = round(df_e10["e10_prix"].astype(float).mean(), 3)
    nb_fr_sp95 = int(df_e10.shape[0])
    source_fr_sp95 = "E10"
else:
    prix_fr_sp95 = round(df_sp95["sp95_prix"].astype(float).mean(), 3)
    nb_fr_sp95 = int(df_sp95.shape[0])
    source_fr_sp95 = "SP95"

print("France - Gazole :", prix_fr_gazole, f"({nb_fr_gazole} stations)")
print("France - SP95 (E10) :", prix_fr_sp95, f"({nb_fr_sp95} stations, source {source_fr_sp95})")

# =========================
# 2) BELGIQUE
# =========================
HEADERS = {"User-Agent": "Mozilla/5.0"}

def clean_price(series):
    return pd.to_numeric(
        series.astype(str)
        .str.replace("€", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace("-", "", regex=False)
        .str.strip(),
        errors="coerce"
    )

dfs_be = []

for province, url in BELGIQUE_URLS.items():
    print(f"\nLecture Belgique : {province}")
    html = requests.get(url, headers=HEADERS, timeout=30).text
    df_be_province = pd.read_html(StringIO(html))[0].copy()

    col_localite = df_be_province.columns[0]
    col_sp95 = "E10"
    col_gazole = "GO"

    df_be_province[col_sp95] = clean_price(df_be_province[col_sp95])
    df_be_province[col_gazole] = clean_price(df_be_province[col_gazole])

    tmp = df_be_province[[col_localite, col_sp95, col_gazole]].copy()
    tmp.columns = ["localite", "sp95_e10", "gazole"]
    tmp["province"] = province
    dfs_be.append(tmp)

df_be = pd.concat(dfs_be, ignore_index=True)

print("\n--- Belgique : nombre de lignes par province ---")
print(df_be["province"].value_counts())

prix_be_sp95 = round(df_be["sp95_e10"].dropna().median(), 3)
prix_be_gazole = round(df_be["gazole"].dropna().median(), 3)

prix_be_sp95_min = round(df_be["sp95_e10"].dropna().min(), 3)
prix_be_gazole_min = round(df_be["gazole"].dropna().min(), 3)

province_sp95_min = df_be.loc[df_be["sp95_e10"].idxmin(), "province"]
province_gazole_min = df_be.loc[df_be["gazole"].idxmin(), "province"]

nb_be_sp95 = int(df_be["sp95_e10"].notna().sum())
nb_be_gazole = int(df_be["gazole"].notna().sum())

print("\nBelgique - SP95 (E10) médiane globale :", prix_be_sp95)
print("Belgique - Gazole médiane globale :", prix_be_gazole)

# =========================
# 3) CALCULS
# =========================
def calc_distance(prix_fr, prix_be, litres=PLEIN_LITRES, conso=CONSO_L_100):
    ecart = round(prix_fr - prix_be, 3)
    economie = round(ecart * litres, 2)
    cout_km = (conso / 100) * prix_fr

    if ecart <= 0 or cout_km <= 0:
        return {
            "ecart_prix": ecart,
            "economie_plein_40l": economie,
            "distance_max_aller_km": 0,
            "distance_max_ar_km": 0
        }

    distance_ar = economie / cout_km
    distance_aller = distance_ar / 2

    return {
        "ecart_prix": ecart,
        "economie_plein_40l": economie,
        "distance_max_aller_km": round(distance_aller, 1),
        "distance_max_ar_km": round(distance_ar, 1)
    }

calc_gazole = calc_distance(prix_fr_gazole, prix_be_gazole)
calc_sp95 = calc_distance(prix_fr_sp95, prix_be_sp95)

# =========================
# 4) JSON FINAL
# =========================
output = {
    "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "sources": {
        "france": "API officielle prix carburants France, moyenne des stations du 59 et du 62 mises à jour au cours des trois derniers jours",
        "belgique": "Carbu Belgique, médiane des prix observés dans les localités du Hainaut et de Flandre occidentale"
    },
    "scope": {
        "france_departements": ["59", "62"],
        "france_max_age_days": MAX_AGE_DAYS,
        "belgique_provinces": list(BELGIQUE_URLS.keys()),
        "belgique_aggregation": "median_all_localites"
    },
    "hypotheses": {
        "plein_litres": PLEIN_LITRES,
        "consommation_l_100": CONSO_L_100
    },
    "prices": {
        "gazole": {
            "france": prix_fr_gazole,
            "belgique": prix_be_gazole,
            "france_sample_size": nb_fr_gazole,
            "belgique_sample_size": nb_be_gazole,
            "belgique_min": prix_be_gazole_min,
            "belgique_min_province": province_gazole_min
        },
        "sp95": {
            "france": prix_fr_sp95,
            "belgique": prix_be_sp95,
            "france_sample_size": nb_fr_sp95,
            "france_source": source_fr_sp95,
            "belgique_source": "E10",
            "belgique_sample_size": nb_be_sp95,
            "belgique_min": prix_be_sp95_min,
            "belgique_min_province": province_sp95_min
        }
    },
    "calculs": {
        "gazole": calc_gazole,
        "sp95": calc_sp95
    }
}

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")

print(f"\nFichier créé : {OUTPUT_PATH}")
print(json.dumps(output, ensure_ascii=False, indent=2))
