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

BASE_URL_FR = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2/records"
HEADERS = {"User-Agent": "Mozilla/5.0"}


# =========================
# OUTILS
# =========================
def clean_price(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str)
        .str.replace("€", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace("-", "", regex=False)
        .str.strip(),
        errors="coerce",
    )


def calc_distance(prix_reference: float, prix_destination: float, litres: int = PLEIN_LITRES, conso: int = CONSO_L_100) -> dict:
    """
    prix_reference = prix du pays où l'automobiliste habite
    prix_destination = prix du pays où il va faire son plein
    """
    ecart = round(prix_reference - prix_destination, 3)
    economie = round(ecart * litres, 2)
    cout_km = (conso / 100) * prix_destination

    if ecart <= 0 or cout_km <= 0:
        return {
            "ecart_prix": ecart,
            "economie_plein_40l": economie,
            "distance_max_aller_km": 0,
            "distance_max_ar_km": 0,
        }

    distance_ar = economie / cout_km
    distance_aller = distance_ar / 2

    return {
        "ecart_prix": ecart,
        "economie_plein_40l": economie,
        "distance_max_aller_km": round(distance_aller, 1),
        "distance_max_ar_km": round(distance_ar, 1),
    }


# =========================
# 1) FRANCE
# =========================
def get_france_prices() -> dict:
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
        df["gazole_prix"].notna()
        & df["gazole_maj"].notna()
        & (df["gazole_maj"] >= cutoff)
    ].copy()

    prix_fr_gazole = round(df_gazole["gazole_prix"].astype(float).mean(), 3)
    nb_fr_gazole = int(df_gazole.shape[0])

    df_e10 = df[
        df["e10_prix"].notna()
        & df["e10_maj"].notna()
        & (df["e10_maj"] >= cutoff)
    ].copy()

    df_sp95 = df[
        df["sp95_prix"].notna()
        & df["sp95_maj"].notna()
        & (df["sp95_maj"] >= cutoff)
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

    return {
        "gazole": {
            "prix": prix_fr_gazole,
            "sample_size": nb_fr_gazole,
        },
        "sp95": {
            "prix": prix_fr_sp95,
            "sample_size": nb_fr_sp95,
            "source": source_fr_sp95,
        },
    }


# =========================
# 2) BELGIQUE
# =========================
def get_belgique_prices() -> dict:
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

    return {
        "gazole": {
            "prix": prix_be_gazole,
            "sample_size": nb_be_gazole,
            "min": prix_be_gazole_min,
            "min_province": province_gazole_min,
        },
        "sp95": {
            "prix": prix_be_sp95,
            "sample_size": nb_be_sp95,
            "source": "E10",
            "min": prix_be_sp95_min,
            "min_province": province_sp95_min,
        },
    }


# =========================
# 3) CALCULS DANS LES DEUX SENS
# =========================
def build_direction_payload(direction: str, france: dict, belgique: dict) -> dict:
    if direction == "fr_to_be":
        gazole_calc = calc_distance(france["gazole"]["prix"], belgique["gazole"]["prix"])
        sp95_calc = calc_distance(france["sp95"]["prix"], belgique["sp95"]["prix"])
        title = "Faire son plein en Belgique vaut-il le coup pour les Français ?"
    elif direction == "be_to_fr":
        gazole_calc = calc_distance(belgique["gazole"]["prix"], france["gazole"]["prix"])
        sp95_calc = calc_distance(belgique["sp95"]["prix"], france["sp95"]["prix"])
        title = "Faire son plein en France vaut-il le coup pour les Belges ?"
    else:
        raise ValueError(f"Direction inconnue : {direction}")

    return {
        "direction": direction,
        "title": title,
        "prices": {
            "gazole": {
                "france": france["gazole"]["prix"],
                "belgique": belgique["gazole"]["prix"],
                "france_sample_size": france["gazole"]["sample_size"],
                "belgique_sample_size": belgique["gazole"]["sample_size"],
                "belgique_min": belgique["gazole"]["min"],
                "belgique_min_province": belgique["gazole"]["min_province"],
            },
            "sp95": {
                "france": france["sp95"]["prix"],
                "belgique": belgique["sp95"]["prix"],
                "france_sample_size": france["sp95"]["sample_size"],
                "france_source": france["sp95"]["source"],
                "belgique_source": belgique["sp95"]["source"],
                "belgique_sample_size": belgique["sp95"]["sample_size"],
                "belgique_min": belgique["sp95"]["min"],
                "belgique_min_province": belgique["sp95"]["min_province"],
            },
        },
        "calculs": {
            "gazole": gazole_calc,
            "sp95": sp95_calc,
        },
    }


# =========================
# 4) JSON FINAL
# =========================
def main() -> None:
    france = get_france_prices()
    belgique = get_belgique_prices()

    output = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sources": {
            "france": "API officielle prix carburants France, moyenne des stations du 59 et du 62 mises à jour au cours des trois derniers jours",
            "belgique": "Carbu Belgique, médiane des prix observés dans les localités du Hainaut et de Flandre occidentale",
        },
        "scope": {
            "france_departements": ["59", "62"],
            "france_max_age_days": MAX_AGE_DAYS,
            "belgique_provinces": list(BELGIQUE_URLS.keys()),
            "belgique_aggregation": "median_all_localites",
        },
        "hypotheses": {
            "plein_litres": PLEIN_LITRES,
            "consommation_l_100": CONSO_L_100,
        },
        "comparaisons": {
            "fr_to_be": build_direction_payload("fr_to_be", france, belgique),
            "be_to_fr": build_direction_payload("be_to_fr", france, belgique),
        },
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\nFichier créé : {OUTPUT_PATH}")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
