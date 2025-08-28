# Könyvtárak importálása
import streamlit as st
import pandas as pd
import json
from google.oauth2 import service_account
from google.cloud import bigquery

# GCP kulcs beolvasása Secretsből
gcp_key_json = st.secrets["GCP_SERVICE_ACCOUNT_KEY"]
gcp_key_dict = json.loads(gcp_key_json) 

credentials = service_account.Credentials.from_service_account_info(gcp_key_dict)
client = bigquery.Client(credentials=credentials, project=gcp_key_dict["project_id"])

st.set_page_config(layout="wide")

# Jelszó beolvasása Secretsből
PASSWORD = st.secrets["PASSWORD"]

def check_password():
    pwd = st.sidebar.text_input("Jelszó:", type="password")
    if pwd != PASSWORD:
        st.error("Hibás jelszó!")
        return False
    return True

# BigQuery adatok lekérése
def get_data():
    query = """
    SELECT
        p.azonosito AS Projekt_azonosito,
        p.szam AS Samsung_szam,
        p.felelos AS Felelos,
        a.pjt_nev AS Projektnev,
        a.vegosszeg AS Vegosszeg,
        a.ajanlatkero AS Ajanlatkero,
        a.datum AS Ajanlatadas_datuma,
        a.keszito AS Keszito,
        CONCAT(
          REGEXP_EXTRACT(a.pjt_nev, r'^(\S+\s+\S+\s+\S+\s+\S+\s+\S+)'),
          ' ',
          a.ajanlatkero
        ) AS Egyedi_azonosito,
        m.megjegyzesek AS Megjegyzes
    FROM
        `ajanlatok_dataset.projektlista` AS p
    LEFT JOIN
        `ajanlatok_dataset.ajanlatok` AS a ON p.azonosito = a.pjt_azonosito
    LEFT JOIN
        `ajanlatok_dataset.megjegyzesek` AS m
        ON CONCAT(
          REGEXP_EXTRACT(a.pjt_nev, r'^(\S+\s+\S+\s+\S+\s+\S+\s+\S+)'),
          ' ',
          a.ajanlatkero
        ) = m.azonositok
    """
    df = client.query(query).result().to_dataframe()
    return df

# <<< VÁLTOZOTT: Új függvény az egyedi kulcs előállítására
def generate_unique_id(projektnev: str, ajanlatkero: str) -> str:
    if pd.isna(projektnev):
        projektnev = ""
    if pd.isna(ajanlatkero):
        ajanlatkero = ""
    parts = projektnev.split(" ")
    truncated = " ".join(parts[:5])  # első 5 "szó" megtartása
    return f"{truncated} {ajanlatkero}".strip()

# BigQuery upsert
def upsert_megjegyzes(egyedi_azon: str, megjegyzes):
    merge_sql = """
    MERGE `ajanlatok_dataset.megjegyzesek` T
    USING (SELECT @egyedi_azon AS egyedi_azon, @megjegyzes AS megjegyzes) S
    ON T.azonositok = S.egyedi_azon
    WHEN MATCHED THEN
      UPDATE SET megjegyzesek = S.megjegyzes
    WHEN NOT MATCHED THEN
      INSERT (azonositok, megjegyzesek) VALUES (S.egyedi_azon, S.megjegyzes)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("egyedi_azon", "STRING", str(egyedi_azon)),
            bigquery.ScalarQueryParameter("megjegyzes", "STRING", megjegyzes),
        ]
    )
    client.query(merge_sql, job_config=job_config).result()

# <<< VÁLTOZOTT: save_changes_bulk most az Egyedi_azonosito alapján dolgozik
def save_changes_bulk(original_df: pd.DataFrame, edited_df: pd.DataFrame):
    key_col = "Egyedi_azonosito"

    original_df = original_df[[key_col, "Megjegyzes"]].drop_duplicates(subset=[key_col], keep="last")
    edited_df = edited_df[[key_col, "Megjegyzes"]].drop_duplicates(subset=[key_col], keep="last")

    orig = original_df.set_index(key_col)["Megjegyzes"].fillna("")
    edit = edited_df.set_index(key_col)["Megjegyzes"].fillna("")

    changed_mask = orig.ne(edit)
    changed_ids = orig.index[changed_mask].tolist()

    if not changed_ids:
        st.info("Nincs mentendő változás (Az egyedi azonosító hiányozhat).")
        return

    for key in changed_ids:
        val = edit.loc[key]
        val = None if pd.isna(val) or val == "" else str(val)
        upsert_megjegyzes(key, val)

    st.success(f"Sikeres mentés: {len(changed_ids)} sor frissítve.")

if check_password():
    st.title("Kimenő ajánlatok")
        
    df = get_data()

    # <<< VÁLTOZOTT: Új oszlop hozzáadása azonnal lekérdezés után
    df["Egyedi_azonosito"] = df.apply(
        lambda row: generate_unique_id(row["Projektnev"], row["Ajanlatkero"]),
        axis=1
    )

    # Szűrők
    valasztott_ajanlatkero = st.multiselect("Ajánlatkérő(k):", options=df["Ajanlatkero"].unique(), default=None)
    samsung_keres = st.text_input("Samsung_szam:")
    projektnev_szuro = st.text_input("Projektnev:")
    valasztott_keszito = st.multiselect("Készítő(k):", options=df["Keszito"].unique(), default=None)

    # Ajanlatadas_datuma oszlopot biztosan datetime-ra alakítjuk
    df["Ajanlatadas_datuma"] = pd.to_datetime(df["Ajanlatadas_datuma"], errors="coerce")
    
    # Csak a nem NaT értékekből vesszük a min/max dátumot
    min_date = df["Ajanlatadas_datuma"].dropna().min().date()
    max_date = df["Ajanlatadas_datuma"].dropna().max().date()

    
    datum_intervallum = st.date_input(
        "Ajánlatadás dátum (intervallum vagy konkrét nap):",
        value=[min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )

    
    df_szurt = df.copy()

    if valasztott_keszito:
        df_szurt = df_szurt[df_szurt["Keszito"].isin(valasztott_keszito)]
    
    if valasztott_ajanlatkero:
        df_szurt = df_szurt[df_szurt["Ajanlatkero"].isin(valasztott_ajanlatkero)]

    if samsung_keres:
        df_szurt = df_szurt[df_szurt["Samsung_szam"].str.contains(samsung_keres, case=False, na=False)]

    if projektnev_szuro:
        df_szurt = df_szurt[df_szurt["Projektnev"].str.contains(projektnev_szuro, case=False, na=False)]

    if isinstance(datum_intervallum, list) and len(datum_intervallum) == 2:
        start_date, end_date = datum_intervallum
        df_szurt = df_szurt[
            (pd.to_datetime(df_szurt["Ajanlatadas_datuma"]).dt.date >= start_date) &
            (pd.to_datetime(df_szurt["Ajanlatadas_datuma"]).dt.date <= end_date)
        ]
        
    elif not isinstance(datum_intervallum, list):
        df_szurt = df_szurt[
            pd.to_datetime(df_szurt["Ajanlatadas_datuma"]).dt.date == datum_intervallum
        ]
    
    df_szurt = df_szurt.sort_values(by="Ajanlatadas_datuma", ascending=True, na_position="first")
    
    df_szurt["Vegosszeg"] = df_szurt["Vegosszeg"].apply(
        lambda x: f"{int(x):,}".replace(",", " ") if pd.notnull(x) else "-"
    )

    st.write(f"Találatok száma: {len(df_szurt)}")

    edited_df = st.data_editor(
        df_szurt,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Egyedi_azonosito": st.column_config.TextColumn("Egyedi azonosító", disabled=True),  # <<< VÁLTOZOTT
            "Projekt_azonosito": st.column_config.TextColumn("Projekt azonosító", disabled=True),
            "Samsung_szam": st.column_config.TextColumn("Samsung szám", disabled=True),
            "Felelos": st.column_config.TextColumn("Felelős", disabled=True),
            "Projektnev": st.column_config.TextColumn("Projekt név", disabled=True),
            "Vegosszeg": st.column_config.TextColumn("Végösszeg (HUF)", disabled=True),
            "Ajanlatkero": st.column_config.TextColumn("Ajánlatkérő", disabled=True),
            "Ajanlatadas_datuma": st.column_config.DatetimeColumn("Ajánlatadás dátuma", disabled=True),
            "Keszito": st.column_config.TextColumn("Készítő", disabled=True),
            "Megjegyzes": st.column_config.TextColumn("Megjegyzés", help="Szerkeszthető mező"),
        },
    )

    if st.button("Megjegyzések mentése"):
        df_for_compare = df_szurt.copy()
        edited_for_save = edited_df.copy()

        try:
            save_changes_bulk(df_for_compare, edited_for_save)
            st.rerun()
        except Exception as e:
            st.error(f"Hiba mentés közben: {e}")

else:
    st.stop()









