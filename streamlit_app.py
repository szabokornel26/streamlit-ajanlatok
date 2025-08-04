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
        p.szam AS Samsung_szam,
        p.felelos AS Felelos,
        a.pjt_nev AS Projektnev,
        a.vegosszeg AS Vegosszeg,
        a.ajanlatkero AS Ajanlatkero,
        a.datum AS Ajanlatadas_datuma,
        a.keszito AS Keszito
    FROM
        `ajanlatok_dataset.projektlista` AS p
    LEFT JOIN
        `ajanlatok_dataset.ajanlatok` AS a ON p.azonosito = a.pjt_azonosito
    """
    df = client.query(query).result().to_dataframe()
    return df

if check_password():
    st.title("Kimenő ajánlatok")

        
    df = get_data()

    # Szűrők létrehozása
    valasztott_ajanlatkero = st.multiselect("Ajánlatkérő(k):", options=df["Ajanlatkero"].unique(), default=None)
    samsung_keres = st.text_input("Samsung_szam:")
    projektnev_szuro = st.text_input("Projektnev:")

    df_szurt = df.copy()

    if valasztott_ajanlatkero:
        df_szurt = df_szurt[df_szurt["Ajanlatkero"].isin(valasztott_ajanlatkero)]

    if samsung_keres:
        df_szurt = df_szurt[df_szurt["Samsung_szam"].str.contains(samsung_keres, case=False, na=False)]

    if projektnev_szuro:
        df_szurt = df_szurt[df_szurt["Projektnev"].str.contains(projektnev_szuro, case=False, na=False)]

    # Végösszeg oszlop formázása
    df_szurt["Vegosszeg"] = df_szurt["Vegosszeg"].apply(
        lambda x: f"{int(x):,}".replace(",", " ") if pd.notnull(x) else "-"
    )

    # Megjelenítés és találtszám jelzés
    st.write(f"Találatok száma: {len(df_szurt)}")
    st.dataframe(df_szurt, use_container_width=True)


else:
    st.stop()
