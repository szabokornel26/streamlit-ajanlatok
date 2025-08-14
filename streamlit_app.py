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
        m.megjegyzes AS Megjegyzes
    FROM
        `ajanlatok_dataset.projektlista` AS p
    LEFT JOIN
        `ajanlatok_dataset.ajanlatok` AS a ON p.azonosito = a.pjt_azonosito
    LEFT JOIN
        `ajanlatok_dataset.megjegyzesek` AS m ON p.azonosito = m.azonositok
    """
    df = client.query(query).result().to_dataframe()
    return df

# Megjegyzések beszúrása

def update_megjegyzes(pjt_azonosito, szoveg):
    table_id = "ajanlatok_dataset.megjegyzesek"
    rows_to_insert = [{"pjt_azonosito": pjt_azonosito, "megjegyzes": szoveg}]

    # Előző sor törlése (ha volt)
    client.query(f"""
        DELETE FROM `{table_id}` WHERE pjt_azonosito = '{pjt_azonosito}'
    """).result()

    # Új sor beszúrása
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        st.error(f"Hiba a mentésnél: {errors}")
    else:
        st.success("Megjegyzés mentve!")

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

    for idx, row in df_szurt.iterrows():
        st.markdown(f"### {row['Projektnev']} ({row['Samsung_szam']})")
        uj_megjegyzes = st.text_area(
            "Megjegyzés:",
            value=row["Megjegyzes"] or "",
            key=f"megj_{row['Projekt_azonosito']}"
        )
        if st.button("Mentés", key=f"save_{row['Projekt_azonosito']}"):
            update_megjegyzes(row["Projekt_azonosito"], uj_megjegyzes)
    
    st.dataframe(df_szurt, use_container_width=True)


else:
    st.stop()


