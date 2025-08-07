# Könyvtárak importálása
import streamlit as st
import pandas as pd
import json
from google.oauth2 import service_account
from google.cloud import bigquery

# GCP kulcs és jelszó beolvasása
gcp_key_json = st.secrets["GCP_SERVICE_ACCOUNT_KEY"]
gcp_key_dict = json.loads(gcp_key_json)
PASSWORD = st.secrets["PASSWORD"]

credentials = service_account.Credentials.from_service_account_info(gcp_key_dict)
client = bigquery.Client(credentials=credentials, project=gcp_key_dict["project_id"])

st.set_page_config(layout="wide")


# Jelszó check
def check_password():
    pwd = st.sidebar.text_input("Jelszó:", type="password")
    if pwd != PASSWORD:
        st.error("Hibás jelszó!")
        return False
    return True

# Fő adatlekérdezés + megjegyzések join
@st.cache_data(ttl=600)
def get_data():
    query = """
    SELECT
        p.szam AS Samsung_szam,
        p.felelos AS Felelos,
        a.pjt_nev AS Projektnev,
        a.vegosszeg AS Vegosszeg,
        a.ajanlatkero AS Ajanlatkero,
        a.datum AS Ajanlatadas_datuma,
        a.keszito AS Keszito,
        a.pjt_nev || ' ' || a.ajanlatkero AS raw_azonosito
    FROM
        `ajanlatok_dataset.projektlista` AS p
    LEFT JOIN
        `ajanlatok_dataset.ajanlatok` AS a ON p.azonosito = a.pjt_azonosito
    """
    df = client.query(query).result().to_dataframe()

    # Egyedi azonosító generálása (projektnév 5. szóköz után vágva + ajánlatkérő)
    def generate_azonosito(row):
        nev = row["Projektnev"]
        parts = nev.split(" ")
        truncated_nev = " ".join(parts[:5])
        return f"{truncated_nev} {row['Ajanlatkero']}" if pd.notnull(row["Ajanlatkero"]) else truncated_nev

    df["azonosito"] = df.apply(generate_azonosito, axis=1)

    # Megjegyzések join
    try:
        megj_df = client.query("SELECT * FROM `ajanlatok_dataset.megjegyzesek`").result().to_dataframe()
        df = df.merge(megj_df, how="left", on="azonosito")
    except:
        df["megjegyzes"] = ""

    return df

# Megjegyzések mentése BigQuery-be
def save_megjegyzesek(megjegyzesek_dict):
    rows = [{"azonosito": k, "megjegyzes": v} for k, v in megjegyzesek_dict.items()]

    table_id = "ajanlatok_dataset.megjegyzesek"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # teljes felülírás
        schema=[
            bigquery.SchemaField("azonosito", "STRING"),
            bigquery.SchemaField("megjegyzes", "STRING"),
        ],
    )

    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()

if check_password():
    st.title("Kimenő ajánlatok")

    df = get_data()

    # Szűrők
    valasztott_ajanlatkero = st.multiselect("Ajánlatkérő(k):", options=df["Ajanlatkero"].dropna().unique(), default=None)
    samsung_keres = st.text_input("Samsung_szam:")
    projektnev_szuro = st.text_input("Projektnev:")

    df_szurt = df.copy()

    if valasztott_ajanlatkero:
        df_szurt = df_szurt[df_szurt["Ajanlatkero"].isin(valasztott_ajanlatkero)]

    if samsung_keres:
        df_szurt = df_szurt[df_szurt["Samsung_szam"].str.contains(samsung_keres, case=False, na=False)]

    if projektnev_szuro:
        df_szurt = df_szurt[df_szurt["Projektnev"].str.contains(projektnev_szuro, case=False, na=False)]

    # Végösszeg formázás
    df_szurt["Vegosszeg"] = df_szurt["Vegosszeg"].apply(
        lambda x: f"{int(x):,}".replace(",", " ") if pd.notnull(x) else "-"
    )

    # Megjegyzések szerkesztése
    st.write(f"Találatok száma: {len(df_szurt)}")
    megjegyzesek_dict = {}
    for idx, row in df_szurt.iterrows():
        st.markdown("---")
        col1, col2 = st.columns([3, 2])
        with col1:
            st.write(f"**{row['Projektnev']}** – {row['Ajanlatkero']}")
        with col2:
            default_text = row["megjegyzes"] if pd.notnull(row["megjegyzes"]) else ""
            updated = st.text_input(f"✏️ Megjegyzés:", value=default_text, key=row["azonosito"])
            megjegyzesek_dict[row["azonosito"]] = updated

    if st.button("💾 Megjegyzések mentése"):
        save_megjegyzesek(megjegyzesek_dict)
        st.success("Megjegyzések elmentve! Frissítsd az oldalt a változások megtekintéséhez.")

else:
    st.stop()


