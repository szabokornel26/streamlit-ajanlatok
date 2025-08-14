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
        m.megjegyzesek AS Megjegyzes
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

# Upsert 1 sorra (MERGE) ---
def upsert_megjegyzes(pjt_azonosito: str, megjegyzes):
    merge_sql = """
    MERGE `ajanlatok_dataset.megjegyzesek` T
    USING (SELECT @pjt_azonosito AS pjt_azonosito, @megjegyzes AS megjegyzes) S
    ON T.azonositok = S.pjt_azonosito
    WHEN MATCHED THEN
      UPDATE SET megjegyzesek = S.megjegyzes
    WHEN NOT MATCHED THEN
      INSERT (azonositok, megjegyzesek) VALUES (S.pjt_azonosito, S.megjegyzes)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("pjt_azonosito", "STRING", str(pjt_azonosito)),
            bigquery.ScalarQueryParameter("megjegyzes", "STRING", megjegyzes),
        ]
    )
    client.query(merge_sql, job_config=job_config).result()


# --- Tömeges mentés: csak a változott sorokat írjuk vissza ---
def save_changes_bulk(original_df: pd.DataFrame, edited_df: pd.DataFrame):
    # indexeljünk kulccsal, és csak a Megjegyzes oszlopot hasonlítsuk
    orig = original_df.set_index("Projekt_azonosito")["Megjegyzes"].fillna("")
    edit = edited_df.set_index("Projekt_azonosito")["Megjegyzes"].fillna("")
    changed_mask = orig.ne(edit).fillna(False)
    changed_ids = edit.index[changed_mask].tolist()

    if len(changed_ids) == 0:
        st.info("Nincs mentendő változás.")
        return


    # upsert soronként (tipikusan kevés lesz egyszerre)
    for pid in changed_ids:
        upsert_megjegyzes(pid, edit.loc[pid])

    st.success(f"Sikeres mentés: {len(changed_ids)} sor frissítve.")

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

    # --- TÁBLÁN BELÜLI SZERKESZTÉS ---
    # csak a Megjegyzes legyen szerkeszthető; a kulcs és többi oszlop zárolt
    edited_df = st.data_editor(
        df_szurt,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Projekt_azonosito": st.column_config.TextColumn("Projekt azonosító", help="Belső azonosító", disabled=True),
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

    # --- Mentés gomb: csak megváltozott Megjegyzes értékeket írjuk vissza ---
    if st.button("Változtatások mentése BigQuery-be"):
        # FIGYELEM: a mentéshez az eredeti (nem formázott) df_szurt kell, hogy pontosan hasonlítsunk!
        # ezért visszarakjuk a szerkesztett Megjegyzes oszlopot a nem formázott df_szurt-ra
        df_for_compare = df_szurt.copy()
        df_for_compare = df_for_compare[["Projekt_azonosito", "Megjegyzes"]].copy()

        edited_for_save = edited_df.copy()
        edited_for_save = edited_for_save[["Projekt_azonosito", "Megjegyzes"]].copy()

        try:
            save_changes_bulk(df_for_compare, edited_for_save)
            # opcionálisan frissítés
            st.rerun()
        except Exception as e:
            st.error(f"Hiba mentés közben: {e}")

else:
    st.stop()







