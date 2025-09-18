# --- Module Purpose ---
# This application is a quotation tracking tool that:
# - retrieves quotation data from Google BigQuery,
# - allows users to add or update notes,
# - and saves changes back to BigQuery using an upsert logic.

import streamlit as st
import pandas as pd
import json
from google.oauth2 import service_account
from google.cloud import bigquery

# --- Authentication Setup ---
# Load service account credentials from Streamlit secrets
# and initialize the BigQuery client.

gcp_key_json = st.secrets["GCP_SERVICE_ACCOUNT_KEY"]
gcp_key_dict = json.loads(gcp_key_json) 
credentials = service_account.Credentials.from_service_account_info(gcp_key_dict)
client = bigquery.Client(credentials=credentials, project=gcp_key_dict["project_id"])

# --- Streamlit Page Configuration ---
# Configure the layout to use full width for better readability.

st.set_page_config(layout="wide")

# --- Simple Password Protection ---
# Require the user to enter the correct password in the sidebar
# before accessing the application.

PASSWORD = st.secrets["PASSWORD"]

# --- Password Check Function ---
# Displays a password input field in the sidebar.
# If the entered password does not match the stored one,
# an error message is shown and access is denied.

def check_password():
    pwd = st.sidebar.text_input("Jelszó:", type="password")
    empty_space = ""
    if pwd == empty_space:
        st.info("Írd be a jelszót!")
        return False
    elif pwd != PASSWORD:
        st.error("Hibás jelszó!")
        return False
    return True

# --- Retrieve Data from BigQuery ---
# Executes a SQL query that joins multiple tables:
# - project list
# - quotations
# - notes
# It returns the result as a Pandas DataFrame for further processing.

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

# --- Unique Identifier Generator ---
# Creates a reproducible key for each quotation by:
# - taking the first 5 words of the project name,
# - appending the client name.
# Used to match quotations with notes across tables.

def generate_unique_id(projektnev: str, ajanlatkero: str) -> str:
    if pd.isna(projektnev):
        projektnev = ""
    if pd.isna(ajanlatkero):
        ajanlatkero = ""
    parts = projektnev.split(" ")
    truncated = " ".join(parts[:5])  # Keep first 5 word
    return f"{truncated} {ajanlatkero}".strip()

# --- BigQuery Upsert for Notes ---
# Inserts or updates a note in the 'megjegyzesek' table.
# Uses MERGE to update if the record exists, or insert if not.

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

# --- Save Notes in Bulk ---
# Compares the original DataFrame with the edited DataFrame
# based on the unique identifier column ("Egyedi_azonosito").
# Detects changes in the 'Megjegyzes' column only.
# For each modified row:
#   - If a note was added or updated, it calls the upsert function.
#   - If no changes are detected, no database operation is performed.

def save_changes_bulk(original_df: pd.DataFrame, edited_df: pd.DataFrame):
    key_col = "Egyedi_azonosito"

    # Keep only the unique identifier and the 'Megjegyzes' column,
    # ensuring duplicates are removed to avoid conflicting updates.
    
    original_df = original_df[[key_col, "Megjegyzes"]].drop_duplicates(subset=[key_col], keep="last")
    edited_df = edited_df[[key_col, "Megjegyzes"]].drop_duplicates(subset=[key_col], keep="last")

    # Create Series indexed by the unique ID for both original and edited data.
    # Replace NaN values with empty strings to ensure proper comparison.
    
    orig = original_df.set_index(key_col)["Megjegyzes"].fillna("")
    edit = edited_df.set_index(key_col)["Megjegyzes"].fillna("")

    # Identify rows where the note text has changed.
    
    changed_mask = orig.ne(edit)
    changed_ids = orig.index[changed_mask].tolist()

    if not changed_ids:
        st.info("Nincs mentendő változás (Az egyedi azonosító hiányozhat).")
        return

    # Loop through all modified rows and upsert each note into BigQuery.
    
    for key in changed_ids:
        val = edit.loc[key]
        val = None if pd.isna(val) or val == "" else str(val)
        upsert_megjegyzes(key, val)

    st.success(f"Sikeres mentés: {len(changed_ids)} sor frissítve.")

# --- Main Streamlit Interface ---
# Only displayed if the user enters the correct password.
# Loads the quotation data and sets up filters and editable table.

if check_password():
    st.title("Kimenő ajánlatok")

    # Retrieve all quotation data from BigQuery.
    
    df = get_data()

    # "Ajanlatadas datuma" changed to datetime
    df["Ajanlatadas_datuma"] = pd.to_datetime(df["Ajanlatadas_datuma"], errors="coerce")
    
    # Generate a unique identifier for each row immediately after data retrieval.
    # This will be used to match notes with quotations.
    
    df["Egyedi_azonosito"] = df.apply(
        lambda row: generate_unique_id(row["Projektnev"], row["Ajanlatkero"]),
        axis=1
    )

    # --- Filters ---
    # Allow the user to filter quotations by:
    # - client(s)
    # - Samsung number
    # - project name
    # - creator(s)
    
    valasztott_ajanlatkero = st.multiselect("Ajánlatkérő(k):", options=df["Ajanlatkero"].unique(), default=None)
    samsung_keres = st.text_input("Samsung szám:")
    projektnev_szuro = st.text_input("Projektnév:")
    valasztott_keszito = st.multiselect("Készítő(k):", options=df["Keszito"].unique(), default=None)

    
    min_date = df["Ajanlatadas_datuma"].min().date()
    max_date = df["Ajanlatadas_datuma"].max().date()
    
    
    datum_szuro = st.date_input(
        "Ajánlatadás dátum szűrő:",
        value=(min_date, max_date),  # tuple -> interval
        min_value=min_date,
        max_value=max_date
    )
    
    # Create a filtered DataFrame to apply user-selected filters.
    
    df_szurt = df.copy()

    # Apply each filter conditionally if the user has selected any options.
    
    if valasztott_keszito:
        df_szurt = df_szurt[df_szurt["Keszito"].isin(valasztott_keszito)]
    
    if valasztott_ajanlatkero:
        df_szurt = df_szurt[df_szurt["Ajanlatkero"].isin(valasztott_ajanlatkero)]

    if samsung_keres:
        df_szurt = df_szurt[df_szurt["Samsung_szam"].str.contains(samsung_keres, case=False, na=False)]

    if projektnev_szuro:
        df_szurt = df_szurt[df_szurt["Projektnev"].str.contains(projektnev_szuro, case=False, na=False)]

    import datetime

    if datum_szuro:

        if isinstance(datum_szuro, (list, tuple)) and len(datum_szuro) == 2:
            start, end = datum_szuro
            df_szurt = df_szurt[
                (df_szurt["Ajanlatadas_datuma"].dt.date >= start) &
                (df_szurt["Ajanlatadas_datuma"].dt.date <= end)
            ]
        else:

            df_szurt = df_szurt[df_szurt["Ajanlatadas_datuma"].dt.date == datum_szuro]
    
    # Sort the filtered DataFrame by quotation date ascending.
    # Null dates are placed first.
    
    df_szurt = df_szurt.sort_values(by="Ajanlatadas_datuma", ascending=False, na_position="last")

 

    # Display the total number of filtered results.
    
    st.write(f"Találatok száma: {len(df_szurt)}")

    # --- Editable Table ---
    # Use Streamlit's data_editor to allow editing of the 'Megjegyzes' column.
    # All other columns are read-only.
    
    edited_df = st.data_editor(
        df_szurt,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Egyedi_azonosito": st.column_config.TextColumn("Egyedi azonosító", disabled=True), 
            "Projekt_azonosito": st.column_config.TextColumn("Projekt azonosító", disabled=True),
            "Samsung_szam": st.column_config.TextColumn("Samsung szám", disabled=True),
            "Felelos": st.column_config.TextColumn("Felelős", disabled=True),
            "Projektnev": st.column_config.TextColumn("Projekt név", disabled=True),
            "Vegosszeg": st.column_config.NumberColumn("Végösszeg (HUF)", disabled=True, step=1, min_value=0),
            "Ajanlatkero": st.column_config.TextColumn("Ajánlatkérő", disabled=True),
            "Ajanlatadas_datuma": st.column_config.DateColumn("Ajánlatadás dátuma", disabled=True),
            "Keszito": st.column_config.TextColumn("Készítő", disabled=True),
            "Megjegyzes": st.column_config.TextColumn("Megjegyzés", help="Szerkeszthető mező, csak ahol létezik egyedi azonosító"),
        },
    )

    # --- Save Button ---
    # When clicked, compare edited data with original,
    # and save changes to BigQuery using the bulk save function.
    # If an error occurs during saving, display an error message.
    
    if st.button("Megjegyzések mentése"):
        df_for_compare = df_szurt.copy()
        edited_for_save = edited_df.copy()

        try:
            save_changes_bulk(df_for_compare, edited_for_save)
            st.rerun()
        except Exception as e:
            st.error(f"Hiba mentés közben: {e}")

# Stop execution if the password check failed.

else:
    st.stop()








