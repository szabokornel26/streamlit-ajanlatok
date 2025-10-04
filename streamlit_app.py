import streamlit as st
import pandas as pd

from auth import get_bigquery_client
from functions.security import check_password
from functions.data import get_data, upsert_megjegyzes
from functions.logic import generate_unique_id, save_changes_bulk


def main():
    st.set_page_config(page_title="Sales Dashboard", page_icon="📈", layout="wide")

    # --- Jelszóellenőrzés ---
    password_input = st.sidebar.text_input("Jelszó:", type="password")
    if not check_password(password_input):
        st.info("Írd be a jelszót a belépéshez.")
        st.stop()

    st.title("Kimenő ajánlatok")

    # --- BigQuery kliens létrehozása ---
    client = get_bigquery_client()

    # --- Adatok lekérése ---
    df = get_data(client)
    df["Ajanlatadas_datuma"] = pd.to_datetime(df["Ajanlatadas_datuma"], errors="coerce")
    df["Egyedi_azonosito"] = df.apply(
        lambda row: generate_unique_id(row["Projektnev"], row["Ajanlatkero"]), axis=1
    )

    # --- Szűrők ---
    valasztott_ajanlatkero = st.multiselect(
        "Ajánlatkérő(k):",
        options=df["Ajanlatkero"].dropna().unique(),
        placeholder="Válassz ajánlatkérő(ke)t!",
    )
    samsung_keres = st.text_input("Samsung szám:")
    projektnev_szuro = st.text_input("Projektnév:")
    valasztott_keszito = st.multiselect(
        "Készítő(k):",
        options=df["Keszito"].dropna().unique(),
        placeholder="Válassz készítő(ke)t!",
    )

    min_date = df["Ajanlatadas_datuma"].min().date()
    max_date = df["Ajanlatadas_datuma"].max().date()
    datum_szuro = st.date_input(
        "Ajánlatadás dátum szűrő:",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    min_vegosszeg = int(df["Vegosszeg"].min())
    max_vegosszeg = int(df["Vegosszeg"].max())
    vegosszeg_range = st.slider(
        "Végösszeg szűrő:",
        min_value=min_vegosszeg,
        max_value=max_vegosszeg,
        value=(min_vegosszeg, max_vegosszeg),
        step=1000000,
        format="%d",
    )

    # --- Szűrés alkalmazása ---
    df_szurt = df.copy()
    if valasztott_keszito:
        df_szurt = df_szurt[df_szurt["Keszito"].isin(valasztott_keszito)]
    if valasztott_ajanlatkero:
        df_szurt = df_szurt[df_szurt["Ajanlatkero"].isin(valasztott_ajanlatkero)]
    if samsung_keres:
        df_szurt = df_szurt[
            df_szurt["Samsung_szam"].str.contains(samsung_keres, case=False, na=False)
        ]
    if projektnev_szuro:
        df_szurt = df_szurt[
            df_szurt["Projektnev"].str.contains(projektnev_szuro, case=False, na=False)
        ]
    if datum_szuro and isinstance(datum_szuro, (list, tuple)) and len(datum_szuro) == 2:
        start, end = datum_szuro
        df_szurt = df_szurt[
            (df_szurt["Ajanlatadas_datuma"].dt.date >= start)
            & (df_szurt["Ajanlatadas_datuma"].dt.date <= end)
        ]
    if vegosszeg_range:
        lower, upper = vegosszeg_range
        df_szurt = df_szurt[
            (df_szurt["Vegosszeg"] >= lower) & (df_szurt["Vegosszeg"] <= upper)
        ]

    df_szurt = df_szurt.sort_values(
        by="Ajanlatadas_datuma", ascending=False, na_position="last"
    )

    st.write(f"Találatok száma: {len(df_szurt)}")

    # --- Szerkeszthető tábla ---
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
            "Vegosszeg": st.column_config.NumberColumn("Végösszeg (HUF)", disabled=True, format="accounting", step=1),
            "Ajanlatkero": st.column_config.TextColumn("Ajánlatkérő", disabled=True),
            "Ajanlatadas_datuma": st.column_config.DateColumn("Ajánlatadás dátuma", disabled=True),
            "Keszito": st.column_config.TextColumn("Készítő", disabled=True),
            "Megjegyzes": st.column_config.TextColumn("Megjegyzés", help="Szerkeszthető mező, csak ahol létezik egyedi azonosító"),
        },
    )

    # --- Mentés gomb ---
    if st.button("Megjegyzések mentése"):
        try:
            changes = save_changes_bulk(
                original_df=df_szurt,
                edited_df=edited_df,
                upsert_fn=lambda key, note: upsert_megjegyzes(client, key, note)
            )
            if changes > 0:
                st.success(f"Sikeres mentés: {changes} sor frissítve.")
                st.rerun()
            else:
                st.info("Nincs mentendő változás.")
        except Exception as e:
            st.error(f"Hiba mentés közben: {e}")


if __name__ == "__main__":
    main()
