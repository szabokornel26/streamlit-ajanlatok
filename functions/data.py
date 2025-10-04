from google.cloud import bigquery
import pandas as pd

def get_data(client: bigquery.Client) -> pd.DataFrame:
    query = r"""
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
    return client.query(query).result().to_dataframe()

def upsert_megjegyzes(client: bigquery.Client, egyedi_azon: str, megjegyzes: str | None):
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
