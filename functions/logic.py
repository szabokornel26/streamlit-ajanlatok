import pandas as pd
from typing import Callable

def generate_unique_id(projektnev: str, ajanlatkero: str) -> str:
    if pd.isna(projektnev):
        projektnev = ""
    if pd.isna(ajanlatkero):
        ajanlatkero = ""
    parts = projektnev.split(" ")
    truncated = " ".join(parts[:5])
    return f"{truncated} {ajanlatkero}".strip()

def save_changes_bulk(
    original_df: pd.DataFrame,
    edited_df: pd.DataFrame,
    upsert_fn: Callable[[str, str | None], None]
) -> int:
    key_col = "Egyedi_azonosito"
    original_df = original_df[[key_col, "Megjegyzes"]].drop_duplicates(subset=[key_col], keep="last")
    edited_df = edited_df[[key_col, "Megjegyzes"]].drop_duplicates(subset=[key_col], keep="last")

    orig = original_df.set_index(key_col)["Megjegyzes"].fillna("")
    edit = edited_df.set_index(key_col)["Megjegyzes"].fillna("")

    changed_mask = orig.ne(edit)
    changed_ids = orig.index[changed_mask].tolist()

    for key in changed_ids:
        val = edit.loc[key]
        val = None if pd.isna(val) or val == "" else str(val)
        upsert_fn(key, val)

    return len(changed_ids)
