import streamlit as st

def check_password(password: str) -> bool:
    expected = st.secrets["PASSWORD"]
    if password == "":
        return False
    return password == expected
