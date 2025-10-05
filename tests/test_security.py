import pytest
import streamlit as st
from streamlit_app import check_password

def test_check_password_correct(monkeypatch):
    monkeypatch.setattr(st, "secrets", {"PASSWORD": "titok123"})
    assert check_password("titok123") is True

def test_check_password_incorrect(monkeypatch):
    monkeypatch.setattr(st, "secrets", {"PASSWORD": "titok123"})
    assert check_password("rossz") is False

def test_check_password_empty(monkeypatch):
    monkeypatch.setattr(st, "secrets", {"PASSWORD": "titok123"})
    assert check_password("") is False
