import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from functions.logic import generate_unique_id

def test_generate_unique_id_long_project_name():
    projektnev = "Ez egy nagyon hosszú projekt név amit tesztelünk"
    ajanlatkero = "Teszt Kft."
    result = generate_unique_id(projektnev, ajanlatkero)
    assert result == "Ez egy nagyon hosszú projekt Teszt Kft."

def test_generate_unique_id_short_project_name():
    projektnev = "Rövid projektnév"
    ajanlatkero = "Teszt Kft."
    result = generate_unique_id(projektnev, ajanlatkero)
    assert result == "Rövid projektnév Teszt Kft."

def test_generate_unique_id_empty_inputs():
    projektnev = ""
    ajanlatkero = ""
    result = generate_unique_id(projektnev, ajanlatkero)
    assert result == ""