import json
from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st

def get_bigquery_client():
    gcp_key_json = st.secrets["GCP_SERVICE_ACCOUNT_KEY"]
    gcp_key_dict = json.loads(gcp_key_json)
    credentials = service_account.Credentials.from_service_account_info(gcp_key_dict)
    return bigquery.Client(credentials=credentials, project=gcp_key_dict["project_id"])
