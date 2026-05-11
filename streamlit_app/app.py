import os
import boto3
import pandas as pd
import streamlit as st
from botocore.client import Config

st.set_page_config(page_title="BDM Project Dashboard", layout="wide")

st.title("Historical Conversational AI — Data Lakehouse Demo")
st.write("Visible dashboard for inspecting the Bronze Landing Zone in MinIO.")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "landing-zone")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

st.sidebar.header("Connection")
st.sidebar.write(f"Bucket: `{MINIO_BUCKET}`")
st.sidebar.write(f"Endpoint: `{MINIO_ENDPOINT}`")

try:
    response = s3.list_objects_v2(Bucket=MINIO_BUCKET)
    objects = response.get("Contents", [])

    if not objects:
        st.warning("No files found yet. Run the Airflow ingestion DAG first.")
    else:
        df = pd.DataFrame(objects)
        df["Size_MB"] = df["Size"] / (1024 * 1024)

        st.metric("Files in Landing Zone", len(df))
        st.metric("Total Size MB", round(df["Size_MB"].sum(), 2))

        st.subheader("Landing Zone Objects")
        st.dataframe(df[["Key", "Size_MB", "LastModified"]], use_container_width=True)

        st.subheader("Files by Source Folder")
        df["Source"] = df["Key"].apply(lambda x: x.split("/")[0])
        counts = df.groupby("Source").size().reset_index(name="Files")
        st.bar_chart(counts.set_index("Source"))

except Exception as e:
    st.error("Could not connect to MinIO.")
    st.exception(e)