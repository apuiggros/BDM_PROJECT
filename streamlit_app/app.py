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

import glob

st.header("Streaming Mentions — 1 Minute Aggregates")

STREAMING_PATH = "/repo/trusted/streaming/fact_mentions_1m"

parquet_files = glob.glob(f"{STREAMING_PATH}/*.parquet")

if not parquet_files:
    st.warning("No streaming aggregate files found yet.")
else:
    df_stream = pd.read_parquet(STREAMING_PATH)

    st.metric("Streaming Aggregate Rows", len(df_stream))

    st.dataframe(df_stream, use_container_width=True)

    st.subheader("Mentions by Character")
    mentions_by_character = (
        df_stream.groupby("character_name")["mention_count"]
        .sum()
        .sort_values(ascending=False)
    )

    st.bar_chart(mentions_by_character)
    
import glob

st.header("Streaming Mentions Analytics")

STREAMING_PATH = "/repo/trusted/streaming/fact_mentions_1m"

parquet_files = glob.glob(f"{STREAMING_PATH}/*.parquet")

if parquet_files:

    df_stream = pd.read_parquet(STREAMING_PATH)

    st.metric("Aggregated Rows", len(df_stream))

    st.dataframe(df_stream, use_container_width=True)

    mentions_chart = (
        df_stream.groupby("character_name")["mention_count"]
        .sum()
        .sort_values(ascending=False)
    )

    st.subheader("Mentions by Character")

    st.bar_chart(mentions_chart)

else:
    st.warning("No streaming parquet files detected.")