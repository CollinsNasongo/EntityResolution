from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime
import pandas as pd
import os
import glob
import requests
import zipfile
import shutil

# ---------------------------------------------------------
# ✅ DATA DIRECTORIES (OUTSIDE dags/)
# ---------------------------------------------------------
BASE_DIR = "/opt/airflow/data/NorthCarolina"
RAW_DIR = os.path.join(BASE_DIR, "raw_files")
STANDARDIZED_DIR = os.path.join(BASE_DIR, "standardized")
CLEANED_DIR = os.path.join(BASE_DIR, "cleaned")
MATCHING_READY_DIR = os.path.join(BASE_DIR, "matching_ready")

for d in [RAW_DIR, STANDARDIZED_DIR, CLEANED_DIR, MATCHING_READY_DIR]:
    os.makedirs(d, exist_ok=True)

# ---------------------------------------------------------
# ✅ STAGE 0 — DOWNLOAD + UNZIP
# ---------------------------------------------------------
DOWNLOAD_URLS = [
    "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter13.zip",
    "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter84.zip",
]

def download_and_extract_raw_files():
    if not DOWNLOAD_URLS:
        print("⚠️ No download URLs configured yet.")
        return

    for url in DOWNLOAD_URLS:
        zip_filename = os.path.basename(url)
        zip_path = os.path.join(RAW_DIR, zip_filename)

        # ✅ Download ZIP if not already present
        if not os.path.exists(zip_path):
            print(f"⬇️ Downloading ZIP: {zip_filename}")
            response = requests.get(url, timeout=120)
            response.raise_for_status()

            with open(zip_path, "wb") as f:
                f.write(response.content)

            print(f"✅ Downloaded: {zip_path}")
        else:
            print(f"⏭️ ZIP already exists: {zip_filename}")

        # ✅ Extract TXT from ZIP
        print(f"📦 Extracting ZIP: {zip_filename}")

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            for member in zip_ref.namelist():
                if member.lower().endswith(".txt"):
                    extracted_path = zip_ref.extract(member, RAW_DIR)

                    final_txt_path = os.path.join(
                        RAW_DIR,
                        os.path.basename(member)
                    )

                    shutil.move(extracted_path, final_txt_path)
                    print(f"✅ Extracted TXT: {final_txt_path}")

        # ✅ Cleanup ZIP (recommended)
        os.remove(zip_path)
        print(f"🗑️ Removed ZIP: {zip_filename}")

# ---------------------------------------------------------
# ✅ STAGE 1 — CHECK FOR RAW FILES
# ---------------------------------------------------------
def raw_files_exist():
    txt_files = glob.glob(os.path.join(RAW_DIR, "*.txt"))
    if not txt_files:
        print("⏳ No raw TXT files found.")
        return False

    print(f"✅ Found {len(txt_files)} raw file(s).")
    return True

# ---------------------------------------------------------
# ✅ STAGE 2 — STANDARDIZE STRUCTURE (ENCODING + COLUMN SELECTION)
# ---------------------------------------------------------
def standardize_structure():
    txt_files = glob.glob(os.path.join(RAW_DIR, "*.txt"))

    for txt_file in txt_files:
        dataset_name = os.path.splitext(os.path.basename(txt_file))[0]
        output_file = os.path.join(STANDARDIZED_DIR, f"{dataset_name}.csv")

        if os.path.exists(output_file):
            print(f"⏭️ Already standardized: {dataset_name}")
            continue

        print(f"✅ Standardizing: {dataset_name}")

        try:
            df = pd.read_csv(
                txt_file, sep="\t", quotechar='"',
                dtype=str, encoding="utf-8", low_memory=False
            )
        except UnicodeDecodeError:
            df = pd.read_csv(
                txt_file, sep="\t", quotechar='"',
                dtype=str, encoding="latin-1", low_memory=False
            )

        er_columns = [
            "first_name",
            "middle_name",
            "last_name",
            "gender_code",
            "birth_year",
            "res_city_desc",
            "zip_code",
            "county_desc",
        ]

        df = df[er_columns]
        df.to_csv(output_file, index=False)
        print(f"✅ Standardized output: {output_file}")

# ---------------------------------------------------------
# ✅ STAGE 3 — CLEAN & NORMALIZE (TRUE NULLS + FORMAT FIXES)
# ---------------------------------------------------------
def clean_and_normalize():
    csv_files = glob.glob(os.path.join(STANDARDIZED_DIR, "*.csv"))

    for csv_file in csv_files:
        dataset_name = os.path.splitext(os.path.basename(csv_file))[0]
        output_file = os.path.join(CLEANED_DIR, f"{dataset_name}.csv")

        if os.path.exists(output_file):
            print(f"⏭️ Already cleaned: {dataset_name}")
            continue

        print(f"✅ Cleaning & normalizing: {dataset_name}")
        df = pd.read_csv(csv_file, dtype="string")

        for col in df.columns:
            df[col] = (
                df[col]
                .astype("string")
                .str.strip()
                .str.lower()
                .replace({
                    "": pd.NA,
                    "nan": pd.NA,
                    "none": pd.NA,
                    "null": pd.NA,
                    "na": pd.NA
                })
            )

        # ✅ Remove titles
        for name_col in ["first_name", "middle_name", "last_name"]:
            df[name_col] = df[name_col].str.replace(
                r"^(mr|mrs|ms|dr)\s+", "", regex=True
            )

        # ✅ Remove special characters
        for name_col in ["first_name", "middle_name", "last_name"]:
            df[name_col] = df[name_col].str.replace(
                r"[^a-z\s]", "", regex=True
            )

        # ✅ Birth year numeric
        df["birth_year"] = pd.to_numeric(df["birth_year"], errors="coerce")

        # ✅ ZIP standardisation
        df["zip_code"] = df["zip_code"].str.extract(r"(\d{5})")

        df.to_csv(output_file, index=False)
        print(f"✅ Cleaned output: {output_file}")

# ---------------------------------------------------------
# ✅ STAGE 4 — ADD UNIQUE IDS (SPLINK-READY)
# ---------------------------------------------------------
def add_unique_ids():
    csv_files = glob.glob(os.path.join(CLEANED_DIR, "*.csv"))

    for csv_file in csv_files:
        dataset_name = os.path.splitext(os.path.basename(csv_file))[0]
        output_file = os.path.join(MATCHING_READY_DIR, f"{dataset_name}.csv")

        if os.path.exists(output_file):
            print(f"⏭️ Already enriched: {dataset_name}")
            continue

        print(f"✅ Adding unique IDs: {dataset_name}")
        df = pd.read_csv(csv_file, dtype="string")

        df.insert(
            0,
            "unique_id",
            [f"{dataset_name}_{i+1}" for i in range(len(df))]
        )

        df.to_csv(output_file, index=False)
        print(f"✅ Final matching-ready file: {output_file}")
        print(f"✅ Rows: {len(df):,}")

# ---------------------------------------------------------
# ✅ DAG DEFINITION
# ---------------------------------------------------------
with DAG(
    dag_id="get_nc_county_voter_data",
    start_date=datetime(2024, 1, 1),
    schedule="*/60 * * * *",
    catchup=False,
    tags=["entity-resolution", "splink", "automation"],
) as dag:

    download_task = PythonOperator(
        task_id="download_and_extract_raw_files",
        python_callable=download_and_extract_raw_files,
    )

    check_for_files = ShortCircuitOperator(
        task_id="check_for_raw_files",
        python_callable=raw_files_exist,
    )

    standardize_task = PythonOperator(
        task_id="standardize_structure",
        python_callable=standardize_structure,
    )

    clean_task = PythonOperator(
        task_id="clean_and_normalize",
        python_callable=clean_and_normalize,
    )

    id_task = PythonOperator(
        task_id="add_unique_ids",
        python_callable=add_unique_ids,
    )

    download_task >> check_for_files >> standardize_task >> clean_task >> id_task
