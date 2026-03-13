from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import glob

# ---------------------------------------------------------
# ✅ DATA DIRECTORIES
# ---------------------------------------------------------
BASE_DIR = "/opt/airflow/data/NorthCarolina"

MATCHING_READY_DIR = os.path.join(BASE_DIR, "matching_ready")
MATCHING_RESULTS_DIR = os.path.join(BASE_DIR, "matching_results")

os.makedirs(MATCHING_RESULTS_DIR, exist_ok=True)

ALL_RECORDS_PATH = os.path.join(MATCHING_RESULTS_DIR, "all_records.csv")
SPLINK_PREDICTIONS_PATH = os.path.join(MATCHING_RESULTS_DIR, "splink_predictions.csv")
GOLDEN_ID_MAPPING_PATH = os.path.join(MATCHING_RESULTS_DIR, "golden_id_mapping.csv")
MULTI_COUNTY_PEOPLE_PATH = os.path.join(MATCHING_RESULTS_DIR, "multi_county_people.csv")

# ---------------------------------------------------------
# ✅ STAGE 1 — LOAD & CONSOLIDATE DATA
# ---------------------------------------------------------
def load_matching_ready_data():
    csv_files = glob.glob(os.path.join(MATCHING_READY_DIR, "*.csv"))

    if not csv_files:
        raise ValueError("No matching-ready files found.")

    dfs = []
    for f in csv_files:
        df = pd.read_csv(f, dtype="string")
        dfs.append(df)

    all_df = pd.concat(dfs, ignore_index=True)
    all_df = all_df.dropna(subset=["unique_id"])

    # -----------------------------------------------------
    # ✅ HARD LIMIT TO 5,000 RECORDS FOR MATCHING
    #    Recommended sizes:
    #    • 5,000   → Fast debugging & Docker-safe testing
    #    • 25,000  → Realistic proof-of-concept scale
    #    • 50,000+ → Near-production scale (ensure enough RAM)
    # -----------------------------------------------------
    TARGET_RECORDS = 25000

    if len(all_df) > TARGET_RECORDS:
        all_df = all_df.sample(
            n=TARGET_RECORDS,
            random_state=42
        )
        print(f"⚠️ Matching dataset force-limited to {TARGET_RECORDS:,} records")
    else:
        print(f"✅ Dataset already under {TARGET_RECORDS:,} records")

    all_df.to_csv(ALL_RECORDS_PATH, index=False)
    print(f"✅ Consolidated {len(all_df):,} records for matching")

# ---------------------------------------------------------
# ✅ STAGE 2 — RUN SPLINK MATCHING (CORRECT v4+ API)
# ---------------------------------------------------------
def run_splink_matching():
    import pandas as pd
    import gc

    # ✅ All Splink imports INSIDE the task (Airflow-safe)
    from splink import Linker, SettingsCreator, block_on, DuckDBAPI
    import splink.comparison_library as cl

    print("✅ Loading dataset (record count already limited upstream)...")

    # -----------------------------------------------------
    # ✅ 1️⃣ LOW-MEMORY PANDAS LOAD (NO ROW LIMIT HERE)
    # -----------------------------------------------------
    df = pd.read_csv(
        ALL_RECORDS_PATH,
        usecols=[
            "unique_id",
            "first_name",
            "middle_name",
            "last_name",
            "gender_code",
            "birth_year",
            "res_city_desc",
            "zip_code",
            "county_desc",
        ],
        dtype={
            "unique_id": "string",
            "first_name": "string",
            "middle_name": "string",
            "last_name": "string",
            "gender_code": "category",
            "zip_code": "category",
            "res_city_desc": "category",
            "county_desc": "category",
        },
        low_memory=False
    )

    # ✅ Safe numeric coercion after load
    df["birth_year"] = pd.to_numeric(df["birth_year"], errors="coerce").astype("Int16")

    print(f"✅ Loaded {len(df):,} rows for matching")
    print(f"✅ DataFrame memory usage: {df.memory_usage(deep=True).sum() / 1e6:.2f} MB")

    # -----------------------------------------------------
    # ✅ 2️⃣ SPLINK SETTINGS
    # -----------------------------------------------------
    settings = SettingsCreator(
        link_type="link_only",
        unique_id_column_name="unique_id",
        source_dataset_column_name="county_desc",

        comparisons=[
            cl.NameComparison("first_name"),
            cl.NameComparison("middle_name"),
            cl.NameComparison("last_name"),
            cl.ExactMatch("gender_code"),
            cl.ExactMatch("birth_year"),
            cl.ExactMatch("zip_code"),
            cl.LevenshteinAtThresholds("res_city_desc", 2),
        ],

        blocking_rules_to_generate_predictions=[
            block_on("birth_year", "first_name", "last_name"),
            block_on("zip_code"),
        ],

        retain_intermediate_calculation_columns=False,
    )

    # -----------------------------------------------------
    # ✅ 3️⃣ DUCKDB + LINKER
    # -----------------------------------------------------
    db_api = DuckDBAPI(connection=":memory:")
    linker = Linker(df, settings, db_api=db_api)

    # -----------------------------------------------------
    # ✅ 4️⃣ BASELINE MATCH PROBABILITY (MEMORY-SAFE)
    # -----------------------------------------------------
    print("✅ Estimating baseline match probability...")

    deterministic_rules = [
        block_on("first_name", "birth_year"),
        block_on("last_name"),
    ]

    linker.training.estimate_probability_two_random_records_match(
        deterministic_rules,
        recall=0.7
    )

    # -----------------------------------------------------
    # ✅ 5️⃣ EM TRAINING
    # -----------------------------------------------------
    print("✅ Running EM parameter estimation...")

    training_blocking_rule = block_on("birth_year", "last_name")

    linker.training.estimate_parameters_using_expectation_maximisation(
        training_blocking_rule
    )

    # -----------------------------------------------------
    # ✅ 6️⃣ PREDICTION
    # -----------------------------------------------------
    print("✅ Predicting matches...")

    predictions = linker.inference.predict()
    predictions_df = predictions.as_pandas_dataframe()

    predictions_df.to_csv(SPLINK_PREDICTIONS_PATH, index=False)
    print(f"✅ Saved Splink predictions to: {SPLINK_PREDICTIONS_PATH}")

    # -----------------------------------------------------
    # ✅ 7️⃣ CLEANUP
    # -----------------------------------------------------
    del df
    del predictions_df
    gc.collect()




# ---------------------------------------------------------
# ✅ STAGE 3 — CLUSTERING & GOLDEN ID ASSIGNMENT
# ---------------------------------------------------------
def build_clusters_and_golden_ids():
    all_df = pd.read_csv(ALL_RECORDS_PATH, dtype="string")
    preds = pd.read_csv(SPLINK_PREDICTIONS_PATH)

    # Keep strong matches only
    matches = preds[preds["match_probability"] >= 0.90]

    parent = {}

    def find(x):
        parent.setdefault(x, x)
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for uid in all_df["unique_id"]:
        parent[uid] = uid

    for _, row in matches.iterrows():
        union(row["unique_id_l"], row["unique_id_r"])

    clusters = {}
    for uid in all_df["unique_id"]:
        root = find(uid)
        clusters.setdefault(root, []).append(uid)

    golden_rows = []
    for i, (root, members) in enumerate(clusters.items(), start=1):
        for uid in members:
            golden_rows.append({"golden_id": i, "unique_id": uid})

    golden_df = pd.DataFrame(golden_rows)
    golden_df = golden_df.merge(all_df, on="unique_id", how="left")

    golden_df.to_csv(GOLDEN_ID_MAPPING_PATH, index=False)

    multi = (
        golden_df.groupby("golden_id")["county_desc"]
        .nunique()
        .reset_index(name="county_count")
    )
    multi = multi[multi["county_count"] > 1]
    multi = multi.merge(golden_df, on="golden_id", how="left")

    multi.to_csv(MULTI_COUNTY_PEOPLE_PATH, index=False)

    print(f"✅ Golden IDs created: {golden_df['golden_id'].nunique():,}")
    print(f"✅ Multi-county people: {multi['golden_id'].nunique():,}")

# ---------------------------------------------------------
# ✅ DAG DEFINITION
# ---------------------------------------------------------
with DAG(
    dag_id="entity_resolution",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["splink", "entity-resolution", "golden-id"],
) as dag:

    load_data_task = PythonOperator(
        task_id="load_matching_ready_data",
        python_callable=load_matching_ready_data,
    )

    splink_matching_task = PythonOperator(
        task_id="run_splink_matching",
        python_callable=run_splink_matching,
    )

    cluster_task = PythonOperator(
        task_id="build_clusters_and_golden_ids",
        python_callable=build_clusters_and_golden_ids,
    )

    load_data_task >> splink_matching_task >> cluster_task
