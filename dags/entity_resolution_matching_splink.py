from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import glob

# ---------------------------------------------------------
# DATA DIRECTORIES
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
# STAGE 1 — LOAD & CONSOLIDATE DATA
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

    TARGET_RECORDS = 25000

    if len(all_df) > TARGET_RECORDS:
        all_df = all_df.sample(
            n=TARGET_RECORDS,
            random_state=42
        )
        print(f"Dataset limited to {TARGET_RECORDS:,} records")
    else:
        print(f"Dataset already under {TARGET_RECORDS:,}")

    run_timestamp = pd.Timestamp.utcnow()
    all_df["pipeline_run_timestamp"] = run_timestamp

    all_df.to_csv(ALL_RECORDS_PATH, index=False)

    print(f"Consolidated {len(all_df):,} records for matching")


# ---------------------------------------------------------
# STAGE 2 — RUN SPLINK MATCHING
# ---------------------------------------------------------
def run_splink_matching():

    import gc
    from splink import Linker, SettingsCreator, block_on, DuckDBAPI
    import splink.comparison_library as cl

    print("Loading dataset...")

    df = pd.read_csv(
        ALL_RECORDS_PATH,
        usecols=[
            "unique_id",
            "first_name",
            "middle_name",
            "last_name",
            "gender_code",
            "birth_year",
            "county_desc",
            "pipeline_run_timestamp"
        ],
        dtype={
            "unique_id": "string",
            "first_name": "string",
            "middle_name": "string",
            "last_name": "string",
            "gender_code": "category",
            "county_desc": "category",
        },
        low_memory=False
    )

    df["birth_year"] = pd.to_numeric(df["birth_year"], errors="coerce").astype("Int16")

    # -----------------------------------------------------
    # NORMALIZE NAMES
    # -----------------------------------------------------
    df["first_name"] = df["first_name"].str.lower().fillna("")
    df["middle_name"] = df["middle_name"].str.lower().fillna("")
    df["last_name"] = df["last_name"].str.lower().fillna("")

    # -----------------------------------------------------
    # CREATE FULL NAME + FIRST INITIAL
    # -----------------------------------------------------
    df["full_name"] = (
        df["first_name"].fillna("") + " " +
        df["middle_name"].fillna("") + " " +
        df["last_name"].fillna("")
    ).str.replace(r"\s+", " ", regex=True).str.strip()

    df["first_initial"] = df["first_name"].str[0].fillna("")

    print(f"Loaded {len(df):,} rows for matching")

    # -----------------------------------------------------
    # SPLINK SETTINGS
    # -----------------------------------------------------
    settings = SettingsCreator(
        link_type="link_only",
        unique_id_column_name="unique_id",
        source_dataset_column_name="county_desc",

        comparisons=[

            # -------------------------
            # FIRST NAME
            # -------------------------
            cl.JaroWinklerAtThresholds(
                "first_name",
                [0.95, 0.90]
            ),

            # -------------------------
            # MIDDLE NAME (often missing)
            # -------------------------
            cl.JaroWinklerAtThresholds(
                "middle_name",
                [0.90]
            ),

            # -------------------------
            # LAST NAME (strong signal)
            # -------------------------
            cl.ExactMatch("last_name"),

            cl.JaroWinklerAtThresholds(
                "last_name",
                [0.97, 0.92]
            ),

            # -------------------------
            # FULL NAME
            # -------------------------
            cl.JaroWinklerAtThresholds(
                "full_name",
                [0.92]
            ),

            # -------------------------
            # DEMOGRAPHICS
            # -------------------------
            cl.ExactMatch("birth_year"),

            cl.ExactMatch("gender_code"),
        ],

        blocking_rules_to_generate_predictions=[

            block_on("birth_year", "first_name", "last_name"),

            block_on("birth_year", "first_name"),

        ],

        retain_intermediate_calculation_columns=True,
    )

    db_api = DuckDBAPI(connection=":memory:")
    linker = Linker(df, settings, db_api=db_api)

    print("Estimating baseline match probability...")

    deterministic_rules = [
        block_on("first_name", "birth_year"),
        block_on("last_name"),
    ]

    linker.training.estimate_probability_two_random_records_match(
        deterministic_rules,
        recall=0.7
    )

    print("Running EM parameter estimation...")

    training_blocking_rule = block_on("birth_year", "last_name")

    linker.training.estimate_parameters_using_expectation_maximisation(
        training_blocking_rule
    )

    print("Predicting matches...")

    predictions = linker.inference.predict(
        threshold_match_probability=0.92
    )

    predictions_df = predictions.as_pandas_dataframe()

    predictions_df["pipeline_run_timestamp"] = pd.Timestamp.utcnow()

    predictions_df.to_csv(SPLINK_PREDICTIONS_PATH, index=False)

    print(f"Saved Splink predictions to: {SPLINK_PREDICTIONS_PATH}")

    del df
    del predictions_df
    gc.collect()


# ---------------------------------------------------------
# STAGE 3 — CLUSTERING & GOLDEN ID ASSIGNMENT
# ---------------------------------------------------------
def build_clusters_and_golden_ids():

    all_df = pd.read_csv(ALL_RECORDS_PATH, dtype="string")
    preds = pd.read_csv(SPLINK_PREDICTIONS_PATH)

    preds["birth_year_l"] = pd.to_numeric(preds["birth_year_l"], errors="coerce")
    preds["birth_year_r"] = pd.to_numeric(preds["birth_year_r"], errors="coerce")

    print(f"Total candidate pairs: {len(preds):,}")

    # -----------------------------------------------------
    # STRICT MATCH FILTER
    # -----------------------------------------------------
    matches = preds[
        (preds["match_probability"] >= 0.93)
        & (preds["gender_code_l"] == preds["gender_code_r"])
        & (preds["birth_year_l"] == preds["birth_year_r"])
        & (preds["gamma_last_name"] > 0)
        ]

    print(f"Candidate pairs after filtering: {len(matches):,}")

    parent = {}

    def find(x):
        parent.setdefault(x, x)
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(a, b):
        ra = find(a)
        rb = find(b)
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
            golden_rows.append({
                "golden_id": i,
                "unique_id": uid
            })

    golden_df = pd.DataFrame(golden_rows)

    golden_df = golden_df.merge(all_df, on="unique_id", how="left")

    golden_df["pipeline_run_timestamp"] = pd.Timestamp.utcnow()

    golden_df.to_csv(GOLDEN_ID_MAPPING_PATH, index=False)

    multi = (
        golden_df.groupby("golden_id")["county_desc"]
        .nunique()
        .reset_index(name="county_count")
    )

    multi = multi[multi["county_count"] > 1]

    multi = multi.merge(golden_df, on="golden_id", how="left")

    multi["pipeline_run_timestamp"] = pd.Timestamp.utcnow()

    multi.to_csv(MULTI_COUNTY_PEOPLE_PATH, index=False)

    print(f"Golden IDs created: {golden_df['golden_id'].nunique():,}")
    print(f"Multi-county people: {multi['golden_id'].nunique():,}")


# ---------------------------------------------------------
# DAG DEFINITION
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