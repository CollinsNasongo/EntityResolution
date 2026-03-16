from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import glob
import stat

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
# HELPER — SAFE FILE WRITE
# Deletes and re-creates output files before writing so
# that stale files from failed previous runs (which may
# have wrong permissions) never block a fresh run.
# ---------------------------------------------------------
def safe_remove(path: str) -> None:
    """Remove a file if it exists, fixing permissions first if needed."""
    if os.path.exists(path):
        try:
            os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
            os.remove(path)
            print(f"Removed stale file: {path}")
        except Exception as e:
            raise RuntimeError(f"Cannot remove {path}: {e}")


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

    TARGET_RECORDS = 100000

    if len(all_df) > TARGET_RECORDS:
        all_df = all_df.sample(n=TARGET_RECORDS, random_state=42)
        print(f"Dataset limited to {TARGET_RECORDS:,} records")
    else:
        print(f"Dataset already under {TARGET_RECORDS:,}")

    all_df["pipeline_run_timestamp"] = pd.Timestamp.utcnow()

    safe_remove(ALL_RECORDS_PATH)
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
            "pipeline_run_timestamp",
        ],
        dtype={
            "unique_id": "string",
            "first_name": "string",
            "middle_name": "string",
            "last_name": "string",
            "gender_code": "category",
            "county_desc": "category",
        },
        low_memory=False,
    )

    df["source_dataset"] = df["county_desc"]

    df["birth_year"] = pd.to_numeric(df["birth_year"], errors="coerce").astype("Int16")

    # -----------------------------------------------------
    # NORMALIZE NAMES
    # -----------------------------------------------------
    df["first_name"] = df["first_name"].str.lower().str.strip().fillna("")
    df["middle_name"] = df["middle_name"].str.lower().str.strip().fillna("")
    df["last_name"] = df["last_name"].str.lower().str.strip().fillna("")

    print(f"Loaded {len(df):,} rows for matching")

    # -----------------------------------------------------
    # SPLINK SETTINGS
    #
    # LOOSENED — JaroWinkler lower bounds relaxed:
    #   first_name: [0.80, 0.88]  (was [0.85, 0.92])
    #   last_name:  [0.85, 0.92]  (was [0.88, 0.95])
    # This catches records where names are spelled
    # slightly differently (e.g. nicknames, typos).
    #
    # gender_code: ExactMatch — kept strict, different
    # gender codes should never be linked.
    #
    # birth_year: ExactMatch — kept strict at model level;
    # a ±1 tolerance is applied in stage 3 instead so
    # it only affects the final cluster filter, not
    # the scoring weights.
    # -----------------------------------------------------
    settings = SettingsCreator(
        link_type="link_only",
        unique_id_column_name="unique_id",
        source_dataset_column_name="source_dataset",

        comparisons=[
            # LOOSENED — lower JaroWinkler thresholds
            cl.JaroWinklerAtThresholds("first_name", [0.80, 0.88]),
            cl.JaroWinklerAtThresholds("last_name", [0.85, 0.92]),
            cl.ExactMatch("gender_code"),
            cl.ExactMatch("birth_year"),
        ],

        blocking_rules_to_generate_predictions=[
            block_on("birth_year", "last_name", "first_name"),
            block_on("birth_year", "last_name"),
            block_on("birth_year", "first_name"),
            block_on("gender_code", "last_name"),
        ],

        retain_intermediate_calculation_columns=True,
    )

    db_api = DuckDBAPI(connection=":memory:")
    linker = Linker(df, settings, db_api=db_api)

    # -----------------------------------------------------
    # ESTIMATE PRIOR MATCH PROBABILITY
    # -----------------------------------------------------
    print("Estimating baseline match probability...")

    linker.training.estimate_probability_two_random_records_match(
        [
            block_on("first_name", "birth_year"),
            block_on("last_name"),
        ],
        recall=0.7,
    )

    # -----------------------------------------------------
    # ESTIMATE U VALUES
    # -----------------------------------------------------
    print("Estimating u values via random sampling...")

    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

    # -----------------------------------------------------
    # EM TRAINING
    # -----------------------------------------------------
    print("Running EM session 1 (block on birth_year, last_name)...")

    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("birth_year", "last_name")
    )

    print("Running EM session 2 (block on first_name, gender_code)...")

    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("first_name", "gender_code")
    )

    # -----------------------------------------------------
    # PREDICTION
    # LOOSENED — threshold lowered from 0.95 → 0.80
    # More candidates pass through to stage 3 where the
    # hard filters provide the final quality gate.
    # -----------------------------------------------------
    print("Predicting matches...")

    predictions = linker.inference.predict(threshold_match_probability=0.80)

    predictions_df = predictions.as_pandas_dataframe()

    print(predictions_df["match_probability"].describe())

    predictions_df["pipeline_run_timestamp"] = pd.Timestamp.utcnow()

    safe_remove(SPLINK_PREDICTIONS_PATH)
    predictions_df.to_csv(SPLINK_PREDICTIONS_PATH, index=False)

    print(f"Saved {len(predictions_df):,} candidate pairs")

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

    print(f"Total candidate pairs before filtering: {len(preds):,}")

    # -----------------------------------------------------
    # FILTER MATCHES
    #
    # match_probability >= 0.85  (LOOSENED from 0.90)
    #   — lets in more valid matches that scored slightly
    #   lower due to name variations or data entry
    #   differences between counties.
    #
    # gender_code exact match  — kept strict.
    #
    # birth_year ±1 tolerance  (LOOSENED from exact)
    #   — allows for off-by-one data entry errors, e.g.
    #   1979 vs 1980. ExactMatch is still used at the
    #   Splink scoring level; this only affects the
    #   final cluster filter.
    #
    # gamma_last_name > 0  — kept to block pure
    #   last-name-only family member false matches.
    #
    # gamma_first_name filter REMOVED  — was too
    #   aggressive; nicknames and shortened names
    #   (e.g. "Bob" vs "Robert") can legitimately
    #   score at gamma level 0.
    # -----------------------------------------------------
    matches = preds[
        (preds["match_probability"] >= 0.85)
        & (preds["gender_code_l"] == preds["gender_code_r"])
        & (abs(preds["birth_year_l"] - preds["birth_year_r"]) <= 1)   # LOOSENED: ±1
        & (preds["gamma_last_name"] > 0)                               # last name must have some similarity
    ]

    print(f"Candidate pairs after filtering: {len(matches):,}")

    # -----------------------------------------------------
    # UNION-FIND CLUSTERING
    # -----------------------------------------------------
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

    # -----------------------------------------------------
    # GOLDEN ID ASSIGNMENT
    # -----------------------------------------------------
    golden_rows = []

    for i, (root, members) in enumerate(clusters.items(), start=1):
        for uid in members:
            golden_rows.append({
                "golden_id": i,
                "unique_id": uid,
            })

    golden_df = pd.DataFrame(golden_rows)
    golden_df = golden_df.merge(all_df, on="unique_id", how="left")
    golden_df["pipeline_run_timestamp"] = pd.Timestamp.utcnow()

    safe_remove(GOLDEN_ID_MAPPING_PATH)
    golden_df.to_csv(GOLDEN_ID_MAPPING_PATH, index=False)

    # -----------------------------------------------------
    # MULTI-COUNTY DETECTION
    # -----------------------------------------------------
    multi = (
        golden_df.groupby("golden_id")["county_desc"]
        .nunique()
        .reset_index(name="county_count")
    )

    multi = multi[multi["county_count"] > 1]
    multi = multi.merge(golden_df, on="golden_id", how="left")
    multi["pipeline_run_timestamp"] = pd.Timestamp.utcnow()

    safe_remove(MULTI_COUNTY_PEOPLE_PATH)
    multi.to_csv(MULTI_COUNTY_PEOPLE_PATH, index=False)

    total_people = golden_df["golden_id"].nunique()
    multi_county = multi["golden_id"].nunique()
    single_county = total_people - multi_county

    print(f"Golden IDs created   : {total_people:,}")
    print(f"Single-county people : {single_county:,}")
    print(f"Multi-county people  : {multi_county:,}")


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