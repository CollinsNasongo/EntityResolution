from pathlib import Path
import logging

from entityresolver.sources.http_file_source import HttpFileSource
from entityresolver.ingestion.manager import ingest
from entityresolver.ingestion.manifest import Manifest

from entityresolver.parsing import load_dataframe
from entityresolver.schema.extractor import extract_schema, save_schema
from entityresolver.schema.registry import SchemaRegistry
from entityresolver.schema.diff import diff_schemas


# ---------------------------------------------------------
# Setup logging
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)


# ---------------------------------------------------------
# Config
# ---------------------------------------------------------
url = "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter13.zip"

data_dir = Path("data")
data_dir.mkdir(exist_ok=True)

temp_dir = data_dir / "temp"   # 👈 required for ZIP extraction

output = data_dir / "ncvoter13.zip"   # 👈 FIXED
manifest_path = data_dir / "manifest.json"
schema_path = data_dir / "schema.json"
registry_path = data_dir / "schema_registry.json"


# ---------------------------------------------------------
# Init
# ---------------------------------------------------------
source = HttpFileSource(url)
manifest = Manifest(manifest_path)
registry = SchemaRegistry(registry_path)


# ---------------------------------------------------------
# Step 1 — Ingest (download)
# ---------------------------------------------------------
result = ingest(
    source=source,
    destination=output,
    manifest=manifest,
    overwrite=True,
)

print(f"\n✅ File saved to: {result}")
print(f"📦 Size: {result.stat().st_size:,} bytes")


# ---------------------------------------------------------
# Step 2 — Load into DataFrame (ZIP-aware)
# ---------------------------------------------------------
df = load_dataframe(result, temp_dir=temp_dir)  # 👈 IMPORTANT

print(f"\n📊 Loaded DataFrame with {len(df):,} rows")
print(f"🧱 Columns: {list(df.columns)}")


# ---------------------------------------------------------
# Step 3 — Extract schema
# ---------------------------------------------------------
schema = extract_schema(
    df,
    filename=result.name,
    source=source.url,
    file_path=result,
)

print("\n🧾 Extracted schema:")
print(schema)


# ---------------------------------------------------------
# Step 4 — Compare with previous schema
# ---------------------------------------------------------
if not registry.is_empty():
    previous = registry.latest()
    diff = diff_schemas(previous, schema)

    if diff["has_changes"]:
        print("\n⚠️ Schema changes detected:")
        print(diff)
    else:
        print("\n✅ No schema changes detected")
else:
    print("\n🆕 No previous schema found (first run)")


# ---------------------------------------------------------
# Step 5 — Save + Register schema
# ---------------------------------------------------------
save_schema(schema, schema_path)
registry.register(schema)

print("\n📚 Schema saved and registered successfully")