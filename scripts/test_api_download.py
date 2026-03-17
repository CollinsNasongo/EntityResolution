from pathlib import Path
import logging

from entityresolver.sources.api_source import ApiSource
from entityresolver.ingestion.manager import ingest
from entityresolver.ingestion.manifest import Manifest


logging.basicConfig(level=logging.INFO)

url = "https://randomuser.me/api/?results=100"

data_dir = Path("data")
data_dir.mkdir(exist_ok=True)

output = data_dir / "random_users.json"
manifest = Manifest(data_dir / "manifest.json")

source = ApiSource(url)

result = ingest(
    source=source,
    destination=output,
    manifest=manifest,
    overwrite=True,
    min_size_bytes=1,  # important for small JSON
)

print(f"Saved to: {result}")
print(f"Size: {result.stat().st_size} bytes")