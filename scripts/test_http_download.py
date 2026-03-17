from pathlib import Path
import logging

from entityresolver.sources.http_file_source import HttpFileSource
from entityresolver.ingestion.manager import ingest
from entityresolver.ingestion.manifest import Manifest


# ---------------------------------------------------------
# Setup logging (VERY IMPORTANT for debugging)
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)


# ---------------------------------------------------------
# Config
# ---------------------------------------------------------
url = "https://calmcode.io/static/data/titanic.csv"

data_dir = Path("data")
data_dir.mkdir(exist_ok=True)

output = data_dir / "titanic.csv"
manifest_path = data_dir / "manifest.json"


# ---------------------------------------------------------
# Init
# ---------------------------------------------------------
source = HttpFileSource(url)
manifest = Manifest(manifest_path)


# ---------------------------------------------------------
# Run ingestion (force overwrite for debugging)
# ---------------------------------------------------------
result = ingest(
    source=source,
    destination=output,
    manifest=manifest,
    overwrite=True, 
)
