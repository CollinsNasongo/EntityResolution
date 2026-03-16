from pathlib import Path
from entityresolver.connectors.http_connector import http_stream
from entityresolver.download.downloader import save_stream

url = "https://raw.githubusercontent.com/python/cpython/main/README.rst"

stream = http_stream(url)

save_stream(stream, Path("data/readme.rst"))