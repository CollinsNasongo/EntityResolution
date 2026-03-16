from entityresolver.connectors.http_connector import http_stream


def test_http_stream():
    url = "https://raw.githubusercontent.com/python/cpython/main/README.rst"

    stream = http_stream(url)

    data = next(iter(stream))

    assert isinstance(data, bytes)