import pytest
import requests
from unittest.mock import patch, MagicMock

from entityresolver.connectors.http_connector import http_stream


# ---------------------------------------------------------
# SUCCESS CASE
# ---------------------------------------------------------
def test_http_stream_success():
    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b"hello", b"world"]
    mock_response.raise_for_status.return_value = None

    with patch(
        "entityresolver.connectors.http_connector.requests.get",
        return_value=mock_response,
    ):
        stream = http_stream("http://fake-url")
        data = list(stream)

    assert data == [b"hello", b"world"]
    mock_response.raise_for_status.assert_called_once()


# ---------------------------------------------------------
# RETRY SUCCESS (FIRST FAILS, SECOND WORKS)
# ---------------------------------------------------------
def test_http_stream_retry():
    calls = {"count": 0}

    def mock_get(*args, **kwargs):
        calls["count"] += 1

        if calls["count"] == 1:
            raise requests.RequestException("temporary failure")

        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"ok"]
        mock_response.raise_for_status.return_value = None
        return mock_response

    with patch(
        "entityresolver.connectors.http_connector.requests.get",
        side_effect=mock_get,
    ), patch("time.sleep", return_value=None):

        stream = http_stream("http://fake-url", retries=2)
        data = list(stream)

    assert data == [b"ok"]
    assert calls["count"] == 2


# ---------------------------------------------------------
# FAILURE AFTER ALL RETRIES
# ---------------------------------------------------------
def test_http_stream_failure():
    with patch(
        "entityresolver.connectors.http_connector.requests.get",
        side_effect=requests.RequestException("fail"),
    ), patch("time.sleep", return_value=None):

        with pytest.raises(requests.RequestException):
            http_stream("http://fake-url", retries=1)


# ---------------------------------------------------------
# EMPTY RESPONSE
# ---------------------------------------------------------
def test_http_stream_empty():
    mock_response = MagicMock()
    mock_response.iter_content.return_value = []
    mock_response.raise_for_status.return_value = None

    with patch(
        "entityresolver.connectors.http_connector.requests.get",
        return_value=mock_response,
    ):
        stream = http_stream("http://fake-url")
        data = list(stream)

    assert data == []


# ---------------------------------------------------------
# HTTP ERROR (raise_for_status)
# ---------------------------------------------------------
def test_http_stream_http_error():
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("404 error")

    with patch(
        "entityresolver.connectors.http_connector.requests.get",
        return_value=mock_response,
    ):
        with pytest.raises(requests.RequestException):
            http_stream("http://fake-url")


# ---------------------------------------------------------
# CHUNK SIZE PASSED CORRECTLY
# ---------------------------------------------------------
def test_http_stream_chunk_size():
    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b"a", b"b"]
    mock_response.raise_for_status.return_value = None

    with patch(
        "entityresolver.connectors.http_connector.requests.get",
        return_value=mock_response,
    ):
        stream = http_stream("http://fake-url", chunk_size=1234)
        list(stream)

    mock_response.iter_content.assert_called_with(chunk_size=1234)


# ---------------------------------------------------------
# HEADERS PASSED CORRECTLY
# ---------------------------------------------------------
def test_http_stream_headers():
    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b"x"]
    mock_response.raise_for_status.return_value = None

    headers = {"Authorization": "Bearer token"}

    with patch(
        "entityresolver.connectors.http_connector.requests.get",
        return_value=mock_response,
    ) as mock_get:

        list(http_stream("http://fake-url", headers=headers))

    mock_get.assert_called_with(
        "http://fake-url",
        stream=True,
        timeout=30,
        headers=headers,
    )


# ---------------------------------------------------------
# TIMEOUT / NETWORK ERROR
# ---------------------------------------------------------
def test_http_stream_timeout():
    with patch(
        "entityresolver.connectors.http_connector.requests.get",
        side_effect=requests.Timeout("timeout"),
    ), patch("time.sleep", return_value=None):

        with pytest.raises(requests.RequestException):
            http_stream("http://fake-url", retries=1)


# ---------------------------------------------------------
# INTEGRATION TEST (REAL NETWORK)
# ---------------------------------------------------------
@pytest.mark.integration
def test_http_stream_real_url():
    url = "https://raw.githubusercontent.com/python/cpython/main/README.rst"

    stream = http_stream(url)
    first_chunk = next(iter(stream))

    assert isinstance(first_chunk, bytes)
    assert len(first_chunk) > 0