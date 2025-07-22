import time
import pytest
from clients.questrade_client import QuestradeClient


@pytest.fixture(scope="module")
def client():
    return QuestradeClient()


@pytest.mark.parametrize("symbol,expected_company", [
    ("AAPL", "APPLE INC"),
    ("TSLA", "TESLA INC"),
])
def test_stock_search(client, symbol, expected_company):
    results = client.stock_search(prefix=symbol)
    assert results, f"No results returned for symbol: {symbol}"
    first = results[0]

    assert "description" in first, "Missing 'description' key in result"
    assert expected_company in first["description"].upper(), f"{expected_company} not in {first['description']}"
    time.sleep(QuestradeClient.RATE_LIMIT)
