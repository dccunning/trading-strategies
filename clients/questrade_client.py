import time
import urllib.parse
import requests

from clients.database_client import DatabaseClient


class QuestradeClient:
    RATE_LIMIT = 1 / 20

    def __init__(self):
        """Initialize the Questrade client by authenticating and retrieving tokens."""
        self.app_name = 'Trading bot'
        self.refresh_token = None
        self.api_server = None
        self.access_token = None
        self.expiry_time = 0

    def _get_refresh_token(self) -> str:
        return DatabaseClient.run_query(
            f"select refresh_token from questrade.auth where app = '{self.app_name}'"
        )[0].get("refresh_token")

    def _update_refresh_token(self, token: str):
        query = """
        INSERT INTO questrade.auth (refresh_token, app)
        VALUES %s
        ON CONFLICT (app) DO UPDATE SET refresh_token = EXCLUDED.refresh_token;
        """
        DatabaseClient.run_query(query=query, params=[(token, self.app_name)])

    def _refresh_access_token(self):
        """
        Authenticate with the Questrade API using a refresh token stored in a file.
        """
        response = requests.post(
            "https://login.questrade.com/oauth2/token",
            data={"grant_type": "refresh_token", "refresh_token": self._get_refresh_token()},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()
        data = response.json()
        self._update_refresh_token(data["refresh_token"])
        self.access_token = data["access_token"]
        self.api_server = data["api_server"]
        self.expiry_time = time.time() + data["expires_in"]

    def get_access_token(self):
        if not self.access_token or time.time() >= self.expiry_time:
            self._refresh_access_token()
        return self.access_token

    def stock_search(self, prefix: str) -> list[dict]:
        """
        Search for stock symbols from Questrade that start with the given prefix.

        Args:
            prefix: The beginning of the stock symbol to search for.

        Returns:
            A list of symbol dictionaries, example: [{'symbol': 'AAPL', 'symbolId': 8049, 'description': 'APPLE INC', 'securityType': 'Stock', 'listingExchange': 'NASDAQ', 'isTradable': True, 'isQuotable': True, 'currency': 'USD'}]
        """
        access_token = self.get_access_token()
        url = f"{self.api_server.rstrip('/')}/v1/symbols/search?prefix={urllib.parse.quote(prefix)}"
        resp = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
        resp.raise_for_status()
        return resp.json().get("symbols", [])


if __name__ == "__main__":
    client = QuestradeClient()
    # client._update_refresh_token("Cu1brFO4iLITLIFsFHQFnZpaqgeFLptW0")
    result = client.stock_search(prefix="AAPL")
    print(result[0].get("description"))
    result = client.stock_search(prefix="tsla")
    print(result[0].get("description"))
