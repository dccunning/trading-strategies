import os
import json
import tempfile
import subprocess
from typing import List
from pathlib import Path


class PythonExecutor:
    def __init__(self, venv: str = None):
        """
        :param venv: Optional path to virtual environment.
                     Defaults to '../../.venv' in the current project.
        """
        venv_path = Path(venv) if venv else Path(__file__).parent.parent.parent / ".venv" / "bin" / "python"
        self.python_bin = os.path.join(venv_path)

        if not os.path.exists(self.python_bin):
            raise ValueError(f"Python binary not found in venv: {self.python_bin}")

        self.LOCAL_TMP_ROOT = os.path.expanduser("~/.trading-strategy/tmp/")
        os.makedirs(self.LOCAL_TMP_ROOT, exist_ok=True)

    def execute_code(self, script: str, env_var: dict = None, timeout: int = 10) -> List[dict]:
        """
        Execute a Python script in an isolated environment using the venv.

        :param script: Python code defining a variable named 'result' (a list of dicts)
        :param env_var: The env variables where each key is the source domain, like 'openai' or 'weatherapi'
        :param timeout: Maximum execution time for the Python script
        :return: A list of dicts representing rows of data with column names
        """
        with tempfile.TemporaryDirectory(dir=self.LOCAL_TMP_ROOT) as tmp_dir:
            script_path = os.path.join(tmp_dir, "runner.py")
            output_path = os.path.join(tmp_dir, "output.json")

            wrapped_script = f"""
import json, os; os.getenv = lambda k: os.environ[k] if k in os.environ else (_ for _ in ()).throw(ValueError(f"API key '{{k}}' is not defined"))
try:
{self._indent(script)}
    with open("{output_path}", "w") as f:
        json.dump(result, f)
except Exception as e:
    with open("{output_path}", "w") as f:
        json.dump({{"error": str(e)}}, f)
"""
            with open(script_path, "w") as f:
                f.write(wrapped_script)

            try:
                subprocess.run(
                    [self.python_bin, script_path],
                    cwd=tmp_dir,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=timeout,
                    check=False,
                    env=env_var
                )
            except subprocess.TimeoutExpired:
                raise RuntimeError("Script execution timed out")

            if os.path.exists(output_path):
                with open(output_path) as f:
                    result = json.load(f)
                    if "error" in result:
                        raise RuntimeError(f"Script error: {result['error']}")
                    return result

            raise RuntimeError("Script did not produce any output named 'result'")

    @staticmethod
    def _indent(code: str, spaces: int = 4) -> str:
        return "\n".join(" " * spaces + line for line in code.strip().splitlines())


if __name__ == "__main__":
    py = PythonExecutor()
    weatherapi = (
        "Weather forecast in vancouver for the next 7 days",
        """
    import os
    import requests

    API_KEY = os.getenv('weatherapi')
    url = f"https://api.weatherapi.com/v1/forecast.json?q=Vancouver&days=7&key={API_KEY}"

    response = requests.get(url)
    data = response.json()

    # Extract forecast data into rows
    forecast = data["forecast"]["forecastday"]

    result = [
        {
            "date": day["date"],
            "condition": day["day"]["condition"]["text"],
            "min_temp_c": day["day"]["mintemp_c"],
            "max_temp_c": day["day"]["maxtemp_c"],
            "chance_of_rain": day["day"].get("daily_chance_of_rain")
        }
        for day in forecast
    ]"""
    )
    newsapi = """
    import os
    import requests
    from datetime import datetime, timedelta

    API_KEY = os.getenv("newsapi")
    domains = [
        "globalnews.ca", "torontosun.com", "cnbc.com", "yahoo.com", "breitbart.com", "rt.com", "aljazeera.com"
    ]

    to_date = datetime.now().replace(microsecond=0).isoformat()
    from_date = (datetime.now() - timedelta(days=1)).replace(microsecond=0).isoformat()

    all_articles = []

    for domain in domains:
        page = 1
        while True:
            url = (
                f"https://newsapi.org/v2/everything?"
                f"q=*&language=en&pageSize=100&page={page}&sortBy=publishedAt"
                f"&domains={domain}&from={from_date}&to={to_date}&apiKey={API_KEY}"
            )
            response = requests.get(url)
            if response.status_code != 200:
                print(f"Error fetching from {domain} (status {response.status_code}):", response.json())
                break

            data = response.json()
            articles = data.get("articles", [])

            if not articles:
                break

            all_articles.extend(articles)
            page += 1

            if len(articles) < 100:
                break

    # Format results as a list of structured dicts
    result = [
        {
            "title": article["title"],
            "author": article.get("author"),
            "source": article["source"]["name"],
            "description": article.get("description"),
            "content": article.get("content"),
            "published_at": article["publishedAt"],
            "url": article["url"],
            "url_to_image": article.get("urlToImage")
        }
        for article in all_articles
    ]
    """
    news_sources = """
    import os
    import requests
    from urllib.parse import urlparse

    API_KEY = os.getenv('newsapi')
    url = f"https://newsapi.org/v2/sources?language=en&apiKey={API_KEY}"

    response = requests.get(url)
    sources_data = response.json()

    result = []

    for source in sources_data.get("sources", []):
        parsed = urlparse(source["url"])
        domain = parsed.netloc.replace("www.", "")
        result.append({
            "id": source.get("id"),
            "name": source.get("name"),
            "category": source.get("category"),
            "language": source.get("language"),
            "country": source.get("country"),
            "url": source.get("url"),
            "domain": domain
        })

    """
    data = py.execute_code(newsapi, {'weatherapi': '89ed62f017a343878af62150252803', 'newsapi':
        'dcd0b8f9574b4875819c18ab30d70b34'})
    import pandas as pd

    df = pd.DataFrame(data)
    print(df.to_string())

    INSTTRUCTIONS = (
        "1. Write Python code which creates a list called 'result' that contains column/values of table data"
        "2. Use the domain name as the API key environment variable"
        "3. Paginate the requests when necessary for the result"
    )