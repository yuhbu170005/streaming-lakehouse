"""Provision and verify the three local Metabase dashboards."""

import json
import os
import secrets
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE = os.getenv("METABASE_URL", "http://127.0.0.1:3002")
DB_NAME = "Streaming Lakehouse"
DASHBOARDS = {
    "Market overview": [
        (
            "Top traded symbols",
            "table",
            """
            SELECT symbol, hour_start, close, quote_volume, volume_rank
            FROM lake.marts.mart_market_overview
            WHERE hour_start = (SELECT max(hour_start) FROM lake.marts.mart_market_overview)
            ORDER BY volume_rank LIMIT 20
            """,
        ),
        (
            "Top hourly movers",
            "table",
            """
            SELECT symbol, hour_start, round(100 * return_fraction, 3) AS return_pct, mover_rank
            FROM lake.marts.mart_market_overview
            WHERE hour_start = (SELECT max(hour_start) FROM lake.marts.mart_market_overview)
            ORDER BY mover_rank LIMIT 20
            """,
        ),
    ],
    "Symbol drill-down": [
        (
            "One-minute close",
            "line",
            """
            SELECT window_start, close
            FROM lake.marts.stg_candles
            WHERE symbol = coalesce([[{{symbol}},]] CAST(NULL AS VARCHAR), 'BTCUSDT')
            AND window_start >= current_timestamp - INTERVAL '6' HOUR ORDER BY window_start
            """,
        ),
        (
            "Hourly OHLCV by symbol",
            "table",
            """
            SELECT symbol, hour_start, open, high, low, close, volume, trade_count
            FROM lake.marts.mart_symbol_hourly
            WHERE symbol = coalesce([[{{symbol}},]] CAST(NULL AS VARCHAR), 'BTCUSDT')
            ORDER BY hour_start DESC LIMIT 200
            """,
        ),
    ],
    "Data health": [
        (
            "Hourly quality and gaps",
            "table",
            """
            SELECT symbol, hour_start, gaps_detected, open_gaps, arrival_delay_over_30s,
                candles_compared, mismatches, missing_inputs, match_rate
            FROM lake.marts.mart_data_health ORDER BY hour_start DESC, symbol LIMIT 200
            """,
        ),
        (
            "Recent candle reconciliation",
            "table",
            """
            SELECT status, count(*) AS candles
            FROM lake.recon.candle_vs_kline
            WHERE window_start >= current_timestamp - INTERVAL '6' HOUR
            GROUP BY 1 ORDER BY 1
            """,
        ),
    ],
}


def request(method, path, body=None, session=None):
    headers = {"Content-Type": "application/json"}
    if session:
        headers["X-Metabase-Session"] = session
    data = json.dumps(body).encode() if body is not None else None
    req = Request(f"{BASE}/api/{path}", data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=30) as response:
            raw = response.read()
            return json.loads(raw) if raw else None
    except HTTPError as error:
        if "session" in path or path == "setup":
            raise RuntimeError(f"{method} {path}: HTTP {error.code}") from error
        raise RuntimeError(
            f"{method} {path}: HTTP {error.code}: {error.read().decode()}"
        ) from error


def credentials():
    email = os.getenv("METABASE_EMAIL", "admin@lakehouse.local")
    password = os.getenv("METABASE_PASSWORD")
    if password:
        return email, password
    local_file = Path("data/metabase-password")
    local_file.parent.mkdir(exist_ok=True)
    try:
        with local_file.open() as saved:
            return email, saved.read().strip()
    except FileNotFoundError:
        password = f"Aa1!{secrets.token_urlsafe(24)}"
        descriptor = os.open(local_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w") as saved:
            saved.write(password + "\n")
    print(f"Local Metabase login: {email}; password in {local_file}")
    return email, password


def connect():
    for attempt in range(36):
        try:
            properties = request("GET", "session/properties")
            break
        except (RuntimeError, URLError, TimeoutError):
            if attempt == 35:
                raise
            time.sleep(5)
    email, password = credentials()
    token = properties.get("setup-token")
    if token and not properties.get("has-user-setup"):
        request(
            "POST",
            "setup",
            {
                "token": token,
                "user": {
                    "email": email,
                    "password": password,
                    "first_name": "Lakehouse",
                    "last_name": "Admin",
                },
                "prefs": {"site_name": "Streaming Lakehouse", "allow_tracking": False},
            },
        )
    return request("POST", "session", {"username": email, "password": password})["id"]


def main():
    session = connect()
    databases = request("GET", "database", session=session)["data"]
    database = next((db for db in databases if db["name"] == DB_NAME), None)
    if database is None:
        database = request(
            "POST",
            "database",
            {
                "name": DB_NAME,
                "engine": "starburst",
                "details": {
                    "host": "trino",
                    "port": 8080,
                    "catalog": "lake",
                    "user": "lakehouse",
                    "ssl": False,
                },
                "is_full_sync": False,
            },
            session,
        )
    existing = {
        dashboard["name"]: dashboard
        for dashboard in request("GET", "dashboard", session=session)
    }
    for name, specs in DASHBOARDS.items():
        dashboard = existing.get(name)
        if dashboard is None:
            dashboard = request("POST", "dashboard", {"name": name}, session)
        dashboard_id = dashboard["id"]
        dashboard = request("GET", f"dashboard/{dashboard_id}", session=session)
        cards = {
            dashcard["card"]["name"]: dashcard["card"]
            for dashcard in dashboard.get("dashcards", [])
            if dashcard.get("card")
        }
        filter_symbol = name == "Symbol drill-down"
        for title, display, sql in specs:
            card = cards.get(title)
            payload = {
                "name": title,
                "display": display,
                "visualization_settings": {},
                "dashboard_id": dashboard_id,
                "dataset_query": {
                    "database": database["id"],
                    "type": "native",
                    "native": {
                        "query": sql.strip(),
                        "template-tags": {
                            "symbol": {
                                "name": "symbol",
                                "display-name": "Symbol",
                                "id": "symbol",
                                "type": "text",
                            }
                        }
                        if filter_symbol
                        else {},
                    },
                },
            }
            if card is None:
                card = request("POST", "card", payload, session)
            elif filter_symbol:
                card = request("PUT", f"card/{card['id']}", payload, session)
            params = (
                {
                    "parameters": [
                        {
                            "type": "category",
                            "id": "symbol",
                            "target": ["variable", ["template-tag", "symbol"]],
                            "value": "ETHUSDT",
                        }
                    ],
                }
                if filter_symbol
                else {}
            )
            result = request("POST", f"card/{card['id']}/query", params, session)
            if result.get("status") != "completed":
                raise RuntimeError(
                    f"Dashboard card query failed: {title}: {result.get('error')}"
                )
            print(f"{title}: {result.get('row_count')} rows")
        if filter_symbol:
            add_symbol_filter(dashboard_id, {title for title, _, _ in specs}, session)
        print(f"{name}: {BASE}/dashboard/{dashboard_id}")


def add_symbol_filter(dashboard_id, managed_cards, session):
    dashboard = request("GET", f"dashboard/{dashboard_id}", session=session)
    parameter = {
        "id": "symbol",
        "name": "Symbol",
        "slug": "symbol",
        "type": "string/=",
        "sectionId": "string",
        "isMultiSelect": False,
        "default": ["BTCUSDT"],
    }
    parameters = [p for p in dashboard["parameters"] if p["id"] != "symbol"] + [
        parameter
    ]
    dashcards = []
    for dashcard in dashboard["dashcards"]:
        card = dashcard.get("card") or {}
        mappings = [
            p for p in dashcard["parameter_mappings"] if p["parameter_id"] != "symbol"
        ]
        if card.get("name") in managed_cards:
            mappings.append(
                {
                    "parameter_id": "symbol",
                    "card_id": card["id"],
                    "target": ["variable", ["template-tag", "symbol"]],
                }
            )
        layout = {
            key: dashcard[key]
            for key in ("id", "card_id", "row", "col", "size_x", "size_y")
        }
        dashcards.append({**layout, "parameter_mappings": mappings})
    request(
        "PUT",
        f"dashboard/{dashboard_id}",
        {"parameters": parameters, "dashcards": dashcards},
        session,
    )


if __name__ == "__main__":
    main()
