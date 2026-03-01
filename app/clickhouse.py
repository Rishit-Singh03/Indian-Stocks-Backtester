from __future__ import annotations

import json
import re
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def validate_identifier(identifier: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(f"Invalid SQL identifier: {identifier!r}")
    return identifier


def sql_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.75,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "stock-dashboard-api/1.0"})
    return session


class ClickHouseClient:
    def __init__(self, base_url: str, user: str, password: str, timeout: int = 30) -> None:
        self.base_url = base_url
        self.user = user
        self.password = password
        self.timeout = timeout
        self.session = make_session()

    def query_text(self, query: str, data: bytes | None = None, content_type: str | None = None) -> str:
        headers: dict[str, str] = {}
        if content_type:
            headers["Content-Type"] = content_type
        response = self.session.post(
            self.base_url,
            params={"query": query},
            data=data,
            auth=(self.user, self.password),
            timeout=self.timeout,
            headers=headers,
        )
        if response.status_code >= 400:
            detail = response.text.strip()
            raise RuntimeError(
                f"ClickHouse error {response.status_code}.\nQuery: {query}\nDetails: {detail}"
            )
        return response.text

    def query_rows(self, query: str) -> list[dict[str, Any]]:
        text = self.query_text(query)
        rows: list[dict[str, Any]] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
        return rows

    def insert_json_each_row(self, query: str, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows).encode("utf-8")
        self.query_text(query=query, data=payload, content_type="application/json")
        return len(rows)
