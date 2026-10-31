"""Exercise public SDK normalization through the HTTP response boundary."""

from types import SimpleNamespace

import pandas as pd
import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from tdxhub.http import create_app  # noqa: E402
from tdxhub.http.service import MarketDataService  # noqa: E402
from tdxhub.quotes import StdQuotes  # noqa: E402


def http(q):
    return TestClient(create_app(MarketDataService(q)))


def test_quote_mixed_stock_and_index_only_nulls_index_book():
    rows = [
        {"market": 1, "code": "000001", "price": 3911.87, "bid1": 0, "ask1": 0, "bid_vol1": 1756, "ask_vol1": 519},
        {"market": 0, "code": "000001", "price": 11.7, "bid1": 11.7, "ask1": 11.71, "bid_vol1": 100, "ask_vol1": 200},
    ]
    q = object.__new__(StdQuotes)
    q.client = SimpleNamespace(get_security_quotes=lambda symbols: rows)
    # legacy 报文语义用例: 显式声明 classic 快照, 不受 round2 默认影响
    q._round2_quotes_required = False
    q._round2_client = None
    response = http(q).get("/quote", params={"codes": "sh000001,sz000001"})
    assert response.status_code == 200
    index, stock = response.json()["data"]
    assert index["bid1"] is None and index["ask_vol1"] is None
    assert index["up_count"] == 1756 and index["down_count"] == 519
    assert stock["bid1"] == 11.7 and stock["ask_vol1"] == 200
    assert rows[0]["bid_vol1"] == 1756  # Raw record is not mutated.


@pytest.mark.parametrize("price,status", [(None, "missing"), (10.0, "derived"), (20.0, "inconsistent")])
def test_auction_null_and_status_survive_http(price, status):
    bars = pd.DataFrame(
        [{"open": 10.0, "close": 10.0, "low": 9.9, "high": 10.1, "vol": 1000.0, "amount": 10000.0}],
        index=pd.DatetimeIndex(["2026-09-18 09:31"], name="datetime"),
    )
    trades = pd.DataFrame([] if price is None else [{"time": "09:25", "price": price, "vol": 1, "buyorsell": 2}])
    q = object.__new__(StdQuotes)
    q.bars_all = lambda **kw: bars
    q.transactions_all = lambda **kw: trades
    response = http(q).get("/kline/minute/241", params={"code": "sz000001", "since": "20260918"})
    assert response.status_code == 200
    auction, first = response.json()["data"]
    assert auction["auction_status"] == status
    assert auction["close"] == (price if status == "derived" else None)
    assert first["vol"] == (900 if status == "derived" else 1000)


def test_unmarked_legacy_finance_still_has_unverified_diagnostics():
    raw = {"zongguben": 100.0, "jingzichan": 20000.0, "meigujingzichan": 20.0}
    q = object.__new__(StdQuotes)
    q.client = SimpleNamespace(get_finance_info=lambda **kw: raw)
    row = http(q).get("/finance", params={"exchange": "sh", "code": "600519"}).json()["data"][0]
    assert row["jingzichan"] == raw["jingzichan"]
    assert row["data_quality"]["status"] == "legacy_unverified"
    assert row["data_quality"]["net_assets_per_share_ratio"] == 10.0


def test_count_beijing_matches_directory_not_raw_protocol():
    q = object.__new__(StdQuotes)
    q.client = SimpleNamespace(get_security_count=lambda **kw: 383)
    q.stocks = lambda **kw: pd.DataFrame({"code": ["920001", "920002"]})
    assert http(q).get("/count", params={"exchange": "bj"}).json()["data"] == 2


def test_capital_flow_rejects_unverified_units_through_http():
    q = object.__new__(StdQuotes)
    response = http(q).get("/capital_flow", params={"code": "sh113052", "date": "20260918"})
    assert response.status_code == 400
    assert response.json()["code"] == 1
    assert "成交量单位" in response.json()["msg"]


@pytest.mark.parametrize("exchange,code", [("sh", "600519"), ("sz", "000001"), ("bj", "920002")])
def test_normalized_finance_through_http(exchange, code):
    from tests.tdx.test_finance_info import DEPRECATED, parse

    raw = parse(code)
    q = object.__new__(StdQuotes)
    q.client = SimpleNamespace(get_finance_info=lambda **kw: raw)
    response = http(q).get("/finance", params={"exchange": exchange, "code": code})
    assert response.status_code == 200
    row = response.json()["data"][0]
    assert row["finance_schema"] == "tdx_finance_v2"
    assert row["zongzichan"] == raw["zongzichan"]
    assert row["meigushouyi"] == raw["meigushouyi"]
    assert row["shangniantongqijinglirun"] == raw["shangniantongqijinglirun"]
    assert row["raw_fields"] == raw["raw_fields"]
    assert all(row[key] is None for key in DEPRECATED)
    assert row["data_quality"]["status"] == "normalized"
    assert row["data_quality"]["issues"] == []


def test_empty_finance_through_http():
    q = object.__new__(StdQuotes)
    q.client = SimpleNamespace(get_finance_info=lambda **kw: {})
    response = http(q).get("/finance", params={"exchange": "sh", "code": "600519"})
    assert response.status_code == 200
    assert response.json()["data"] == []


@pytest.mark.parametrize('path,params', [
    ('/kline/day/all', {'code': '600000'}),
    ('/capital_flow/sector', {'codes': '600000,600001'}),
    ('/capital_flow/history', {'code': '600000', 'days': 5}),
])
def test_incomplete_data_is_never_an_http_success_payload(path, params):
    from tdxhub.exceptions import TdxhubConnectionError
    from tests.quotes.test_aggregate_accuracy import flow, history_quote

    q = history_quote()
    q.client = SimpleNamespace(get_security_bars=lambda *a: None, close=lambda: None)

    def fetch(symbol, **kwargs):
        if symbol == '600001' or path.endswith('/history'):
            raise TdxhubConnectionError('scripted failure')
        return flow()

    q.capital_flow = fetch
    if path == '/kline/day/all':
        del q.bars  # Exercise the real bars -> None guard, not the history fixture.
    response = http(q).get(path, params=params)
    assert response.json()['code'] == 1
    assert response.json()['data'] is None


def test_history_insufficient_window_is_null_with_coverage_over_http():
    from tests.quotes.test_aggregate_accuracy import history_quote

    response = http(history_quote(6)).get('/capital_flow/history', params={'code': '600000', 'days': 5})
    assert response.json()['code'] == 0
    rows = response.json()['data']
    assert len(rows) == 5
    assert all(row['main_20d_net'] is None and row['main_20d_pct'] is None for row in rows)
    assert [row['window_20d_days'] for row in rows] == [2, 3, 4, 5, 6]
    assert all(row['window_20d_complete'] is False for row in rows)


@pytest.mark.parametrize("path,filename,fields,expected", [
    ("/tdx/stat2", "tdxstat2.cfg", {2: "20260918", 3: "14.59", 5: "35.32", 16: "bad"},
     {"amount": 145900.0, "amount_prev": 353200.0, "ipo_price": None, "date": "20260918"}),
    ("/tdx/stat", "tdxstat.cfg", {3: "", 4: "20260918", 5: "bad", 6: "0"},
     {"pe_ttm": None, "trend_days": None, "change_pct": 0.0, "date": "20260918"}),
    ("/ipo", "xgsg.cfg", {2: "20260928", 3: "", 14: "新股"},
     {"issue_price": None, "date": "20260928"}),
])
def test_official_report_units_and_nulls_through_http(path, filename, fields, expected):
    raw = [""] * 31
    raw[:2] = ["0", "159729"]
    for index, value in fields.items():
        raw[index] = value
    q = object.__new__(StdQuotes)

    def download(name):
        assert name == filename
        return "|".join(raw).encode("gbk")

    q._get_zhb_file = download
    response = http(q).get(path)
    assert response.status_code == 200
    body = response.json()
    assert body["code"] == 0
    row = body["data"][0]
    for key, value in expected.items():
        assert row[key] == value
    assert row["raw_fields"] == raw
