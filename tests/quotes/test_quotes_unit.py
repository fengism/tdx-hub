from __future__ import annotations

from unittest import mock

import pandas as pd
import pytest

from tdxhub import config
from tdxhub.exceptions import TdxhubValidationException
from tdxhub.quotes import Quotes, StdQuotes, valid_server
from tdxhub.utils import to_data


class FakeHqApi:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.client = None
        self.connected = None

    def connect(self, ip, port, time_out):
        self.connected = (ip, port, time_out)
        return self

    def close(self):
        self.client = None

    def get_security_bars(self, *args):
        return [{"close": 1.0}]


def test_std_client_uses_default_when_bestip_is_empty(monkeypatch):
    monkeypatch.setattr("tdxhub.quotes.StandardClient", FakeHqApi)
    config.setup(force=True)
    config.set("BESTIP.HQ", "")

    client = Quotes.factory("std", timeout=3)

    expected = tuple(config.get("SERVER.HQ")[0][1:3])
    assert client.server == expected
    assert client.client.connected == (*expected, 3)


@pytest.mark.parametrize("server", [(), ("127.0.0.1",), ("bad-ip", 7709), ("127.0.0.1", 0)])
def test_valid_server_rejects_invalid_values(server):
    with pytest.raises(ValueError):
        valid_server(server)


def test_factory_rejects_unknown_market():
    with pytest.raises(ValueError, match="market"):
        Quotes.factory("unknown")


class _BarsClient:
    def __init__(self):
        self.calls = []
        self.index_calls = []

    def get_security_bars(self, *args):
        self.calls.append(args)
        return [{"datetime": "2024-01-02 00:00", "open": 1.0}]

    def get_index_bars(self, *args):
        self.index_calls.append(args)
        return [{"datetime": "2024-01-02 00:00", "open": 1.0, "close": 1.0}]


def test_bars_normalizes_prefixed_code_and_validates_window():
    quotes = object.__new__(StdQuotes)
    quotes.client = _BarsClient()
    quotes._round2_bars_required = False
    quotes._round2_client = None

    result = quotes.bars("SH.600000", offset=1)

    assert not result.empty
    assert quotes.client.calls == [(9, 1, "600000", 0, 1)]
    with pytest.raises(TdxhubValidationException, match="offset"):
        quotes.bars("600000", offset=0)


def test_index_routing_prefixes_and_market_override():
    from tdxhub.consts import MARKET_BJ, MARKET_SH, MARKET_SZ

    quotes = object.__new__(StdQuotes)
    quotes.client = _BarsClient()

    # 1. Automatic routing by code prefix
    quotes.index("000001", offset=1)
    quotes.index("399001", offset=1)
    quotes.index("899050", offset=1)

    # 2. Market prefixes (SH / SZ / BJ, case-insensitive, with/without dots)
    quotes.index("sh000001", offset=1)
    quotes.index("SZ.399001", offset=1)
    quotes.index("bj899050", offset=1)

    # 3. Explicit market overrides
    quotes.index("899050", market=MARKET_BJ, offset=1)
    quotes.index("899050", market="bj", offset=1)
    quotes.index("000001", market=MARKET_SZ, offset=1)

    # 4. index_bars method
    quotes.index_bars("sh000001", offset=1)

    assert quotes.client.index_calls == [
        (9, 1, "000001", 0, 1),
        (9, 0, "399001", 0, 1),
        (9, 2, "899050", 0, 1),
        (9, 1, "000001", 0, 1),
        (9, 0, "399001", 0, 1),
        (9, 2, "899050", 0, 1),
        (9, 2, "899050", 0, 1),
        (9, 2, "899050", 0, 1),
        (9, 0, "000001", 0, 1),
        (9, 1, "000001", 0, 1),
    ]

    # Validation checks
    with pytest.raises(TdxhubValidationException, match="offset"):
        quotes.index("000001", offset=0)
    with pytest.raises(TdxhubValidationException, match="证券代码错误"):
        quotes.index(123456)
    with pytest.raises(TdxhubValidationException, match="不支持的证券市场"):
        quotes.index("000001", market="invalid_mkt")


def test_get_k_data_returns_empty_for_empty_or_reversed_ranges():
    quotes = object.__new__(StdQuotes)
    quotes.client = _BarsClient()
    assert quotes.get_k_data("600000", "2024-01-01", "2024-01-01").empty
    assert quotes.get_k_data("600000", None, "2024-01-02").empty


def test_to_data_accepts_numpy_like_values():
    import numpy as np

    result = to_data(np.array([[1, 2], [3, 4]]))
    assert isinstance(result, pd.DataFrame)
    assert result.shape == (2, 2)


def test_money_flow_calculation_and_validation():
    class _MockTradeQuotes(StdQuotes):
        def __init__(self, trades):
            self._mock_trades = trades

        def transaction_all(self, symbol="", **kwargs):
            return self._mock_trades

        def transactions_all(self, symbol="", date="", **kwargs):
            return self._mock_trades

    # Mock trades:
    # 1. 超大单买入: 1500手 * 10元 = 1,500,000 元 (buyorsell=0)
    # 2. 大单卖出: 500手 * 10元 = 500,000 元 (buyorsell=1)
    # 3. 中单买入: 100手 * 10元 = 100,000 元 (buyorsell=0)
    # 4. 小单卖出: 10手 * 10元 = 10,000 元 (buyorsell=1)
    mock_df = pd.DataFrame([
        {"time": "09:30", "price": 10.0, "vol": 1500, "num": 1, "buyorsell": 0},
        {"time": "09:31", "price": 10.0, "vol": 500, "num": 1, "buyorsell": 1},
        {"time": "09:32", "price": 10.0, "vol": 100, "num": 1, "buyorsell": 0},
        {"time": "09:33", "price": 10.0, "vol": 10, "num": 1, "buyorsell": 1},
    ])
    client = _MockTradeQuotes(mock_df)

    res = client.money_flow("600000")
    assert not res.empty
    assert list(res.index) == ["超大单", "大单", "中单", "小单", "主力(超大+大单)", "散户(中+小单)", "合计"]

    assert res.loc["超大单", "buy_amount"] == 1_500_000.0
    assert res.loc["超大单", "sell_amount"] == 0.0
    assert res.loc["超大单", "net_amount"] == 1_500_000.0

    assert res.loc["大单", "buy_amount"] == 0.0
    assert res.loc["大单", "sell_amount"] == 500_000.0
    assert res.loc["大单", "net_amount"] == -500_000.0

    assert res.loc["中单", "buy_amount"] == 100_000.0
    assert res.loc["中单", "sell_amount"] == 0.0
    assert res.loc["中单", "net_amount"] == 100_000.0

    assert res.loc["小单", "buy_amount"] == 0.0
    assert res.loc["小单", "sell_amount"] == 10_000.0
    assert res.loc["小单", "net_amount"] == -10_000.0

    assert res.loc["主力(超大+大单)", "net_amount"] == 1_000_000.0
    assert res.loc["散户(中+小单)", "net_amount"] == 90_000.0
    assert res.loc["合计", "net_amount"] == 1_090_000.0

    assert res.attrs["main_net"] == 1_000_000.0
    assert res.attrs["retail_net"] == 90_000.0
    assert res.attrs["trade_count"] == 4

    # Empty validation
    with pytest.raises(TdxhubValidationException, match="symbol"):
        client.money_flow("")

    # Empty trades returns formatted empty DataFrame
    empty_client = _MockTradeQuotes(pd.DataFrame())
    empty_res = empty_client.money_flow("600000")
    assert not empty_res.empty  # has 7 rows of 0/empty
    assert empty_res.attrs["trade_count"] == 0


def test_capital_flow_history_calculation():
    class _MockHistoryQuotes(StdQuotes):
        def __init__(self):
            self._capital_flow_cache = {}

        def bars(self, symbol="", frequency=9, offset=25, **kwargs):
            return pd.DataFrame([
                {"datetime": "2026-09-08 15:00", "close": 10.0},
                {"datetime": "2026-09-09 15:00", "close": 10.5},
                {"datetime": "2026-09-10 15:00", "close": 10.2},
                {"datetime": "2026-09-11 15:00", "close": 10.4},
                {"datetime": "2026-09-14 15:00", "close": 10.8},
                {"datetime": "2026-09-15 15:00", "close": 11.0},
            ])

        def transactions_all(self, symbol="", date="", **kwargs):
            return pd.DataFrame([
                {"time": "09:30", "price": 10.0, "vol": 1500, "num": 1, "buyorsell": 0},  # 超大单买 150万
                {"time": "09:31", "price": 10.0, "vol": 500, "num": 1, "buyorsell": 1},   # 大单卖 50万
            ])

        def transaction_all(self, symbol="", **kwargs):
            return self.transactions_all(symbol=symbol)

    client = _MockHistoryQuotes()
    df = client.capital_flow_history("600000", days=5)
    assert len(df) == 5
    assert "main_5d_net" in df.columns
    assert "main_20d_net" in df.columns
    # Each day: buy 1.5M, sell 0.5M => net 1.0M. Total 5 days = 5.0M
    assert df.attrs["main_5d_net"] == 5_000_000.0
    assert pd.isna(df.attrs["main_20d_net"])
    assert df.attrs["window_20d_days"] == 6
    assert df.attrs["window_20d_complete"] is False
    assert df.attrs["days"] == 5

    # Test money_flow routing with days
    mf_df = client.money_flow("600000", days=5)
    assert len(mf_df) == 5
    assert mf_df.attrs["main_5d_net"] == 5_000_000.0


def test_statistics_symbols_filter():
    import unittest.mock as mock

    class _MockStatQuotes(StdQuotes):
        def __init__(self):
            pass

        def _get_zhb_file(self, filename):
            return b"fake"

    quotes = _MockStatQuotes()
    fake_df = pd.DataFrame([
        {
            "market": "sh",
            "code": "600519",
            "pe_ttm": 20.5,
            "pe_static": 19.8,
            "dividend_yield": 2.5,
            "change_pct": 1.5,
            "trend_days": 3,
            "change_5d": 2.0,
            "change_10d": 3.0,
            "change_20d": -1.0,
            "change_60d": 5.0,
            "change_ytd": -5.0,
        },
        {
            "market": "sz",
            "code": "002594",
            "pe_ttm": 25.0,
            "pe_static": 24.0,
            "dividend_yield": 1.2,
            "change_pct": -0.8,
            "trend_days": -2,
            "change_5d": -1.5,
            "change_10d": 0.5,
            "change_20d": -4.0,
            "change_60d": -2.0,
            "change_ytd": -12.0,
        },
    ])
    with mock.patch("tdxhub.official.parse_tdxstat", return_value=fake_df):
        df_all = quotes.statistics()
        assert len(df_all) == 2

        df_single = quotes.statistics("sh600519")
        assert len(df_single) == 1
        assert df_single.iloc[0]["code"] == "600519"
        assert df_single.iloc[0]["pe_ttm"] == 20.5
        assert df_single.iloc[0]["trend_days"] == 3

        df_list = quotes.statistics(["002594"])
        assert len(df_list) == 1
        assert df_list.iloc[0]["code"] == "002594"


def test_sector_capital_flow():
    class _MockSectorQuotes(StdQuotes):
        def __init__(self):
            pass

        def stock_industries(self, symbols=None, **kwargs):
            return pd.DataFrame([
                {"code": "600519", "tdx_industry_name": "白酒", "sw_industry_name": "白酒", "sw_level1_name": "食品饮料"},
                {"code": "000858", "tdx_industry_name": "白酒", "sw_industry_name": "白酒", "sw_level1_name": "食品饮料"},
            ])

        def capital_flow(self, symbol="", **kwargs):
            net = 1_000_000.0 if symbol == "600519" else 500_000.0
            df = pd.DataFrame(
                [{"net_amount": net}],
                index=["超大单"]
            )
            df.attrs = {
                "main_net": net,
                "main_net_pct": 10.0,
                "retail_net": -net,
                "total_turnover": 10_000_000.0,
            }
            return df

    client = _MockSectorQuotes()
    res = client.sector_capital_flow(name="白酒")
    assert len(res) == 2
    assert res.attrs["sector_name"] == "白酒"
    assert res.attrs["main_net"] == 1_500_000.0
    assert res.attrs["total_turnover"] == 20_000_000.0
    assert res.attrs["top_inflow_code"] == "600519"

    # Test with custom symbols
    res_sym = client.sector_capital_flow(symbols=["600519"])
    assert len(res_sym) == 1
    assert res_sym.attrs["main_net"] == 1_000_000.0


def test_xgsg_and_ipo_aliases():
    quotes = object.__new__(StdQuotes)
    quotes._get_zhb_file = mock.Mock(return_value=b"mock xgsg.cfg")
    fake_xgsg_df = pd.DataFrame([
        {
            "market": "sz",
            "code": "001246",
            "date": "20260918",
            "issue_price": 5.15,
            "name": "力勤资源",
            "source": "xgsg.cfg",
            "raw_fields": ["0", "001246"],
        },
        {
            "market": "bj",
            "code": "920025",
            "date": "20260914",
            "issue_price": 4.26,
            "name": "凯达重工",
            "source": "xgsg.cfg",
            "raw_fields": ["2", "920025"],
        },
    ])

    with mock.patch("tdxhub.official.parse_xgsg", return_value=fake_xgsg_df) as parse_xgsg:
        # 1. Fetch all
        all_ipo = quotes.xgsg()
        assert len(all_ipo) == 2

        # 2. ipo alias
        ipo_res = quotes.ipo()
        assert len(ipo_res) == 2

        # 3. Filter by symbol
        single = quotes.xgsg("001246")
        assert len(single) == 1
        assert single.iloc[0]["name"] == "力勤资源"

        # 4. Filter by prefixed symbol
        single_prefix = quotes.ipo("sz001246")
        assert len(single_prefix) == 1
        assert single_prefix.iloc[0]["code"] == "001246"

        assert quotes._get_zhb_file.call_args_list == [mock.call("xgsg.cfg")] * 4
        assert parse_xgsg.call_args_list == [mock.call(b"mock xgsg.cfg")] * 4
