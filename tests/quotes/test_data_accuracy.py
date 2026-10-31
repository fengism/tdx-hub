from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from tdxhub.exceptions import TdxhubValidationException
from tdxhub.quotes import StdQuotes


def test_get_k_data_weekend_does_not_skip_friday(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 9, 20)

    monkeypatch.setattr("tdxhub.quotes.datetime", Clock)
    calls = []
    rows = [{"datetime": f"2026-09-{day} 15:00", "close": day} for day in range(14, 19)]

    def load(frequency, market, code, start, size):
        calls.append(start)
        return rows[: len(rows) - start] if start < len(rows) else []

    q = object.__new__(StdQuotes)
    q.client = SimpleNamespace(get_security_bars=load, to_df=pd.DataFrame)
    q._round2_bars_required = False
    q._round2_client = None
    result = q.get_k_data("sh600519", "20260914", "20260919")
    assert list(result.index) == [f"2026-09-{day}" for day in range(14, 19)]
    assert calls == [0]


def test_get_k_data_paginates_until_real_start_date():
    dates = pd.bdate_range("2018-01-01", periods=1603)
    rows = [{"datetime": str(d), "close": 1} for d in dates]
    calls = []

    def load(frequency, market, code, start, size):
        calls.append(start)
        return rows[max(0, len(rows) - start - size) : len(rows) - start]

    q = object.__new__(StdQuotes)
    q.client = SimpleNamespace(get_security_bars=load, to_df=pd.DataFrame)
    q._round2_bars_required = False
    q._round2_client = None
    result = q.get_k_data("sh600519", dates[1], dates[-1])
    assert len(result) == 1601
    assert calls == [0, 800, 1600]
    assert result.index.is_monotonic_increasing
    assert result.iloc[0]["date"] == str(dates[1].date())


def test_capital_flow_only_counts_valid_normal_executions():
    q = object.__new__(StdQuotes)
    q.transactions_all = lambda **kw: pd.DataFrame(
        [
            {"price": 4.582, "vol": vol, "buyorsell": side}
            for vol, side in [(10, 0), (20, 1), (30, 2), (40, 5), (0, 8), (-1, 0)]
        ]
    )
    result = q.capital_flow("sh510300", date="20260918")
    assert result.attrs["total_turnover"] == pytest.approx(4.582 * 60 * 100)
    assert result.attrs["trade_count"] == 3
    assert result.attrs["excluded_trade_count"] == 3
    assert result.attrs["unknown_side_counts"] == {5: 1, 8: 1}
    assert result.attrs["volume_multiplier"] == 100
    assert result.attrs["neutral_turnover"] == pytest.approx(4.582 * 30 * 100)


def test_capital_flow_rejects_unverified_bond_volume_unit():
    q = object.__new__(StdQuotes)
    q.transactions_all = lambda **kw: pytest.fail("must validate before fetching")
    with pytest.raises(TdxhubValidationException, match="成交量单位"):
        q.capital_flow("sh113052", date="20260918")


def test_beijing_count_matches_report_directory_and_keeps_raw_opt_in():
    q = object.__new__(StdQuotes)
    q.client = SimpleNamespace(get_security_count=lambda **kw: 383)
    q.stocks = lambda **kw: pd.DataFrame({"code": ["920001", "920002"]})
    assert q.stock_count(2) == 2
    assert q.stock_count(2, raw=True) == 383
    assert q.stock_count(1) == 383
    with pytest.raises(TdxhubValidationException):
        q.stock_count(2, security_type="stock", raw=True)


def test_unmarked_legacy_finance_is_not_silently_normalized():
    raw = {"zongguben": 1250081562.5, "farengu": 893893520000, "jingzichan": 2512536000000, "meigujingzichan": 200.9900}
    q = object.__new__(StdQuotes)
    q.client = SimpleNamespace(get_finance_info=lambda **kw: raw)
    result = q.finance("sh600519")
    assert result.iloc[0]["jingzichan"] == raw["jingzichan"]
    quality = result.iloc[0]["data_quality"]
    assert quality["status"] == "legacy_unverified"
    assert quality["issues"] == ["share_components_exceed_total", "net_assets_per_share_mismatch"]
    assert quality["net_assets_per_share_ratio"] == pytest.approx(10, rel=0.001)
    assert "data_quality" not in raw
