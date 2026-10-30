"""Dividends and splits must share one affine price-adjustment path."""
from types import SimpleNamespace

import pandas as pd
import pytest

from tdxhub.quotes import StdQuotes
from tdxhub.reader import StdReader
from tdxhub.tools.reversion import reversion


def prices(values, dates=None):
    return pd.DataFrame({**{c: values for c in ['open', 'high', 'low', 'close']},
                         'volume': 100.0, 'vol': 100.0, 'amount': 10000.0},
                        index=pd.to_datetime(dates or ['2026-09-17', '2026-09-18']))


@pytest.mark.parametrize('symbol', ['510300', 'SH.510300', 'sh#510300', '180101', 'SZ180101',
                                    '520001', '560001', '580001', '159001', '160001', '500001'])
@pytest.mark.parametrize('method,expected', [('qfq', [4.9, 4.9]), ('hfq', [5.0, 5.0])])
def test_fund_cash_dividend_and_symbol_normalization(symbol, method, expected):
    raw = prices([5.0, 4.9])
    actions = pd.DataFrame([dict(date='2026-09-18', category=1, fenhong=1.0)])
    saved = raw.copy(deep=True)
    saved_actions = actions.copy(deep=True)
    result = reversion(symbol, raw, actions, method)
    assert result.close.tolist() == expected
    pd.testing.assert_frame_equal(result[['vol', 'volume', 'amount']], raw[['vol', 'volume', 'amount']])
    pd.testing.assert_frame_equal(raw, saved)
    pd.testing.assert_frame_equal(actions, saved_actions)


@pytest.mark.parametrize('method,expected', [('qfq', [2.5, 2.5, 2.5]), ('hfq', [10, 10, 10])])
def test_multiple_splits_compound(method, expected):
    raw = prices([10.0, 5.0, 2.5], ['2026-09-16', '2026-09-17', '2026-09-18'])
    actions = pd.DataFrame([dict(date=d, category=11, suogu=2) for d in ['2026-09-17', '2026-09-18']])
    result = reversion('510300', raw, actions, method)
    assert result.close.tolist() == expected
    assert result.preclose.iloc[1:].tolist() == expected[1:]
    assert result.volume.tolist() == [100, 100, 100]


@pytest.mark.parametrize('same_day', [True, False])
@pytest.mark.parametrize('split_first,ex_price', [(True, 4.9), (False, 4.95)])
def test_cash_and_split_compose_in_event_order(same_day, split_first, ex_price):
    cash = dict(category=1, fenhong=1)
    split = dict(category=11, suogu=2)
    events = [split, cash] if split_first else [cash, split]
    for event, date in zip(events, ['2026-09-12', '2026-09-12' if same_day else '2026-09-13'], strict=True):
        event['date'] = date
    raw = prices([10, ex_price], ['2026-09-11', '2026-09-14'])
    actions = pd.DataFrame(events)
    qfq = reversion('510300', raw, actions, 'qfq')
    hfq = reversion('510300', raw, actions, 'hfq')
    assert qfq.close.tolist() == [ex_price, ex_price]
    assert hfq.close.tolist() == [10, 10]
    assert qfq.preclose.iloc[-1] == ex_price
    assert hfq.preclose.iloc[-1] == 10


def test_fund_keeps_milliyuan_precision():
    actions = pd.DataFrame([dict(date='2026-09-18', category=1, fenhong=0.01)])
    assert reversion('510300', prices([1.235, 1.234]), actions).close.tolist() == [1.234, 1.234]


def test_future_and_first_date_actions_do_not_adjust_earlier_nonexistent_rows():
    actions = pd.DataFrame([dict(date=d, category=11, suogu=2) for d in ['2026-09-17', '2026-09-21']])
    raw = prices([5.0, 5.1])
    assert reversion('510300', raw, actions).close.tolist() == [5.0, 5.1]


@pytest.mark.parametrize('ratio', [0, -1, float('inf'), float('nan'), None])
def test_invalid_split_ratio_is_rejected(ratio):
    actions = pd.DataFrame([dict(date='2026-09-18', category=11, suogu=ratio)])
    with pytest.raises(ValueError, match='suogu'):
        reversion('510300', prices([10.0, 5.0]), actions)


@pytest.mark.parametrize('method', ['qfq', 'hfq'])
@pytest.mark.parametrize('entry', ['bars', 'bars_all', 'reader'])
def test_adjustment_never_changes_actual_turnover(method, entry, monkeypatch):
    raw = prices([10.0, 5.0])
    raw['datetime'] = raw.index.astype(str)
    actions = pd.DataFrame([
        dict(date='2026-09-16', category=5, panhouliutong=1_000_000),
        dict(date='2026-09-18', category=1, songzhuangu=10),
        dict(date='2026-09-18', category=5, panhouliutong=2_000_000),
    ])
    if entry == 'reader':
        reader = object.__new__(StdReader)
        reader.find_path = lambda *a, **kw: 'fixture.day'
        monkeypatch.setattr('tdxhub.reader.TdxDailyBarReader.get_df', lambda *a: raw.copy())
        read = reader.daily
    else:
        q = object.__new__(StdQuotes)
        q.client = SimpleNamespace(get_security_bars=lambda *a: raw.copy(), close=lambda: None)
        q._round2_bars_required = False
        q._round2_client = None
        read = getattr(q, entry)
    plain = read('600000', turnover=True, xdxr=actions)
    adjusted = read('600000', turnover=True, xdxr=actions, adjust=method)
    assert plain.turnover.tolist() == adjusted.turnover.tolist() == [1.0, 0.5]
    assert adjusted.vol.tolist() == adjusted.volume.tolist() == [100, 100]
    assert adjusted.amount.tolist() == [10000, 10000]
