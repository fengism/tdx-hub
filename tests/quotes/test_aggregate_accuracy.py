"""Offline regressions for complete pagination and fund-flow window semantics."""
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from tdxhub.exceptions import TdxhubConnectionError, TdxhubIncompleteDataError, TdxhubValidationException
from tdxhub.failover import EndpointPool, FailoverClient
from tdxhub.quotes import StdQuotes


def quote():
    result = object.__new__(StdQuotes)
    result.client = SimpleNamespace(close=lambda: None)
    # K 线相关用例按经典 wire 语义编写, 显式关闭 round2 默认优先
    result._round2_bars_required = False
    result._round2_client = None
    return result


def flow(net=100.0, *, trade_count=2):
    result = pd.DataFrame({'net_amount': [net]}, index=['大单'])
    result.attrs = dict(main_net=net, main_net_pct=10.0, retail_net=-net,
                        retail_net_pct=-10.0, total_turnover=1000.0, trade_count=trade_count)
    return result


@pytest.mark.parametrize('method,wire,size,args', [
    ('bars_all', 'get_security_bars', 800, {'symbol': '600000'}),
    ('index_all', 'get_index_bars', 800, {'symbol': 'sh000001'}),
    ('transaction_all', 'get_transaction_data', 1800, {'symbol': '600000'}),
    ('transactions_all', 'get_history_transaction_data', 2000, {'symbol': '600000', 'date': '20260918'}),
    ('get_k_data', 'get_security_bars', 800,
     {'code': '600000', 'start_date': '20000101', 'end_date': '20260919'}),
])
@pytest.mark.parametrize('failure_page', [0, 1])
def test_failover_none_is_not_end_of_pagination(method, wire, size, args, failure_page):
    calls = []
    rows = [{'datetime': str(d), 'close': 10.0} for d in pd.bdate_range(end='2026-09-18', periods=size)]

    def load(*unused):
        calls.append(unused)
        if len(calls) > failure_page:
            raise OSError('scripted transport failure')
        return rows

    api = SimpleNamespace(connect=lambda *a, **kw: True, close=lambda: None, **{wire: load})
    q = quote()
    q.client = FailoverClient(EndpointPool([('127.0.0.1', 7709)]), lambda: api,
                              max_failovers=0, raise_exception=False)
    q.client.connect()
    try:
        with pytest.raises(TdxhubConnectionError, match='分页请求失败'):
            getattr(q, method)(**args)
        assert len(calls) == failure_page + 1
    finally:
        q.close()


@pytest.mark.parametrize('last_page', [[], [{'datetime': '2024-01-01', 'close': 1}]])
def test_pagination_empty_and_short_pages_are_success(last_page):
    loader = Mock(side_effect=[[{'datetime': '2024-01-02'}, {'datetime': '2024-01-03'}], last_page])
    result = quote()._collect_pages(loader, 2)
    assert len(result) == 2 + len(last_page)
    assert result.index.is_monotonic_increasing
    assert loader.call_count == 2


def test_pagination_since_exact_boundary_needs_no_extra_page():
    loader = Mock(return_value=[{'datetime': '2024-01-02'}, {'datetime': '2024-01-03'}])
    result = quote()._collect_pages(loader, 2, since='20240102')
    assert len(result) == 2
    loader.assert_called_once_with(0, 2)


def test_pagination_offset_limit_is_not_full_history():
    page = pd.DataFrame({'time': range(2000)})
    with pytest.raises(TdxhubIncompleteDataError) as err:
        quote()._collect_pages(lambda start, size: page, 2000)
    assert err.value.data['reason'] == 'offset_limit'
    assert err.value.data['next_start'] == 66000


def test_sector_deduplicates_aliases_but_not_cross_market_codes():
    q = quote()
    q.capital_flow = Mock(return_value=flow())
    result = q.sector_capital_flow(symbols=['600000', 'SH.600000', 'sh#600000', '600000', 'sz600000'])
    assert q.capital_flow.call_count == 2
    assert result.attrs['stock_count'] == 2
    assert result.attrs['main_net'] == 200
    assert result.code.tolist() == ['600000', 'sz600000']


def test_sector_preserves_failure_context():
    q = quote()
    failure = OSError('scripted error')
    q.capital_flow = Mock(side_effect=[flow(), failure])
    with pytest.raises(TdxhubIncompleteDataError, match='600001') as err:
        q.sector_capital_flow(symbols=['600000', '600001'])
    assert err.value.__cause__ is failure
    assert err.value.data['reason'] == 'component_failed'


@pytest.mark.parametrize('empty', [None, pd.DataFrame(), flow(0, trade_count=0)])
def test_sector_no_trades_is_not_zero_net(empty):
    q = quote()
    q.capital_flow = Mock(return_value=empty)
    with pytest.raises(TdxhubIncompleteDataError, match='无成交数据'):
        q.sector_capital_flow(symbols=['600000'])


def test_sector_real_zero_net_is_valid():
    q = quote()
    q.capital_flow = Mock(return_value=flow(0))
    assert q.sector_capital_flow(symbols=['600000']).attrs['main_net'] == 0


@pytest.mark.parametrize('symbols', [[], '', ' , '])
def test_sector_rejects_empty_component_list(symbols):
    with pytest.raises(TdxhubValidationException):
        quote().sector_capital_flow(name='custom', symbols=symbols)


def history_quote(count=60):
    q = quote()
    bars = pd.DataFrame({'datetime': pd.bdate_range(end='2026-09-18', periods=count), 'close': 10.0})

    def load(symbol, frequency, start=0, offset=800, **kwargs):
        end = len(bars) - start
        return bars.iloc[max(0, end - offset):max(0, end)].copy()

    q.bars = Mock(side_effect=load)
    q.capital_flow = Mock(return_value=flow())
    return q


def test_history_same_end_date_has_same_complete_windows():
    q = history_quote()
    short = q.capital_flow_history('600000', days=5)
    long = q.capital_flow_history('600000', days=20)
    assert len(short) == 5 and len(long) == 20
    for result in (short, long):
        assert result['main_20d_net'].tolist() == [2000] * len(result)
        assert result.attrs['main_5d_net'] == 500
        assert result.attrs['main_20d_net'] == 2000
        assert result.attrs['retail_20d_net'] == -2000
        assert result.attrs['total_20d_amount'] == 20000
        assert result.attrs['window_20d_complete'] is True
        assert result.window_20d_days.eq(20).all()
        assert result.attrs['main_20d_pct'] == 10
    pd.testing.assert_frame_equal(short, long.tail(5).reset_index(drop=True))
    assert q.bars.call_args_list[0].kwargs['offset'] == 25


def test_history_short_life_returns_null_and_coverage_not_partial_total():
    q = history_quote(6)
    result = q.capital_flow_history('600000', days=5)
    assert result.main_20d_net.isna().all()
    assert result.main_20d_pct.isna().all()
    assert result.window_20d_days.tolist() == [2, 3, 4, 5, 6]
    assert result.window_20d_complete.eq(False).all()
    assert result.attrs['main_5d_net'] == 500
    assert pd.isna(result.attrs['main_20d_net'])
    assert pd.isna(result.attrs['total_20d_amount'])
    assert pd.isna(result.attrs['retail_20d_net'])
    assert result.attrs['window_20d_days'] == 6


def test_history_more_than_one_bar_page_is_not_silently_capped():
    q = history_quote(900)
    result = q.capital_flow_history('600000', days=801)
    assert len(result) == 801
    assert result.main_20d_net.eq(2000).all()
    assert [(c.kwargs['start'], c.kwargs['offset']) for c in q.bars.call_args_list] == [(0, 800), (800, 21)]
    assert q.capital_flow.call_count == 820


@pytest.mark.parametrize('days', [0, -1, 1.5, True, '5', None])
def test_history_requires_positive_integer(days):
    q = history_quote()
    with pytest.raises(TdxhubValidationException):
        q.capital_flow_history('600000', days=days)
    q.bars.assert_not_called()


def test_history_empty_bars_does_not_invent_zero_summary():
    with pytest.raises(TdxhubIncompleteDataError, match='无可用日 K'):
        history_quote(0).capital_flow_history('600000')


def test_history_no_previous_close_does_not_invent_zero_return():
    result = history_quote(1).capital_flow_history('600000', days=5)
    assert pd.isna(result.iloc[0].change_pct)
    assert pd.isna(result.attrs['main_5d_net'])
    assert result.attrs['window_5d_days'] == 1


def test_history_missing_warmup_trades_is_failure():
    q = history_quote()
    q.capital_flow = Mock(side_effect=[flow(), flow(trade_count=0)])
    with pytest.raises(TdxhubIncompleteDataError, match='缺少成交数据'):
        q.capital_flow_history('600000', days=5)


def test_history_flow_exception_propagates():
    q = history_quote()
    q.capital_flow = Mock(side_effect=TdxhubConnectionError('tick failure'))
    with pytest.raises(TdxhubConnectionError, match='tick failure'):
        q.capital_flow_history('600000')


@pytest.mark.parametrize('actions_arg', ['xdxr', 'gbbq'])
@pytest.mark.parametrize('adjust,expected_close', [('qfq', 5.0), ('hfq', 10.0)])
def test_history_adjusts_collected_prices_without_adjusting_tick_flows(actions_arg, adjust, expected_close):
    q = quote()
    raw = pd.DataFrame({
        'datetime': pd.bdate_range(end='2026-09-18', periods=25).astype(str),
        **{column: [10.0] * 24 + [5.0] for column in ['open', 'high', 'low', 'close']},
        'vol': 100.0,
        'amount': 10000.0,
    })
    q.client.get_security_bars = Mock(return_value=raw)
    q.capital_flow = Mock(return_value=flow())
    plain = q.capital_flow_history('600000', days=5)
    q.capital_flow.reset_mock()
    actions = pd.DataFrame([dict(date='2026-09-18', category=1, songzhuangu=10)])
    thresholds = (50000.0, 300000.0, 2000000.0)
    adjusted = q.capital_flow_history(
        '600000', days=5, adjust=adjust, turnover=True, thresholds=thresholds,
        **{actions_arg: actions},
    )
    assert plain.close.tolist() == [10, 10, 10, 10, 5]
    assert adjusted.close.tolist() == [expected_close] * 5
    assert adjusted.change_pct.eq(0).all()
    pd.testing.assert_frame_equal(
        plain.drop(columns=['close', 'change_pct']),
        adjusted.drop(columns=['close', 'change_pct']),
    )
    assert adjusted.attrs == plain.attrs
    assert q.capital_flow.call_count == 24
    for call in q.capital_flow.call_args_list:
        assert set(call.kwargs) == {'symbol', 'date', 'thresholds'}
        assert call.kwargs['thresholds'] == thresholds
