"""Offline availability regressions; no public quote servers are contacted."""

import threading
from collections import deque

import pytest

from tdxhub.failover import EndpointPool, FailoverClient
from tdxhub.tdx.errors import ProtocolError
from tests.quotes.test_failover import A, B, FakeClock, ScriptedApi


def test_request_budget_is_shared_across_failover():
    from tdxhub.tdx.deadline import remaining

    clock = FakeClock()
    observed = []

    class Api(ScriptedApi):
        def get_security_bars(self, *args):
            observed.append(remaining())
            clock.now += 2
            raise ConnectionError("offline")

    connections = []
    proxy = FailoverClient(
        EndpointPool([A, B]),
        lambda: Api({}, connections),
        timeout=3,
        request_timeout=3,
        clock=clock,
    )
    proxy.connect()
    with pytest.raises(TimeoutError):
        proxy.get_security_bars(9, 1, "600000", 0, 1)
    assert observed == [3, 1]
    assert clock.now == 104
    assert len(connections) == 2


def test_budget_exhausted_on_first_server_does_not_connect_backup():
    clock = FakeClock()

    class Api(ScriptedApi):
        def get_security_bars(self, *args):
            clock.now += 3
            raise ConnectionError("offline")

    connections = []
    proxy = FailoverClient(
        EndpointPool([A, B]),
        lambda: Api({}, connections),
        timeout=3,
        request_timeout=3,
        clock=clock,
        raise_exception=False,
    )
    proxy.connect()
    assert proxy.get_security_bars(9, 1, "600000", 0, 1) is None
    assert connections == [(A, 3)]
    assert proxy.server_status()[1]["failures"] == 0


def test_heartbeat_quarantines_current_connection_and_ignores_stale_event():
    created = []

    def factory():
        api = ScriptedApi({B: deque([[{"close": 1}]])}, [])
        created.append(api)
        return api

    proxy = FailoverClient(EndpointPool([A, B]), factory)
    proxy.connect()
    old = created[0]
    old.heartbeat_error_callback(ConnectionError("heartbeat EOF"))
    assert not proxy.is_connected
    assert old.closed
    assert proxy.endpoint_pool.available() == [B]
    assert proxy.get_security_bars(9, 1, "600000", 0, 1) == [{"close": 1}]
    old.heartbeat_error_callback(ConnectionError("late heartbeat"))
    assert proxy.is_connected
    assert proxy.endpoint == B
    assert proxy.server_status()[0]["failures"] == 1
    proxy.close()
    created[-1].heartbeat_error_callback(ConnectionError("after close"))
    assert proxy.server_status()[1]["failures"] == 0


def test_proxy_lock_wait_is_bounded_and_does_not_quarantine_server():
    proxy = FailoverClient(EndpointPool([A]), lambda: ScriptedApi({}, []), request_timeout=0.02)
    proxy.connect()
    ready, release = threading.Event(), threading.Event()

    def hold_lock():
        with proxy._lock:
            ready.set()
            release.wait(2)

    thread = threading.Thread(target=hold_lock)
    thread.start()
    try:
        assert ready.wait(1)
        with pytest.raises(TimeoutError):
            proxy.get_security_bars(9, 1, "600000", 0, 1)
    finally:
        release.set()
        thread.join(1)
    assert proxy.endpoint_pool.available() == [A]


def test_target_probes_separate_markets_interfaces_and_keep_empty_unknown(monkeypatch):
    from tdxhub import config
    from tdxhub.quotes import StdQuotes

    calls = []

    class Api(ScriptedApi):
        def __init__(self, **kwargs):
            super().__init__({}, [])

        def get_security_quotes(self, stocks):
            market, _ = stocks[0]
            calls.append((self.endpoint, "quotes", market))
            return [] if market == 2 else [{"price": 1}]

    values = {"SERVER.HQ": [("a", *A), ("b", *B)]}
    monkeypatch.setattr(config, "setup", lambda **kwargs: True)
    monkeypatch.setattr(config, "get", lambda key, default=None: values.get(key, default))
    monkeypatch.setattr(config, "set", lambda *args: None)
    monkeypatch.setattr("tdxhub.quotes.StandardClient", Api)
    with StdQuotes(probe_symbols=["sh600000", "bj920001"], probe_frequencies=[9, 8]) as quotes:
        pool = quotes.client.endpoint_pool
        # 探测不再使用已停用的旧 K 线命令: 未探测能力保持 unknown, 不限制 failover
        assert pool.capability_status(A, ("get_security_bars", (2,), 8)) == "unknown"
        assert pool.capability_status(A, ("get_security_bars", (1,), 9)) == "unknown"
        assert pool.capability_status(A, ("get_security_quotes", (2,), None)) == "unknown"
        assert pool.capability_status(A, ("get_security_quotes", (1,), None)) == "supported"
        assert set(pool.available(("get_security_bars", (2,), 8))) == {A, B}
        assert set(pool.available(("get_security_quotes", (1,), None))) == {A, B}
    assert len(calls) == 4


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_invalid_request_timeout_rejected(timeout):
    with pytest.raises(ValueError):
        FailoverClient(EndpointPool([A]), lambda: None, request_timeout=timeout)


@pytest.mark.parametrize("strict", [True, False])
def test_managed_extended_client_has_only_one_failover_budget(strict):
    from tdxhub.quotes import ExtQuotes

    calls = []

    class Api(ScriptedApi):
        def get_instrument_quote(self, *args, **kwargs):
            calls.append(self.endpoint)
            assert self.auto_retry is False
            raise ConnectionError("offline fixture")

    quotes = object.__new__(ExtQuotes)
    quotes.client = FailoverClient(EndpointPool([A, B]), lambda: Api({}, []), raise_exception=strict)
    quotes.client.connect()
    if strict:
        with pytest.raises(ConnectionError, match="offline fixture"):
            quotes.quote(market=31, symbol="00700")
    else:
        assert quotes.quote(market=31, symbol="00700").empty
    assert calls == [A, B]


def test_managed_extended_client_does_not_retry_valid_empty_result():
    from tdxhub.quotes import ExtQuotes

    calls = []

    class Api(ScriptedApi):
        def get_instrument_quote(self, *args, **kwargs):
            calls.append(self.endpoint)
            return []

    quotes = object.__new__(ExtQuotes)
    quotes.client = FailoverClient(EndpointPool([A, B]), lambda: Api({}, []))
    quotes.client.connect()
    assert quotes.quote(market=31, symbol="00700").empty
    assert calls == [A]


def test_probe_selection_keeps_complementary_capabilities_before_redundant_nodes():
    from tdxhub.quotes import _probe_quote_endpoints
    from tests.quotes.test_failover import C

    calls = []

    class Api(ScriptedApi):
        def get_security_quotes(self, stocks):
            market = stocks[0][0]
            calls.append((self.endpoint, market))
            if self.endpoint != C and market == 2:
                raise ProtocolError("BJ unsupported")
            if self.endpoint == C and market == 1:
                raise ProtocolError("SH unsupported")
            return [{"price": 1}]

    requests = [("get_security_quotes", ([(1, "600000")],)), ("get_security_quotes", ([(2, "920002")],))]
    endpoints, _, _ = _probe_quote_endpoints([A, B, C], lambda: Api({}, []), None, 3, 2, requests)
    assert endpoints == [A, C]
    assert len(calls) == 6


@pytest.mark.parametrize(
    "symbols, frequencies",
    [
        ([], [9]),
        (["sh600000", "sh600519"], [9]),
        (["bj920002"], [12]),
        (["bad"], [9]),
        (["sh6000000"], [9]),
        (["SH６０００００"], [9]),
        (["XX600000"], [9]),
        ([600000], [9]),
    ],
)
def test_invalid_probe_options_fail_before_connect(monkeypatch, symbols, frequencies):
    from tdxhub.quotes import StdQuotes

    def unexpected(**kwargs):
        pytest.fail("must validate before setup or connecting")

    monkeypatch.setattr("tdxhub.quotes.config.setup", unexpected)
    monkeypatch.setattr("tdxhub.quotes.StandardClient", unexpected)
    with pytest.raises(ValueError):
        StdQuotes(probe_symbols=symbols, probe_frequencies=frequencies, bestip=True)


def test_quotes_only_probe_accepts_empty_response_as_unknown():
    from tdxhub.quotes import _probe_quote_endpoints, _standard_probe_requests

    connections = []

    class Api(ScriptedApi):
        def get_security_quotes(self, stocks):
            assert stocks == [(2, "920002")]
            return []

    requests = _standard_probe_requests(["bj920002"], [])
    healthy, error, observations = _probe_quote_endpoints([A], lambda: Api({}, connections), None, 3, 1, requests)
    assert healthy == [A]
    assert error is None
    assert observations == [(A, ("get_security_quotes", (2,), None), "unknown", None)]
    assert connections == [(A, 3)]


@pytest.mark.parametrize("transport_failure", [True, False])
def test_probe_rejects_transport_failure_or_no_successful_response(transport_failure):
    from tdxhub.quotes import _probe_quote_endpoints, _standard_probe_requests

    created = []

    class Api(ScriptedApi):
        def get_security_quotes(self, stocks):
            if transport_failure:
                raise ConnectionError("offline probe")
            raise ProtocolError("malformed quotes")

    def factory():
        api = Api({}, [])
        created.append(api)
        return api

    requests = _standard_probe_requests(["bj920002"], [9])
    healthy, error, observations = _probe_quote_endpoints([A], factory, None, 3, 1, requests)
    assert healthy == []
    assert isinstance(error, ConnectionError if transport_failure else ProtocolError)
    assert observations == []  # Rejected nodes do not seed the runtime pool.
    assert all(api.closed for api in created)
