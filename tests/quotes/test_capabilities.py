from tdxhub.failover import EndpointPool
from tdxhub.tdx.errors import ProtocolError
from tests.quotes.test_failover import A, B, C, FakeClock, client_for

BJ_MINUTE = ("get_security_bars", (2,), 8)
SH_DAY = ("get_security_bars", (1,), 9)


def test_capability_ttl_isolation_and_preference():
    clock = FakeClock()
    pool = EndpointPool([A, B], clock=clock, capability_ttl=30)
    pool.report_capability(A, BJ_MINUTE, "failed", "bad packet")
    pool.report_capability(B, BJ_MINUTE, "supported")
    assert pool.available(BJ_MINUTE) == [B]
    assert pool.available(SH_DAY) == [A, B]
    pool.report_success(A)
    assert pool.available(BJ_MINUTE) == [B]
    assert pool.snapshot()[0]["capabilities"][0]["status"] == "failed"
    clock.now += 31
    assert pool.available(BJ_MINUTE) == [A, B]
    assert pool.snapshot()[0].get("capabilities", []) == []


def test_protocol_failure_is_scoped_and_replayed():
    client, connections = client_for({A: [ProtocolError("short body")], B: [[{"close": 2}]], C: []})
    assert client.get_security_bars(8, 2, "920001", 0, 1) == [{"close": 2}]
    assert connections == [(A, 3), (B, 3)]
    assert client.endpoint_pool.available(BJ_MINUTE) == [B, C]
    assert client.endpoint_pool.available(SH_DAY) == [A, B, C]
    assert client.server_status()[0]["failures"] == 0


def test_empty_result_does_not_claim_support_or_retry():
    client, connections = client_for({A: [[]], B: [], C: []})
    assert client.get_security_bars(8, 2, "920001", 0, 1) == []
    assert connections == [(A, 3)]
    assert client.server_status()[0]["capabilities"][0]["status"] == "unknown"


def test_learned_support_preferred_on_other_client_sharing_pool():
    client, connections = client_for({A: [[{"close": 1}]], B: [[{"close": 2}]], C: []})
    client.endpoint_pool.report_capability(B, BJ_MINUTE, "supported")
    fork = client.fork()
    fork.connect()
    assert fork.get_security_bars(8, 2, "920001", 0, 1) == [{"close": 2}]
    assert fork.endpoint == B
    assert client.endpoint == A


def test_keyword_call_learns_same_key_and_nonraising_error_is_not_empty_success():
    client, _ = client_for({A: [ProtocolError("bad")], B: [], C: []}, max_failovers=0, raise_exception=False)
    # ScriptedApi intentionally accepts only positional args, so use the pool key
    # extraction directly for keyword equivalence before exercising the failure.
    from tdxhub.failover import request_capability

    assert request_capability("get_security_bars", (), {"category": 8, "market": 2}) == BJ_MINUTE
    assert client.get_security_bars(8, 2, "920001", 0, 1) is None
    assert client.endpoint_pool.available(BJ_MINUTE) == [B, C]


def test_transport_eof_is_globally_quarantined_not_capability_failure():
    from tdxhub.tdx.protocol.base import ResponseRecvFails

    client, _ = client_for({A: [ResponseRecvFails("EOF")], B: [[{"close": 2}]], C: []})
    client.get_security_bars(8, 2, "920001", 0, 1)
    assert client.endpoint_pool.available(SH_DAY) == [B, C]
    assert client.server_status()[0]["failures"] == 1
    assert client.server_status()[0].get("capabilities", []) == []


def test_real_wrapped_parser_error_triggers_scoped_failover():
    from tdxhub.failover import FailoverClient
    from tdxhub.tdx.client import StandardClient
    from tests.tdx.test_transport import StreamSocket, frame

    class Api(StandardClient):
        def connect(self, address, port, time_out):
            body = bytes.fromhex("2003") if address == A[0] else bytes.fromhex("0000")
            self.client = StreamSocket(frame(body))
            return self

        def close(self):
            self.client = None

    client = FailoverClient(EndpointPool([A, B]), lambda: Api(raise_exception=True))
    client.connect()
    assert client.get_security_bars(8, 2, "920001", 0, 1) == []
    assert client.endpoint == B
    assert client.server_status()[0]["capabilities"][0]["status"] == "failed"
    assert "body_length=2" in client.server_status()[0]["capabilities"][0]["last_error"]


def test_mixed_market_capability_does_not_claim_single_market_support():
    from tdxhub.failover import request_capability

    key = request_capability("get_security_quotes", ([(1, "600000"), (2, "920001")],), {})
    pool = EndpointPool([A, B])
    pool.report_capability(A, key, "failed")
    assert pool.available(key) == [B]
    assert pool.available(("get_security_quotes", (2,), None)) == [A, B]
    assert request_capability("get_security_quotes", ((2, "920001"),), {}) is None
