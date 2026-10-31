"""Offline wire/file fixtures for the project-owned pure-Python implementation."""

import json
import struct
import warnings
import zlib
from collections import deque
from pathlib import Path
from threading import Lock

import pytest

from tdxhub.quotes import StdQuotes
from tdxhub.tdx.client import StandardClient
from tdxhub.tdx.codec import get_security_coefficient, get_security_type
from tdxhub.tdx.files import TdxDailyBarReader
from tdxhub.tdx.protocol.base import (
    ResponseHeaderRecvFails,
    ResponseRecvFails,
    SendRequestPkgFails,
)
from tdxhub.tdx.protocol.ext.ex_get_instrument_count import GetInstrumentCount
from tdxhub.tdx.protocol.raw_parser import RawParser
from tdxhub.tdx.protocol.std.get_security_count import GetSecurityCountCmd


class SocketFixture:
    def __init__(self, chunks, send_size=None):
        self.chunks = deque(chunks)
        self.send_size = send_size
        self.sent = []
        self.send_pkg_num = self.send_pkg_bytes = self.last_api_send_bytes = 0
        self.recv_pkg_num = self.recv_pkg_bytes = self.last_api_recv_bytes = 0
        self.first_pkg_send_time = None

    def send(self, data):
        self.sent.append(bytes(data))
        return len(data) if self.send_size is None else self.send_size

    def recv(self, size):
        chunk = self.chunks.popleft() if self.chunks else b""
        assert len(chunk) <= size
        return chunk


def response_header(body, wire_body=None):
    return struct.pack("<IIIHH", 0, 0, 0, len(body if wire_body is None else wire_body), len(body))


@pytest.mark.parametrize("market", [0, 1, 2])
def test_standard_count_request_and_response(market):
    body = struct.pack("<H", 4321)
    sock = SocketFixture([response_header(body), body])
    parser = GetSecurityCountCmd(sock)
    parser.setParams(market)

    assert parser.call_api() == 4321
    assert sock.sent == [bytes.fromhex("0c0c186c0001080008004e04") + struct.pack("<H", market) + b"\x75\xc7\x33\x01"]


def test_extended_count_request_and_response():
    body = bytes(19) + struct.pack("<I", 123456)
    sock = SocketFixture([response_header(body), body])
    assert GetInstrumentCount(sock).call_api() == 123456
    assert sock.sent == [bytes.fromhex("01034866000102000200f023")]


@pytest.mark.parametrize("compressed", [False, True])
@pytest.mark.parametrize("locked", [False, True])
def test_transport_decodes_fragmented_response_and_tracks_traffic(compressed, locked):
    body = b"tdx-response" * 32
    wire = zlib.compress(body) if compressed else body
    sock = SocketFixture([response_header(body, wire), wire[:3], wire[3:]])
    lock = Lock() if locked else None
    parser = RawParser(sock, lock=lock)
    parser.setParams(b"request")

    assert parser.call_api() == body
    assert sock.sent == [b"request"]
    assert sock.send_pkg_num == 1
    assert sock.send_pkg_bytes == sock.last_api_send_bytes == 7
    assert sock.recv_pkg_num == 3
    assert sock.recv_pkg_bytes == sock.last_api_recv_bytes == 16 + len(wire)
    assert sock.first_pkg_send_time is not None
    if lock is not None:
        assert not lock.locked()


@pytest.mark.parametrize(
    ("chunks", "send_size", "error"),
    [
        ([], 0, SendRequestPkgFails),
        ([b"short"], None, ResponseHeaderRecvFails),
        ([response_header(b"payload"), b""], None, ResponseRecvFails),
        ([response_header(b"payload"), b"pay", b""], None, ResponseRecvFails),
    ],
)
def test_transport_preserves_failure_types(chunks, send_size, error):
    lock = Lock()
    parser = RawParser(SocketFixture(chunks, send_size), lock=lock)
    parser.setParams(b"request")
    with pytest.raises(error):
        parser.call_api()
    assert not lock.locked()


def test_daily_file_decoding(tmp_path):
    path = tmp_path / "sh600000.day"
    path.write_bytes(
        struct.pack("<IIIIIfII", 20260917, 1000, 1200, 900, 1100, 123456.0, 10000, 0)
        + struct.pack("<IIIIIfII", 20260918, 1100, 1300, 1000, 1200, 234567.0, 20000, 0)
    )

    frame = TdxDailyBarReader().get_df(str(path))
    assert frame.index.strftime("%Y-%m-%d").tolist() == ["2026-09-17", "2026-09-18"]
    assert list(frame.columns) == ["open", "high", "low", "close", "amount", "volume"]
    assert frame.iloc[0].to_dict() == {
        "open": 10.0,
        "high": 12.0,
        "low": 9.0,
        "close": 11.0,
        "amount": 123456.0,
        "volume": 100.0,
    }
    assert frame.iloc[1]["close"] == 12.0
    assert frame.iloc[1]["volume"] == 200.0


@pytest.fixture
def beijing_quotes_wire():
    return json.loads((Path(__file__).parent / "fixtures" / "beijing_quotes.json").read_text(encoding="utf-8"))


def quote_api(body):
    api = StandardClient(auto_retry=False, raise_exception=True)
    api.client = SocketFixture([response_header(body), body])
    return api


def assert_quote_values(row, expected):
    for field, value in expected.items():
        if isinstance(value, float):
            assert row[field] == pytest.approx(value), field
        else:
            assert row[field] == value, field


@pytest.mark.parametrize("market", [2, "bj", "BJ"])
@pytest.mark.parametrize("code", ["920002", "430090", "830799", "872925"])
def test_beijing_stock_has_explicit_price_coefficient(market, code, caplog):
    assert get_security_type(market, code) == "BJ_A_STOCK"
    assert get_security_coefficient(market, code) == 0.01
    assert "NotImplementedError" not in caplog.text


@pytest.mark.parametrize("code", ["600519", "000001", "930001", "92000", "920abc", ""])
def test_beijing_classification_does_not_claim_index_or_unknown_codes(code):
    with pytest.raises(NotImplementedError):
        get_security_type(2, code)


@pytest.mark.parametrize(
    ("market", "code", "security_type", "coefficient"),
    [
        (1, "600519", "SH_A_STOCK", 0.01),
        (0, "000001", "SZ_A_STOCK", 0.01),
        (1, "510300", "SH_FUND", 0.001),
        (0, "159915", "SZ_FUND", 0.001),
    ],
)
def test_existing_security_coefficients_are_unchanged(market, code, security_type, coefficient):
    assert get_security_type(market, code) == security_type
    assert get_security_coefficient(market, code) == coefficient


@pytest.mark.parametrize("args", [(2, "920002"), ((2, "920002"),), ([(2, "920002")],)])
def test_beijing_quotes_all_api_call_forms_send_request(args, beijing_quotes_wire, caplog):
    api = quote_api(bytes.fromhex(beijing_quotes_wire["beijing_body_hex"]))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        rows = api.get_security_quotes(*args)

    assert rows is not None and len(rows) == 1
    assert_quote_values(rows[0], beijing_quotes_wire["expected"][1])
    assert api.client.sent == [bytes.fromhex("0c0120630002130013003e050500000000000000010002393230303032")]
    assert not caught
    assert "NotImplementedError" not in caplog.text


@pytest.mark.parametrize("symbol", ["920002", "bj920002", ["920002"], ["600519", "920002"]])
def test_public_quotes_decodes_beijing_and_mixed_market_snapshots(symbol, beijing_quotes_wire, caplog):
    mixed = isinstance(symbol, list) and len(symbol) == 2
    body_key = "body_hex" if mixed else "beijing_body_hex"
    quotes = object.__new__(StdQuotes)
    quotes.client = quote_api(bytes.fromhex(beijing_quotes_wire[body_key]))
    quotes._round2_quotes_required = False
    quotes._round2_client = None
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = quotes.quotes(symbol)

    expected = beijing_quotes_wire["expected"] if mixed else beijing_quotes_wire["expected"][1:]
    assert len(result) == len(expected)
    for row, values in zip(result.to_dict("records"), expected, strict=True):
        assert_quote_values(row, values)
    assert not any(column.startswith(("active", "reversed_bytes")) for column in result.columns)
    if mixed:
        assert quotes.client.client.sent == [bytes.fromhex(beijing_quotes_wire["request_hex"])]
    else:
        assert quotes.client.client.sent == [
            bytes.fromhex("0c0120630002130013003e050500000000000000010002393230303032")
        ]
    assert not caught
    assert "NotImplementedError" not in caplog.text


@pytest.mark.parametrize("market", [2, "bj", "BJ"])
def test_beijing_index_has_explicit_type(market):
    assert get_security_type(market, "899050") == "BJ_INDEX"
    assert get_security_coefficient(market, "899050") == 0.01
