from __future__ import annotations

import struct

import pytest

from tdxhub.quotes import StdQuotes
from tdxhub.tdx.protocol.std.get_security_list import GetSecurityList
from tdxhub.utils import get_stock_market
from tests.tdx.test_protocol import quote_api


@pytest.mark.parametrize("code", ["520500", "560010", "588000", "589000"])
def test_shanghai_fund_prefixes_route_to_shanghai(code):
    assert get_stock_market(code) == 1


def encode_price(value):
    """Fixture encoder: signed first 6 bits, then unsigned 7-bit groups."""
    magnitude = abs(value)
    first = (magnitude & 0x3F) | (0x40 if value < 0 else 0)
    magnitude >>= 6
    encoded = bytearray([first | (0x80 if magnitude else 0)])
    while magnitude:
        byte = magnitude & 0x7F
        magnitude >>= 7
        encoded.append(byte | (0x80 if magnitude else 0))
    return bytes(encoded)


def snapshot_body(market, code):
    body = struct.pack("<HHB6sH", 0, 1, market, code.encode("ascii"), 1)
    # Price, close/open/high/low deltas, two reserved fields, volume/current volume.
    body += b"".join(map(encode_price, [1639, 20, -10, 30, -40, 0, 0, 123, 7]))
    body += struct.pack("<f", 456.0)
    body += b"".join(map(encode_price, [50, 73, 0, 0]))
    for level in range(1, 6):
        body += b"".join(map(encode_price, [-level, level, 78 + level, 89 + level]))
    return body + struct.pack("<H", 0) + b"\0" * 4 + struct.pack("<hH", 0, 1)


@pytest.mark.parametrize("public", [False, True], ids=["client", "quotes"])
@pytest.mark.parametrize(
    "market, code, coefficient",
    [
        (1, "588000", 0.001),
        (1, "560010", 0.001),
        (1, "520500", 0.001),
        (1, "510300", 0.001),
        (1, "589000", 0.001),
        (0, "159915", 0.001),
        (0, "180101", 0.001),
        (1, "600000", 0.01),
        (2, "920002", 0.01),
    ],
)
def test_snapshot_prices_are_decoded_once(market, code, coefficient, public):
    client = quote_api(snapshot_body(market, code))
    if public:
        quotes = object.__new__(StdQuotes)
        quotes.client = client
        quotes._round2_quotes_required = False
        quotes._round2_client = None
        row = quotes.quotes(code).iloc[0]
        assert "active1" not in row
        assert "reversed_bytes0" not in row
    else:
        row = client.get_security_quotes([(market, code)])[0]
    for field, delta in {"price": 0, "last_close": 20, "open": -10, "high": 30, "low": -40}.items():
        assert row[field] == pytest.approx((1639 + delta) * coefficient)
    for level in range(1, 6):
        assert row[f"bid{level}"] == pytest.approx((1639 - level) * coefficient)
        assert row[f"ask{level}"] == pytest.approx((1639 + level) * coefficient)
        assert row[f"bid_vol{level}"] == 78 + level
        assert row[f"ask_vol{level}"] == 89 + level
    assert row["vol"] == 123
    assert row["cur_vol"] == 7
    assert row["amount"] == 456.0


@pytest.mark.parametrize(
    "code, raw_hex, expected",
    [
        ("159915", "79e95640", 3.358),
        ("510020", "96437b40", 3.926),
        ("510050", "25064140", 3.016),
        ("510300", "77be9340", 4.617),
        ("588000", "1d5ad43f", 1.659),
    ],
)
def test_security_list_decodes_pre_close_as_little_endian_float(code, raw_hex, expected):
    name = "ETF".encode("gbk").ljust(8, b"\x00")
    record = struct.pack(
        "<6sH8s4sB4s4s",
        code.encode("ascii"),
        100,
        name,
        b"\x00" * 4,
        3,
        bytes.fromhex(raw_hex),
        b"\x00" * 4,
    )
    body = struct.pack("<H", 1) + record

    rows = GetSecurityList(client=None).parseResponse(body)

    assert rows[0]["code"] == code
    assert rows[0]["pre_close"] == pytest.approx(expected, abs=1e-6)


@pytest.mark.parametrize('market,code', [(1, '000001'), (0, '399001'), (2, '899050')])
def test_public_index_quote_does_not_expose_repurposed_order_book(market, code):
    import pandas as pd

    q = object.__new__(StdQuotes)
    q.client = quote_api(snapshot_body(market, code))
    q._round2_quotes_required = False
    q._round2_client = None
    row = q.quotes({0: 'sz', 1: 'sh', 2: 'bj'}[market] + code).iloc[0]
    assert row['price'] == pytest.approx(16.39)
    for level in range(1, 6):
        for field in [f'bid{level}', f'ask{level}', f'bid_vol{level}', f'ask_vol{level}']:
            assert pd.isna(row[field])
    if market != 2:
        assert row['up_count'] == 79
        assert row['down_count'] == 90
    else:
        assert pd.isna(row['up_count'])
        assert pd.isna(row['down_count'])


def test_security_name_strips_only_fixed_width_trailing_nuls():
    record = struct.pack('<6sH8s4sBf4s', b'159915', 100, b'ETF\0A\0\0\0', b'\0'*4, 3, 3.391, b'\0'*4)
    row = GetSecurityList(client=None).parseResponse(struct.pack('<H', 1) + record)[0]
    assert row['name'] == 'ETF\0A'
