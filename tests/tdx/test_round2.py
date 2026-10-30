"""round2 协议封包/解码与客户端链路测试。

K 线样本为 2026-10-30 经代理在真实主站 (广发 119.29.19.242) 抓取的
round2 响应,浦发银行 600000 近 3 根日 K,作为服务端兼容回归锚点。
"""

import socket
import struct
import threading
import zlib
from datetime import date

import pytest

from tdxhub.tdx.round2 import (
    COMMAND_HANDSHAKE,
    COMMAND_KLINES,
    COMMAND_SNAPSHOTS,
    Round2Client,
    decode_compact_float,
    decode_kline_datetime,
    decode_varint,
    encode_frame,
    is_convertible,
    is_index_code,
    kline_request_body,
    normalize_period,
    parse_kline_payload,
    parse_response_header,
    parse_snapshot_payload,
    snapshot_request_body,
)

KLINE_RESPONSE_HEAD = bytes.fromhex('b1cb74000c02000000002d0539003900')
KLINE_PAYLOAD = bytes.fromhex(
    '0300a4283501889901328202c60195ea764ccbec174ea5283501008c02a0024aeccd8f4c7e77324ea62835014a143cd00166cc4f4c6d91014e'
)


def test_frame_encode():
    raw = encode_frame(1, COMMAND_HANDSHAKE, b'\x01')
    assert raw[0] == 0x0C
    assert struct.unpack('<IB', raw[1:6]) == (1, 0x01)
    assert struct.unpack('<HH', raw[6:10]) == (3, 3)
    assert struct.unpack('<H', raw[10:12])[0] == COMMAND_HANDSHAKE
    assert raw[12:] == b'\x01'


def test_frame_rejects_oversize():
    with pytest.raises(Exception, match='过长'):
        encode_frame(1, COMMAND_KLINES, b'\x00' * 0xFFFFAF)


def test_parse_response_header_real_sample():
    meta = parse_response_header(KLINE_RESPONSE_HEAD)
    assert meta['msg_id'] == 2
    assert meta['msg_type'] == COMMAND_KLINES
    assert meta['zip_size'] == meta['unzip_size'] == 57


def test_parse_response_header_rejects_magic():
    with pytest.raises(Exception, match='魔法'):
        parse_response_header(b'\x00' * 16)


def test_normalize_period():
    assert normalize_period('day') == (4, 1)
    assert normalize_period('5m') == (0, 1)
    assert normalize_period('1m') == (7, 1)
    assert normalize_period('quarter') == (10, 1)
    assert normalize_period(9) == (4, 1)
    assert normalize_period(0) == (0, 1)
    assert normalize_period('10m') == (8, 10)
    assert normalize_period('2d') == (9, 2)
    assert normalize_period('5s') == (13, 5)
    assert normalize_period((4, 2)) == (4, 2)
    with pytest.raises(Exception, match='周期'):
        normalize_period('xyz')


def test_kline_request_body_layout_and_adjust():
    body = kline_request_body(1, '600000', 'day', 0, 3, 1, 0)
    assert len(body) == 42
    market, code, period_raw, period_param, start, count, adjust, anchor = struct.unpack('<H6sHHHHHI', body[:22])
    assert market == 1 and code == b'600000'
    assert (period_raw, period_param, start, count, adjust, anchor) == (4, 1, 0, 3, 1, 0)
    assert body[22:] == b'\x00' * 20

    body_hfq = kline_request_body(0, '000001', 'day', 10, 2, 2, 0)
    assert struct.unpack('<H6sHHHHHI', body_hfq[:22])[6] == 2


def test_kline_request_count_bounds():
    with pytest.raises(Exception, match='1..800'):
        kline_request_body(1, '600000', 'day', 0, 801, 0, 0)
    with pytest.raises(Exception, match='1..800'):
        kline_request_body(1, '600000', 'day', 0, 0, 0, 0)


def test_varint_decode():
    # 真实样例: 浦发银行首根开盘差分 varint(9800->9.8 元)
    value, pos = decode_varint(bytes.fromhex('889901'), 0)
    assert pos == 3
    assert value == 9800
    negative, _n = decode_varint(bytes.fromhex('e428'), 0)  # 首字节 0x40 位 = 负差分
    assert negative == -2596


def test_compact_float_zero():
    assert decode_compact_float(0) == 0.0


def test_kline_datetime_day_and_minute():
    assert decode_kline_datetime(struct.pack('<I', 20261030), 4) == date(2026, 10, 30)
    packed_date = (22 << 11) | 918  # 2026-09-18
    dt = decode_kline_datetime(struct.pack('<HH', packed_date, 13 * 60 + 25), 0)
    assert (dt.year, dt.month, dt.day, dt.hour, dt.minute) == (2026, 9, 18, 13, 25)


def test_parse_kline_payload_real_capture():
    bars = parse_kline_payload(KLINE_PAYLOAD, 4)
    assert len(bars) == 3
    first, second, third = bars
    assert first['time'] == date(2026, 10, 28)
    assert second['time'] == date(2026, 10, 29)
    assert third['time'] == date(2026, 10, 30)
    assert third['open'] == pytest.approx(9.98)
    assert third['close'] == pytest.approx(10.00)
    assert third['high'] == pytest.approx(10.04, abs=0.01)
    assert third['low'] == pytest.approx(9.90, abs=0.01)
    assert first['last_close'] is None
    assert second['last_close'] == pytest.approx(first['close'], abs=0.01)
    assert bars[-1]['up_count'] is None
    assert abs(third['amount'] - 543447872) < 3e6
    assert abs(third['vol'] - 544731.12) < 2000


def test_parse_kline_payload_truncated_rejected():
    with pytest.raises(Exception, match='截断'):
        parse_kline_payload(b'\x20\x00' + b'\x00' * 4, 4)


def test_parse_kline_payload_index_breadth():
    stock = parse_kline_payload(KLINE_PAYLOAD, 4)
    payload = (struct.pack('<I', 20261030) + bytes([0x18] * 4) + struct.pack('<II', 731232, 931360200)) + struct.pack('<HH', 512, 389)
    bars = parse_kline_payload(b'\x01\x00' + payload, 4, index=True)
    assert len(bars) == 1
    assert bars[0]['up_count'] == 512
    assert bars[0]['down_count'] == 389
    assert stock[0]['up_count'] is None


def test_convertible_detection():
    assert is_convertible(1, '110059')
    assert is_convertible(0, '123456')
    assert is_convertible(0, '127021')
    assert not is_convertible(1, '600000')
    assert is_index_code(1, '000001')
    assert is_index_code(0, '399001')
    assert not is_index_code(1, '600000')


class _StaticRound2Server(threading.Thread):
    """最小 round2 服务器:按 msg_type 回放预置响应报文。"""

    def __init__(self, responses):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(('127.0.0.1', 0))
        server.listen(1)
        server.settimeout(5)
        self.server = server
        self.port = server.getsockname()[1]
        self.responses = responses
        super().__init__(daemon=True)

    def run(self):
        try:
            conn, _ = self.server.accept()
            conn.settimeout(5)
            while True:
                head = b''
                while len(head) < 12:  # round2 请求头为 12 字节
                    chunk = conn.recv(12 - len(head))
                    if not chunk:
                        return
                    head += chunk
                if head[0] != 0x0C:
                    return
                (declared,) = struct.unpack('<H', head[6:8])
                pkt_type = struct.unpack('<H', head[10:12])[0]
                conn.recv(max(0, declared - 2))
                response = self.responses.get(pkt_type)
                if response is None:
                    return
                conn.sendall(response)
        finally:
            self.server.close()


def _synthetic_handshake_payload():
    """189+ 字节握手 payload,字段位置对照 eltdx parse_handshake_payload。"""

    payload = bytearray(189)
    payload[0:2] = struct.pack('<H', 9 * 60)   # 交易时段占位
    payload[42:46] = struct.pack('<I', 20261030)
    payload[50:54] = struct.pack('<I', 20261231)
    payload[68:152] = '上海双线主站1'.encode('gbk').ljust(84, b'\x00')
    payload[160:180] = b'ROUND2SMOKE'.ljust(20, b'\x00')
    return bytes(payload)


def test_client_full_chain_via_local_server():
    payload = _synthetic_handshake_payload()
    compressed = zlib.compress(payload)
    prefix = bytes.fromhex('b1cb7400')
    hs_head = prefix + struct.pack('<BIBHHH', 0x1C, 1, 0, COMMAND_HANDSHAKE, len(compressed), len(payload))
    kline_head = prefix + struct.pack('<BIBHHH', 0x0C, 2, 0, COMMAND_KLINES, len(KLINE_PAYLOAD), len(KLINE_PAYLOAD))
    server = _StaticRound2Server({COMMAND_HANDSHAKE: hs_head + compressed, COMMAND_KLINES: kline_head + KLINE_PAYLOAD})
    server.start()

    client = Round2Client()
    assert client.connect('127.0.0.1', server.port, time_out=5) is not False
    assert client.handshake_result['server_name'] == '上海双线主站1'
    assert client.handshake_result['product_tag'] == 'ROUND2SMOKE'
    assert client.handshake_result['server_date'] == date(2026, 10, 30)
    df = client.get_security_bars(9, 1, '600000', 0, 3)
    client.close()
    assert len(df) == 3
    assert df.iloc[-1]['open'] == pytest.approx(9.98)
    assert df.iloc[-1]['close'] == pytest.approx(10.00)
    assert [str(item)[:10] for item in df['time']] == ['2026-10-28', '2026-10-29', '2026-10-30']


SNAPSHOT_PAYLOAD = bytes.fromhex(
    '00000200013630303030306a10aa0f4344024cb7c6cb0eea0fa09a4f9ac401016f1a4e89f52498a52a008291084100a2'
    '01b405d6160a000200c02f404b00000000242a8d4c00a127c9c4290140f36e93c20000000000000000000000000000000000000000000000006a100030303030303173128c124a4b035895c4cb0ecc12b5c8e301ac8904ed4a004f858762b0c1810100bc9b10000183199b35d601f0ff07002041144cf8ff00002cbe9c4dd4881fcdee25be3ff47a17c20000000000000000000000000000000000000000000000007312'
)


def test_snapshot_request_body():
    body = snapshot_request_body([(1, '600000'), (0, '000001')])
    assert body[:8] == bytes([5, 0, 0, 0, 0, 0, 0, 0])
    assert struct.unpack('<H', body[8:10])[0] == 2
    assert body[10:17] == struct.pack('<B6s', 1, b'600000')
    assert body[17:] == struct.pack('<B6s', 0, b'000001')
    with pytest.raises(Exception, match='为空'):
        snapshot_request_body([])


def test_parse_snapshot_payload_real_capture():
    rows = parse_snapshot_payload(SNAPSHOT_PAYLOAD, [(1, '600000'), (0, '000001')])
    assert len(rows) == 2
    sh, sz = rows
    assert sh['code'] == '600000' and sh['market'] == 1
    assert sh['price'] == pytest.approx(10.02)
    assert sh['pre_close'] == pytest.approx(9.99, abs=0.01)
    assert sh['open'] == pytest.approx(9.98)
    assert sh['high'] == pytest.approx(10.04)
    assert sh['low'] == pytest.approx(9.90, abs=0.01)
    assert sh['vol'] == 648864
    assert abs(sh['amount'] - 647741504.0) < 1.0
    assert abs(sh['inside'] + sh['outer'] - sh['vol']) <= 2  # 内外盘与总手在快照非原子更新下可能有 ±1 抖动
    assert sz['code'] == '000001' and sz['market'] == 0
    assert sz['price'] == pytest.approx(11.64, abs=0.01)
    assert sz['amount'] == pytest.approx(2152393984.0, rel=1e-3)


def test_parse_snapshot_payload_count_mismatch():
    with pytest.raises(Exception, match='数量不匹配'):
        parse_snapshot_payload(SNAPSHOT_PAYLOAD, [(1, '600000')])


def test_snapshot_live_end_to_end(tmp_path):
    prefix = bytes.fromhex('b1cb7400')
    hs_body = zlib.compress(_synthetic_handshake_payload())
    hs_head = prefix + struct.pack('<BIBHHH', 0x1C, 1, 0, COMMAND_HANDSHAKE, len(hs_body), 189)
    snap_head = prefix + struct.pack('<BIBHHH', 0xD4, 2, 0, COMMAND_SNAPSHOTS, len(SNAPSHOT_PAYLOAD), len(SNAPSHOT_PAYLOAD))
    server = _StaticRound2Server({COMMAND_HANDSHAKE: hs_head + hs_body, COMMAND_SNAPSHOTS: snap_head + SNAPSHOT_PAYLOAD})
    server.start()

    client = Round2Client()
    client.connect('127.0.0.1', server.port, time_out=5)
    df = client.get_security_quotes(['600000', ('000001', 0)])
    client.close()
    assert df.iloc[0]['code'] == '600000'
    assert df.iloc[0]['price'] == pytest.approx(10.02)
    assert df.iloc[1]['code'] == '000001'
