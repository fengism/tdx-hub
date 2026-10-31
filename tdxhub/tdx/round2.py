"""通达信 2025+ “round2” 线缆协议客户端。

服务器在 2025 年起要求 K 线等新增接口使用新帧格式；旧 pytdx 封包的
``0x052d`` 已被统一以错误码 2003 拒绝。本模块实现:

- 12 字节请求头 ``0x0c + msg_id(u32) + control(u8) + len(u16)x2 + type(u16)``
- 16 字节响应头 ``b1cb7400 + control + msg_id + reserved + type + zip_len + unzip_len``
- ``0x000d`` 握手(必须作为连接首帧)、``0x0004`` 心跳
- ``0x052d`` K 线请求(服务端复权)与响应解码

协议对照 eltdx ``crates/eltdx-protocol`` 逐字节校准，已在真实主站
(广发/上海电信/杭州电信)与 HTTP CONNECT 代理隧道下实测。
"""

import contextlib
import datetime
import socket
import struct
import threading
import zlib
from zoneinfo import ZoneInfo

import pandas as pd

from tdxhub.tdx.deadline import socket_budget
from tdxhub.tdx.errors import (
    ProtocolError,
    ResponseHeaderRecvFails,
    ResponseRecvFails,
    SendRequestPkgFails,
    TdxConnectionError,
    ValidationException,
)
from tdxhub.tdx.logger import logger
from tdxhub.tdx.proxy import apply_proxy, parse_proxy

SHANGHAI = ZoneInfo('Asia/Shanghai')

COMMAND_HANDSHAKE = 0x000D
COMMAND_HEARTBEAT = 0x0004
COMMAND_KLINES = 0x052D
COMMAND_SNAPSHOTS = 0x054C

REQUEST_PREFIX = 0x0C
RESPONSE_PREFIX = b'\xb1\xcb\x74\x00'
REQUEST_HEADER_SIZE = 12
RESPONSE_HEADER_SIZE = 16
MAX_KLINE_PAGE_SIZE = 800

ADJUST_NONE = 0
ADJUST_QFQ = 1
ADJUST_HFQ = 2
ADJUST_FIXED_QFQ = 3
ADJUST_FIXED_HFQ = 4
_ADJUST_ENUM = {
    None: ADJUST_NONE,
    '': ADJUST_NONE,
    'none': ADJUST_NONE,
    'qfq': ADJUST_QFQ,
    'front': ADJUST_QFQ,
    'hfq': ADJUST_HFQ,
    'back': ADJUST_HFQ,
    'fixed_qfq': ADJUST_FIXED_QFQ,
    'fixed_front': ADJUST_FIXED_QFQ,
    'fixed_hfq': ADJUST_FIXED_HFQ,
    'fixed_back': ADJUST_FIXED_HFQ,
}

_SH_CONVERTIBLE_PREFIXES = ('110', '113', '118')
_SZ_CONVERTIBLE_PREFIXES = ('123', '127', '128')

# 命名周期 -> (period_raw, period_parameter)，对照 eltdx KlinePeriod::normalize
PERIODS = {
    '5m': (0, 1),
    '15m': (1, 1),
    '30m': (2, 1),
    '60m': (3, 1),
    'day': (4, 1),
    'week': (5, 1),
    'month': (6, 1),
    '1m': (7, 1),
    'quarter': (10, 1),
    'year': (11, 1),
}
# 旧 pytdx category 兼容映射
_CATEGORY_ALIASES = {0: '5m', 1: '15m', 2: '30m', 3: '60m', 4: 'day', 5: 'week', 6: 'month', 7: '1m', 9: 'day', 10: 'quarter', 11: 'year'}
_MINUTE_DATETIME_PERIODS = {0, 1, 2, 3, 7, 8}


def normalize_period(period):
    """把周期参数规范化为 ``(period_raw, period_parameter)``。

    支持命名周期(``day``/``5m``/...) 、旧 pytdx category 数字，以及
    ``10m``/``2d``/``5s`` 这类扩展周期(8/N、9/N、13/N)。
    """

    if isinstance(period, (tuple, list)) and len(period) == 2:
        raw, parameter = int(period[0]), int(period[1])
        if raw < 0 or parameter < 1:
            raise ValidationException('period 周期参数非法')
        return raw, parameter

    name = str(period).lower()
    if name in PERIODS:
        return PERIODS[name]
    if name.isdigit():
        mapped = _CATEGORY_ALIASES.get(int(name))
        if mapped:
            return PERIODS[mapped]
    number, unit = name[:-1], name[-1:]
    if number.isdigit() and unit in ('m', 'd', 's'):
        value = int(number)
        if not 1 <= value <= 0xFFFF:
            raise ValidationException(f'不支持的扩展周期: {period}')
        return {'m': 8, 'd': 9, 's': 13}[unit], value
    raise ValidationException(f'不支持的K线周期: {period}')


def encode_frame(msg_id, msg_type, data=b''):
    """编码 round2 请求帧。"""

    if not isinstance(data, (bytes, bytearray)):
        raise ValidationException('round2 请求体必须是 bytes')
    data = bytes(data)
    length = len(data) + 2
    if length > 0xFFFF:
        raise ValidationException('round2 请求体过长')
    return b''.join([
        bytes([REQUEST_PREFIX]),
        struct.pack('<IB', msg_id, 0x01),
        struct.pack('<HH', length, length),
        struct.pack('<H', msg_type),
        data,
    ])


def parse_response_header(head):
    """解析 16 字节 round2 响应头。"""

    if len(head) != RESPONSE_HEADER_SIZE:
        raise ProtocolError(f'round2 响应头长度错误: {len(head)}')
    if head[:4] != RESPONSE_PREFIX:
        raise ProtocolError(f'round2 响应头魔法错误: {head[:4].hex()}')
    packet_type, zip_size, unzip_size = struct.unpack('<HHH', head[10:16])
    return {
        'control': head[4],
        'msg_id': struct.unpack('<I', head[5:9])[0],
        'reserved': head[9],
        'msg_type': packet_type,
        'zip_size': zip_size,
        'unzip_size': unzip_size,
    }


def decode_varint(buf, pos):
    """解 varint 差分价格，返回 ``(value, new_pos)``；首字节 ``0x40`` 位为负数标记。"""

    if pos >= len(buf):
        raise ProtocolError('varint 越界')
    magnitude = 0
    shift = 0
    start = pos
    first = True
    count = 0
    while True:
        if pos >= len(buf) or count > 10:
            raise ProtocolError('varint 未终止')
        byte = buf[pos]
        pos += 1
        magnitude += (byte & 0x3F) if first else ((byte & 0x7F) << shift)
        first = False
        count += 1
        if not byte & 0x80:
            break
        shift = 6 if count == 1 else shift + 7
    return (-magnitude if buf[start] & 0x40 else magnitude), pos


def decode_compact_float(value):
    """TDX compact-float 量/额解码。"""

    if value == 0:
        return 0.0
    logpoint, rest = value >> 24, value & 0x00FFFFFF
    hleax, lheax, lleax = (rest >> 16) & 0xFF, (rest >> 8) & 0xFF, rest & 0xFF
    base = 2.0 ** (logpoint * 2 - 0x7F)
    high = base * (64 + (hleax & 0x7F)) / 64.0 if hleax > 0x80 else base * hleax / 128.0
    scale = 2.0 if hleax & 0x80 else 1.0
    return base + high + base * lheax / 32768.0 * scale + base * lleax / 8388608.0 * scale


def _from_yyyymmdd(raw):
    try:
        return datetime.date(raw // 10000, raw % 10000 // 100, raw % 100)
    except ValueError as exc:
        raise ProtocolError(f'K线日期非法: {raw}') from exc


def decode_kline_datetime(raw_value, period_raw):
    """K 线时间解码：分钟级周期返回带 Asia/Shanghai 时区的 datetime，日级以上返回 ``date``。"""

    if not isinstance(raw_value, bytes) or len(raw_value) != 4:
        raise ProtocolError('K线时间字段长度错误')
    if period_raw in _MINUTE_DATETIME_PERIODS:
        date_packed, minute_of_day = struct.unpack('<HH', raw_value)
        year = (date_packed >> 11) + 2004
        month, day = (date_packed % 2048) // 100, (date_packed % 2048) % 100
        date = _from_yyyymmdd(year * 10000 + month * 100 + day)
        return datetime.datetime(date.year, date.month, date.day, minute_of_day // 60, minute_of_day % 60, tzinfo=SHANGHAI)
    if period_raw == 13:
        seconds, = struct.unpack('<I', raw_value)
        epoch = datetime.datetime(2003, 12, 31, tzinfo=SHANGHAI)
        return epoch + datetime.timedelta(seconds=seconds)
    return _from_yyyymmdd(struct.unpack('<I', raw_value)[0])


def _decode_gbk(raw):
    return raw.rstrip(b'\x00 ').decode('gbk', 'replace').strip()


def parse_handshake_payload(payload):
    """解析 ``0x000d`` 握手响应(189 字节以上)。"""

    if len(payload) < 189:
        raise ProtocolError(f'握手响应不完整: {len(payload)} < 189')
    date_1, = struct.unpack('<I', payload[42:46])
    date_2, = struct.unpack('<I', payload[50:54])
    return {
        'server_date': _from_yyyymmdd(date_1) if date_1 >= 20000101 else None,
        'server_date_alt': _from_yyyymmdd(date_2) if date_2 >= 20000101 else None,
        'server_name': _decode_gbk(payload[68:152]),
        'product_tag': _decode_gbk(payload[160:189]),
    }


def kline_request_body(market, code, period, start, count, adjust, anchor_date_raw=0):
    """构造 ``0x052d`` 请求体: market+code+period+param+start+count+adjust+anchor+20x0。"""

    if not 1 <= int(count) <= MAX_KLINE_PAGE_SIZE:
        raise ValidationException(f'K线条数必须在 1..{MAX_KLINE_PAGE_SIZE} 之间')
    period_raw, period_param = normalize_period(period)
    code_bytes = str(code).encode('ascii', 'ignore')[:6]
    return b''.join([
        struct.pack('<H', int(market)),
        code_bytes.ljust(6, b'\x00'),
        struct.pack('<HHHHHI', period_raw, period_param, int(start), int(count), int(adjust), int(anchor_date_raw or 0)),
        b'\x00' * 20,
    ])


def normalize_adjust(adjust):
    try:
        return _ADJUST_ENUM[(adjust.lower() if isinstance(adjust, str) else adjust)]
    except KeyError as exc:
        raise ValidationException(f'不支持的复权模式: {adjust}') from exc


def date_to_raw(anchor_date):
    """把定点复权日期转换为 yyyymmdd u32。"""

    if isinstance(anchor_date, str):
        return int(anchor_date.replace('-', '')[:8])
    if isinstance(anchor_date, (datetime.date, datetime.datetime)):
        return anchor_date.year * 10000 + anchor_date.month * 100 + anchor_date.day
    return int(anchor_date)


def is_convertible(market, code):
    code = str(code)
    available = _SH_CONVERTIBLE_PREFIXES if int(market) == 1 else _SZ_CONVERTIBLE_PREFIXES
    return len(code) == 6 and int(market) in (0, 1) and code.startswith(available)


def is_index_code(market, code):
    code = str(code)
    market_name = {0: 'sz', 1: 'sh', 2: 'bj'}.get(int(market))
    if market_name is None:
        return False
    if market_name == 'sh' and code.startswith(('000', '880', '881', '999')):
        return True
    if market_name == 'sz' and code.startswith('399'):
        return True
    return market_name == 'bj' and code.startswith('899')


def parse_kline_payload(payload, period_raw, *, index=False, convertible=False):
    """解码 ``0x052d`` 响应体为 K 线列表(dict: time/open/high/low/close/last_close/vol/amount)。"""

    if len(payload) < 2:
        raise ProtocolError('K线响应体不完整')
    count, = struct.unpack('<H', payload[:2])
    minimum_record = 20 if index else 16
    if count > (len(payload) - 2) // minimum_record:
        raise ProtocolError(f'K线响应体被截断: count={count}(payload={len(payload)} 字节)')
    offset = 2
    last_close_milli = 0
    bars = []
    for _position in range(count):
        time_raw = payload[offset:offset + 4]
        offset += 4
        open_delta, offset = decode_varint(payload, offset)
        close_delta, offset = decode_varint(payload, offset)
        high_delta, offset = decode_varint(payload, offset)
        low_delta, offset = decode_varint(payload, offset)
        previous_close = last_close_milli if bars else None
        open_milli = last_close_milli + open_delta
        close_milli = open_milli + close_delta
        high_milli = open_milli + high_delta
        low_milli = open_milli + low_delta
        last_close_milli = close_milli
        if offset + 8 > len(payload):
            raise ProtocolError('K线量/额字段被截断')
        volume_raw, amount_raw = struct.unpack('<II', payload[offset:offset + 8])
        offset += 8
        if index:
            if offset + 4 > len(payload):
                raise ProtocolError('K线涨跌家数字段被截断')
            up_count, down_count = struct.unpack('<HH', payload[offset:offset + 4])
            offset += 4
        else:
            up_count = down_count = None
        volume = decode_compact_float(volume_raw)
        bars.append({
            'time': decode_kline_datetime(time_raw, period_raw),
            'open': open_milli / 1000.0,
            'close': close_milli / 1000.0,
            'high': high_milli / 1000.0,
            'low': low_milli / 1000.0,
            'last_close': previous_close / 1000.0 if previous_close is not None else None,
            'vol': volume if convertible else volume / 100.0,
            'amount': decode_compact_float(amount_raw),
            'up_count': up_count,
            'down_count': down_count,
        })
    if offset != len(payload):
        raise ProtocolError(f'K线响应尾部有多余字节: {len(payload) - offset}')
    return bars


QUOTE_LIST_PREFIX = bytes([5, 0, 0, 0, 0, 0, 0, 0])
PRICING_ETF_PREFIXES = ('15', '16', '50', '51', '52', '53', '56', '58')


def snapshot_request_body(codes):
    """构造 ``0x054c`` 快照请求体: 8 字节前缀 + 数量 + 每只 ``市场 u8 + 6 位代码``。"""

    if not codes:
        raise ValidationException('快照请求代码列表为空')
    body = QUOTE_LIST_PREFIX + struct.pack('<H', len(codes))
    for market, code in codes:
        body += struct.pack('<B6s', int(market), str(code).encode('ascii', 'ignore')[:6])
    return body


def _quote_price_divisor(market, code):
    """价格标度: A 股 milli 直接显示; ETF/LOF 位价再除 10(eltdx PRICING_ETF_PREFIXES)。"""

    del market
    return 10 if str(code)[:2] in PRICING_ETF_PREFIXES else 1


def parse_snapshot_payload(payload, codes):
    """解码 ``0x054c`` 快照响应。

    记录变长: 按 ``市场 u8 + 六位代码`` marker 切片后逐字段 varint 还原
    (价格基准为 ``当前价毫值``, bid/ask 为基于其的差分), 全部字段对照
    eltdx ``parse_snapshot_record``。
    """

    if len(payload) < 4:
        raise ProtocolError('快照响应体不完整')
    count, = struct.unpack('<H', payload[2:4])
    if count != len(codes):
        raise ProtocolError(f'快照响应数量不匹配: {count} != {len(codes)}')
    data = payload[4:]
    starts = []
    search_from = 0
    for market, code in codes:
        marker = struct.pack('<B6s', int(market), str(code).encode('ascii', 'ignore')[:6])
        found = data[search_from:].find(marker)
        if found < 0:
            raise ProtocolError(f'快照记录 marker 未找到: {marker.hex()}')
        position = search_from + found
        starts.append(position)
        search_from = position + len(marker)
    segments = [data[a:b] for a, b in zip(starts, starts[1:] + [len(data)], strict=False)]
    snapshots = []
    for record, (market, code) in zip(segments, codes, strict=False):
        if len(record) < 9:
            raise ProtocolError(f'快照记录被截断: {len(record)} 字节')
        market_id, code_raw, active1 = struct.unpack('<B6sH', record[:9])
        offset = 9
        current_delta, offset = decode_varint(record, offset)
        last_close_delta, offset = decode_varint(record, offset)
        open_delta, offset = decode_varint(record, offset)
        high_delta, offset = decode_varint(record, offset)
        low_delta, offset = decode_varint(record, offset)
        divisor = _quote_price_divisor(market, code)
        price_milli = current_delta * 10
        price = price_milli / 1000.0 / divisor
        fields = {
            'price': price,
            'pre_close': (price_milli + last_close_delta * 10) / 1000.0 / divisor,
            'open': (price_milli + open_delta * 10) / 1000.0 / divisor,
            'high': (price_milli + high_delta * 10) / 1000.0 / divisor,
            'low': (price_milli + low_delta * 10) / 1000.0 / divisor,
        }
        _time_raw, offset = decode_varint(record, offset)
        _unknown1, offset = decode_varint(record, offset)
        total_hand, offset = decode_varint(record, offset)
        current_hand, offset = decode_varint(record, offset)
        amount_raw, = struct.unpack('<I', record[offset:offset + 4])
        offset += 4
        inside_dish, offset = decode_varint(record, offset)
        outer_disc, offset = decode_varint(record, offset)
        _unknown2, offset = decode_varint(record, offset)
        open_amount, offset = decode_varint(record, offset)
        bid_delta, offset = decode_varint(record, offset)
        ask_delta, offset = decode_varint(record, offset)
        bid_volume, offset = decode_varint(record, offset)
        ask_volume, offset = decode_varint(record, offset)
        snapshots.append({
            'market': int(market_id),
            'code': code_raw.decode('ascii', 'replace'),
            'active1': active1,
            **fields,
            'vol': total_hand,
            'cur_vol': current_hand,
            'amount': decode_compact_float(amount_raw),
            'inside': inside_dish,
            'outer': outer_disc,
            'open_amount': open_amount * 100.0,
            'bid1': (price_milli + bid_delta * 10) / 1000.0 / divisor,
            'ask1': (price_milli + ask_delta * 10) / 1000.0 / divisor,
            'bid_vol1': bid_volume,
            'ask_vol1': ask_volume,
        })
    return snapshots


class Round2Client:
    """round2 协议客户端：握手 + 心跳 + K 线(服务端复权)，与 HTTP CONNECT / SOCKS5 代理兼容。

    用法::

        from tdxhub.tdx.round2 import Round2Client

        client = Round2Client(proxy='http://user:pass@host:2260')
        client.connect('119.29.19.242', 7709, time_out=10)
        df = client.get_security_bars(9, 1, '600000', 0, 5, adjust='qfq')
        client.close()
    """

    def __init__(self, *, proxy=None, raise_exception=True, connect_timeout=5.0):
        self.client = None
        self.ip, self.port = None, None
        self.proxy = parse_proxy(proxy)
        self.raise_exception = raise_exception
        self.connect_timeout = connect_timeout
        self.handshake_result = None
        self.lock = threading.Lock()
        self._msg_id = 0
        self.closed = False

    def connect(self, ip=None, port=7709, time_out=None, proxy=None):
        """连接主站，并以 ``0x000d`` 握手作为连接的首帧。"""

        if not ip:
            raise ValidationException('IP Address bad.')
        self.ip, self.port = ip, int(port)
        if proxy is not None:
            self.proxy = parse_proxy(proxy) or self.proxy
        timeout = time_out or self.connect_timeout
        logger.debug(f'round2 connecting to server: {self.ip} on port: {self.port}')
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        try:
            if self.proxy:
                apply_proxy(sock, self.ip, self.port, self.proxy, timeout)
            else:
                sock.connect((self.ip, self.port))
        except Exception as exc:
            sock.close()
            via = f'{self.proxy["kind"]}://{self.proxy["host"]}:{self.proxy["port"]}' if self.proxy else 'direct'
            message = f'round2 连接失败 {self.ip}:{self.port} via {via}: {exc}'
            if self.raise_exception:
                raise TdxConnectionError(message) from exc
            logger.debug(message)
            return False
        self.client = sock
        self.closed = False
        self._msg_id = 0
        try:
            self.handshake_result = parse_handshake_payload(self._request(COMMAND_HANDSHAKE, b'\x01'))
        except Exception:
            self.disconnect()
            raise
        logger.debug(f'round2 connected: {self.handshake_result.get("server_name")!r}')
        return self

    def reconnect(self):
        if self.ip is None:
            raise ValidationException('尚未连接过服务器，无重连目标')
        return self.connect(self.ip, self.port)

    def disconnect(self):
        self.closed = True
        client, self.client = self.client, None
        if client is None:
            return
        try:
            client.shutdown(socket.SHUT_RDWR)
        finally:
            with contextlib.suppress(OSError):
                client.close()
        logger.debug('round2 disconnected')

    def close(self):
        self.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _send_frame(self, frame):
        sent = 0
        while sent < len(frame):
            try:
                with socket_budget(self.client):
                    count = self.client.send(frame[sent:])
            except (TimeoutError, OSError) as exc:
                raise SendRequestPkgFails(f'round2 发送失败(已发送 {sent}/{len(frame)}): {exc}') from exc
            if count <= 0:
                raise SendRequestPkgFails(f'round2 发送 0 字节(已发送 {sent}/{len(frame)})')
            sent += count

    def _read_exact(self, size, error_type, stage):
        data = bytearray()
        while len(data) < size:
            try:
                with socket_budget(self.client):
                    chunk = self.client.recv(size - len(data))
            except (TimeoutError, OSError) as exc:
                raise error_type(f'round2 {stage} 接收 {len(data)}/{size} 字节失败: {exc}') from exc
            if not chunk:
                raise error_type(f'round2 {stage} 对端关闭: {len(data)}/{size} 字节')
            data.extend(chunk)
        return bytes(data)

    def _request(self, command, body=b''):
        if self.client is None:
            raise TdxConnectionError('round2 客户端未连接')
        msg_id = self._next_msg_id()
        frame = encode_frame(msg_id, command, body)
        with self.lock:
            self._send_frame(frame)
            head = self._read_exact(RESPONSE_HEADER_SIZE, ResponseHeaderRecvFails, f'命令 0x{command:04x} 响应头')
            meta = parse_response_header(head)
            if meta['msg_id'] != msg_id or meta['msg_type'] != command:
                raise ProtocolError(
                    f'round2 响应不匹配: 期望 type=0x{command:04x} msg={msg_id}, '
                    f'实际 type=0x{meta["msg_type"]:04x} msg={meta["msg_id"]}'
                )
            raw_body = self._read_exact(meta['zip_size'], ResponseRecvFails, f'命令 0x{command:04x} 响应体')
            if meta['zip_size'] == meta['unzip_size']:
                body_out = raw_body
            else:
                decoder = zlib.decompressobj()
                body_out = decoder.decompress(raw_body, meta['unzip_size'] + 1)
                if len(body_out) != meta['unzip_size'] or decoder.unused_data:
                    raise ProtocolError(f'round2 响应体解压不完整: {len(body_out)} != {meta["unzip_size"]}')
        return body_out

    def _next_msg_id(self):
        self._msg_id = (self._msg_id % 0xFFFFFFFF) + 1
        return self._msg_id

    def handshake(self, refresh=False):
        """最近一次 ``0x000d`` 握手结果；``refresh=True`` 时重发握手(返回原始 payload)。"""

        return self.handshake_result

    def heartbeat(self):
        """``0x0004`` 心跳，返回服务器日期(datetime.date)。"""

        payload = self._request(COMMAND_HEARTBEAT, b'')
        if len(payload) < 10:
            return None
        server_date_raw, = struct.unpack('<I', payload[6:10])
        try:
            return _from_yyyymmdd(server_date_raw)
        except ProtocolError:
            return None

    def get_security_bars(self, category, market, code, start=0, count=100, adjust=None, anchor_date=None, index=None):
        """K 线接口(服务端复权)。

        :param category: 周期(``'day'``/``'5m'``/旧 pytdx category 数字/``(raw, param)`` 元组)
        :param market: 0 深市 / 1 沪市 / 2 北交所
        :param code: 六位代码
        :param start: 起始位置(0 为最新，向历史翻页)
        :param count: 条数(1..800)
        :param adjust: ``None/'none'/'qfq'/'hfq'/'fixed_qfq'/'fixed_hfq'``
        :param anchor_date: 定点复权基准日期(``fixed_*`` 模式使用)
        :param index: 指数 K 线(解析涨跌家数)；默认按代码自动识别
        :returns: pandas.DataFrame(列 time/open/close/high/low/last_close/vol/amount)
        """

        period_raw, _param = normalize_period(category)
        adjust_mode = normalize_adjust(adjust)
        anchor_raw = date_to_raw(anchor_date) if anchor_date else 0
        is_index = is_index_code(market, code) if index is None else bool(index)
        payload = self._request(COMMAND_KLINES, kline_request_body(market, code, category, start, count, adjust_mode, anchor_raw))
        bars = parse_kline_payload(
            payload, period_raw, index=is_index, convertible=is_convertible(market, code))
        return pd.DataFrame(
            bars,
            columns=['time', 'open', 'close', 'high', 'low', 'last_close', 'vol', 'amount', 'up_count', 'down_count'],
        )

    def get_bars(self, code, period='day', count=100, adjust=None, anchor_date=None, market=None, index=None):
        """便捷入口：未显式传 market 时按代码前缀自动推断市场。

        ``code`` 支持带 ``sh/sz/bj`` 前缀；可转债等跨市场前缀需显式传 market。
        """

        code = str(code).lower()
        if code[:3] in ('sh:', 'sz:', 'bj:'):
            code = code[3:]
        if code[:2] in ('sh', 'sz', 'bj') and market is None and len(code) == 8:
            market = {'sh': 1, 'sz': 0, 'bj': 2}[code[:2]]
            code = code[2:]
        if market is None:
            market = 1 if code[0] in '69' else (2 if code[0] == '8' else 0)
        elif isinstance(market, str) and market.lower() in ('sh', 'sz', 'bj'):
            market = {'sh': 1, 'sz': 0, 'bj': 2}[market.lower()]
        return self.get_security_bars(period, int(market), code, 0, count, adjust=adjust, anchor_date=anchor_date, index=index)

    def get_security_quotes(self, codes):
        """实时快照(``0x054c``)。

        :param codes: ``['600000', ('000001', 0), (1, '600000')]`` —
            六位代码可带 ``sh/sz/bj`` 前缀; 元组既支持 ``(code, market)``,
            也支持经典顺序 ``(market, code)``(int 在前)
        :returns: DataFrame(market/code/price/pre_close/open/high/low/vol/cur_vol/amount/
            inside/outer/open_amount/bid1/ask1/bid_vol1/ask_vol1)
        """

        import pandas as pd

        suffix_market = {'sh': 1, 'sz': 0, 'bj': 2}
        normalized = []
        for item in codes:
            if isinstance(item, (tuple, list)):
                a, b = (item[0], item[1]) if len(item) == 2 else (None, None)
                if isinstance(b, int) and not isinstance(a, int):
                    normalized.append((int(b), str(a)))
                elif isinstance(a, int) and isinstance(b, str):
                    normalized.append((int(a), str(b)))
                elif isinstance(b, str) and str(b).lower() in suffix_market:
                    normalized.append((suffix_market[str(b).lower()], str(a)))
                else:
                    normalized.append((0, str(item[0])))
                continue
            raw = str(item).lower()
            if raw[:2] in ('sh', 'sz', 'bj'):
                normalized.append(({'sh': 1, 'sz': 0, 'bj': 2}[raw[:2]], raw[2:]))
            else:
                market = 1 if raw[0] in '69' else (2 if raw[0] == '8' else 0)
                normalized.append((market, raw))
        payload = self._request(COMMAND_SNAPSHOTS, snapshot_request_body(normalized))
        return pd.DataFrame(
            parse_snapshot_payload(payload, normalized),
            columns=['market', 'code', 'active1', 'price', 'pre_close', 'open', 'high', 'low',
                     'vol', 'cur_vol', 'amount', 'inside', 'outer', 'open_amount',
                     'bid1', 'ask1', 'bid_vol1', 'ask_vol1'],
        )
