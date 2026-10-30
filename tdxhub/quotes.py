import ipaddress
import math
import os
import re
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager, suppress
from copy import deepcopy
from datetime import datetime
from functools import wraps
from io import BytesIO
from pathlib import Path
from zipfile import BadZipFile, ZipFile
from zoneinfo import ZoneInfo

import pandas
import pandas as pd
from tenacity import retry, retry_if_exception_type, retry_if_result, stop_after_attempt, wait_random
from tqdm import tqdm

from tdxhub import config
from tdxhub.cache import PersistentDataFrameCache
from tdxhub.consts import MARKET_BJ, MARKET_SH, MARKET_SZ, return_last_value
from tdxhub.exceptions import TdxhubConnectionError, TdxhubIncompleteDataError, TdxhubValidationException
from tdxhub.f10 import f10_frame, normalize_f10_content
from tdxhub.failover import EndpointPool, FailoverClient, FailoverClientPool, _is_protocol_error, request_capability
from tdxhub.logger import logger
from tdxhub.security import filter_security_directory, normalize_security_type
from tdxhub.server import check_server
from tdxhub.tdx.client import StandardClient
from tdxhub.tdx.deadline import remaining, request_budget
from tdxhub.tdx.errors import TdxConnectionError, TdxFunctionCallError, ValidationException
from tdxhub.tdx.extended import ExtendedClient
from tdxhub.utils import _normalize_symbol, get_config_path, get_frequency, get_stock_markets, to_data


def _page_to_data(raw, **kwargs):
    # None means transport failure; an empty list is a successful empty page.
    if raw is None:
        raise TdxhubConnectionError("行情分页请求失败，不能将失败响应视为数据结束")
    return to_data(raw, **kwargs)


def _has_flow_data(flow):
    return flow is not None and not flow.empty and flow.attrs.get("trade_count") != 0


_QUOTE_METADATA_CACHE = PersistentDataFrameCache()
_QUOTE_METADATA_CACHE_VERSION = 3


def _quote_metadata_cache_directory() -> Path:
    configured = config.get('CACHE.QUOTE_METADATA.DIRECTORY', 'caches/quotes')
    return Path(get_config_path(str(configured)))


def _quote_metadata_ttl(name: str, default: float) -> float:
    value = config.get(f'CACHE.QUOTE_METADATA.{name}', default)
    try:
        ttl = float(value)
    except (TypeError, ValueError):
        logger.warning('忽略无效缓存周期 CACHE.QUOTE_METADATA.%s=%r', name, value)
        return default
    if not math.isfinite(ttl) or ttl < 0:
        logger.warning('忽略无效缓存周期 CACHE.QUOTE_METADATA.%s=%r', name, value)
        return default
    return ttl


def _quote_metadata_cache_file(name: str) -> Path:
    return _quote_metadata_cache_directory() / f'{name}-v{_QUOTE_METADATA_CACHE_VERSION}.pkl'


class Quotes:
    @staticmethod
    def factory(market='std', **kwargs):
        """
        股票市场 工厂方法

        :param market:  std 股票市场, ext 扩展市场， 默认股票市场
        :param kwargs:  可变参数
        :return: object
        """

        logger.debug(kwargs)

        if not isinstance(market, str):
            raise TdxhubValidationException("market 必须是 'std' 或 'ext'")
        market = market.lower()
        if market == 'ext':
            return ExtQuotes(**kwargs)
        if market == 'std':
            return StdQuotes(**kwargs)
        raise TdxhubValidationException("market 必须是 'std' 或 'ext'")


def valid_server(server):
    if server is None:
        return None
    if not isinstance(server, (tuple, list)) or len(server) != 2:
        raise ValueError('Server 格式错误. 例如: server = ("127.0.0.1", 7709)')
    address, port = server
    try:
        address = str(ipaddress.ip_address(address))
        port = int(port)
    except (ValueError, TypeError) as exc:
        raise ValueError('Server 格式错误. 例如: server = ("127.0.0.1", 7709)') from exc
    if not 1 <= port <= 65535:
        raise ValueError('Server 端口必须在 1..65535 之间')
    return address, port


class BaseQuotes:
    verbose = False
    timeout = 15

    def __init__(self, server=None, bestip: bool = False, timeout: int = None, proxy: str = None, **kwargs) -> None:
        self._client_local = threading.local()
        self._client = None
        self.server = None
        self.bestip = None
        self.proxy = proxy or os.environ.get('TDXHUB_PROXY') or os.environ.get('TDXHUB_PROXY_URL')
        if self.proxy:
            logger.info(f'proxy => {self.proxy}')
        logger.debug('config.setup()')
        config.setup()

        logger.debug(f'server => {server}')
        self.server = valid_server(server)

        logger.debug(f'bestip => {bestip}')
        bestip and check_server(sync=True)

        self.timeout = 15 if timeout is None else timeout
        if not math.isfinite(self.timeout) or self.timeout <= 0:
            raise TdxhubValidationException('timeout 必须大于 0')
        logger.debug(f'timeout => {self.timeout}')

        self.verbose = kwargs.get('verbose', False)
        logger.debug(f'verbose => {self.verbose}')

    @property
    def client(self):
        local = getattr(self, '_client_local', None)
        if local is not None and hasattr(local, 'override'):
            return local.override
        return getattr(self, '_client', None)

    @client.setter
    def client(self, value):
        self._client = value

    @contextmanager
    def _using_client(self, client):
        """Temporarily bind one client to the current worker thread."""

        local = getattr(self, '_client_local', None)
        if local is None:
            local = threading.local()
            self._client_local = local
        missing = object()
        previous = getattr(local, 'override', missing)
        local.override = client
        try:
            yield
        finally:
            if previous is missing:
                del local.override
            else:
                local.override = previous

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        with suppress(Exception):
            self.close()

    def reconnect(self):
        if self.closed:
            logger.debug('服务器连接已断开，正进行重新连接...')
            if isinstance(self.client, FailoverClient):
                self.client.connect()
            else:
                self.client.connect(*self.server, time_out=self.timeout)

    def close(self):
        logger.debug('close')
        client_pool = getattr(self, '_stock_info_client_pool', None)
        if client_pool is not None:
            client_pool.close()
        elif self.client is not None and hasattr(self.client, 'close'):
            self.client.close()

    @property
    def closed(self) -> bool:
        if isinstance(self.client, FailoverClient):
            return not self.client.is_connected
        socket_client = getattr(self.client, 'client', None)
        return socket_client is None or bool(getattr(socket_client, '_closed', False))

    def server_status(self):
        """Return health information for configured runtime quote servers."""

        if isinstance(self.client, FailoverClient):
            return self.client.server_status()
        return [{
            'server': self.server,
            'active': not self.closed,
            'failures': 0,
            'cooldown_remaining': 0.0,
            'last_error': None,
        }]

    def pool(self):
        ...



def _validate_failover_options(max_failovers, unhealthy_cooldown, max_candidates):
    if isinstance(max_failovers, bool) or not isinstance(max_failovers, int) or max_failovers < 0:
        raise TdxhubValidationException('max_failovers 必须是大于等于 0 的整数')
    if isinstance(max_candidates, bool) or not isinstance(max_candidates, int) or max_candidates <= 0:
        raise TdxhubValidationException('max_candidates 必须是大于 0 的整数')
    try:
        unhealthy_cooldown = float(unhealthy_cooldown)
    except (TypeError, ValueError) as exc:
        raise TdxhubValidationException('unhealthy_cooldown 必须是大于等于 0 的数字') from exc
    if not math.isfinite(unhealthy_cooldown) or unhealthy_cooldown < 0:
        raise TdxhubValidationException('unhealthy_cooldown 必须是大于等于 0 的数字')
    return unhealthy_cooldown


def _quote_candidates(kind, requested_server, fallback_servers):
    hosts = config.get(f'SERVER.{kind}', []) or []
    configured = []
    configured_best = config.get(f'BESTIP.{kind}')
    if configured_best:
        configured.append(valid_server(configured_best))
    configured.extend(valid_server(item[1:3]) for item in hosts)

    if requested_server:
        candidates = [requested_server]
        if fallback_servers:
            candidates.extend(configured)
    else:
        candidates = configured

    return list(dict.fromkeys(candidates))


def _probe_quote_endpoints(candidates, client_factory, healthcheck, timeout, max_candidates, requests=None):
    def probe(endpoint):
        client = client_factory()
        observations = []
        healthy, last_error = False, None
        try:
            with request_budget(min(timeout, 3)):
                connected = client.connect(*endpoint, time_out=min(timeout, 3))
                if not connected:
                    raise TdxConnectionError(f'无法连接 {endpoint[0]}:{endpoint[1]}')
                if requests is None:
                    try:
                        healthy = bool(healthcheck(client))
                    except (OSError, TdxConnectionError, TdxFunctionCallError, ValidationException) as exc:
                        if not _is_protocol_error(exc):
                            raise
                        # 协议级错误(如服务器停用旧K线返回 2003)证明连接可用，仅能力缺失
                        healthy = True
                        last_error = exc
                else:
                    for name, args in requests:
                        remaining()
                        capability = request_capability(name, args, {})
                        try:
                            if not connected:
                                connected = client.connect(*endpoint, time_out=min(timeout, 3))
                                if not connected:
                                    raise TdxConnectionError(f'无法连接 {endpoint[0]}:{endpoint[1]}')
                            result = getattr(client, name)(*args)
                            remaining()
                        except (OSError, TdxConnectionError, TdxFunctionCallError) as exc:
                            if not _is_protocol_error(exc):
                                raise
                            observations.append((endpoint, capability, 'failed', str(exc)))
                            last_error = exc
                            with suppress(Exception):
                                client.close()
                            connected = False
                        else:
                            # Valid empty data proves protocol liveness, not capability support.
                            healthy = True
                            observations.append((endpoint, capability, 'supported' if result else 'unknown', None))
        except (OSError, TdxConnectionError, TdxFunctionCallError, ValidationException) as exc:
            return endpoint, False, exc, observations
        finally:
            with suppress(Exception):
                client.close()
        return endpoint, healthy, last_error, observations

    if not candidates:
        return [], None, []
    workers = min(32, len(candidates))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='tdxhub-quotes') as pool:
        checks = list(pool.map(probe, candidates))
    available_checks = [check for check in checks if check[1]]
    if requests is not None:
        # Keep candidates covering different demonstrated capabilities before redundant ones.
        selected, covered = [], set()
        while available_checks and len(selected) < max_candidates:
            def score(check):
                supported = {key for _, key, status, _ in check[3] if status == 'supported'}
                failed = sum(status == 'failed' for _, _, status, _ in check[3])
                return len(supported - covered), -failed, len(supported)

            best = max(available_checks, key=score)
            available_checks.remove(best)
            selected.append(best)
            covered.update(key for _, key, status, _ in best[3] if status == 'supported')
        available_checks = selected
    healthy = [endpoint for endpoint, _, _, _ in available_checks][:max_candidates]
    last_error = next((error for _, _, error, _ in reversed(checks) if error is not None), None)
    observations = [item for endpoint, _, _, items in checks if endpoint in healthy for item in items]
    return healthy, last_error, observations


def _standard_probe_requests(symbols, frequencies):
    if symbols is None:
        return None
    if not isinstance(symbols, (list, tuple)) or not symbols:
        raise TdxhubValidationException('probe_symbols 必须是非空证券代码列表')
    if not isinstance(frequencies, (list, tuple)) or any(
        isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 11 for value in frequencies
    ):
        raise TdxhubValidationException('probe_frequencies 必须是 0..11 的周期列表')
    samples = [StdQuotes._code_market(symbol) for symbol in symbols]
    if any(len(code) != 6 or not code.isascii() or not code.isdigit() for _, code in samples):
        raise TdxhubValidationException('probe_symbols 中的证券代码必须是六位数字')
    if len({market for market, _ in samples}) != len(samples):
        raise TdxhubValidationException('probe_symbols 每个市场只需提供一个样本')
    requests = []
    for market, code in samples:
        requests.append(('get_security_quotes', ([(market, code)],)))
        requests.extend(('get_security_bars', (category, market, code, 0, 1)) for category in dict.fromkeys(frequencies))
    return requests


def _legacy_retry(**options):
    """Keep injected/raw-client compatibility without retrying a managed proxy twice."""
    def decorate(method):
        legacy = retry(**options)(method)

        @wraps(legacy)
        def call(self, *args, **kwargs):
            if isinstance(self.client, FailoverClient):
                return method(self, *args, **kwargs)
            return legacy(self, *args, **kwargs)

        return call
    return decorate


def check_empty(value):
    """
    重试判断函数

    :param value: 要判断的值
    :return:
    """
    return value.empty if isinstance(value, pd.DataFrame) else not value


class StdQuotes(BaseQuotes):
    """
    股票市场实时行情"""

    _round2_client = None
    _round2_bars_required = True

    @staticmethod
    def _code_market(symbol):
        """Return the numeric market and bare code expected by the TDX client."""
        try:
            market, code = get_stock_markets([symbol])[0]
        except (TypeError, ValueError, IndexError) as exc:
            raise TdxhubValidationException(f'证券代码错误: {symbol!r}') from exc
        return int(market), code

    @staticmethod
    def _index_code_market(symbol, market=None):
        """Return the numeric market and bare code for index requests."""
        if not isinstance(symbol, str):
            raise TdxhubValidationException(f'证券代码错误: {symbol!r}')
        try:
            prefix, bare_code = _normalize_symbol(symbol)
        except (TypeError, ValueError) as exc:
            raise TdxhubValidationException(f'证券代码错误: {symbol!r}') from exc

        if market is not None:
            if isinstance(market, str):
                market_key = market.strip().lower()
                market_map = {'sz': MARKET_SZ, 'sh': MARKET_SH, 'bj': MARKET_BJ}
                if market_key in market_map:
                    return market_map[market_key], bare_code
                if market_key.isdigit() and int(market_key) in {MARKET_SZ, MARKET_SH, MARKET_BJ}:
                    return int(market_key), bare_code
                raise TdxhubValidationException(f'不支持的证券市场: {market!r}')
            if isinstance(market, int) and market in {MARKET_SZ, MARKET_SH, MARKET_BJ}:
                return market, bare_code
            raise TdxhubValidationException(f'不支持的证券市场: {market!r}')

        if prefix is not None:
            prefix_map = {'SH': MARKET_SH, 'SZ': MARKET_SZ, 'BJ': MARKET_BJ}
            if prefix.upper() in prefix_map:
                return prefix_map[prefix.upper()], bare_code
            raise TdxhubValidationException(f'不支持的证券市场: {prefix!r}')

        if bare_code.startswith(('89', '899')):
            return MARKET_BJ, bare_code
        if bare_code.startswith(('00', '88', '99')):
            return MARKET_SH, bare_code
        if bare_code.startswith('39'):
            return MARKET_SZ, bare_code

        return MARKET_SZ, bare_code

    def __init__(
        self,
        server=None,
        bestip=False,
        timeout=15,
        heartbeat=False,
        auto_retry=True,
        raise_exception=False,
        failover=True,
        fallback_servers=False,
        max_failovers=2,
        unhealthy_cooldown=60.0,
        max_candidates=5,
        request_timeout=None,
        probe_symbols=None,
        probe_frequencies=(9,),
        proxy=None,
        **kwargs,
    ):
        """Create a standard quote client with bounded runtime failover."""

        request_timeout = timeout if request_timeout is None else request_timeout
        if request_timeout is None:
            request_timeout = 15
        if not isinstance(request_timeout, (int, float)) or not math.isfinite(request_timeout) or request_timeout <= 0:
            raise TdxhubValidationException('request_timeout 必须是大于 0 的有限数字')
        probe_requests = _standard_probe_requests(probe_symbols, probe_frequencies)
        super().__init__(bestip=bestip, timeout=timeout, server=server, proxy=proxy, **kwargs)
        unhealthy_cooldown = _validate_failover_options(max_failovers, unhealthy_cooldown, max_candidates)
        requested_server = self.server
        if requested_server:
            config.set('BESTIP.HQ', requested_server)

        candidates = _quote_candidates('HQ', requested_server, fallback_servers)
        if not candidates:
            raise TdxhubValidationException('没有可用的标准行情服务器配置')

        def probe_factory():
            return StandardClient(heartbeat=False, auto_retry=False, raise_exception=True, proxy=self.proxy)
        healthy_endpoints, last_error, observations = _probe_quote_endpoints(
            candidates,
            probe_factory,
            lambda client: client.get_security_bars(9, MARKET_SH, '600000', 0, 1),
            self.timeout,
            max_candidates,
            requests=probe_requests,
        )
        if not healthy_endpoints:
            message = f'无法连接任何可用的标准行情服务器，已尝试: {candidates}'
            if last_error:
                message = f'{message}: {last_error}'
            raise TdxhubConnectionError(message, data={'servers': candidates}) from last_error

        api_options = {}
        if 'multithread' in kwargs:
            api_options['multithread'] = kwargs['multithread']

        endpoint_pool = EndpointPool(healthy_endpoints, cooldown=unhealthy_cooldown)
        for endpoint, capability, status, error in observations:
            endpoint_pool.report_capability(endpoint, capability, status, error)
        self.client = FailoverClient(
            endpoint_pool,
            lambda: StandardClient(
                heartbeat=heartbeat,
                auto_retry=False,
                raise_exception=True,
                proxy=self.proxy,
                **api_options,
            ),
            timeout=self.timeout,
            request_timeout=request_timeout,
            max_failovers=max_failovers if failover else 0,
            raise_exception=raise_exception,
            on_switch=lambda endpoint: setattr(self, 'server', endpoint),
        )
        try:
            connected = self.client.connect()
        except (OSError, TdxConnectionError, TdxFunctionCallError) as exc:
            raise TdxhubConnectionError(
                f'无法连接任何可用的标准行情服务器，已尝试: {healthy_endpoints}: {exc}',
                data={'servers': healthy_endpoints},
            ) from exc
        if not connected:
            raise TdxhubConnectionError(
                f'无法连接任何可用的标准行情服务器，已尝试: {healthy_endpoints}',
                data={'servers': healthy_endpoints},
            )
        self._round2_client = None
        if os.environ.get('TDXHUB_KLINE_PROTOCOL', '').lower() in ('legacy', 'classic'):
            logger.info('TDXHUB_KLINE_PROTOCOL=legacy: K 线优先尝试经典命令(0x052d 旧格式)')
            self._round2_bars_required = False

        self._stock_info_client_pool = FailoverClientPool(self.client, max_size=4)
        self._capital_flow_cache = {}
        logger.debug(f'server: {self.server}')

    def traffic(self):
        return self.client.get_traffic_stats()

    def quotes(self, symbol=None, *, batch_size=80, diagnostics=False, **kwargs):
        """Fetch snapshots in bounded batches, preserving first-request order.

        Optional diagnostics live in DataFrame.attrs, not in quote columns.
        Request failure never returns an apparently complete partial snapshot.
        """
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or not 1 <= batch_size <= 80:
            raise ValueError("batch_size 必须是 1 到 80 的整数")
        info = {"status": "ok", "requested": [], "duplicates_removed": 0,
                "batches": 0, "missing": [], "unexpected": []}

        def finish(rows, status, symbols=None):
            info["status"] = status
            from tdxhub.data_quality import normalize_index_quotes

            data = to_data(normalize_index_quotes(rows), symbol=symbols, client=self, **kwargs)
            internal_columns = [column for column in data.columns
                                if column in {"active1", "active2"}
                                or (isinstance(column, str) and column.startswith("reversed_bytes"))]
            data = data.drop(columns=internal_columns)
            if diagnostics:
                data.attrs["diagnostics"] = info
            return data

        if symbol is None or (isinstance(symbol, str) and not symbol.strip()):
            return finish(None, "empty_input")
        if isinstance(symbol, (list, tuple)) and not symbol:
            return finish(None, "empty_input")
        symbols = get_stock_markets([symbol] if isinstance(symbol, str) else symbol)
        # Reject rather than silently truncate overlong codes in the six-byte wire field.
        if any(len(code) != 6 or not code.isascii() or not code.isdigit() for _, code in symbols):
            raise ValueError("证券代码必须是六位数字")
        requested = list(dict.fromkeys(tuple(item) for item in symbols))
        info["requested"] = requested
        info["duplicates_removed"] = len(symbols) - len(requested)
        rows_by_symbol = {}
        unexpected = set()
        for start in range(0, len(requested), batch_size):
            batch = requested[start:start + batch_size]
            info["batches"] += 1
            try:
                rows = self.client.get_security_quotes([list(item) for item in batch])
            except ValidationException as exc:
                info["error"] = str(exc)
                return finish(None, "invalid_input")
            if rows is None:
                info["failed_batch"] = batch
                return finish(None, "request_failed")
            for row in rows:
                key = (row["market"], row["code"])
                if key in batch:
                    rows_by_symbol.setdefault(key, row)
                elif key not in unexpected:
                    unexpected.add(key)
                    info["unexpected"].append(key)
        info["missing"] = [key for key in requested if key not in rows_by_symbol]
        return finish([rows_by_symbol[key] for key in requested if key in rows_by_symbol],
                      "missing" if info["missing"] else "ok", requested)

    def bars(self, symbol='000001', frequency=9, start=0, offset=800, **kwargs):
        """
        获取实时日K线数据

        :param symbol: 股票代码
        :param frequency: 数据频次
        :param start: 开始位置
        :param offset: 每次获取条数
        :return: pd.dataFrame or None
        """
        frequency = get_frequency(frequency)
        options = dict(kwargs)
        with_turnover = bool(options.pop("turnover", False))
        gbbq = options.pop("gbbq", None)
        if with_turnover and frequency != 9:
            raise TdxhubValidationException("换手率仅支持股票日 K")

        actions = gbbq if gbbq is not None else options.get("xdxr")
        market, code = self._code_market(symbol)
        try:
            offset = min(int(offset), 800)
            start = int(start)
        except (TypeError, ValueError) as exc:
            raise TdxhubValidationException('start 和 offset 必须是整数') from exc
        if start < 0 or offset <= 0:
            raise TdxhubValidationException('start 必须大于等于 0，offset 必须大于 0')

        try:
            if self._round2_bars_required:
                raw = self._round2_klines(int(frequency), market, code, start, offset)
            else:
                raw = self.client.get_security_bars(int(frequency), market, code, start, offset)
                if raw is None:
                    # 2003(旧K线命令已停用)或网络重试耗尽都会表现为 None; 降级 round2
                    logger.info('经典 get_security_bars 无数据, 自动切换 round2 新协议: %s', code)
                    self._round2_bars_required = True
                    raw = self._round2_klines(int(frequency), market, code, start, offset)
        except (TdxConnectionError, OSError) as exc:
            raise TdxhubConnectionError('行情分页请求失败，不能将失败响应视为数据结束') from exc
        if with_turnover and actions is None and raw is not None and len(raw) > 0:
            actions = self.xdxr(symbol)
        if options.get("xdxr") is None and actions is not None:
            options["xdxr"] = actions

        result = _page_to_data(raw, symbol=code, client=self, **options)
        if not with_turnover:
            return result

        from tdxhub.gbbq import enrich_turnover

        return enrich_turnover(result, actions, code=code)

    def _round2_klines(self, frequency, market, code, start, offset):
        """服务器停用旧 K 线命令(2003)时, 用 round2(0x052d) 新协议降级取数。

        复用当前 failover 已选中的端点; 产出与经典 ``get_security_bars``
        完全同构的行列表(open/close/high/low/vol/amount/.../datetime)。
        """

        from tdxhub.tdx.round2 import Round2Client, is_convertible

        client = self._round2_client
        endpoint = getattr(self.client, 'endpoint', None)
        if endpoint:
            target = (str(endpoint[0]), int(endpoint[1]))
        elif getattr(self, 'server', None):
            target = (str(self.server[0]), int(self.server[1]))
        else:
            raise TdxConnectionError('无可用行情端点')
        if client is None:
            client = Round2Client(proxy=getattr(self, 'proxy', None))
            self._round2_client = client
        stale = client.client is None or client.ip != target[0] or client.port != target[1]
        if stale:
            client.disconnect()
            client.connect(target[0], target[1], time_out=max(self.timeout, 10))
        convertible = is_convertible(market, code)
        frame = client.get_security_bars(int(frequency), market, code, start, int(offset))
        rows = []
        for _, row in frame.iterrows():
            moment = row['time']
            hour = int(getattr(moment, 'hour', 15))
            minute = int(getattr(moment, 'minute', 0))
            rows.append(OrderedDict([
                ('open', float(row['open'])),
                ('close', float(row['close'])),
                ('high', float(row['high'])),
                ('low', float(row['low'])),
                ('vol', float(row['vol']) * 100 if not convertible else float(row['vol'])),
                ('amount', float(row['amount'])),
                ('year', int(moment.year)),
                ('month', int(moment.month)),
                ('day', int(moment.day)),
                ('hour', hour),
                ('minute', minute),
                ('datetime', f'{moment.year:04d}-{moment.month:02d}-{moment.day:02d} {hour:02d}:{minute:02d}'),
            ]))
        return rows

    def stock_count(self, market=MARKET_SH, security_type=None, *, raw=False):
        """Return a market directory count, optionally filtered by security type."""
        if market not in [0, 1, 2]:
            raise TdxhubValidationException('市场代码错误')

        normalized_type = normalize_security_type(security_type)
        if raw and normalized_type is not None:
            raise TdxhubValidationException('raw 协议计数不能按 security_type 过滤')
        if normalized_type is not None or (market == MARKET_BJ and not raw):
            return len(self.stocks(market=market, security_type=normalized_type))
        return self.client.get_security_count(market=market)

    def _load_stock_directory(self, market):
        if market == MARKET_BJ:
            return self._beijing_stocks()

        counts = self.stock_count(market=market)
        frames = []
        if counts > 0:
            for start in tqdm(range(0, counts, 1000), ascii=True):
                page = self.client.get_security_list(market=market, start=start)
                frames.append(to_data(page))
        return pandas.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def stocks(self, market=MARKET_SH, security_type=None, *, refresh=False):
        """Return a cached market directory, optionally filtered by type.

        Complete directories are shared by all clients in this process and are
        persisted below ``~/.tdxhub/caches/quotes``. They refresh lazily every
        six hours by default; pass ``refresh=True`` to force an update.
        """
        if market not in [MARKET_SZ, MARKET_SH, MARKET_BJ]:
            raise TdxhubValidationException('市场代码错误, 目前只支持沪深京市场')
        if not isinstance(refresh, bool):
            raise TdxhubValidationException('refresh 必须是布尔值')

        normalized_type = normalize_security_type(security_type)
        cached = _QUOTE_METADATA_CACHE.get(
            _quote_metadata_cache_file(f'stocks-{market}'),
            lambda: self._load_stock_directory(market),
            ttl=_quote_metadata_ttl('STOCKS_TTL', 6 * 3600),
            refresh=refresh,
        )
        return filter_security_directory(cached, market, normalized_type)

    def stock_list(self, market=MARKET_SH, start=0):
        """Return one security-directory page, matching the TDX 1000-row API."""
        if market not in [0, 1, 2]:
            raise TdxhubValidationException('市场代码错误, 目前只支持沪深京市场')
        try:
            start = int(start)
        except (TypeError, ValueError) as exc:
            raise TdxhubValidationException('start 必须是整数') from exc
        if not 0 <= start <= 65535:
            raise TdxhubValidationException('start 必须在 0..65535 之间')

        if market == 2:
            return self._beijing_stocks().iloc[start:start + 1000].reset_index(drop=True)

        return to_data(self.client.get_security_list(market=market, start=start))

    def _get_zhb_file(self, filename):
        """Download one named member from the official ``zhb.zip`` report."""
        content = self.client.get_report_file_by_size('zhb.zip')
        if not content:
            return b''

        try:
            with ZipFile(BytesIO(content)) as archive:
                member = next(
                    (name for name in archive.namelist() if Path(name).name.lower() == filename.lower()),
                    None,
                )
                if member is None:
                    raise TdxhubValidationException(f'zhb.zip 中缺少 {filename}')
                return archive.read(member)
        except BadZipFile as exc:
            raise TdxhubValidationException('zhb.zip 文件格式错误') from exc

    def _beijing_stocks(self):
        """Download and parse the BSE directory without querying unsupported market 2."""
        from tdxhub.official import parse_tdxbjmore

        return parse_tdxbjmore(self._get_zhb_file('tdxbjmore.cfg'))

    def stock_industries(self, symbols=None, *, refresh=False):
        """Return online TongDaXin and Shenwan industry assignments.

        ``symbols`` may be one security code or a list/tuple of codes. The
        complete association is cached process-wide and on disk; pass
        ``refresh=True`` to download the two official source files again.
        """
        from tdxhub.official import associate_industries, parse_incon, parse_tdxhy

        if not isinstance(refresh, bool):
            raise TdxhubValidationException('refresh 必须是布尔值')

        if symbols is None:
            requested = None
        elif isinstance(symbols, str):
            requested = [symbols]
        elif isinstance(symbols, (list, tuple)):
            requested = list(symbols)
        else:
            raise TdxhubValidationException('证券代码必须是字符串、列表或元组')

        market_names = {MARKET_SZ: 'sz', MARKET_SH: 'sh', MARKET_BJ: 'bj'}
        keys = None
        if requested is not None:
            keys = []
            for symbol in requested:
                market, code = self._code_market(symbol)
                keys.append((market_names[market], code))
            if not keys:
                empty_assignments = parse_tdxhy(b'')
                empty_dictionary = parse_incon(b'')
                return associate_industries(empty_assignments, empty_dictionary)

        def load_industries():
            assignment_content = self.client.get_report_file_by_size('tdxhy.cfg') or b''
            if isinstance(assignment_content, (bytearray, memoryview)):
                assignment_content = bytes(assignment_content)
            dictionary_content = self._get_zhb_file('incon.dat')
            assignments = parse_tdxhy(assignment_content)
            dictionary = parse_incon(dictionary_content)
            return associate_industries(assignments, dictionary)

        cached = _QUOTE_METADATA_CACHE.get(
            _quote_metadata_cache_file('stock-industries'),
            load_industries,
            ttl=_quote_metadata_ttl('INDUSTRIES_TTL', 24 * 3600),
            refresh=refresh,
        )

        if keys is None:
            result = cached.copy(deep=True)
        else:
            rows = []
            for market, code in keys:
                matched = cached.loc[(cached['market'] == market) & (cached['code'] == code)]
                if not matched.empty:
                    rows.append(matched)
            result = (
                pd.concat(rows, ignore_index=True)
                if rows
                else cached.iloc[0:0].copy(deep=True).reset_index(drop=True)
            )

        if 'raw_fields' in result:
            result['raw_fields'] = result['raw_fields'].map(deepcopy)
        result.attrs = deepcopy(cached.attrs)
        return result

    def _stock_info_directory(self, market, *, refresh=False):
        if refresh:
            return self.stocks(market, refresh=True)
        return self.stocks(market)

    def _stock_info_source(self, source, symbol):
        client_pool = getattr(self, '_stock_info_client_pool', None)
        if client_pool is None:
            return getattr(self, source)(symbol)
        with client_pool.connection() as client, self._using_client(client):
            return getattr(self, source)(symbol)

    def stock_info(
        self,
        symbols,
        *,
        refresh_industries=False,
        refresh_directories=False,
        max_workers=4,
    ):
        """Return one merged security, quote, finance and industry row per symbol."""
        from tdxhub.stock_info import (
            INDUSTRY_COLUMNS,
            STOCK_INFO_COLUMNS,
            build_stock_info_row,
            empty_stock_info_frame,
            frame_record,
            normalize_code,
            normalize_market_id,
        )

        if not isinstance(refresh_industries, bool):
            raise TdxhubValidationException('refresh_industries 必须是布尔值')
        if not isinstance(refresh_directories, bool):
            raise TdxhubValidationException('refresh_directories 必须是布尔值')
        if (
            isinstance(max_workers, bool)
            or not isinstance(max_workers, int)
            or not 1 <= max_workers <= 16
        ):
            raise TdxhubValidationException('max_workers 必须是 1..16 之间的整数')
        if isinstance(symbols, str):
            requested = [symbols]
        elif isinstance(symbols, (list, tuple)):
            requested = list(symbols)
        else:
            raise TdxhubValidationException('证券代码必须是字符串、列表或元组')
        if not requested:
            return empty_stock_info_frame()

        exchanges = {MARKET_SZ: 'sz', MARKET_SH: 'sh', MARKET_BJ: 'bj'}
        normalized = []
        for symbol in requested:
            if not isinstance(symbol, str) or not symbol.strip():
                raise TdxhubValidationException(f'证券代码错误: {symbol!r}')
            market_id, code = self._code_market(symbol)
            if market_id not in exchanges or not code.isdigit() or len(code) != 6:
                raise TdxhubValidationException(f'证券代码错误: {symbol!r}')
            normalized.append((market_id, exchanges[market_id], code))

        unique = list(dict.fromkeys(normalized))
        full_codes = [f'{exchange}{code}' for _, exchange, code in unique]

        securities = {}
        for market_id in dict.fromkeys(item[0] for item in unique):
            directory = self._stock_info_directory(market_id, refresh=refresh_directories)
            if directory is None or directory.empty:
                continue
            for record in directory.to_dict('records'):
                record_code = normalize_code(record.get('code'))
                if record_code is not None:
                    securities[(market_id, record_code)] = record

        quote_rows = {}
        quote_code_counts = {}
        realtime_symbols = [
            f'{exchange}{code}'
            for market_id, exchange, code in unique
            if market_id in (MARKET_SZ, MARKET_SH, MARKET_BJ)
        ]
        if realtime_symbols:
            realtime = self.quotes(realtime_symbols)
            if realtime is not None:
                for record in realtime.to_dict('records'):
                    record_code = normalize_code(record.get('code'))
                    if record_code is None:
                        continue
                    record_market = normalize_market_id(record.get('market'))
                    if record_market is not None:
                        quote_rows[(record_market, record_code)] = record
                    quote_code_counts.setdefault(record_code, []).append(record)

        industry_rows = {}
        industries = self.stock_industries(full_codes, refresh=refresh_industries)
        if industries is not None:
            for record in industries.to_dict('records'):
                record_code = normalize_code(record.get('code'))
                record_market = normalize_market_id(record.get('market'))
                if record_code is not None and record_market is not None:
                    industry_rows[(record_market, record_code)] = {
                        column: record.get(column) for column in INDUSTRY_COLUMNS
                    }

        source_results = {item: {} for item in unique}
        jobs = [
            (item, source, f'{item[1]}{item[2]}')
            for item in unique
            for source in ('finance', 'xdxr', 'call_auction')
            # Beijing quotes, finance and XDXR are supported independently;
            # do not drop them just because auction coverage is unverified.
            if source != 'call_auction' or item[0] in (MARKET_SZ, MARKET_SH)
        ]
        client_pool = getattr(self, '_stock_info_client_pool', None)
        if client_pool is not None:
            client_pool.ensure_capacity(max_workers)
        if jobs:
            workers = min(max_workers, len(jobs))
            with ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix='tdxhub-stock-info',
            ) as executor:
                futures = {
                    executor.submit(self._stock_info_source, source, full_code): (item, source, full_code)
                    for item, source, full_code in jobs
                }
                for future in as_completed(futures):
                    item, source, full_code = futures[future]
                    try:
                        source_results[item][source] = future.result()
                    except Exception as exc:
                        exc.add_note(f'汇总证券 {full_code} 数据失败')
                        raise

        unique_rows = {}
        for market_id, exchange, code in unique:
            item = (market_id, exchange, code)
            quote = quote_rows.get((market_id, code))
            if quote is None and len(quote_code_counts.get(code, ())) == 1:
                quote = quote_code_counts[code][0]
            sources = source_results[item]

            unique_rows[item] = build_stock_info_row(
                market_id=market_id,
                exchange=exchange,
                code=code,
                security=securities.get((market_id, code)),
                quote=quote,
                finance=frame_record(sources.get('finance')),
                actions=sources.get('xdxr'),
                auction=sources.get('call_auction'),
                industry=industry_rows.get((market_id, code)),
            )

        rows = [deepcopy(unique_rows[item]) for item in normalized]
        result = pd.DataFrame(rows, columns=STOCK_INFO_COLUMNS, dtype=object)
        result.attrs['skipped_sources'] = {
            f'{exchange}{code}': ['call_auction']
            for market_id, exchange, code in unique if market_id == MARKET_BJ
        }
        return result

    def call_auction(self, symbol, trade_date=None, **kwargs):
        """Return call-auction snapshots for one standard-market security."""
        from tdxhub.call_auction import build_call_auction_request, decode_call_auction

        market, code = self._code_market(symbol)
        try:
            request = build_call_auction_request(market, code)
        except ValueError as exc:
            raise TdxhubValidationException(f'证券代码错误: {symbol!r}') from exc
        payload = self.client.send_raw_pkg(request)
        result = decode_call_auction(payload, trade_date=trade_date)
        return to_data(result, symbol=code, client=self, **kwargs)

    def statistics(self, symbols=None):
        """获取通达信官方综合统计数据（tdxstat.cfg）.

        包含市盈率TTM (pe_ttm)、静态市盈 (pe_static)、股息率 (dividend_yield)、
        涨跌幅 (change_pct)、连涨连跌天数 (trend_days)、以及区间涨跌幅
        (change_5d, change_10d, change_20d, change_60d, change_ytd 等).

        :param symbols: 可选，单个股票代码或代码列表（如 "600519"、"sh600519"、["002594", "600519"]），None 返回全市场
        :return: pd.DataFrame 包含全市场或指定个股统计指标
        """
        from tdxhub.official import parse_tdxstat

        df = parse_tdxstat(self._get_zhb_file('tdxstat.cfg'))
        if symbols is None:
            return df

        if isinstance(symbols, str):
            symbols_list = [symbols]
        elif isinstance(symbols, (list, tuple, set)):
            symbols_list = list(symbols)
        else:
            symbols_list = [str(symbols)]

        clean_codes = set()
        for s in symbols_list:
            if isinstance(s, str):
                s_strip = s.strip().lower()
                for prefix in ("sh", "sz", "bj"):
                    if s_strip.startswith(prefix):
                        s_strip = s_strip[len(prefix):]
                        break
                clean_codes.add(s_strip)

        return df[df["code"].isin(clean_codes)].reset_index(drop=True)

    def money_flow(
        self,
        symbol=None,
        date=None,
        days=None,
        thresholds=(40_000.0, 200_000.0, 1_000_000.0),
        **kwargs,
    ):
        """获取资金流向数据.

        - 当 symbol 为 None 时：返回官方报告包中的成交额与板块归属（tdxstat2.cfg）；
          amount / amount_prev 单位为元，未知数值为空，date 保留报告日期（可能滞后）
        - 当指定 symbol 且 days is not None 时：获取该标的近 N 日历史资金流向及 5日/20日主力累计净流入
        - 当指定 symbol 时：基于逐笔分笔（Tick）计算该标的的超大单、大单、中单、小单及主力/散户资金流向
        """
        if symbol is None:
            from tdxhub.official import parse_tdxstat2

            return parse_tdxstat2(self._get_zhb_file('tdxstat2.cfg'))

        if days is not None:
            return self.capital_flow_history(
                symbol=symbol, days=days, thresholds=thresholds, **kwargs
            )

        return self.capital_flow(symbol=symbol, date=date, thresholds=thresholds, **kwargs)

    def xgsg(self, symbol=None):
        """获取通达信官方新股申购日历及配置（xgsg.cfg / IPO）.

        包含近期拟上市新股名称、证券代码、申购日期、发行价格及所属市场（沪/深/北）。

        :param symbol: 可选，按股票代码过滤（如 "001246" 或 "sz001246"），None 返回近期全部新股
        :return: pd.DataFrame
        """
        from tdxhub.official import parse_xgsg

        df = parse_xgsg(self._get_zhb_file('xgsg.cfg'))
        if symbol is None:
            return df

        clean_code = str(symbol).strip().lower()
        for prefix in ("sh", "sz", "bj"):
            if clean_code.startswith(prefix):
                clean_code = clean_code[len(prefix):]
                break
        return df[df["code"] == clean_code].reset_index(drop=True)

    ipo = xgsg
    new_stocks = xgsg


    def stock_all(self, security_type=None):
        """Return the combined Shenzhen, Shanghai and Beijing security directories."""
        normalized_type = normalize_security_type(security_type)
        return pandas.concat(
            [self.stocks(market, security_type=normalized_type) for market in [0, 1, 2]],
            ignore_index=True,
        )

    def index_bars(self, symbol='000001', frequency=9, start=0, offset=800, **kwargs):
        """
        获取指数k线

        :param symbol: 股票代码
        :param frequency: 数据频次
        :param start: 开始位置
        :param offset: 获取数量
        :return:
        """

        frequency = get_frequency(frequency)
        options = dict(kwargs)
        market_arg = options.pop('market', None)
        try:
            offset = min(int(offset), 800)
            start = int(start)
        except (TypeError, ValueError) as exc:
            raise TdxhubValidationException('start 和 offset 必须是整数') from exc
        if start < 0 or offset <= 0:
            raise TdxhubValidationException('start 必须大于等于 0，offset 必须大于 0')

        market, code = self._index_code_market(symbol, market=market_arg)
        result = self.client.get_index_bars(int(frequency), int(market), str(code), int(start), int(offset))

        return _page_to_data(result, symbol=code, client=self, **options)

    @staticmethod
    def _parse_since(since):
        if since is None:
            return None
        if not isinstance(since, str) or re.fullmatch(r'\d{8}', since) is None:
            raise TdxhubValidationException('since 必须使用 YYYYMMDD 格式')
        try:
            return pd.Timestamp(datetime.strptime(since, '%Y%m%d'))
        except ValueError as exc:
            raise TdxhubValidationException('since 必须使用有效的 YYYYMMDD 日期') from exc

    @staticmethod
    def _page_datetimes(frame):
        if 'datetime' in frame.columns:
            return pd.to_datetime(frame['datetime'], errors='coerce')
        if 'date' in frame.columns:
            return pd.to_datetime(frame['date'], errors='coerce')
        if isinstance(frame.index, pd.DatetimeIndex):
            return pd.Series(frame.index, index=frame.index)
        return None

    def _collect_pages(self, loader, page_size, since=None, *, max_rows=None):
        """Collect newest-first protocol pages into an oldest-first frame."""
        since_date = self._parse_since(since)
        frames = []
        start = 0

        while start <= 65535:
            size = min(page_size, max_rows - start) if max_rows is not None else page_size
            page = loader(start, size)
            if not isinstance(page, pd.DataFrame):
                page = _page_to_data(page)
            if page.empty:
                break

            frames.insert(0, page)
            stop_at_since = False
            if since_date is not None:
                page_datetimes = self._page_datetimes(page)
                if page_datetimes is None:
                    raise TdxhubValidationException('K线数据缺少 datetime/date 字段')
                stop_at_since = bool((page_datetimes <= since_date).fillna(False).any())

            if len(page) < size or stop_at_since or (max_rows is not None and start + len(page) >= max_rows):
                break
            start += size
        else:
            raise TdxhubIncompleteDataError(
                "已达到行情协议分页偏移上限，无法确认全量数据完整性",
                data={"reason": "offset_limit", "next_start": start, "page_size": page_size},
            )

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames)
        if since_date is not None:
            datetimes = self._page_datetimes(result)
            result = result.loc[(datetimes >= since_date).fillna(False)]
        return result

    @staticmethod
    def _adjust_collected(result, code, adjust, xdxr):
        # Pages already have normalized datetime indices. Do not parse the
        # original string columns again: pages may use different date formats.
        if result.empty or str(adjust).lower() not in {"qfq", "01", "before", "hfq", "02", "after"}:
            return result
        from tdxhub.utils.adjust import to_adjust

        return to_adjust(result, symbol=code, adjust=adjust, xdxr=xdxr)

    def bars_all(self, symbol='000001', frequency=9, since=None, **kwargs):
        """Return all available stock K-lines in chronological order."""
        frequency = get_frequency(frequency)
        options = dict(kwargs)
        adjust = options.pop("adjust", None)
        with_turnover = bool(options.pop("turnover", False))
        gbbq = options.pop("gbbq", None)
        if with_turnover and frequency != 9:
            raise TdxhubValidationException("换手率仅支持股票日 K")

        actions = gbbq if gbbq is not None else options.get("xdxr")
        result = self._collect_pages(
            lambda start, size: self.bars(
                symbol=symbol,
                frequency=frequency,
                start=start,
                offset=size,
                **options,
            ),
            800,
            since=since,
        )
        if with_turnover and actions is None and not result.empty:
            actions = self.xdxr(symbol)
        if options.get("xdxr") is None and actions is not None:
            options["xdxr"] = actions

        _, code = self._code_market(symbol)
        result = self._adjust_collected(result, code, adjust, options.get("xdxr"))
        if not with_turnover:
            return result

        from tdxhub.gbbq import enrich_turnover

        return enrich_turnover(result, actions, code=code)

    def minute_241(self, symbol="000001", since=None):
        """Return latest-day or ranged minute bars with a 09:30 auction bar."""
        from zoneinfo import ZoneInfo

        from tdxhub.data_units import trade_volume_multiplier
        from tdxhub.minute import build_minute_241, minute_241_dates, select_latest_minute_bars

        market, code = self._code_market(symbol)
        volume_multiplier = trade_volume_multiplier(market, code)
        bars = self.bars_all(symbol=symbol, frequency=8, since=since)
        if since is None:
            bars = select_latest_minute_bars(bars)
        if bars.empty:
            return bars

        today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        trades_by_date = {}
        for trade_date in minute_241_dates(bars):
            if trade_date == today:
                trades = self.transaction_all(symbol=symbol)
            else:
                trades = self.transactions_all(symbol=symbol, date=trade_date.strftime("%Y%m%d"))
            trades_by_date[trade_date] = trades
        return build_minute_241(bars, trades_by_date, volume_multiplier=volume_multiplier)

    def index_all(self, symbol='000001', frequency=9, since=None, **kwargs):
        """Return all available index K-lines in chronological order."""
        adjust = kwargs.pop("adjust", None)
        result = self._collect_pages(
            lambda start, size: self.index(
                symbol=symbol,
                frequency=frequency,
                start=start,
                offset=size,
                **kwargs,
            ),
            800,
            since=since,
        )
        _, code = self._index_code_market(symbol, market=kwargs.get("market"))
        return self._adjust_collected(result, code, adjust, kwargs.get("xdxr"))

    def minute(self, symbol=None, **kwargs):
        """
        获取实时分时数据

        :param symbol: 股票代码
        :return: pd.DataFrame
        """

        from zoneinfo import ZoneInfo

        today = datetime.now(ZoneInfo("Asia/Shanghai")).strftime('%Y%m%d')
        return self.minutes(symbol=symbol, date=today, **kwargs)

    def minutes(self, symbol=None, date='20191023', **kwargs):
        """
        分时历史数据

        :param symbol:  股票代码
        :param date:    查询日期
        :return: pd.dataFrame or None
        """

        market, code = self._code_market(symbol)

        if market not in [0, 1, 2]:
            raise TdxhubValidationException('市场代码错误, 目前只支持沪深北市场')
        result = self.client.get_history_minute_time_data(market=market, code=code, date=date)
        data = to_data(result, symbol=code, client=self, **kwargs)

        from tdxhub.minute import attach_minute_timestamps

        return attach_minute_timestamps(data, date)

    def transaction(self, symbol='', start=0, offset=800, **kwargs):
        """
        查询分笔成交

        :param symbol:  股票代码
        :param start:   起始位置
        :param offset:  结束位置
        :return: pd.dataFrame or None
        """

        market, code = self._code_market(symbol)

        result = self.client.get_transaction_data(market, code, start, offset)

        return _page_to_data(result, symbol=code, client=self, **kwargs)

    def transactions(self, symbol='', start=0, offset=800, date='20170209', **kwargs):
        """
        查询历史分笔成交

        :param symbol:  股票代码
        :param start:   起始位置
        :param offset:  获取数量
        :param date:    查询日期
        :return: pd.dataFrame or None
        """

        market, code = self._code_market(symbol)

        if market not in [0, 1, 2]:
            raise TdxhubValidationException('市场代码错误, 目前只支持沪深北市场')

        result = self.client.get_history_transaction_data(market, code, start, offset, int(date))
        return _page_to_data(result, symbol=code, client=self, **kwargs)

    def transaction_all(self, symbol='', **kwargs):
        """Return all server-retained current trades, including after close.

        The protocol supplies times but no trading date. On non-trading days
        the server may retain the previous session; use transactions_all(date=)
        when a verified historical date is required.
        """
        return self._collect_pages(
            lambda start, size: self.transaction(
                symbol=symbol,
                start=start,
                offset=size,
                **kwargs,
            ),
            1800,
        )

    def transactions_all(self, symbol='', date='20170209', **kwargs):
        """Return all trades for one historical day in chronological order."""
        return self._collect_pages(
            lambda start, size: self.transactions(
                symbol=symbol,
                date=date,
                start=start,
                offset=size,
                **kwargs,
            ),
            2000,
        )

    def capital_flow(
        self,
        symbol="",
        date=None,
        thresholds=(40_000.0, 200_000.0, 1_000_000.0),
        **kwargs,
    ):
        """计算基于逐笔成交明细（Tick）的资金流向指标.

        :param symbol:      股票代码（支持如 "600519"、"sh600519"）
        :param date:        查询日期（None 表示当日实时；历史日期形如 "20260910" 或 "2026-09-10"）
        :param thresholds:  单笔成交金额划分阈值（元），默认 (小单上限 4万, 中单上限 20万, 大单上限 100万)
        :return: 包含超大单、大单、中单、小单及主力/散户汇总统计的 pd.DataFrame
        """
        if not symbol or (isinstance(symbol, str) and not symbol.strip()):
            raise TdxhubValidationException("symbol 不能为空")

        from tdxhub.data_units import trade_volume_multiplier

        market, code = self._code_market(symbol)
        volume_multiplier = trade_volume_multiplier(market, code)
        try:
            today_str = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")
        except Exception:
            today_str = datetime.now().strftime("%Y%m%d")

        if date is None:
            date_clean = today_str
            trades = self.transaction_all(symbol=symbol, **kwargs)
        else:
            date_clean = str(date).replace("-", "").replace(".", "").strip()
            cache_key = (str(symbol), date_clean, tuple(thresholds))
            if (
                date_clean < today_str
                and hasattr(self, "_capital_flow_cache")
                and cache_key in self._capital_flow_cache
            ):
                return self._capital_flow_cache[cache_key].copy()

            if date_clean == today_str:
                trades = self.transaction_all(symbol=symbol, **kwargs)
                if trades is None or trades.empty:
                    trades = self.transactions_all(symbol=symbol, date=date_clean, **kwargs)
            else:
                trades = self.transactions_all(symbol=symbol, date=date_clean, **kwargs)

        tiers = ["超大单", "大单", "中单", "小单"]
        columns = [
            "buy_amount",
            "sell_amount",
            "net_amount",
            "net_pct",
            "buy_volume",
            "sell_volume",
            "net_volume",
        ]

        if trades is None or trades.empty:
            empty_df = pd.DataFrame(columns=columns, index=tiers + ["主力(超大+大单)", "散户(中+小单)", "合计"])
            empty_df.index.name = "level"
            empty_df.attrs = {
                "symbol": symbol,
                "date": date,
                "main_net": 0.0,
                "main_net_pct": 0.0,
                "retail_net": 0.0,
                "retail_net_pct": 0.0,
                "total_turnover": 0.0,
                "total_volume": 0.0,
                "trade_count": 0,
                "excluded_trade_count": 0,
                "unknown_side_counts": {},
                "volume_multiplier": volume_multiplier,
                "volume_unit": "lot",
                "method": "tick_estimate",
                "neutral_turnover": 0.0,
            }
            return empty_df

        trades = trades.copy()
        vol_col = "volume" if "volume" in trades.columns else "vol"
        source_count = len(trades)
        unknown_sides = trades.loc[~trades["buyorsell"].isin([0, 1, 2]), "buyorsell"].value_counts().to_dict()
        prices = pd.to_numeric(trades["price"], errors="coerce")
        volumes = pd.to_numeric(trades[vol_col], errors="coerce")
        valid = (trades["buyorsell"].isin([0, 1, 2])
                 & prices.gt(0) & prices.lt(float("inf"))
                 & volumes.gt(0) & volumes.lt(float("inf")))
        trades = trades.loc[valid].copy()
        trades["price"] = prices.loc[valid]
        trades[vol_col] = volumes.loc[valid]
        trades["amount_yuan"] = trades["price"] * trades[vol_col] * volume_multiplier

        t_small, t_medium, t_large = thresholds
        bins = [-1.0, float(t_small), float(t_medium), float(t_large), float("inf")]
        labels = ["小单", "中单", "大单", "超大单"]
        trades["level"] = pd.cut(trades["amount_yuan"], bins=bins, labels=labels)

        buy_trades = trades[trades["buyorsell"] == 0]
        sell_trades = trades[trades["buyorsell"] == 1]

        buy_amt = buy_trades.groupby("level", observed=False)["amount_yuan"].sum()
        sell_amt = sell_trades.groupby("level", observed=False)["amount_yuan"].sum()
        buy_vol = buy_trades.groupby("level", observed=False)[vol_col].sum()
        sell_vol = sell_trades.groupby("level", observed=False)[vol_col].sum()

        total_amount = float(trades["amount_yuan"].sum())
        total_vol = float(trades[vol_col].sum())

        rows = []
        for t in tiers:
            b_a = float(buy_amt.get(t, 0.0))
            s_a = float(sell_amt.get(t, 0.0))
            n_a = b_a - s_a
            pct = round(n_a / total_amount * 100.0, 2) if total_amount > 0 else 0.0
            b_v = float(buy_vol.get(t, 0.0))
            s_v = float(sell_vol.get(t, 0.0))
            n_v = b_v - s_v
            rows.append({
                "level": t,
                "buy_amount": b_a,
                "sell_amount": s_a,
                "net_amount": n_a,
                "net_pct": pct,
                "buy_volume": b_v,
                "sell_volume": s_v,
                "net_volume": n_v,
            })

        tier_df = pd.DataFrame(rows).set_index("level")

        main_buy = float(tier_df.loc[["超大单", "大单"], "buy_amount"].sum())
        main_sell = float(tier_df.loc[["超大单", "大单"], "sell_amount"].sum())
        main_net = main_buy - main_sell
        main_pct = round(main_net / total_amount * 100.0, 2) if total_amount > 0 else 0.0
        main_b_v = float(tier_df.loc[["超大单", "大单"], "buy_volume"].sum())
        main_s_v = float(tier_df.loc[["超大单", "大单"], "sell_volume"].sum())
        main_n_v = main_b_v - main_s_v

        retail_buy = float(tier_df.loc[["中单", "小单"], "buy_amount"].sum())
        retail_sell = float(tier_df.loc[["中单", "小单"], "sell_amount"].sum())
        retail_net = retail_buy - retail_sell
        retail_pct = round(retail_net / total_amount * 100.0, 2) if total_amount > 0 else 0.0
        retail_b_v = float(tier_df.loc[["中单", "小单"], "buy_volume"].sum())
        retail_s_v = float(tier_df.loc[["中单", "小单"], "sell_volume"].sum())
        retail_n_v = retail_b_v - retail_s_v

        tot_buy = float(tier_df["buy_amount"].sum())
        tot_sell = float(tier_df["sell_amount"].sum())
        tot_net = tot_buy - tot_sell
        tot_pct = round(tot_net / total_amount * 100.0, 2) if total_amount > 0 else 0.0
        tot_b_v = float(tier_df["buy_volume"].sum())
        tot_s_v = float(tier_df["sell_volume"].sum())
        tot_n_v = tot_b_v - tot_s_v

        summary = pd.DataFrame([
            {
                "buy_amount": main_buy,
                "sell_amount": main_sell,
                "net_amount": main_net,
                "net_pct": main_pct,
                "buy_volume": main_b_v,
                "sell_volume": main_s_v,
                "net_volume": main_n_v,
            },
            {
                "buy_amount": retail_buy,
                "sell_amount": retail_sell,
                "net_amount": retail_net,
                "net_pct": retail_pct,
                "buy_volume": retail_b_v,
                "sell_volume": retail_s_v,
                "net_volume": retail_n_v,
            },
            {
                "buy_amount": tot_buy,
                "sell_amount": tot_sell,
                "net_amount": tot_net,
                "net_pct": tot_pct,
                "buy_volume": tot_b_v,
                "sell_volume": tot_s_v,
                "net_volume": tot_n_v,
            },
        ], index=["主力(超大+大单)", "散户(中+小单)", "合计"])
        summary.index.name = "level"

        result = pd.concat([tier_df, summary])
        result.attrs = {
            "symbol": symbol,
            "date": date,
            "main_net": main_net,
            "main_net_pct": main_pct,
            "retail_net": retail_net,
            "retail_net_pct": retail_pct,
            "total_turnover": total_amount,
            "total_volume": total_vol,
            "trade_count": len(trades),
            "excluded_trade_count": source_count - len(trades),
            "unknown_side_counts": unknown_sides,
            "volume_multiplier": volume_multiplier,
            "volume_unit": "lot",
            "method": "tick_estimate",
            "neutral_turnover": float(trades.loc[trades["buyorsell"] == 2, "amount_yuan"].sum()),
        }
        if date is not None and date_clean < today_str and hasattr(self, "_capital_flow_cache"):
            self._capital_flow_cache[cache_key] = result.copy()
        return result

    def capital_flow_history(
        self,
        symbol="",
        days=20,
        thresholds=(40_000.0, 200_000.0, 1_000_000.0),
        **kwargs,
    ):
        """获取个股近 N 个交易日的逐日资金流向明细，并计算 5日/20日主力累计净流入/出.

        :param symbol:      股票代码（支持如 "600519"、"002594"、"sh600519"）
        :param days:        回溯交易日天数（默认 20，支持 5、10、20 等任意正整数）
        :param thresholds:  单笔成交金额划分阈值（元），默认 (4万, 20万, 100万)
        :return: pd.DataFrame 包含逐日主力/散户流向及 5日/20日滚动累计净流入，并在 attrs 注入 5日/20日汇总数据
        """
        if not symbol or (isinstance(symbol, str) and not symbol.strip()):
            raise TdxhubValidationException("symbol 不能为空")

        if isinstance(days, bool) or not isinstance(days, int) or days <= 0:
            raise TdxhubValidationException("days 必须是正整数")
        # N returned days need 19 warm-up days and one previous close. Fetch in
        # bounded pages rather than allowing bars() to silently cap at 800.
        options = dict(kwargs)
        adjust = options.pop("adjust", None)
        options.pop("turnover", None)
        actions = options.pop("gbbq", None)
        if actions is not None:
            options["xdxr"] = actions
        bars = self._collect_pages(
            lambda start, size: self.bars(symbol, frequency=9, start=start, offset=size, **options),
            800,
            max_rows=days + 20,
        )
        _, code = self._code_market(symbol)
        bars = self._adjust_collected(bars, code, adjust, options.get("xdxr"))
        flow_options = {key: value for key, value in options.items() if key != "xdxr"}
        if bars.empty:
            raise TdxhubIncompleteDataError(
                f"{symbol} 无可用日 K，不能计算资金流窗口",
                data={"symbol": symbol, "reason": "empty_bars"},
            )

        bars = bars.copy()
        dates = self._page_datetimes(bars)
        if dates is None or pd.isna(dates).any():
            raise TdxhubIncompleteDataError(f"{symbol} 日 K 缺少有效日期")
        bars["date_str"] = pd.DatetimeIndex(dates).strftime("%Y%m%d")
        bars["date_fmt"] = pd.DatetimeIndex(dates).strftime("%Y-%m-%d")
        bars = bars.sort_values("date_str")
        if bars["date_str"].duplicated().any():
            raise TdxhubIncompleteDataError(f"{symbol} 日 K 存在重复交易日")
        # Unknown first previous close stays NaN, not a fabricated 0% return.
        bars["change_pct"] = (bars["close"].pct_change(fill_method=None) * 100.0).round(2)
        target_bars = bars.iloc[-(days + 19):].copy()

        records = []
        for _, row in target_bars.iterrows():
            d_clean = row["date_str"]
            d_fmt = row["date_fmt"]
            close_val = float(row.get("close", 0.0))
            chg_val = float(row.get("change_pct", 0.0))

            flow = self.capital_flow(
                symbol=symbol, date=d_clean, thresholds=thresholds, **flow_options
            )

            if not _has_flow_data(flow):
                raise TdxhubIncompleteDataError(
                    f"{symbol} 在 {d_clean} 缺少成交数据，不能计算资金流窗口",
                    data={"symbol": symbol, "date": d_clean, "reason": "empty_trades"},
                )
            main_net = float(flow.attrs.get("main_net", 0.0))
            main_pct = float(flow.attrs.get("main_net_pct", 0.0))
            retail_net = float(flow.attrs.get("retail_net", 0.0))
            retail_pct = float(flow.attrs.get("retail_net_pct", 0.0))
            total_amt = float(flow.attrs.get("total_turnover", 0.0))

            super_net = float(flow.loc["超大单", "net_amount"]) if "超大单" in flow.index else 0.0
            large_net = float(flow.loc["大单", "net_amount"]) if "大单" in flow.index else 0.0
            medium_net = float(flow.loc["中单", "net_amount"]) if "中单" in flow.index else 0.0
            small_net = float(flow.loc["小单", "net_amount"]) if "小单" in flow.index else 0.0

            records.append({
                "date": d_fmt,
                "close": close_val,
                "change_pct": chg_val,
                "total_amount": total_amt,
                "main_net": main_net,
                "main_pct": main_pct,
                "super_net": super_net,
                "large_net": large_net,
                "medium_net": medium_net,
                "small_net": small_net,
                "retail_net": retail_net,
                "retail_pct": retail_pct,
            })

        df = pd.DataFrame(records)
        summary = {"symbol": symbol, "days": min(days, len(df))}
        for window in (5, 20):
            net = df["main_net"].rolling(window, min_periods=window).sum()
            amount = df["total_amount"].rolling(window, min_periods=window).sum()
            df[f"main_{window}d_net"] = net
            df[f"main_{window}d_pct"] = (net / amount.where(amount.ne(0)) * 100.0).round(2)
            df[f"window_{window}d_days"] = [min(i + 1, window) for i in range(len(df))]
            df[f"window_{window}d_complete"] = df[f"window_{window}d_days"].eq(window)
            summary.update({
                f"main_{window}d_net": float(net.iloc[-1]),
                f"main_{window}d_pct": float(df[f"main_{window}d_pct"].iloc[-1]),
                f"retail_{window}d_net": float(df["retail_net"].rolling(window, min_periods=window).sum().iloc[-1]),
                f"total_{window}d_amount": float(amount.iloc[-1]),
                f"window_{window}d_days": min(len(df), window),
                f"window_{window}d_complete": len(df) >= window,
            })
        result = df.tail(days).reset_index(drop=True)
        result.attrs = summary
        return result

    def sector_capital_flow(
        self,
        name="",
        symbols=None,
        date=None,
        thresholds=(40_000.0, 200_000.0, 1_000_000.0),
        **kwargs,
    ):
        """获取指定行业或板块的主力资金流向汇总与成分股明细.

        :param name:        板块/行业名称（支持通达信行业如 "白酒"、"银行"，或申万一级行业如 "食品饮料"、"电子"）
        :param symbols:     可选，自定义成分股代码列表。若提供则优先使用该列表
        :param date:        查询日期（None 表示当日实时盘中；历史日期形如 "20260914"）
        :param thresholds:  单笔成交金额划分阈值（元），默认 (4万, 20万, 100万)
        :return: pd.DataFrame 包含成分股逐笔资金流向明细（按主力净额排序），并在 attrs 中提供板块汇总指标
        """
        if not symbols and (not name or not isinstance(name, str) or not name.strip()):
            raise TdxhubValidationException("name 或 symbols 至少需要提供一个")

        if symbols is not None:
            if isinstance(symbols, str):
                target_codes = [s.strip() for s in symbols.split(",") if s.strip()]
            else:
                target_codes = list(symbols)
            sector_name = name or "custom"
        else:
            sector_name = name.strip()
            ind_df = self.stock_industries()
            industry_cols = [
                c
                for c in (
                    "tdx_industry_name",
                    "sw_industry_name",
                    "sw_level1_name",
                    "sw_level2_name",
                    "sw_level3_name",
                )
                if c in ind_df.columns
            ]
            match = pd.Series(False, index=ind_df.index)
            for c in industry_cols:
                match = match | (ind_df[c] == sector_name)
            matched_df = ind_df[match]
            if matched_df.empty:
                for c in industry_cols:
                    match = match | ind_df[c].fillna("").astype(str).str.contains(sector_name)
                matched_df = ind_df[match]

            if matched_df.empty:
                raise TdxhubValidationException(f"未找到板块或行业: {sector_name!r}")

            target_codes = matched_df["code"].tolist()

        # Preserve the first spelling for output/calls, but deduplicate by market
        # and code so aliases cannot double-count and cross-market codes stay distinct.
        unique_codes = {}
        for code in target_codes:
            unique_codes.setdefault(self._code_market(code), code)
        if not unique_codes:
            raise TdxhubValidationException("成分股列表不能为空")

        records = []
        for code in unique_codes.values():
            try:
                flow = self.capital_flow(
                    symbol=code, date=date, thresholds=thresholds, **kwargs
                )
            except Exception as exc:
                raise TdxhubIncompleteDataError(
                    f"成分股 {code} 资金流查询失败，不能返回残缺的板块汇总",
                    data={"symbol": code, "date": date, "reason": "component_failed"},
                ) from exc
            if not _has_flow_data(flow):
                raise TdxhubIncompleteDataError(
                    f"成分股 {code} 无成交数据，不能返回残缺的板块汇总",
                    data={"symbol": code, "date": date, "reason": "empty_trades"},
                )
            main_net = float(flow.attrs.get("main_net", 0.0))
            main_pct = float(flow.attrs.get("main_net_pct", 0.0))
            retail_net = float(flow.attrs.get("retail_net", 0.0))
            turnover = float(flow.attrs.get("total_turnover", 0.0))
            super_net = (
                float(flow.loc["超大单", "net_amount"]) if "超大单" in flow.index else 0.0
            )
            large_net = (
                float(flow.loc["大单", "net_amount"]) if "大单" in flow.index else 0.0
            )

            records.append({
                "code": str(code),
                "main_net": main_net,
                "main_pct": main_pct,
                "retail_net": retail_net,
                "super_net": super_net,
                "large_net": large_net,
                "total_amount": turnover,
            })

        df = pd.DataFrame(records).sort_values("main_net", ascending=False).reset_index(drop=True)

        tot_main_net = float(df["main_net"].sum())
        tot_retail_net = float(df["retail_net"].sum())
        tot_super_net = float(df["super_net"].sum())
        tot_large_net = float(df["large_net"].sum())
        tot_turnover = float(df["total_amount"].sum())
        tot_main_pct = (
            round(tot_main_net / tot_turnover * 100.0, 2) if tot_turnover > 0 else 0.0
        )

        top_inflow = str(df.iloc[0]["code"]) if not df.empty else None
        top_outflow = str(df.iloc[-1]["code"]) if not df.empty else None

        df.attrs = {
            "sector_name": sector_name,
            "date": date,
            "stock_count": len(df),
            "main_net": tot_main_net,
            "main_net_pct": tot_main_pct,
            "retail_net": tot_retail_net,
            "super_net": tot_super_net,
            "large_net": tot_large_net,
            "total_turnover": tot_turnover,
            "top_inflow_code": top_inflow,
            "top_outflow_code": top_outflow,
        }
        return df

    block_capital_flow = sector_capital_flow

    def F10C(self, symbol=''):  # noqa
        """
        查询公司信息目录

        :param symbol: 股票代码
        :return: pd.dataFrame or None
        """

        market, symbol = self._code_market(symbol)

        if market not in [0, 1]:
            raise TdxhubValidationException('市场代码错误, 目前只支持沪深市场')

        result = self.client.get_company_info_category(market, symbol)

        return result

    def company_content(self, symbol='', filename='', start=0, length=0):
        """Read an explicit byte range from a company information file."""
        market, code = self._code_market(symbol)

        if market not in [0, 1]:
            raise TdxhubValidationException('市场代码错误, 目前只支持沪深市场')
        if not isinstance(filename, str) or not filename.strip():
            raise TdxhubValidationException('filename 不能为空')
        try:
            start = int(start)
            length = int(length)
        except (TypeError, ValueError) as exc:
            raise TdxhubValidationException('start 和 length 必须是整数') from exc
        if start < 0 or length < 0:
            raise TdxhubValidationException('start 和 length 必须大于等于 0')

        return self.client.get_company_info_content(
            market=market,
            code=code,
            filename=filename.strip(),
            start=start,
            length=length,
        )

    def F10(self, symbol='', name=''):  # noqa
        """
        读取规范化的公司信息详情

        :param name: 公司 F10 标题
        :param symbol: 股票代码
        :return: 固定列 pd.DataFrame
        """

        if not isinstance(name, str):
            raise TdxhubValidationException('name 必须是字符串')
        section = name.strip()
        market, code = self._code_market(symbol)

        if market not in [0, 1]:
            raise TdxhubValidationException('市场代码错误, 目前只支持沪深市场')

        categories = self.client.get_company_info_category(market, code) or []
        if section:
            categories = [item for item in categories if item['name'] == section]

        exchange = {MARKET_SZ: 'sz', MARKET_SH: 'sh'}[market]
        records = []
        for item in categories:
            content = self.client.get_company_info_content(
                market=market,
                code=code,
                filename=item['filename'],
                start=item['start'],
                length=item['length'],
            )
            records.append({
                'full_code': f'{exchange}{code}',
                'exchange': exchange,
                'market_id': market,
                'code': code,
                'section': item['name'],
                'filename': item['filename'],
                'start': item['start'],
                'length': item['length'],
                'content': normalize_f10_content(content),
            })

        return f10_frame(records)

    def xdxr(self, symbol='', **kwargs):
        """
        读取除权除息信息

        :param symbol: 股票代码
        :return: pd.dataFrame or None
        """

        market, symbol = self._code_market(symbol)
        result = self.client.get_xdxr_info(market, symbol)
        frame = to_data(result, symbol=symbol, client=self, **kwargs)
        frame.attrs["equity_unit"] = "ten_thousand_shares"
        return frame

    def finance(self, symbol='000001', **kwargs):
        """
        读取财务信息

        :param symbol: 股票代码
        :return:
        """

        market, symbol = self._code_market(symbol)
        result = self.client.get_finance_info(market=market, code=symbol)
        frame = to_data(result, symbol=symbol, client=self, **kwargs)
        if not frame.empty:
            from tdxhub.data_quality import finance_quality

            frame["data_quality"] = [finance_quality(row) for row in frame.to_dict("records")]
        return frame

    def k(self, symbol='', begin=None, end=None, **kwargs):
        """
        读取k线信息

        :param symbol:  股票代码
        :param begin:   开始日期
        :param end:     截止日期
        :return: pd.dataFrame or None
        """

        result = self.get_k_data(symbol, begin, end)
        return to_data(result, symbol=symbol, **kwargs)

    def ohlc(self, **kwargs):
        return self.k(**kwargs)

    def get_k_data(self, code, start_date, end_date):
        if start_date is None or end_date is None:
            return pd.DataFrame()
        try:
            start = pd.Timestamp(start_date)
            end = pd.Timestamp(end_date)
        except (TypeError, ValueError) as exc:
            raise TdxhubValidationException('日期格式错误') from exc
        if start >= end:
            return pd.DataFrame()

        if pd.isna(start) or pd.isna(end):
            raise TdxhubValidationException('日期格式错误')
        # Wire offsets count records, not calendar days. Always start at the
        # newest page and stop only after observing the requested start date.
        market, code = self._code_market(code)
        data = self._collect_pages(
            lambda offset, size: self.client.get_security_bars(9, market, code, offset, size),
            800,
            since=start.strftime('%Y%m%d'),
        )
        if data.empty:
            return pd.DataFrame()
        if 'datetime' not in data:
            return pd.DataFrame()
        data = data.assign(date=data['datetime'].apply(lambda x: str(x)[0:10])).assign(code=str(code))
        data = data.set_index('date', drop=False, inplace=False)
        data = data.drop(['year', 'month', 'day', 'hour', 'minute', 'datetime'], axis=1, errors='ignore')
        data = data.loc[(data.date >= start.strftime('%Y-%m-%d')) & (data.date < end.strftime('%Y-%m-%d'))]
        data = data.sort_index()

        return data

    def index(self, symbol='000001', frequency=9, start=0, offset=800, **kwargs):
        """
        获取指数k线

        K线种类:
        - 0 5分钟K线
        - 1 15分钟K线
        - 2 30分钟K线
        - 3 1小时K线
        - 4 日K线
        - 5 周K线
        - 6 月K线
        - 7 1分钟
        - 8 1分钟K线
        - 9 日K线
        - 10 季K线
        - 11 年K线

        :param symbol:      股票代码
        :param frequency:   数据频次
        :param market:      证券市场
        :param start:       开始位置
        :param offset:      每次获取条数
        :return: pd.dataFrame or None
        """
        return self.index_bars(symbol=symbol, frequency=frequency, start=start, offset=offset, **kwargs)

    def block(self, tofile='block.dat', **kwargs):
        """
        获取证券板块信息

        :param tofile: 保存文件
        :return: pd.dataFrame or None
        """

        result = self.client.get_and_parse_block_info(tofile)
        return to_data(result, **kwargs)


class ExtQuotes(BaseQuotes):
    """扩展市场实时行情"""

    # server = ("112.74.214.43", 7727)

    def __init__(
        self,
        server=None,
        bestip=False,
        timeout=15,
        heartbeat=False,
        auto_retry=True,
        raise_exception=False,
        failover=True,
        fallback_servers=False,
        max_failovers=2,
        unhealthy_cooldown=60.0,
        max_candidates=5,
        request_timeout=None,
        proxy=None,
        **kwargs,
    ):
        """Create an extended-market quote client with bounded failover."""

        request_timeout = timeout if request_timeout is None else request_timeout
        if request_timeout is None:
            request_timeout = 15
        if not isinstance(request_timeout, (int, float)) or not math.isfinite(request_timeout) or request_timeout <= 0:
            raise TdxhubValidationException('request_timeout 必须是大于 0 的有限数字')
        super().__init__(bestip=bestip, timeout=timeout, server=server, proxy=proxy, **kwargs)
        unhealthy_cooldown = _validate_failover_options(max_failovers, unhealthy_cooldown, max_candidates)
        requested_server = self.server
        if requested_server:
            config.set('BESTIP.EX', requested_server)

        candidates = _quote_candidates('EX', requested_server, fallback_servers)
        if not candidates:
            raise TdxhubValidationException('没有可用的扩展行情服务器配置')

        def probe_factory():
            return ExtendedClient(heartbeat=False, auto_retry=False, raise_exception=True, proxy=self.proxy)

        healthy_endpoints, last_error, observations = _probe_quote_endpoints(
            candidates,
            probe_factory,
            lambda client: client.get_instrument_count(),
            self.timeout,
            max_candidates,
        )
        if not healthy_endpoints:
            message = f'无法连接任何可用的扩展行情服务器，已尝试: {candidates}'
            if last_error:
                message = f'{message}: {last_error}'
            raise TdxhubConnectionError(message, data={'servers': candidates}) from last_error

        api_options = {}
        if 'multithread' in kwargs:
            api_options['multithread'] = kwargs['multithread']

        endpoint_pool = EndpointPool(healthy_endpoints, cooldown=unhealthy_cooldown)
        for endpoint, capability, status, error in observations:
            endpoint_pool.report_capability(endpoint, capability, status, error)
        self.client = FailoverClient(
            endpoint_pool,
            lambda: ExtendedClient(
                heartbeat=heartbeat,
                auto_retry=False,
                raise_exception=True,
                proxy=self.proxy,
                **api_options,
            ),
            timeout=self.timeout,
            request_timeout=request_timeout,
            max_failovers=max_failovers if failover else 0,
            raise_exception=raise_exception,
            on_switch=lambda endpoint: setattr(self, 'server', endpoint),
        )
        try:
            connected = self.client.connect()
        except (OSError, TdxConnectionError, TdxFunctionCallError) as exc:
            raise TdxhubConnectionError(
                f'无法连接任何可用的扩展行情服务器，已尝试: {healthy_endpoints}: {exc}',
                data={'servers': healthy_endpoints},
            ) from exc
        if not connected:
            raise TdxhubConnectionError(
                f'无法连接任何可用的扩展行情服务器，已尝试: {healthy_endpoints}',
                data={'servers': healthy_endpoints},
            )

    @staticmethod
    def validate(market, symbol):
        """
        验证股票市场

        :param market: 股票市场
        :param symbol: 股票代码
        :return: tuple
        """

        if not market and len(symbol.split('#')) > 1:
            market = symbol.split('#')[0]
            symbol = symbol.split('#')[1]

        if not market:
            raise ValueError('市场参数错误, 市场参数不能为空.')

        return int(market), symbol

    @_legacy_retry(
        wait=wait_random(min=1, max=10),
        stop=stop_after_attempt(3),
        retry_error_callback=return_last_value,
        retry=(retry_if_exception_type() | retry_if_result(check_empty)),
    )
    def markets(self, **kwargs):
        """
        获取实时市场列表

        :return: pd.dataFrame or None
        """

        result = self.client.get_markets()
        return to_data(result, **kwargs)

    @_legacy_retry(
        wait=wait_random(min=1, max=10),
        stop=stop_after_attempt(3),
        retry_error_callback=return_last_value,
        retry=(retry_if_exception_type() | retry_if_result(check_empty)),
    )
    def instrument(self, start=0, offset=800, **kwargs):
        """
        查询代码列表

        :param start:   开始位置
        :param offset:  获取数量
        :return:
        """

        result = self.client.get_instrument_info(start=start, count=offset)
        return to_data(result, **kwargs)

    @_legacy_retry(
        wait=wait_random(min=1, max=10),
        stop=stop_after_attempt(3),
        retry_error_callback=return_last_value,
        retry=(retry_if_exception_type() | retry_if_result(check_empty)),
    )
    def instrument_count(self):
        """
        市场商品数量

        :return:
        """

        result = self.client.get_instrument_count()

        return result

    @_legacy_retry(
        wait=wait_random(min=1, max=10),
        stop=stop_after_attempt(3),
        retry_error_callback=return_last_value,
        retry=(retry_if_exception_type() | retry_if_result(check_empty)),
    )
    def instruments(self, **kwargs):
        """
        查询所有代码列表

        :return:
        """

        result = []

        count = self.client.get_instrument_count()
        pages = math.ceil(count / 100)

        for page in tqdm(range(0, pages), ascii=True):
            result += self.client.get_instrument_info(page * 100, 100)

        return to_data(result, **kwargs)

    @_legacy_retry(
        wait=wait_random(min=1, max=10),
        stop=stop_after_attempt(3),
        retry_error_callback=return_last_value,
        retry=(retry_if_exception_type() | retry_if_result(check_empty)),
    )
    def quote_list(self, market='', category=0, start=0, offset=80, **kwargs):
        """Query one page of extended-market quotes."""
        if market in (None, ''):
            raise ValueError('市场参数错误, 市场参数不能为空.')
        result = self.client.get_instrument_quote_list(
            market=int(market), category=int(category), start=int(start), count=int(offset)
        )
        return to_data(result, client=self, **kwargs)

    @_legacy_retry(
        wait=wait_random(min=1, max=10),
        stop=stop_after_attempt(3),
        retry_error_callback=return_last_value,
        retry=(retry_if_exception_type() | retry_if_result(check_empty)),
    )
    def bars_range(self, market='', symbol='', start_date='', end_date='', **kwargs):
        """Query extended-market K-lines in an inclusive protocol date range."""
        market, symbol = self.validate(market, symbol)
        result = self.client.get_history_instrument_bars_range(
            market=market,
            code=symbol,
            start=int(start_date),
            end=int(end_date),
        )
        return to_data(result, symbol=symbol, client=self, **kwargs)

    @_legacy_retry(
        wait=wait_random(min=1, max=10),
        stop=stop_after_attempt(3),
        retry_error_callback=return_last_value,
        retry=(retry_if_exception_type() | retry_if_result(check_empty)),
    )
    def quote(self, market='', symbol='', **kwargs):
        """
        查询五档行情

        :param market: 市场ID
        :param symbol: 证券代码
        :return:
        """

        market, symbol = self.validate(market, symbol)
        result = self.client.get_instrument_quote(market, symbol)

        return to_data(result, symbol=symbol, client=self, **kwargs)

    @_legacy_retry(
        wait=wait_random(min=1, max=10),
        stop=stop_after_attempt(3),
        retry_error_callback=return_last_value,
        retry=(retry_if_exception_type() | retry_if_result(check_empty)),
    )
    def minute(self, market='', symbol='', **kwargs):
        """
        查询分时行情

        :param market: 市场ID
        :param symbol: 证券代码
        :return:
        """

        market, symbol = self.validate(market, symbol)
        result = self.client.get_minute_time_data(market, symbol)

        return to_data(result, symbol=symbol, client=self, **kwargs)

    @_legacy_retry(
        wait=wait_random(min=1, max=10),
        stop=stop_after_attempt(3),
        retry_error_callback=return_last_value,
        retry=(retry_if_exception_type() | retry_if_result(check_empty)),
    )
    def minutes(self, market=None, symbol='', date='', **kwargs):
        """
        查询历史分时行情

        :param market:  市场ID
        :param symbol:  证券代码
        :param date:    查询日期
        :return:
        """

        market, symbol = self.validate(market, symbol)
        result = self.client.get_history_minute_time_data(market, symbol, date)

        return to_data(result, symbol=symbol, client=self, **kwargs)

    @_legacy_retry(
        wait=wait_random(min=1, max=10),
        stop=stop_after_attempt(3),
        retry_error_callback=return_last_value,
        retry=(retry_if_exception_type() | retry_if_result(check_empty)),
    )
    def bars(self, frequency='', market='', symbol='', start=0, offset=800, **kwargs):
        """
        查询k线数据

        :param frequency: 数据频次, K线周期
        :param market: 市场ID
        :param symbol: 证券代码
        :param start:  起始位置
        :param offset: 获取数量
        :return:
        """

        frequency = get_frequency(frequency)
        market, symbol = self.validate(market, symbol)
        result = self.client.get_instrument_bars(
            category=frequency, market=market, code=symbol, start=start, count=offset
        )

        return to_data(result, symbol=symbol, **kwargs)

    @_legacy_retry(
        wait=wait_random(min=1, max=10),
        stop=stop_after_attempt(3),
        retry_error_callback=return_last_value,
        retry=(retry_if_exception_type() | retry_if_result(check_empty)),
    )
    def transaction(self, market=None, symbol='', start=0, offset=800, **kwargs):
        """
        查询分笔成交

        :param market: 市场ID
        :param symbol: 证券代码
        :param start:  开始位置
        :param offset: 获取数量
        :return:
        """

        market, symbol = self.validate(market, symbol)
        result = self.client.get_transaction_data(market=market, code=symbol, start=start, count=offset)

        return to_data(result, symbol=symbol, client=self, **kwargs)

    @_legacy_retry(
        wait=wait_random(min=1, max=10),
        stop=stop_after_attempt(3),
        retry_error_callback=return_last_value,
        retry=(retry_if_exception_type() | retry_if_result(check_empty)),
    )
    def transactions(self, market=None, symbol='', date='', start=0, offset=800, **kwargs):
        """
        查询历史分笔成交

        :param market:  市场ID
        :param symbol:  证券代码
        :param date:    查询日期
        :param start:   开始位置
        :param offset:  获取数量
        :return:
        """

        market, symbol = self.validate(market, symbol)
        result = self.client.get_history_transaction_data(
            market=market, code=symbol, date=int(date), start=start, count=offset
        )

        return to_data(result, symbol=symbol, client=self, **kwargs)
