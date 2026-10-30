import datetime
import functools
import socket
import threading
import time

import pandas as pd

from tdxhub.tdx.deadline import socket_budget
from tdxhub.tdx.errors import TdxConnectionError, TdxFunctionCallError, ValidationException
from tdxhub.tdx.heartbeat import HeartBeatThread
from tdxhub.tdx.logger import logger
from tdxhub.tdx.protocol.raw_parser import RawParser
from tdxhub.tdx.proxy import apply_proxy, parse_proxy, proxy_description

DEFAULT_HEARTBEAT_INTERVAL = 10.0

CONNECT_TIMEOUT = 5.000
RECV_HEADER_LEN = 0x10


def last_ack_time(func):
    """
    装饰器: 更新最后 ack 时间
    :param func:
    :return:
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kw):
        ret = None

        try:
            ret = func(self, *args, **kw)
            self.last_ack_time = time.monotonic()
        except (TypeError, ValueError) as e:
            raise ValidationException(*e.args) from e
        except Exception as e:
            # logger.exception(e)
            current_exception = e
            logger.debug(f"hit exception on req exception is {e}")

            if self.auto_retry and self.client:
                for time_interval in self.retry_strategy.generate():
                    try:
                        time.sleep(time_interval)

                        self.disconnect()
                        self.connect(self.ip, self.port, time_out=self._connect_timeout)

                        ret = func(self, *args, **kw)
                        self.last_ack_time = time.monotonic()
                        return ret

                    except Exception as retry_e:
                        logger.debug(f"hit exception on *retry* req exception is {retry_e}")
                        current_exception = retry_e

                logger.debug("perform auto retry on req ")

            # 最后交易失败
            # self last_transaction_failed is True

            # 如果 raise_exception=True 抛出异常
            if self.raise_exception:
                to_raise = TdxFunctionCallError(f"{func.__name__}: {current_exception}")
                to_raise.original_exception = current_exception
                raise to_raise from current_exception

        return ret

    return wrapper


class RetryStrategy:
    @classmethod
    def generate(cls):
        raise NotImplementedError("need to override")


class DefaultRetryStrategy(RetryStrategy):
    """
    默认的重试策略，您可以通过写自己的重试策略替代本策略, 改策略主要实现 generate 方法，该方法是一个生成器，
    返回下次重试的间隔时间, 单位为秒，我们会使用 time.sleep在这里同步等待之后进行重新connect,然后再重新发起
    源请求，直到 generate 结束。
    """

    @classmethod
    def generate(cls):
        yield from [0.1, 0.5, 1, 2]


class TrafficStatSocket(socket.socket):
    """
    实现支持流量统计的socket类
    """

    def __init__(self, sock, mode):
        super().__init__(sock, mode)

        # 流量统计相关
        self.send_pkg_num = 0  # 发送次数
        self.recv_pkg_num = 0  # 接收次数
        self.send_pkg_bytes = 0  # 发送字节
        self.recv_pkg_bytes = 0  # 接收字节数
        self.first_pkg_send_time = None  # 第一个数据包发送时间

        self.last_api_send_bytes = 0  # 最近的一次api调用的发送字节数
        self.last_api_recv_bytes = 0  # 最近一次api调用的接收字节数


class BaseSocketClient:
    def __init__(self, multithread=False, heartbeat=False, auto_retry=False, raise_exception=False, proxy=None):
        """
        构造函数
        :param multithread: 是否多线程
        :param heartbeat: 是否心跳
        :param auto_retry: 是否自动回复
        :param raise_exception: 是否抛出异常
        :param proxy: 代理地址，形如 http://user:pass@host:port 或 socks5://user:pass@host:port
        """

        self.need_setup = True
        self.closed = True
        self.client = None

        self.lock = (multithread or heartbeat) and threading.Lock() or None

        self.heartbeat = heartbeat
        self.heartbeat_thread = None

        self.stop_event = None
        self.heartbeat_interval = DEFAULT_HEARTBEAT_INTERVAL  # 默认10秒一个心跳包

        self.last_ack_time = time.monotonic()
        # self last_transaction_failed = False

        self.ip, self.port = None, None
        self._connect_timeout = CONNECT_TIMEOUT
        self.heartbeat_runner = None
        self.heartbeat_error_callback = None

        # 是否重试
        self.auto_retry = auto_retry

        # 可以覆盖这个属性，使用新的重试策略
        self.retry_strategy = DefaultRetryStrategy()

        # 是否在函数调用出错的时候抛出异常
        self.raise_exception = raise_exception

        # 代理配置（已解析为 dict），None 表示直连
        self.proxy = parse_proxy(proxy)

    def connect(self, ip: str = None, port: int = 7709, time_out=CONNECT_TIMEOUT, bind_port=None, bind_ip="0.0.0.0"):
        """
        连接服务器

        :param ip:  服务器ip 地址
        :param port:  服务器端口
        :param time_out: 连接超时时间
        :param bind_port: 绑定的本地端口
        :param bind_ip: 绑定的本地ip
        :return: 是否连接成功 True/False
        """

        if not ip:
            raise ValidationException("IP Address bad.")

        self._connect_timeout = time_out
        self.client = TrafficStatSocket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.settimeout(time_out)

        logger.debug(f"connecting to server: {ip} on port: {port}")

        try:
            bind_port and self.client.bind((bind_ip, bind_port))

            self.ip, self.port = ip, port
            with socket_budget(self.client):
                if self.proxy:
                    logger.debug(f"connecting to server: {ip} on port: {port} via proxy: {proxy_description(self.proxy)}")
                    apply_proxy(self.client, ip, port, self.proxy, time_out)
                else:
                    self.client.connect((ip, port))
        except TimeoutError as e:
            logger.debug("connection expired")
            self.client.close()
            self.client = None

            if self.raise_exception:
                raise TdxConnectionError(f"connection timeout error: {e}") from e

            return False
        except Exception as e:  # noqa
            logger.debug(f"connection {e}")
            self.client.close()
            self.client = None
            if self.raise_exception:
                raise TdxConnectionError(f"connection error: {e}") from e

            return False

        self.closed = False
        logger.debug("connected!")

        try:
            if self.need_setup:
                self.setup()
        except Exception:
            self.disconnect()
            raise

        # 启动心跳包
        if self.heartbeat:
            self.stop_event = threading.Event()
            self.heartbeat_thread = HeartBeatThread(
                client=self, stop_event=self.stop_event, interval=self.heartbeat_interval
            )
            self.heartbeat_thread.start()

        return self

    def disconnect(self):
        """
        断开连接
        """

        # 停止心跳事件
        if self.stop_event is not None:
            self.stop_event.set()
        self.closed = True

        if self.client:
            logger.debug("disconnecting")

            try:
                try:
                    self.client.shutdown(socket.SHUT_RDWR)
                finally:
                    self.client.close()
            except Exception as e:
                logger.debug(f"disconnect err: {e}")
                if self.raise_exception:
                    raise TdxConnectionError(f"disconnect err: {e}") from e
            finally:
                self.client = None

            logger.debug("disconnected")

    def close(self):
        """
        disconnect的别名，为了支持 with closing(obj): 语法
        :return:
        """
        self.disconnect()

    def get_traffic_stats(self):
        """
        获取流量统计的信息
        :return:
        """
        if self.client.first_pkg_send_time is not None:
            total_seconds = (datetime.datetime.now() - self.client.first_pkg_send_time).total_seconds()

            if total_seconds != 0:
                send_bytes_per_second = self.client.send_pkg_bytes // total_seconds
                recv_bytes_per_second = self.client.recv_pkg_bytes // total_seconds
            else:
                send_bytes_per_second = None
                recv_bytes_per_second = None
        else:
            total_seconds = None
            send_bytes_per_second = None
            recv_bytes_per_second = None

        return {
            "send_pkg_num": self.client.send_pkg_num,
            "recv_pkg_num": self.client.recv_pkg_num,
            "send_pkg_bytes": self.client.send_pkg_bytes,
            "recv_pkg_bytes": self.client.recv_pkg_bytes,
            "first_pkg_send_time": self.client.first_pkg_send_time,
            "total_seconds": total_seconds,
            "send_bytes_per_second": send_bytes_per_second,
            "recv_bytes_per_second": recv_bytes_per_second,
            "last_api_send_bytes": self.client.last_api_send_bytes,
            "last_api_recv_bytes": self.client.last_api_recv_bytes,
        }

    def send_raw_pkg(self, pkg):
        """
        发送原始数据包
        # for debuging and testing protocol

        :param pkg:
        :return:
        """
        cmd = RawParser(self.client, lock=self.lock)
        cmd.setParams(pkg)

        return cmd.call_api()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def to_df(v):
        """
        数据转换 df 格式
        :param v:
        :return:
        """

        v = v or ""

        if not v:
            return pd.DataFrame(data=None)

        if isinstance(v, list):
            return pd.DataFrame(data=v)

        if isinstance(v, dict):
            return pd.DataFrame(data=[v])

        return pd.DataFrame(data=[{"value": v}])

    def setup(self):
        """
        方法
        """
        pass
