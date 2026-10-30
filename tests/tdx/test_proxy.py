"""Proxy parsing and handshake behavior for the transport layer."""

import base64
import socket
import threading

import pytest

from tdxhub.tdx.errors import TdxConnectionError, ValidationException
from tdxhub.tdx.proxy import parse_proxy, proxy_description
from tdxhub.tdx.transport import BaseSocketClient


def test_parse_proxy_none():
    assert parse_proxy(None) is None


def test_parse_proxy_http_with_auth():
    proxy = parse_proxy("http://user:p%40ss@1.2.3.4:2260")
    assert proxy["kind"] == "http"
    assert proxy["host"] == "1.2.3.4"
    assert proxy["port"] == 2260
    assert proxy["username"] == "user"
    assert proxy["password"] == "p@ss"


def test_parse_proxy_socks5_default_port():
    proxy = parse_proxy("socks5://example.com")
    assert proxy["kind"] == "socks5"
    assert proxy["port"] == 1080
    assert proxy["username"] is None


def test_parse_proxy_rejects_unknown_scheme():
    with pytest.raises(ValidationException):
        parse_proxy("ftp://1.2.3.4:21")
    with pytest.raises(ValidationException):
        parse_proxy("")


def test_proxy_description_hides_credentials():
    proxy = parse_proxy("http://trade:admin1031@resin.example.com:2260")
    assert proxy_description(proxy) == "http://resin.example.com:2260"


def test_base_client_parses_proxy_at_construction():
    client = BaseSocketClient(proxy="http://user:pass@10.0.0.1:8080")
    assert client.proxy["kind"] == "http"
    assert client.proxy["username"] == "user"


@pytest.fixture
def http_proxy_server():
    """A minimal HTTP CONNECT proxy implemented as a socket server."""

    def _start(handler):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        server.settimeout(5)
        port = server.getsockname()[1]

        def serve():
            try:
                conn, _ = server.accept()
                handler(conn)
            finally:
                server.close()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        return port, thread

    yield _start


def test_http_connect_proxy_connects_with_auth(http_proxy_server):
    def handler(conn):
        request = b""
        while b"\r\n\r\n" not in request:
            request += conn.recv(4096)
        assert b"CONNECT 1.2.3.4:7709 HTTP/1.1" in request
        expected = "Basic " + base64.b64encode(b"user:pass").decode()
        assert b"Proxy-Authorization: Basic" in request
        assert expected.encode() in request
        conn.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
        conn.close()

    port, thread = http_proxy_server(handler)
    proxy = parse_proxy(f"http://user:pass@127.0.0.1:{port}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    from tdxhub.tdx.proxy import apply_proxy

    apply_proxy(sock, "1.2.3.4", 7709, proxy)
    sock.close()
    thread.join(5)


def test_http_connect_proxy_rejects_auth_failure(http_proxy_server):
    def handler(conn):
        while b"\r\n\r\n" not in conn.recv(65536):
            pass
        conn.sendall(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
        conn.close()

    port, _thread = http_proxy_server(handler)
    proxy = parse_proxy(f"http://127.0.0.1:{port}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    from tdxhub.tdx.proxy import apply_proxy

    with pytest.raises(TdxConnectionError):
        apply_proxy(sock, "1.2.3.4", 7709, proxy)
    sock.close()


def test_socks5_handshake_with_local_mock(http_proxy_server):
    def handler(conn):
        assert conn.recv(4096) == b"\x05\x02\x00\x02"
        conn.sendall(b"\x05\x02")
        assert conn.recv(4096) == b"\x01\x04user\x04pass"
        conn.sendall(b"\x01\x00")
        # 目标 CONNECT 请求: version 5, cmd 1, rsv 0, atyp 3, len 7, host, port 7709
        data = conn.recv(4096)
        assert data[:5] == b"\x05\x01\x00\x03\x07" and data[5:] == b"1.2.3.4" + (7709).to_bytes(2, "big")
        conn.sendall(b"\x05\x00\x00\x01\x00\x00\x00\x00" + (0).to_bytes(2, "big"))
        conn.close()

    port, thread = http_proxy_server(handler)
    proxy = parse_proxy(f"socks5://user:pass@127.0.0.1:{port}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    from tdxhub.tdx.proxy import apply_proxy

    apply_proxy(sock, "1.2.3.4", 7709, proxy)
    sock.close()
    thread.join(5)
