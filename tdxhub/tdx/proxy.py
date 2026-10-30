import base64
import struct
from urllib.parse import unquote, urlsplit

from tdxhub.tdx.errors import TdxConnectionError, ValidationException

_PROXY_KIND_HEADERS = {
    "http": "http",
    "https": "http",
    "socks5": "socks5",
    "socks5h": "socks5",
}


def parse_proxy(proxy):
    """解析代理配置字符串，返回 dict(kind/host/port/username/password)。

    支持格式: `http://host:port`、`http://user:pass@host:port`、`socks5://...`。
    传入 None 返回 None，表示不使用代理。
    """

    if proxy is None:
        return None

    if not isinstance(proxy, str) or not proxy.strip():
        raise ValidationException("proxy 必须是形如 http://user:pass@host:port 的字符串")

    parsed = urlsplit(proxy.strip())
    kind = _PROXY_KIND_HEADERS.get(parsed.scheme.lower())
    if kind is None:
        raise ValidationException(f"不支持的代理协议: {parsed.scheme!r}，仅支持 http/https/socks5")

    host = parsed.hostname
    if not host:
        raise ValidationException("proxy 缺少主机地址")

    if parsed.port is not None:
        port = parsed.port
    elif parsed.scheme.lower().startswith("https"):
        port = 443
    elif parsed.scheme.lower().startswith("socks"):
        port = 1080
    else:
        port = 80

    username = unquote(parsed.username) if parsed.username else None
    password = unquote(parsed.password) if parsed.password else None

    return {"kind": kind, "host": host, "port": port, "username": username, "password": password}


def proxy_description(proxy):
    """返回可安全展示的代理描述，隐藏账号密码。"""

    if not proxy:
        return None
    label = f"{proxy['host']}:{proxy['port']}"
    return f"{proxy['kind']}://{label}"


def _recv_exact(sock, size):
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise TdxConnectionError("代理连接在握手期间被对端关闭")
        chunks.extend(chunk)
    return bytes(chunks)


def _socks5_handshake(sock, target_host, target_port, proxy):
    username = proxy.get("username")
    password = proxy.get("password")
    need_auth = username is not None or password is not None

    greeting = b"\x05\x02\x00\x02" if need_auth else b"\x05\x01\x00"
    sock.sendall(greeting)
    reply = _recv_exact(sock, 2)
    if reply[0] != 0x05:
        raise TdxConnectionError(f"代理 SOCKS5 握手失败: 非法响应版本 {reply[0]!r}")
    method = reply[1]
    if method == 0x02:
        if not need_auth:
            raise TdxConnectionError("代理要求账号认证，但未提供用户名/密码")
        user = (username or "").encode()
        pwd = (password or "").encode()
        sock.sendall(b"\x01" + bytes([len(user)]) + user + bytes([len(pwd)]) + pwd)
        auth_reply = _recv_exact(sock, 2)
        if auth_reply[1] != 0x00:
            raise TdxConnectionError("代理 SOCKS5 认证失败: 用户名或密码错误")
    elif method not in (0x00,) and not (method == 0x02 and need_auth):
        raise TdxConnectionError(f"代理 SOCKS5 握手失败: 不支持的认证方式 0x{method:02x}")

    host_bytes = target_host.encode()
    header = b"\x05\x01\x00\x03" + bytes([len(host_bytes)]) + host_bytes
    sock.sendall(header + struct.pack(">H", target_port))

    reply = _recv_exact(sock, 4)
    if reply[0] != 0x05:
        raise TdxConnectionError(f"代理 SOCKS5 握手失败: 非法响应版本 {reply[0]!r}")
    if reply[1] != 0x00:
        reasons = {
            0x01: "一般失败",
            0x02: "规则不允许",
            0x03: "网络不可达",
            0x04: "目标主机不可达",
            0x05: "连接被拒绝",
            0x06: "TTL 超时",
            0x07: "不支持的命令",
            0x08: "不支持的地址类型",
        }
        raise TdxConnectionError(f"代理 SOCKS5 连接目标失败: {reasons.get(reply[1], f'错误码 {reply[1]}')}")

    address_type = reply[3]
    if address_type == 0x01:
        _recv_exact(sock, 4 + 2)
    elif address_type == 0x04:
        _recv_exact(sock, 16 + 2)
    elif address_type == 0x03:
        domain_len = _recv_exact(sock, 1)[0]
        _recv_exact(sock, domain_len + 2)
    else:
        raise TdxConnectionError(f"代理 SOCKS5 握手失败: 未知地址类型 {address_type}")


def _http_connect_handshake(sock, target_host, target_port, proxy):
    headers = [f"CONNECT {target_host}:{target_port} HTTP/1.1", f"Host: {target_host}:{target_port}"]
    username = proxy.get("username")
    password = proxy.get("password")
    if username is not None or password is not None:
        token = base64.b64encode(f"{username or ''}:{password or ''}".encode()).decode()
        headers.append(f"Proxy-Authorization: Basic {token}")
    headers.extend(["Proxy-Connection: keep-alive", ""])
    sock.sendall("\r\n".join(headers).encode() + b"\r\n")

    buffer = bytearray()
    while b"\r\n\r\n" not in buffer:
        chunk = sock.recv(4096)
        if not chunk:
            raise TdxConnectionError("代理连接在 HTTP CONNECT 握手期间被对端关闭")
        buffer.extend(chunk)
        if len(buffer) > 64 * 1024:
            raise TdxConnectionError("代理 HTTP CONNECT 响应头过大")

    head, _, _rest = bytes(buffer).partition(b"\r\n\r\n")
    status_line = head.split(b"\r\n", 1)[0].decode("ascii", "replace").strip()
    try:
        status = int(status_line.split()[1])
    except (IndexError, ValueError):
        raise TdxConnectionError(f"代理 HTTP CONNECT 响应异常: {status_line!r}") from None
    if not 200 <= status < 300:
        raise TdxConnectionError(f"代理 HTTP CONNECT 被拒绝: HTTP {status}")


def apply_proxy(sock, host, port, proxy, timeout=None):
    """在已创建的 TCP socket 上，通过代理与目标 (host, port) 建立会话。

    sock 在调用前无需 connect；本函数负责连接代理完成握手。
    """

    if timeout is not None:
        sock.settimeout(timeout)
    try:
        sock.connect((proxy["host"], proxy["port"]))
        if proxy["kind"] == "socks5":
            _socks5_handshake(sock, host, port, proxy)
        else:
            _http_connect_handshake(sock, host, port, proxy)
    except TimeoutError as exc:
        raise TdxConnectionError(f"代理握手超时: {proxy_description(proxy)}") from exc
    except OSError as exc:
        raise TdxConnectionError(f"代理连接失败: {proxy_description(proxy)}: {exc}") from exc
