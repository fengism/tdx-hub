from tdxhub.consts import BACKUP_EX_HOSTS, BACKUP_HQ_HOSTS, EX_HOSTS, HQ_HOSTS
from tdxhub.server import HOSTS


def _constant_endpoints(hosts):
    return [(host[1], host[2]) for host in hosts]


def _runtime_endpoints(hosts):
    return [(host["addr"], host["port"]) for host in hosts]


def _assert_unique(endpoints):
    assert len(endpoints) == len(set(endpoints))


def test_backup_quote_hosts_are_preserved():
    endpoints = _constant_endpoints(BACKUP_HQ_HOSTS)

    assert len(endpoints) == 79
    _assert_unique(endpoints)
    assert ("8.129.13.54", 7709) in endpoints  # 原 tdxhub 线路
    assert ("110.41.2.72", 7709) in endpoints  # gotdx 主站线路
    assert ("218.6.170.47", 7709) in endpoints  # gotdx 券商线路
    assert ("121.36.248.138", 7709) in endpoints  # gotdx Mac 线路


def test_backup_extended_hosts_are_preserved():
    endpoints = _constant_endpoints(BACKUP_EX_HOSTS)

    assert len(endpoints) == 21
    _assert_unique(endpoints)
    assert ("47.112.95.207", 7720) in endpoints  # 原 tdxhub 线路
    assert ("116.205.143.214", 7727) in endpoints  # gotdx 扩展线路
    assert ("116.205.135.205", 7727) in endpoints  # gotdx Mac 扩展线路


def test_active_quote_hosts_are_unique_and_filtered():
    endpoints = _constant_endpoints(HQ_HOSTS)

    assert len(endpoints) == 43
    _assert_unique(endpoints)
    assert ("117.34.114.13", 7709) in endpoints
    assert ("117.34.114.14", 7709) in endpoints
    assert ("117.34.114.20", 7709) in endpoints
    assert ("117.34.114.27", 7709) in endpoints
    assert ("124.71.187.122", 7709) in endpoints
    assert ("117.34.114.30", 7709) not in endpoints
    assert ("117.34.114.31", 7709) not in endpoints
    assert {(host[1], int(host[2])) for host in HQ_HOSTS} <= {(host[1], int(host[2])) for host in BACKUP_HQ_HOSTS}


def test_active_extended_hosts_are_unique_and_filtered():
    endpoints = _constant_endpoints(EX_HOSTS)

    assert len(endpoints) == 18
    _assert_unique(endpoints)
    assert ("112.74.214.43", 7727) in endpoints
    assert ("49.235.119.116", 7727) in endpoints
    assert ("116.205.143.214", 7727) in endpoints  # gotdx 扩展线路
    assert ("116.205.135.205", 7727) in endpoints  # gotdx Mac 扩展线路
    assert ("47.112.95.207", 7720) not in endpoints
    assert ("218.75.75.18", 7720) not in endpoints
    assert ("58.49.110.76", 7720) not in endpoints


def test_runtime_hosts_are_deduplicated_after_tdxpy_merge():
    for market in ("HQ", "EX", "GP"):
        _assert_unique(_runtime_endpoints(HOSTS[market]))
