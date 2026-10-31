from collections import deque

import pytest

from tdxhub.quotes import StdQuotes
from tdxhub.tdx.errors import TdxFunctionCallError, ValidationException


class BatchApi:
    def __init__(self, responses=()):
        self.calls = []
        self.responses = deque(responses)

    def get_security_quotes(self, symbols):
        self.calls.append(list(symbols))
        if self.responses:
            result = self.responses.popleft()
            if isinstance(result, Exception):
                raise result
            return result
        return [{"market": market, "code": code, "price": 1, "vol": 1} for market, code in reversed(symbols)]


def quotes_with(api):
    quotes = object.__new__(StdQuotes)
    quotes.client = api
    quotes._round2_quotes_required = False
    quotes._round2_client = None
    return quotes


def test_batch_default_boundary_order_and_stable_normalized_dedup():
    api = BatchApi()
    codes = [f"600{i:03d}" for i in range(161)]
    data = quotes_with(api).quotes(codes + ["SH.600000"], diagnostics=True)
    assert [len(call) for call in api.calls] == [80, 80, 1]
    assert list(data.code) == codes
    assert data.attrs["diagnostics"]["status"] == "ok"
    assert data.attrs["diagnostics"]["duplicates_removed"] == 1
    assert data.attrs["diagnostics"]["missing"] == []


def test_missing_is_market_qualified_and_not_a_protocol_error():
    api = BatchApi([[{"market": 1, "code": "000001", "price": 1}]])
    data = quotes_with(api).quotes(["SH.000001", "SZ.000001"], diagnostics=True)
    assert data.attrs["diagnostics"]["missing"] == [(0, "000001")]
    assert data.attrs["diagnostics"]["status"] == "missing"
    assert len(data) == 1


def test_unexpected_and_duplicate_response_rows_do_not_pollute_result():
    api = BatchApi(
        [[{"market": 2, "code": "920001"}, {"market": 2, "code": "920001"}, {"market": 1, "code": "600000"}]]
    )
    data = quotes_with(api).quotes("920001", diagnostics=True)
    assert len(data) == 1
    assert data.attrs["diagnostics"]["unexpected"] == [(1, "600000")]


@pytest.mark.parametrize("batch_size", [0, -1, 81, 1.5, True])
def test_invalid_batch_size_fails_before_io(batch_size):
    api = BatchApi()
    with pytest.raises(ValueError, match="batch_size"):
        quotes_with(api).quotes("600000", batch_size=batch_size)
    assert api.calls == []


@pytest.mark.parametrize("symbol", [None, "", [], ()])
def test_empty_input_diagnostics_without_io(symbol):
    api = BatchApi()
    data = quotes_with(api).quotes(symbol, diagnostics=True)
    assert data.empty and data.attrs["diagnostics"]["status"] == "empty_input"
    assert api.calls == []


def test_all_inputs_validated_before_first_batch():
    api = BatchApi()
    with pytest.raises(ValueError):
        quotes_with(api).quotes(["600000", "1234567"], batch_size=1)
    assert api.calls == []


def test_exception_propagates_without_returning_earlier_partial_batch():
    api = BatchApi([[{"market": 1, "code": "600000"}], TdxFunctionCallError("bad packet")])
    with pytest.raises(TdxFunctionCallError, match="bad packet"):
        quotes_with(api).quotes(["600000", "920001"], batch_size=1)


def test_nonraising_request_failure_is_distinct_from_missing():
    api = BatchApi([[{"market": 1, "code": "600000"}], None])
    data = quotes_with(api).quotes(["600000", "920001"], batch_size=1, diagnostics=True)
    assert data.empty
    assert data.attrs["diagnostics"]["status"] == "request_failed"
    assert data.attrs["diagnostics"]["missing"] == []
    assert data.attrs["diagnostics"]["failed_batch"] == [(2, "920001")]


def test_client_validation_keeps_empty_return_with_diagnostic_reason():
    data = quotes_with(BatchApi([ValidationException("bad")])).quotes("600000", diagnostics=True)
    assert data.empty
    assert data.attrs["diagnostics"]["status"] == "invalid_input"


def test_no_diagnostic_metadata_by_default():
    assert quotes_with(BatchApi()).quotes("600000").attrs == {}
