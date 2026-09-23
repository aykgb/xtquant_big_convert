from types import SimpleNamespace

import pytest

from bigqmt_signal_trader.adapters.order_bigqmt import BigQmtOrderGateway
from bigqmt_signal_trader.adapters.position_bigqmt import BigQmtPositionProvider
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers
from bigqmt_signal_trader.xtquant_compat import BigQmtXtTrader, StockAccount


@pytest.mark.parametrize("kind", ["HUGANGTONG", "SHENGANGTONG"])
def test_query_routes_are_request_scoped_and_leave_default_account_unchanged(kind):
    calls = []

    def query(account_id, account_type, detail_type, *args):
        calls.append((account_type, detail_type))
        if detail_type == "ACCOUNT":
            return [SimpleNamespace(m_dAvailable=100, m_dBalance=100, m_dFrozenCash=0)]
        return []

    positions = BigQmtPositionProvider(query)
    orders = BigQmtOrderGateway(None, get_trade_detail_data_func=query)
    handlers = BigQmtRpcHandlers("test", None, positions, orders)
    # "test" is in no BIGQMT_ACCOUNT_TYPE list: a 港股通 type is still answered
    # as itself, never from the STOCK book.
    params = {"account_type": kind}
    handlers.handle("get_asset", params)
    handlers.handle("get_positions", params)
    handlers.handle("query_orders", params)
    handlers.handle("query_trades", params)
    assert calls == [(kind, "ACCOUNT"), (kind, "POSITION"), (kind, "ORDER"), (kind, "DEAL")]
    handlers.handle("get_asset", {})
    assert calls[-1] == ("STOCK", "ACCOUNT")
    assert positions.account_type == orders.account_type == "STOCK"


@pytest.mark.parametrize("kind", ["HUGANGTONG", "SHENGANGTONG"])
def test_shim_transmits_the_explicit_connect_account_type(kind):
    assert BigQmtXtTrader._connect_params(StockAccount("test", kind)) == {"account_type": kind}
    assert BigQmtXtTrader._connect_params(StockAccount("test", "STOCK")) == {}


def test_empty_connect_asset_is_unknown_not_zero():
    trader = object.__new__(BigQmtXtTrader)
    trader.client = SimpleNamespace(account_id="test", call=lambda *args, **kwargs: {})
    with pytest.raises(RuntimeError, match="unavailable"):
        trader.query_stock_asset(StockAccount("test", "HUGANGTONG"))


def test_connect_cancel_passes_account_type_to_native_qmt():
    calls = []
    gateway = BigQmtOrderGateway(None, cancel_func=lambda *args: calls.append(args) or True,
                                get_trade_detail_data_func=lambda *args: [])
    handlers = BigQmtRpcHandlers("test", None, None, gateway, allow_order_methods=True)
    handlers.handle("cancel_order", {"order_sys_id": "broker-1", "account_type": "SHENGANGTONG"})
    assert calls[0][0:3] == ("broker-1", "test", "SHENGANGTONG")


def test_bridge_status_declares_account_type_routing():
    handlers = BigQmtRpcHandlers("test", None, None, None)
    assert handlers.handle("get_bridge_status", {})["account_type_routing"] is True


def test_connect_cancel_never_settles_from_the_sysid_watch_table():
    # The watch table is keyed by 合同编号 alone; a STOCK order may share it.
    seen = []
    gateway = BigQmtOrderGateway(None, cancel_func=lambda *args: True,
                                get_trade_detail_data_func=lambda *args: [])
    handlers = BigQmtRpcHandlers("test", None, None, gateway, allow_order_methods=True)
    handlers.order_watch_table = SimpleNamespace(
        status_for_sysid=lambda sysid: seen.append(sysid) or 54)
    handlers.handle("cancel_order", {"order_sys_id": "broker-1", "account_type": "HUGANGTONG"})
    assert seen == []
