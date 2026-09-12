# coding: utf-8
"""握手与分通道健康（迁移计划 §6.2、§6.3、§6.7）。

一个 `connected` 布尔值说不清桥接的状态：RPC 通、行情断，和两者都断，对下游
是完全不同的处置。所以服务端在握手里交出协议版本、脱敏账号、实际 transport、
分通道支持和**启动代次**；客户端在这几项对不上时 fail-closed，并按代次判断
"服务端是不是重启过"——重启过则订阅和游标都不再代表原来的意思。

代次挂在 handlers 上而不是 service 上：多账号部署会用同一套 handlers 建好几个
service，它们一起重启，代次也该是同一个。
"""
import os
import sys
import threading
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))
os.environ.setdefault("BIGQMT_LOG_NAME", "bigqmt-test-handshake")

from bigqmt_signal_trader.redis_rpc import (  # noqa: E402
    READ_METHODS,
    RPC_PROTOCOL_VERSION,
    BigQmtRpcHandlers,
)
from bigqmt_signal_trader.version import __version__ as LOCAL_VERSION  # noqa: E402
from bigqmt_signal_trader.whole_quote_session import (  # noqa: E402
    WholeQuoteClientSession,
)
from bigqmt_signal_trader.xtquant_compat import (  # noqa: E402
    BigQmtRpcClient,
    BigQmtXtData,
    RpcCompatibilityError,
)

ACCOUNT = "12345678910"
MASKED = "*******8910"


def _handlers(**kwargs):
    return BigQmtRpcHandlers(
        account_id=ACCOUNT, market_data=None, position_provider=None, **kwargs)


class ServerContractTest(unittest.TestCase):
    def test_ping_carries_what_the_client_fail_closes_on(self):
        info = _handlers()._handle_ping({})

        self.assertEqual(info["protocol_version"], RPC_PROTOCOL_VERSION)
        self.assertEqual(info["account_id_masked"], MASKED)
        self.assertTrue(info["server_generation"])
        self.assertTrue(info["supports"]["rpc"])
        self.assertEqual(info["version"], LOCAL_VERSION)

    def test_the_account_is_masked_everywhere_the_contract_appears(self):
        info = _handlers()._handle_get_bridge_status({})

        self.assertEqual(info["account_id_masked"], MASKED)
        self.assertNotIn(ACCOUNT, str(info))

    def test_bridge_status_is_a_read_method(self):
        """状态查询不能排到 adjust 主线程后面——桥接卡住时它正是要看的那个。"""
        self.assertIn("get_bridge_status", READ_METHODS)

    def test_the_contract_names_the_settlement_budget(self):
        """客户端要把自己的 RPC 预算排在结算等待之上（§8），得先知道它多长。"""
        info = _handlers(order_settle_timeout_seconds=3.0)._handle_ping({})

        self.assertEqual(info["order_settle_timeout_seconds"], 3.0)
        self.assertFalse(info["settle_orders_inline"])

    def test_a_generation_is_stable_within_one_process(self):
        handlers = _handlers()

        self.assertEqual(handlers._handle_ping({})["server_generation"],
                         handlers._handle_ping({})["server_generation"])
        self.assertNotEqual(handlers._handle_ping({})["server_generation"],
                            _handlers()._handle_ping({})["server_generation"])

    def test_the_contract_answers_for_the_asked_account_not_the_backref(self):
        """多账号部署里每个 secondary service 都会覆盖 rpc_service 反向引用。"""
        handlers = _handlers()
        handlers.rpc_service = _StubService()

        info = handlers._bridge_contract("99999999")

        self.assertEqual(info["account_id_masked"], "****9999")
        self.assertEqual(info["transport"]["name"], "redis")


class _StubService(object):
    def contract_status(self):
        return {
            "protocol_version": RPC_PROTOCOL_VERSION,
            "transport": {"name": "redis", "db": 5},
            "supports": {"rpc": True, "quote": True, "trade_events": True,
                         "position_events": True},
            "server_generation": "gen-1",
            "server_started_at": "2026-09-12 10:00:00",
        }


def _client(answers=None):
    client = BigQmtRpcClient(account_id=ACCOUNT, redis_client=object())
    client._answers = list(answers or [])
    client.calls = []

    def call(method, params=None, **kwargs):
        client.calls.append(method)
        return client._answers.pop(0) if client._answers else {}

    client.call = call
    return client


def _pong(**overrides):
    info = {
        "pong": True,
        "version": LOCAL_VERSION,
        "protocol_version": RPC_PROTOCOL_VERSION,
        "account_id_masked": MASKED,
        "server_generation": "gen-1",
        "supports": {"rpc": True},
    }
    info.update(overrides)
    return info


class ClientHandshakeTest(unittest.TestCase):
    def test_a_matching_bridge_is_accepted(self):
        client = _client([_pong()])

        self.assertTrue(client.handshake()["pong"])
        self.assertTrue(client.get_status()["rpc_ready"])

    def test_a_protocol_mismatch_fails_closed(self):
        client = _client([_pong(protocol_version="0.9")])

        self.assertRaises(RpcCompatibilityError, client.handshake)
        self.assertFalse(client.get_status()["rpc_ready"])

    def test_a_version_mismatch_fails_closed(self):
        client = _client([_pong(version="9.9.0")])

        self.assertRaises(RpcCompatibilityError, client.handshake)

    def test_another_account_fails_closed(self):
        client = _client([_pong(account_id_masked="****0000")])

        self.assertRaises(RpcCompatibilityError, client.handshake)

    def test_a_bridge_without_a_generation_fails_closed(self):
        """没有代次就没法发现重启，订阅和游标的意义无从判断。"""
        client = _client([_pong(server_generation="")])

        self.assertRaises(RpcCompatibilityError, client.handshake)

    def test_a_restart_shows_up_as_a_new_connection_generation(self):
        client = _client([_pong(), _pong(), _pong(server_generation="gen-2")])

        client.handshake()
        first = client.get_status()["rpc_generation"]
        client.handshake()
        self.assertEqual(client.get_status()["rpc_generation"], first)

        client.handshake()
        self.assertGreater(client.get_status()["rpc_generation"], first)

    def test_a_stale_success_is_not_ready(self):
        """心跳停了就不算通——上次成功过不是现在还通着（P1-1 那 21 小时）。"""
        client = _client([_pong()])
        client.handshake()
        client._last_rpc_success_at = time.time() - 3600

        status = client.get_status()

        self.assertFalse(status["rpc_ready"])
        self.assertGreater(status["rpc_age_seconds"], 60)


class ClientLifecycleTest(unittest.TestCase):
    def test_stop_stops_the_health_thread_and_is_idempotent(self):
        client = _client([_pong(), _pong(), _pong()])
        client.health_interval_seconds = 0.05
        client.start_health_monitor()
        self.assertTrue(client.get_status()["health_monitor_running"])

        client.stop()
        client.stop()

        self.assertFalse(client.get_status()["health_monitor_running"])
        self.assertNotIn("bigqmt-rpc-health",
                         [t.name for t in threading.enumerate()])

    def test_xtdata_stop_leaves_the_shared_client_alive(self):
        """configure() 把同一个 client 交给 xtdata 和 xt_trader：
        xtdata.stop() 要是把它关了，正在反订阅的交易会话就断在半路。"""
        client = _client([_pong(), _pong()])
        client.start_health_monitor()
        data = BigQmtXtData(client)

        data.stop()

        self.assertTrue(client.get_status()["health_monitor_running"])
        client.stop()


class _StubChannel(object):
    def __init__(self):
        self.topics = None

    def start_subscriber(self, topics, callback):
        self.topics = list(topics)

    def stop(self):
        self.topics = None


class QuoteChannelHealthTest(unittest.TestCase):
    def _session(self, rpc=None):
        return WholeQuoteClientSession(
            rpc or (lambda method, params: {"topic": "T"}),
            _StubChannel(), client_id="c-1", heartbeat_interval_seconds=60.0)

    def test_a_fresh_subscribe_is_ready_without_waiting_for_a_heartbeat(self):
        session = self._session()
        session.start()
        session.subscribe_whole_quote(["600000.SH"])

        status = session.status()

        self.assertTrue(status["quote_ready"])
        self.assertEqual(status["subscriptions"], 1)
        session.stop(unsubscribe=False)

    def test_nothing_subscribed_is_not_ready(self):
        session = self._session()
        session.start()

        self.assertFalse(session.status()["quote_ready"])
        session.stop(unsubscribe=False)

    def test_a_stale_heartbeat_is_not_ready(self):
        session = self._session()
        session.start()
        session.subscribe_whole_quote(["600000.SH"])
        session._last_heartbeat_time = time.monotonic() - 3600

        self.assertFalse(session.status()["quote_ready"])
        session.stop(unsubscribe=False)

    def test_a_resubscribe_round_bumps_the_generation(self):
        session = self._session()
        session.start()
        session.subscribe_whole_quote(["600000.SH"])
        before = session.status()["generation"]

        session.replay_subscriptions()

        self.assertGreater(session.status()["generation"], before)
        session.stop(unsubscribe=False)

    def test_stop_unsubscribes_what_it_opened(self):
        seen = []

        def rpc(method, params):
            seen.append(method)
            return {"topic": "T"}

        session = self._session(rpc)
        session.start()
        session.subscribe_whole_quote(["600000.SH"])
        session.stop()

        self.assertIn("unsubscribe_whole_quote", seen)
        self.assertEqual(session.status()["subscriptions"], 0)


if __name__ == "__main__":
    unittest.main()
