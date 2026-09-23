# coding: utf-8
"""带 client_request_id 的委托只落一次柜台，结果必须明确（迁移计划 §6.4、§8）。

`request_id` 那层去重（#245）只活在进程内存里：桥接策略一重启就没了，
而重启恰恰是最可能重发的时候。所以真正的幂等键落在 redis 上，按交易日分桶，
记录里带请求体摘要——同 ID 同请求体返回原结果，同 ID 不同请求体报冲突。

结果三分法是这套东西存在的理由，不能靠解析错误文本得到：

- `rejected`：能证明 `passorder` 之前就失败了，允许换个 request_id 重来；
- `accepted`：柜台受理，保存规范委托号；
- `ambiguous`：证明不了原生调用没发生——一律不自动重发，交给人。

判据是适配层的 `_last_submit_attempted`：它在调 `passorder` 前一行才置 True。
"""
import collections
import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))
os.environ.setdefault("BIGQMT_LOG_NAME", "bigqmt-test-idempotency")

from bigqmt_signal_trader.adapters.order_bigqmt import BigQmtOrderGateway  # noqa: E402
from bigqmt_signal_trader.models import OrderRequest  # noqa: E402
from bigqmt_signal_trader.redis_rpc import RedisPubSubRpcService  # noqa: E402

ACCOUNT = "acct"


class HashRedis(object):
    """幂等记录只用到 get/set(nx)/hget/hset/hsetnx/hlen/expire。"""

    def __init__(self):
        self.kv = {}
        self.hashes = {}

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True

    def hget(self, key, field):
        return (self.hashes.get(key) or {}).get(field)

    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value
        return 1

    def hsetnx(self, key, field, value):
        bucket = self.hashes.setdefault(key, {})
        if field in bucket:
            return 0
        bucket[field] = value
        return 1

    def hlen(self, key):
        return len(self.hashes.get(key) or {})

    def expire(self, key, ttl):
        return True


class _Handlers(object):
    """记参数，不碰柜台。raises/dispatch_state 由用例摆布。"""

    def __init__(self, raises=None, dispatch_state="submitted"):
        self.dispatched = []
        self._last_server_error = ""
        self._order_dispatch_state = "idle"
        self._raises = raises
        self._dispatch_state = dispatch_state

    def _canonical_method(self, method):
        return {"order_stock": "submit_order",
                "cancel_order_stock": "cancel_order"}.get(method, method)

    def handle(self, method, params=None):
        self.dispatched.append((self._canonical_method(method), dict(params or {})))
        if self._raises is not None:
            self._order_dispatch_state = self._dispatch_state
            raise self._raises
        return {"order_sys_id": "1001"}

    def take_pending_settlement(self):
        return None


def _service(handlers=None, redis_client=None):
    service = RedisPubSubRpcService.__new__(RedisPubSubRpcService)
    service.handlers = handlers or _Handlers()
    service.account_id = ACCOUNT
    service.print_prefix = "[test]"
    service.debug_log_limit = 0
    service._processed_count = 0
    service._deferred_count = 0
    service._order_requests = collections.OrderedDict()
    service._duplicate_order_requests = 0
    service.redis = redis_client if redis_client is not None else HashRedis()
    service.published = []
    service._publish_response = lambda request, response: \
        service.published.append(response)
    return service


def _request(request_id, client_request_id="rule:e1", **params):
    params.setdefault("stock_code", "600000.SH")
    params.setdefault("volume", 100)
    return {"schema_version": 1, "request_id": request_id,
            "account_id": ACCOUNT, "method": "order_stock",
            "client_request_id": client_request_id, "params": params}


class KnownResultTest(unittest.TestCase):
    def test_the_same_id_and_body_never_reaches_passorder_twice(self):
        service = _service()

        first = service.process_request(_request("rpc-1"))
        second = service.process_request(_request("rpc-2"))

        self.assertEqual(len(service.handlers.dispatched), 1)
        self.assertEqual(first["order_outcome"], "accepted")
        self.assertTrue(second["idempotency_replayed"])
        self.assertEqual(second["data"], first["data"])
        # 重放答的是这一次的 request_id，否则落不到客户端等应答的那个键上。
        self.assertEqual(second["request_id"], "rpc-2")

    def test_the_known_result_survives_a_bridge_restart(self):
        redis_client = HashRedis()
        first = _service(redis_client=redis_client).process_request(
            _request("rpc-1"))

        restarted = _service(redis_client=redis_client)
        replay = restarted.process_request(_request("rpc-2"))

        self.assertEqual(restarted.handlers.dispatched, [])
        self.assertTrue(replay["idempotency_replayed"])
        self.assertEqual(replay["data"], first["data"])

    def test_a_different_body_under_the_same_id_is_a_conflict(self):
        service = _service()
        service.process_request(_request("rpc-1", volume=100))

        conflict = service.process_request(_request("rpc-2", volume=200))

        self.assertEqual(len(service.handlers.dispatched), 1)
        self.assertEqual(conflict["order_outcome"], "conflict")
        self.assertEqual(conflict["error_code"], "CLIENT_REQUEST_ID_CONFLICT")

    def test_a_request_id_from_another_trading_day_is_a_conflict(self):
        """跨交易日复用既不是重复委托也不是新请求——不猜，报冲突。"""
        service = _service()
        _daily, marker, _field = service._idempotency_keys(
            ACCOUNT, "rule:e1", "20260101")
        service.redis.set(marker, "20260101")

        response = service.process_request(_request("rpc-1"))

        self.assertEqual(service.handlers.dispatched, [])
        self.assertEqual(response["error_code"], "CLIENT_REQUEST_ID_CROSS_DAY")

    def test_an_unfinished_claim_answers_ambiguous_rather_than_resubmitting(self):
        """上一次崩在 passorder 前后不明的位置：只能是 ambiguous。"""
        service = _service()
        service._claim_durable_order_request(
            ACCOUNT, "rule:e1", "order_stock",
            {"stock_code": "600000.SH", "volume": 100})

        response = service.process_request(_request("rpc-9"))

        self.assertEqual(service.handlers.dispatched, [])
        self.assertEqual(response["order_outcome"], "ambiguous")
        self.assertEqual(response["error_code"], "ORDER_OUTCOME_AMBIGUOUS")


class OutcomeTest(unittest.TestCase):
    def test_a_failure_before_dispatch_is_rejected(self):
        service = _service(_Handlers(raises=ValueError("bad price"),
                                     dispatch_state="pre_dispatch"))

        response = service.process_request(_request("rpc-1"))

        self.assertEqual(response["order_outcome"], "rejected")
        self.assertEqual(response["error_code"], "ORDER_REJECTED_BEFORE_DISPATCH")

    def test_a_failure_after_passorder_was_attempted_is_ambiguous(self):
        service = _service(_Handlers(raises=RuntimeError("rpc died"),
                                     dispatch_state="dispatching"))

        response = service.process_request(_request("rpc-1"))

        self.assertEqual(response["order_outcome"], "ambiguous")
        self.assertEqual(response["error_code"], "ORDER_OUTCOME_AMBIGUOUS")

    def test_a_server_error_downgrades_a_success_to_ambiguous(self):
        handlers = _Handlers()
        handlers._last_server_error = "order not found in system"
        service = _service(handlers)

        response = service.process_request(_request("rpc-1"))

        self.assertEqual(response["order_outcome"], "ambiguous")

    def test_no_durable_store_means_no_order(self):
        """幂等记录写不下去就不下单：内存去重不能当唯一保护。"""
        service = _service(redis_client=object())

        response = service.process_request(_request("rpc-1"))

        self.assertEqual(service.handlers.dispatched, [])
        self.assertEqual(response["order_outcome"], "rejected")
        self.assertEqual(response["error_code"], "IDEMPOTENCY_STORE_UNAVAILABLE")

    def test_a_request_without_the_key_keeps_the_old_behaviour(self):
        service = _service()
        request = _request("rpc-1")
        request.pop("client_request_id")

        response = service.process_request(request)

        self.assertEqual(len(service.handlers.dispatched), 1)
        self.assertNotIn("order_outcome", response)


class DispatchEvidenceTest(unittest.TestCase):
    """rejected 与 ambiguous 的分界线就在这两个标志上。"""

    def _request(self):
        return OrderRequest(
            signal_id="s-1", account_id=ACCOUNT, action="BUY",
            stock_code="600000.SH", volume=100, price=10.0,
            price_type=11, strategy_name="test", remark="r-1")

    def test_a_missing_passorder_never_counts_as_attempted(self):
        gateway = BigQmtOrderGateway(context_info=object(), account_id=ACCOUNT)

        self.assertRaises(RuntimeError, gateway.submit, self._request())
        self.assertFalse(gateway._last_submit_attempted)

    def test_a_passorder_that_raises_counts_as_attempted(self):
        def boom(*args, **kwargs):
            raise RuntimeError("QMT said no")

        gateway = BigQmtOrderGateway(
            context_info=object(), account_id=ACCOUNT, passorder_func=boom)

        self.assertRaises(RuntimeError, gateway.submit, self._request())
        self.assertTrue(gateway._last_submit_attempted)

    def test_a_cancel_that_never_reached_the_broker_is_not_attempted(self):
        gateway = BigQmtOrderGateway(context_info=object(), account_id=ACCOUNT)

        self.assertRaises(RuntimeError, gateway.cancel,
                          _OrderRef("12345"))
        self.assertFalse(gateway._last_cancel_attempted)


class _OrderRef(object):
    def __init__(self, order_sys_id):
        self.order_sys_id = order_sys_id


class RecordShapeTest(unittest.TestCase):
    def test_the_record_names_its_day_and_body(self):
        service = _service()
        service.process_request(_request("rpc-1"))
        daily, _marker, field = service._idempotency_keys(
            ACCOUNT, "rule:e1", service._trading_day())

        record = json.loads(service.redis.hget(daily, field))

        self.assertEqual(record["state"], "accepted")
        self.assertEqual(record["trading_day"], service._trading_day())
        self.assertTrue(record["body_digest"])
        self.assertEqual(record["response"]["data"]["order_sys_id"], "1001")


if __name__ == "__main__":
    unittest.main()
