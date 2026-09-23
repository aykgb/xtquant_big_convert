# coding: utf-8
"""执行事件的游标回放：断线后补的是缺口，不是一整天（迁移计划 §6.6）。

pub/sub 只负责叫醒，事实来源是 redis stream。于是"读到哪了"必须落盘，而且
只能由**投递成功**推进——回调没接住的事件下一轮还要再来一次。

两种"回放不出来"的情形分得很开，因为处理方式相反：

- 有游标、但游标比流里最老的一条还老：中间那段真丢了，从还留着的最老一条
  接着投，同时要求全量对账；
- 根本没有游标（冷启动）：从流尾开始。流按 maxlen 2000 + 1 天 TTL 保留，
  把这一整天重新投给一个全新的消费者不是恢复，是凭空造事件——每次重启都会
  把当天的委托、成交回调再放一遍。冷启动的正确补法是全量查询。
"""
import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))
os.environ.setdefault("BIGQMT_LOG_NAME", "bigqmt-test-cursors")

from bigqmt_signal_trader.exec_events import (  # noqa: E402
    load_exec_cursors,
    order_channel,
    persist_exec_cursors,
    position_channel,
    read_exec_event_streams,
    trade_channel,
)
from bigqmt_signal_trader.xtquant_compat import (  # noqa: E402
    BigQmtXtTrader,
    XtQuantTraderCallback,
)

ACCOUNT = "acct-1"


def _id_tuple(value):
    text = str(value or "0-0")
    left, _dash, right = text.partition("-")
    try:
        return int(left), int(right or 0)
    except (TypeError, ValueError):
        return (0, 0)


class StreamRedis(object):
    """够用的 stream/hash 假件：xadd / xrange / xrevrange / xread + hash。"""

    def __init__(self):
        self.streams = {}
        self.hashes = {}
        self.kv = {}
        self.expires = []
        self._seq = 0

    # -- streams
    def xadd(self, key, fields, maxlen=None, approximate=None):
        self._seq += 1
        entry_id = "%d-0" % self._seq
        entries = self.streams.setdefault(key, [])
        entries.append((entry_id, dict(fields)))
        if maxlen:
            del entries[:max(0, len(entries) - int(maxlen))]
        return entry_id

    def _entries(self, key):
        return list(self.streams.get(key) or [])

    def xrange(self, key, min="-", max="+", count=None):
        entries = self._entries(key)
        return entries[:count] if count else entries

    def xrevrange(self, key, max="+", min="-", count=None):
        entries = list(reversed(self._entries(key)))
        return entries[:count] if count else entries

    def xread(self, query, count=None, block=None):
        rows = []
        for key, cursor in (query or {}).items():
            after = _id_tuple(cursor)
            picked = [(eid, fields) for eid, fields in self._entries(key)
                      if _id_tuple(eid) > after]
            if count:
                picked = picked[:count]
            if picked:
                rows.append((key, picked))
        return rows

    # -- hashes / keys
    def hget(self, key, field):
        return (self.hashes.get(key) or {}).get(field)

    def hgetall(self, key):
        return dict(self.hashes.get(key) or {})

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

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True

    def expire(self, key, ttl):
        self.expires.append((key, ttl))
        return True

    def ping(self):
        return True


def _publish(redis_client, channel, **event):
    return redis_client.xadd(channel, {"payload": json.dumps(event)})


class Recording(XtQuantTraderCallback):
    def __init__(self):
        self.orders = []
        self.trades = []

    def on_stock_order(self, order):
        self.orders.append(order)

    def on_stock_trade(self, trade):
        self.trades.append(trade)


class ColdStartTest(unittest.TestCase):
    def test_no_cursor_starts_at_the_tail_and_asks_for_reconciliation(self):
        redis_client = StreamRedis()
        for i in range(3):
            _publish(redis_client, order_channel(ACCOUNT),
                     event_type="order", order_sys_id="o%d" % i)

        result = read_exec_event_streams(redis_client, ACCOUNT, cursors={})

        self.assertEqual(result["events"], [])
        self.assertIn("order", result["cold_start_streams"])
        self.assertTrue(result["reconciliation_required"])
        self.assertEqual(result["start_cursors"]["order"], "3-0")

    def test_the_tail_cursor_lets_the_next_event_through(self):
        redis_client = StreamRedis()
        _publish(redis_client, order_channel(ACCOUNT), event_type="order",
                 order_sys_id="old")
        first = read_exec_event_streams(redis_client, ACCOUNT, cursors={})
        _publish(redis_client, order_channel(ACCOUNT), event_type="order",
                 order_sys_id="new")

        second = read_exec_event_streams(
            redis_client, ACCOUNT, cursors=first["start_cursors"])

        self.assertEqual([item["event"]["order_sys_id"]
                          for item in second["events"]], ["new"])
        self.assertFalse(second["reconciliation_required"])


class GapTest(unittest.TestCase):
    def test_a_cursor_older_than_retention_replays_what_is_left(self):
        redis_client = StreamRedis()
        for i in range(3):
            _publish(redis_client, trade_channel(ACCOUNT),
                     event_type="trade", traded_id="t%d" % i)
        # 前两条被 maxlen 刷掉，游标停在 1-0。
        redis_client.streams[trade_channel(ACCOUNT)] = \
            redis_client.streams[trade_channel(ACCOUNT)][2:]

        result = read_exec_event_streams(
            redis_client, ACCOUNT, cursors={"trade": "1-0"})

        self.assertIn("trade", result["gap_streams"])
        # 缺口和冷启动是两回事：这条流有游标，只是够不着了。
        self.assertNotIn("trade", result["cold_start_streams"])
        self.assertEqual([item["event"]["traded_id"]
                          for item in result["events"]], ["t2"])

    def test_an_empty_stream_with_a_saved_cursor_is_a_gap(self):
        """流键整个过期了：证明不了没丢，就得当丢过处理。"""
        result = read_exec_event_streams(
            StreamRedis(), ACCOUNT, cursors={"order": "9-0"})

        self.assertIn("order", result["gap_streams"])
        self.assertTrue(result["reconciliation_required"])


class CursorStoreTest(unittest.TestCase):
    def test_cursors_round_trip_per_consumer(self):
        redis_client = StreamRedis()
        persist_exec_cursors(redis_client, ACCOUNT, "gw", {"order": "7-0"})

        self.assertEqual(load_exec_cursors(redis_client, ACCOUNT, "gw"),
                         {"order": "7-0"})
        self.assertEqual(load_exec_cursors(redis_client, ACCOUNT, "other"), {})


class _StubClient(object):
    """只认 call / account_id 的客户端替身，外加可控的通道健康。"""

    def __init__(self, redis_client, rpc_ready=True):
        self.account_id = ACCOUNT
        self.redis_client = redis_client
        self.rpc_ready = rpc_ready
        self.calls = []
        self.answers = {
            "query_execution_snapshot": {"orders": [{"order_sys_id": "o1"}],
                                         "trades": []},
            "query_stock_positions": {"600000.SH": {"volume": 100}},
        }

    def _redis(self):
        return self.redis_client

    def call(self, method, params=None, **kwargs):
        self.calls.append(method)
        return self.answers.get(method, {})

    def get_status(self, refresh=False):
        return {"rpc_ready": self.rpc_ready, "rpc_generation": 1}


def _trader(redis_client, rpc_ready=True):
    trader = BigQmtXtTrader(account_id=ACCOUNT, redis_client=redis_client)
    trader.client = _StubClient(redis_client, rpc_ready=rpc_ready)
    return trader


class ClientReplayTest(unittest.TestCase):
    def test_a_delivered_event_advances_the_persisted_cursor(self):
        redis_client = StreamRedis()
        trader = _trader(redis_client)
        callback = Recording()
        trader.register_callback(callback)
        # 冷启动先记下流尾，之后到的才是这个消费者的事件。
        trader._replay_event_streams(redis_client, ACCOUNT)
        _publish(redis_client, order_channel(ACCOUNT), event_type="order",
                 order_sys_id="o-1", stock_code="600000.SH")

        trader._replay_event_streams(redis_client, ACCOUNT)

        self.assertEqual(len(callback.orders), 1)
        self.assertEqual(
            load_exec_cursors(redis_client, ACCOUNT,
                              trader._event_consumer_id)["order"], "1-0")
        # 再跑一轮不会重投。
        trader._replay_event_streams(redis_client, ACCOUNT)
        self.assertEqual(len(callback.orders), 1)

    def test_an_unaccepted_event_keeps_its_place_in_the_queue(self):
        redis_client = StreamRedis()
        trader = _trader(redis_client)
        trader._replay_event_streams(redis_client, ACCOUNT)   # 冷启动
        _publish(redis_client, order_channel(ACCOUNT), event_type="order",
                 order_sys_id="o-1", stock_code="600000.SH")

        # 还没注册回调：投不出去就不许确认。
        self.assertRaises(RuntimeError, trader._replay_event_streams,
                          redis_client, ACCOUNT)
        self.assertEqual(
            load_exec_cursors(redis_client, ACCOUNT,
                              trader._event_consumer_id)["order"], "0-0")

        callback = Recording()
        trader.register_callback(callback)
        trader._replay_event_streams(redis_client, ACCOUNT)
        self.assertEqual(len(callback.orders), 1)

    def test_an_unrouted_event_type_does_not_block_the_ones_behind_it(self):
        redis_client = StreamRedis()
        trader = _trader(redis_client)
        trader.register_callback(Recording())
        trader._replay_event_streams(redis_client, ACCOUNT)   # 冷启动
        _publish(redis_client, order_channel(ACCOUNT),
                 event_type="something_new", order_sys_id="x")
        _publish(redis_client, order_channel(ACCOUNT), event_type="order",
                 order_sys_id="o-2", stock_code="600000.SH")

        trader._replay_event_streams(redis_client, ACCOUNT)

        self.assertEqual(len(trader.callback.orders), 1)
        self.assertIn("unrouted", trader.get_status()["last_event_error"])

    def test_a_cold_start_reconciles_by_query_instead_of_replaying(self):
        redis_client = StreamRedis()
        for i in range(5):
            _publish(redis_client, order_channel(ACCOUNT),
                     event_type="order", order_sys_id="old-%d" % i)
        trader = _trader(redis_client)
        callback = Recording()
        trader.register_callback(callback)

        trader._replay_event_streams(redis_client, ACCOUNT)

        self.assertEqual(callback.orders, [])
        self.assertIn("query_execution_snapshot", trader.client.calls)
        status = trader.get_status()
        self.assertFalse(status["event_reconciliation_required"])
        self.assertEqual(status["last_event_reconciliation"]["orders"], 1)

    def test_no_reconciliation_while_rpc_is_down(self):
        """RPC 不通时不能假装对过账——状态保持"要求对账"。"""
        redis_client = StreamRedis()
        _publish(redis_client, order_channel(ACCOUNT), event_type="order",
                 order_sys_id="o-1")
        trader = _trader(redis_client, rpc_ready=False)
        trader.register_callback(Recording())

        trader._replay_event_streams(redis_client, ACCOUNT)

        self.assertNotIn("query_execution_snapshot", trader.client.calls)
        self.assertTrue(trader.get_status()["event_reconciliation_required"])

    def test_a_position_snapshot_marks_the_position_channel_fresh(self):
        redis_client = StreamRedis()
        trader = _trader(redis_client)
        trader.register_callback(Recording())
        trader._replay_event_streams(redis_client, ACCOUNT)   # 冷启动
        trader._note_position_status(False)                   # 持仓通道断过
        before = trader.get_status()["position_generation"]
        _publish(redis_client, position_channel(ACCOUNT),
                 account_id=ACCOUNT, positions={})

        trader._replay_event_streams(redis_client, ACCOUNT)

        status = trader.get_status()
        self.assertTrue(status["position_ready"])
        self.assertGreater(status["position_generation"], before)


if __name__ == "__main__":
    unittest.main()
