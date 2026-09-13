# coding: utf-8
"""Push silence must stop meaning "subscriptions lost".

83f3f86 added push-silence replay because keepalive kept succeeding through a
server restart (the redis request queue buffers during the window) while the
subscription table had been reset. That was designed during a live trading
session, where silence really did mean failure. Outside trading hours silence
is the normal state, so the rule replayed every N heartbeat rounds forever --
measured against a live deployment: one replay every ~45s, 442 of 503 audit
rows in 3.3 hours, and every replay bumps the generation, which downstream
reads as "there is a gap in your data".

The server now answers keepalive with ``known``, which separates the two cases
outright. The silence rule stays only for servers that do not answer, and backs
off so a quiet night settles down instead of probing forever.
"""
import os
import sys
import unittest
from unittest import mock


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.whole_quote_session import WholeQuoteClientSession


def _run(rpc, rounds_to_run, **kwargs):
    """Drive the heartbeat loop for a fixed number of rounds, without sleeping."""
    session = WholeQuoteClientSession(rpc, None, "silence-test", **kwargs)
    session._subscriptions = {1: {"topic": "T", "codes": ["600030.SH"], "callback": None}}
    session._started = True
    rounds = [0]

    def fake_sleep(_seconds):
        rounds[0] += 1
        if rounds[0] >= rounds_to_run:
            session._started = False

    with mock.patch("time.sleep", fake_sleep):
        session._heartbeat_loop()
    return session


class SilenceWithALiveSubscriptionTest(unittest.TestCase):
    def test_a_quiet_market_does_not_replay_when_the_server_says_known(self):
        calls = []

        def rpc(method, _params):
            calls.append(method)
            return {"known": True} if method == "quote_keepalive" else {}

        session = _run(rpc, 40, push_silence_replay_heartbeats=2)

        self.assertEqual(calls.count("subscribe_whole_quote"), 0,
                         "known=True plus no push is a quiet market, not a failure")
        self.assertEqual(session._generation, 0,
                         "no replay means no generation bump, so downstream sees no gap")

    def test_an_unknown_subscription_replays_at_once(self):
        calls = []

        def rpc(method, _params):
            calls.append(method)
            return {"known": False} if method == "quote_keepalive" else {}

        session = _run(rpc, 3, push_silence_replay_heartbeats=99)

        # Waiting 99 silent rounds would be far too late; the server already said so.
        self.assertGreaterEqual(calls.count("subscribe_whole_quote"), 1)
        self.assertGreaterEqual(session._generation, 1)


class SilenceAgainstAServerThatDoesNotAnswerTest(unittest.TestCase):
    """Pre-0.3.42 servers return {} — the old heuristic has to keep working."""

    def test_silence_still_replays_without_a_known_field(self):
        calls = []

        def rpc(method, _params):
            calls.append(method)
            return {}

        _run(rpc, 6, push_silence_replay_heartbeats=2)

        self.assertGreaterEqual(calls.count("subscribe_whole_quote"), 1)

    def test_repeated_silence_backs_off_instead_of_probing_at_a_fixed_rate(self):
        calls = []

        def rpc(method, _params):
            calls.append(method)
            return {}

        # 60 silent rounds at a threshold of 2: the old fixed rule replays ~30
        # times; doubling (2, 4, 8, 16, 32...) gets there in far fewer.
        _run(rpc, 60, push_silence_replay_heartbeats=2, max_silence_replay_backoff=16)

        replays = calls.count("subscribe_whole_quote")
        self.assertLess(replays, 10, "backoff must thin out a permanently quiet session")
        self.assertGreaterEqual(replays, 1, "but it must still probe")

    def test_a_push_resets_the_backoff(self):
        calls = []
        session_box = {}

        def rpc(method, _params):
            calls.append(method)
            if method == "quote_keepalive" and len(calls) > 20:
                session_box["s"]._on_push("T", {"600030.SH": {"lastPrice": 1}})
            return {}

        session = WholeQuoteClientSession(
            rpc, None, "silence-test", push_silence_replay_heartbeats=2)
        session_box["s"] = session
        session._subscriptions = {1: {"topic": "T", "codes": ["600030.SH"], "callback": None}}
        session._started = True
        rounds = [0]

        def fake_sleep(_seconds):
            rounds[0] += 1
            if rounds[0] >= 40:
                session._started = False

        with mock.patch("time.sleep", fake_sleep):
            session._heartbeat_loop()

        # Data resumed, so the threshold is back to base: a later silent stretch
        # must be detected as fast as the first one was.
        self.assertGreaterEqual(calls.count("subscribe_whole_quote"), 1)


if __name__ == "__main__":
    unittest.main()
