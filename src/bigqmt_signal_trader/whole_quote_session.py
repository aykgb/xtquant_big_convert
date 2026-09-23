"""Client-side whole-quote subscription session.

Owns the per-process state for ``subscribe_whole_quote``: the local
subscription table, the shared push-channel subscriber thread, and the
keepalive heartbeat thread. One session is shared by every ``subscribe_whole_quote``
call in the process (``BigQmtXtData`` delegates here), so all subscriptions ride
a single push-channel connection and a single heartbeat loop.

The big-QMT whole-quote callback is INCREMENTAL (only changed symbols), so a
subscription does not by itself deliver an initial full snapshot — callers layer
a ``get_full_tick`` prime on top (done in ``BigQmtXtData.subscribe_whole_quote``).
"""

import os
import threading


def _norm_topic(code_list):
    return ",".join(sorted({str(c).strip().upper() for c in (code_list or []) if str(c or "").strip()}))


class WholeQuoteClientSession(object):
    def __init__(self, rpc_call, push_channel, client_id, heartbeat_interval_seconds=3.0, sub_id_func=None,
                 push_silence_replay_heartbeats=10, max_silence_replay_backoff=16):
        """``rpc_call`` is ``client.call``-shaped: fn(method, params) -> dict.
        ``push_channel`` is a QuotePushChannel used purely as a subscriber.
        ``sub_id_func`` (optional) mints subscription ids; defaults to a counter.
        ``push_silence_replay_heartbeats``: after this many heartbeat rounds
        without any push, replay subscriptions (covers server restarts where
        keepalive keeps succeeding because the redis request queue buffers
        during the restart window but the subscription table was reset).

        That silence rule is only a fallback now. A server that answers
        keepalive with ``known`` states outright whether it still holds the
        subscription, which separates "quiet market" from "table was reset" --
        silence alone cannot, and treating every quiet stretch as a failure
        replays forever outside trading hours. When no server answers with
        ``known``, the silence fallback backs off exponentially (doubling up to
        ``max_silence_replay_backoff``) so a permanently quiet session settles
        at one probe every few minutes instead of one every N rounds."""
        self._rpc = rpc_call
        self._channel = push_channel
        self.client_id = str(client_id or "")
        self._heartbeat_interval = float(heartbeat_interval_seconds)
        self._push_silence_replay_heartbeats = int(push_silence_replay_heartbeats)
        self._max_silence_replay_backoff = max(1, int(max_silence_replay_backoff))
        self._sub_id_func = sub_id_func
        self._seq = 0
        self._lock = threading.RLock()
        self._subscriptions = {}  # sub_id -> {"topic": str, "callback": fn, "codes": [...]}
        self._started = False
        self._subscriber_active = False
        self._subscribed_topics = frozenset()  # topic set the subscriber covers now
        self._heartbeat_thread = None
        self._last_push_time = None  # monotonic time of last incoming push
        self._replay_pending = False  # a failed batch must finish despite other subscriptions pushing
        self._generation = 0
        self._last_heartbeat_time = None
        self._last_heartbeat_error = ""
        self._consecutive_heartbeat_failures = 0
        self._last_replay_time = None

    # -- subscription lifecycle ---------------------------------------------
    def subscribe_whole_quote(self, code_list, callback=None):
        import time

        codes = [str(c) for c in (code_list or []) if str(c or "").strip()]
        if not codes:
            raise ValueError("code_list is required")
        with self._lock:
            sub_id = self._next_sub_id()
        result = self._rpc(
            "subscribe_whole_quote",
            {"client_id": self.client_id, "sub_id": sub_id, "codes": codes},
        ) or {}
        topic = str(result.get("topic") or result.get("combo_key") or _norm_topic(codes))
        with self._lock:
            self._subscriptions[sub_id] = {"topic": topic, "callback": callback, "codes": codes}
            # A subscribe that round-tripped is the same evidence a keepalive
            # gives; without stamping it here status() reports not-ready for a
            # whole heartbeat interval after every successful subscribe.
            self._last_heartbeat_time = time.monotonic()
            self._sync_subscriber_locked()
        return sub_id

    def unsubscribe_quote(self, sub_id):
        with self._lock:
            entry = self._subscriptions.pop(sub_id, None)
        if entry is None:
            return 0
        try:
            self._rpc("unsubscribe_whole_quote", {"client_id": self.client_id, "sub_id": sub_id})
        finally:
            with self._lock:
                self._sync_subscriber_locked()
        return 0

    def has_subscription(self, sub_id):
        with self._lock:
            return sub_id in self._subscriptions

    def replay_subscriptions(self):
        """Re-send subscribe for every active sub_id (server restart recovery).
        Idempotent on the server (keyed by client_id+combo), so replays are safe."""
        import time

        with self._lock:
            items = [(sid, dict(entry)) for sid, entry in self._subscriptions.items()]
            self._replay_pending = True
        error = None
        for sub_id, entry in items:
            try:
                self._rpc(
                    "subscribe_whole_quote",
                    {"client_id": self.client_id, "sub_id": sub_id, "codes": entry["codes"]},
                )
            except Exception as exc:
                if error is None:
                    error = exc
        if error is not None:
            raise error
        with self._lock:
            self._replay_pending = False
            self._generation += 1
            self._last_replay_time = time.monotonic()
            self._last_heartbeat_time = self._last_replay_time

    # -- heartbeat -------------------------------------------------------------
    def start(self):
        with self._lock:
            thread = self._heartbeat_thread
            if self._started and thread is not None and thread.is_alive():
                return
            # A past replay exception may have killed the loop while _started
            # stayed True (#231) -- "started" must mean "a live thread",
            # otherwise start() never recovers it.
            self._started = True
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop, name="bigqmt-quote-keepalive", daemon=True
            )
            self._heartbeat_thread.start()

    def stop(self, unsubscribe=True):
        with self._lock:
            self._started = False
            sub_ids = list(self._subscriptions.keys())
        if unsubscribe:
            for sub_id in sub_ids:
                try:
                    self._rpc(
                        "unsubscribe_whole_quote",
                        {"client_id": self.client_id, "sub_id": sub_id})
                except Exception:
                    pass
            with self._lock:
                self._subscriptions.clear()
                self._sync_subscriber_locked()
        thread = self._heartbeat_thread
        if thread is not None:
            thread.join(timeout=max(1.0, self._heartbeat_interval + 0.2))
        self._heartbeat_thread = None

    def status(self):
        import time

        now = time.monotonic()
        with self._lock:
            heartbeat_age = (
                max(0.0, now - self._last_heartbeat_time)
                if self._last_heartbeat_time is not None else None)
            active = len(self._subscriptions)
            ready = bool(
                self._started
                and self._subscriber_active
                and active
                and not self._replay_pending
                and self._consecutive_heartbeat_failures == 0
                and heartbeat_age is not None
                and heartbeat_age <= max(1.0, self._heartbeat_interval * 3.0)
            )
            return {
                "quote_ready": ready,
                "generation": self._generation,
                "subscriptions": active,
                "subscriber_active": bool(self._subscriber_active),
                "heartbeat_running": bool(
                    self._heartbeat_thread is not None
                    and self._heartbeat_thread.is_alive()),
                "last_heartbeat_age_seconds": heartbeat_age,
                "last_push_age_seconds": (
                    max(0.0, now - self._last_push_time)
                    if self._last_push_time is not None else None),
                "consecutive_heartbeat_failures":
                    self._consecutive_heartbeat_failures,
                "last_heartbeat_error": self._last_heartbeat_error,
                "replay_pending": bool(self._replay_pending),
                "last_replay_age_seconds": (
                    max(0.0, now - self._last_replay_time)
                    if self._last_replay_time is not None else None),
            }

    def _heartbeat_loop(self):
        import time

        consecutive_failures = 0
        silence_rounds = 0
        silence_threshold = self._push_silence_replay_heartbeats
        prev_last_push = None
        while True:
            with self._lock:
                if not self._started:
                    return
                sub_ids = list(self._subscriptions.keys())
                last_push = self._last_push_time
            if not sub_ids:
                time.sleep(self._heartbeat_interval)
                continue
            failures = 0
            # None = this server does not report `known` (an older one); only then
            # does the silence heuristic get to decide anything.
            lost_subscription = None
            for sub_id in sub_ids:
                try:
                    reply = self._rpc(
                        "quote_keepalive", {"client_id": self.client_id, "sub_id": sub_id})
                except Exception:
                    failures += 1
                    continue
                known = (reply or {}).get("known") if isinstance(reply, dict) else None
                if known is None:
                    continue
                if lost_subscription is None:
                    lost_subscription = False
                if not known:
                    lost_subscription = True
            recovered = not failures and consecutive_failures >= 3
            if failures:
                consecutive_failures += 1
            else:
                consecutive_failures = 0
            with self._lock:
                self._consecutive_heartbeat_failures = consecutive_failures
                self._last_heartbeat_time = time.monotonic()
                self._last_heartbeat_error = (
                    "%d keepalive request(s) failed" % failures
                    if failures else "")
            # Push-silence detection: a server restart can survive with keepalive
            # succeeding (the redis request queue buffers during the restart
            # window) while the subscription table was reset, so pushes stop.
            # Replay when no push arrived for several heartbeat rounds (also
            # covers the case where the very first prime push never arrived).
            if last_push != prev_last_push:
                silence_rounds = 0  # a push arrived since the last round
                silence_threshold = self._push_silence_replay_heartbeats
            else:
                silence_rounds += 1
            prev_last_push = last_push
            with self._lock:
                replay_pending = self._replay_pending

            # The server's own answer beats the guess. known=True means the
            # subscription is alive, so silence is just a quiet market (nights,
            # the lunch break, a halted name) and replaying would be noise --
            # every replay bumps the generation, and downstream reads a new
            # generation as "there is a gap in your data".
            if lost_subscription is False:
                silence_rounds = 0
                silence_threshold = self._push_silence_replay_heartbeats
            silent_too_long = (lost_subscription is None
                               and silence_rounds >= silence_threshold)
            if recovered or replay_pending or lost_subscription or silent_too_long:
                # Retry the idempotent batch until every subscription succeeds.
                # A's pushes cannot erase B's unresolved replay (#231).
                try:
                    self.replay_subscriptions()
                except Exception:
                    pass
                if silent_too_long:
                    # Still nothing to go on: probe half as often each time, so a
                    # session that is quiet all night settles down instead of
                    # replaying every N rounds until morning.
                    silence_threshold = min(
                        silence_threshold * 2,
                        self._push_silence_replay_heartbeats * self._max_silence_replay_backoff)
                silence_rounds = 0
            time.sleep(self._heartbeat_interval)

    # -- push routing ------------------------------------------------------------
    def _on_push(self, topic, data):
        import time

        now = time.monotonic()
        with self._lock:
            self._last_push_time = now
            callbacks = [
                entry["callback"]
                for entry in self._subscriptions.values()
                if entry["topic"] == topic and entry["callback"] is not None
            ]
        for callback in callbacks:
            try:
                callback(data)
            except Exception:
                pass

    def _sync_subscriber_locked(self):
        """(Re)start the push-channel subscriber to cover exactly the active
        topics. Reuses an existing subscriber when the topic set is unchanged;
        stops it before restarting when the set changed. No-op when nothing is
        subscribed (and stops the running subscriber in that case)."""
        topics = sorted({entry["topic"] for entry in self._subscriptions.values()})
        active = frozenset(topics)
        if active == self._subscribed_topics:
            return
        if not active:
            if self._subscriber_active:
                try:
                    self._channel.stop()
                except Exception:
                    pass
                self._subscriber_active = False
            self._subscribed_topics = active
            return
        if self._subscriber_active:
            try:
                self._channel.stop()
            except Exception:
                pass
        self._channel.start_subscriber(topics, self._on_push)
        self._subscriber_active = True
        self._subscribed_topics = active
        self._generation += 1

    # sub_ids carry the process id. The server keys a subscription by
    # (client_id, sub_id), and client_id is by default one persisted file per
    # user (~/.cache/bigqmt/quote_client_id) -- so two client processes on
    # one machine shared it, both minted sub_id 1, 2, ..., and the server saw
    # ONE subscriber: process B's unsubscribe / heartbeat lapse tore down
    # process A's subscription. Folding the pid in keeps the ids distinct
    # across processes while staying an int, which is what MiniQMT returns
    # and what callers hand back to unsubscribe_quote. A restarted process
    # gets fresh ids; its old ones lapse with the heartbeat, and the shared
    # QMT-side subscription (refcounted per combo) never drops in between.
    SUB_ID_PID_STRIDE = 1000000

    def _next_sub_id(self):
        if self._sub_id_func is not None:
            return self._sub_id_func()
        self._seq += 1
        return os.getpid() * self.SUB_ID_PID_STRIDE + self._seq
