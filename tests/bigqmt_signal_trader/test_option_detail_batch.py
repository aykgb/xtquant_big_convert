# -*- coding: utf-8 -*-
import unittest

from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers
from bigqmt_signal_trader.xtquant_compat import BigQmtXtData


class _Context(object):
    def __init__(self):
        self.calls = []

    def get_option_detail_data(self, code):
        self.calls.append(("option", code))
        if code == "BAD.SHO":
            raise RuntimeError("bad contract")
        return {
            "InstrumentName": code,
            "OptUnit": 10000,
            "OptUndlCode": "510050",
            "OptUndlMarket": "SH",
            "OptExercisePrice": 3.0,
            "ExpireDate": "20260923",
            "optType": "CALL",
        }


class _Client(object):
    def __init__(self):
        self.calls = []

    def call(self, method, params, timeout_seconds=None):
        self.calls.append((method, params, timeout_seconds))
        return {"A.SHO": {"OptUnit": 10000}}


class OptionDetailBatchTest(unittest.TestCase):
    def test_qmt_side_loops_once_per_unique_nonempty_code(self):
        context = _Context()
        provider = BigQmtMarketDataProvider(context)

        result = provider.get_option_detail_data_batch(
            ["A.SHO", "BAD.SHO", "A.SHO", ""])

        self.assertEqual(list(result), ["A.SHO", "BAD.SHO"])
        self.assertEqual(result["A.SHO"]["OptUndlCodeFull"], "510050.SH")
        self.assertEqual(result["BAD.SHO"], {})
        self.assertEqual(
            context.calls,
            [("option", "A.SHO"), ("option", "BAD.SHO")],
        )

    def test_rpc_method_is_whitelisted_and_dispatched(self):
        context = _Context()
        provider = BigQmtMarketDataProvider(context)
        handlers = BigQmtRpcHandlers(
            account_id="acct",
            market_data=provider,
            position_provider=object(),
        )

        result = handlers.handle(
            "get_option_detail_data_batch", {"stockcodes": ["A.SHO"]})

        self.assertEqual(result["A.SHO"]["OptUnit"], 10000)

    def test_client_sends_one_rpc_with_extended_timeout(self):
        client = _Client()
        data = BigQmtXtData(client)

        result = data.get_option_detail_data_batch(["A.SHO", "B.SHO"])

        self.assertEqual(result["A.SHO"]["OptUnit"], 10000)
        self.assertEqual(
            client.calls,
            [(
                "get_option_detail_data_batch",
                {"stockcodes": ["A.SHO", "B.SHO"]},
                300.0,
            )],
        )


if __name__ == "__main__":
    unittest.main()
