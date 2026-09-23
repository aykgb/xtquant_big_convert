"""Regression tests for same-contract stock-option positions by direction."""

import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.adapters.position_bigqmt import BigQmtPositionProvider
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers


class Row:
    def __init__(self, direction, volume):
        self.m_strInstrumentID = "10011041"
        self.m_strExchangeID = "SHO"
        self.m_strInstrumentName = "科创50沽9月1800"
        self.m_nDirection = direction
        self.m_nVolume = volume
        self.m_nCanUseVolume = volume
        self.m_nYesterdayVolume = volume


class BidirectionalPositionTest(unittest.TestCase):
    def setUp(self):
        rows = [Row(48, 1), Row(49, 2)]
        self.provider = BigQmtPositionProvider(
            lambda account, account_type, detail_type: rows
            if detail_type == "POSITION"
            else []
        )
        self.handlers = BigQmtRpcHandlers(
            account_id="acct",
            market_data=object(),
            position_provider=self.provider,
        )

    def test_provider_list_preserves_both_directions(self):
        positions = self.provider.list_positions("acct")

        self.assertEqual(len(positions), 2)
        self.assertEqual(
            [(row.stock_code, row.direction, row.volume) for row in positions],
            [
                ("10011041.SHO", 48, 1),
                ("10011041.SHO", 49, 2),
            ],
        )

    def test_legacy_get_positions_mapping_is_unchanged(self):
        positions = self.handlers.handle("get_positions", {"account_id": "acct"})

        self.assertIsInstance(positions, dict)
        self.assertEqual(list(positions), ["10011041.SHO"])
        self.assertEqual(positions["10011041.SHO"].direction, 49)

    def test_query_stock_positions_returns_both_directions_as_list(self):
        positions = self.handlers.handle(
            "query_stock_positions", {"account_id": "acct"}
        )

        self.assertIsInstance(positions, list)
        self.assertEqual([row.direction for row in positions], [48, 49])


if __name__ == "__main__":
    unittest.main()
