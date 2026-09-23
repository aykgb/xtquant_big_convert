import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.xtquant_compat import BigQmtXtData


class FakeClient:
    def __init__(self):
        self.calls = []

    def call(self, method, params=None, **kwargs):
        self.calls.append((method, params or {}))
        return ["10000001.SHO"]


class OptionListAliasTest(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()
        self.market_data = BigQmtXtData(self.client)

    def test_available_alias_maps_to_legacy_rpc_parameter(self):
        result = self.market_data.get_option_list(
            "510050.SH", "20260909", available=True
        )

        self.assertEqual(result, ["10000001.SHO"])
        self.assertEqual(self.client.calls[-1][0], "get_option_list")
        self.assertTrue(self.client.calls[-1][1]["isavailavle"])

    def test_legacy_spelling_remains_supported(self):
        self.market_data.get_option_list(
            "510050.SH", "20260909", isavailavle=True
        )
        self.assertTrue(self.client.calls[-1][1]["isavailavle"])

    def test_conflicting_keywords_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "must not conflict"):
            self.market_data.get_option_list(
                "510050.SH",
                "20260909",
                isavailavle=True,
                available=False,
            )

        with self.assertRaisesRegex(ValueError, "must not conflict"):
            self.market_data.get_option_list(
                "510050.SH",
                "20260909",
                isavailavle=False,
                available=True,
            )


if __name__ == "__main__":
    unittest.main()
