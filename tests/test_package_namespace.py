from __future__ import annotations

import inspect
import unittest

import data_loader
from primus import historical_market_data as hmd


class TestPackageNamespace(unittest.TestCase):
    def test_namespace_reexports_legacy_reader_implementations(self) -> None:
        for name in hmd.__all__:
            self.assertIs(getattr(hmd, name), getattr(data_loader, name))

    def test_documented_import_keeps_loader_defaults(self) -> None:
        self.assertIs(hmd.CryptoBinance1m, data_loader.CryptoBinance1m)
        self.assertTrue(inspect.signature(hmd.CryptoBinance1m.load).parameters["check_val"].default)


if __name__ == "__main__":
    unittest.main()
