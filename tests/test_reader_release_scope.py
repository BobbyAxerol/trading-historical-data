from __future__ import annotations

import unittest

from data_loader import (
    VnDerivativesContinuous1m,
    VnDerivativesContracts1m,
    VnDerivativesContractsDaily,
    VnFutures1m,
    VnStock1m,
    VnStockDaily,
)


class TestReaderReleaseScope(unittest.TestCase):
    def test_vn_release_ids_keep_unaccepted_paths_fail_closed(self) -> None:
        self.assertEqual(VnStockDaily.RELEASE_DATASET_ID, "vn_stock_daily")
        self.assertEqual(VnDerivativesContinuous1m.RELEASE_DATASET_ID, "vn_derivatives_continuous")
        self.assertEqual(VnStock1m.RELEASE_DATASET_ID, "vn_stock_1m")
        self.assertEqual(VnFutures1m.RELEASE_DATASET_ID, "vn_futures_1m")
        self.assertEqual(VnDerivativesContracts1m.RELEASE_DATASET_ID, "vn_derivatives_contracts_1m")
        self.assertEqual(VnDerivativesContractsDaily.RELEASE_DATASET_ID, "vn_derivatives_contracts_1d")


if __name__ == "__main__":
    unittest.main()
