"""Stable public reader interface for ``primus-historical-market-data``.

The legacy :mod:`data_loader` module remains a compatibility shim.  The public
objects below intentionally reference the same implementations, so moving to
the namespaced import does not alter loader defaults or return behavior.
"""

from data_loader import (
    BinanceFuturesMetrics5m,
    BinanceOptions5m,
    BinanceOrderBookSnapshot1h,
    CryptoBinance1m,
    CryptoBinanceQuarterly1m,
    CryptoBinanceSpot1m,
    CryptoDailyMatrix,
    DeribitOptionOverlay,
    DeribitOptionSnapshots5m,
    DeribitOptionTrades,
    MarketDataLoaderBase,
    VNDailyMatrix,
    VnDerivativesContinuous1m,
    VnDerivativesContinuousDaily,
    VnDerivativesContracts1m,
    VnDerivativesContractsDaily,
    VnFutures1m,
    VnStock1m,
    VnStockDaily,
    load_data,
    validate_data,
)

__all__ = [
    "BinanceFuturesMetrics5m",
    "BinanceOptions5m",
    "BinanceOrderBookSnapshot1h",
    "CryptoBinance1m",
    "CryptoBinanceQuarterly1m",
    "CryptoBinanceSpot1m",
    "CryptoDailyMatrix",
    "DeribitOptionOverlay",
    "DeribitOptionSnapshots5m",
    "DeribitOptionTrades",
    "MarketDataLoaderBase",
    "VNDailyMatrix",
    "VnDerivativesContinuous1m",
    "VnDerivativesContinuousDaily",
    "VnDerivativesContracts1m",
    "VnDerivativesContractsDaily",
    "VnFutures1m",
    "VnStock1m",
    "VnStockDaily",
    "load_data",
    "validate_data",
]
