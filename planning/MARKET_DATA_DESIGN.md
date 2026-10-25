# Market Data Backend — Detailed Design

**Scope:** everything under `backend/app/market/`, plus how the rest of the backend (app startup, watchlist, trades, portfolio, chat) uses it.
**Audience:** the Backend/Market Data agents that build and maintain this subsystem.
**Status:** A first version is already in the repo (see `MARKET_DATA_SUMMARY.md`; 73 tests passing). This document is the authoritative design. It keeps what works, fixes the defects found while reviewing the code against the `massive` SDK, and adds what the rest of the platform needs (daily change %, tracking held positions, configurable polling, an SSE heartbeat). §15 lists every change to the current code as a checklist.

> **Critical defect in the current code:** `MassiveDataSource._poll_once` reads `snap.last_trade.timestamp`, but the SDK's `LastTrade` model has no `timestamp` attribute. The field is `sip_timestamp`, in **nanoseconds**. Every real snapshot raises `AttributeError`, is logged as "Skipping snapshot", and gets dropped, so a real API key never produces a price. The tests build snapshots from `MagicMock`, which invents any attribute, so they hide the bug. §8.3 has the correct parsing and §14.4 has tests that use real SDK models.

---

## Table of Contents

1. [Requirements](#1-requirements)
2. [Architecture](#2-architecture)
3. [File Layout](#3-file-layout)
4. [Data Model — `models.py`](#4-data-model--modelspy)
5. [Price Cache — `cache.py`](#5-price-cache--cachepy)
6. [Unified Interface — `interface.py`](#6-unified-interface--interfacepy)
7. [Simulator — `seed_prices.py`, `simulator.py`](#7-simulator)
8. [Massive API Client — `massive_client.py`](#8-massive-api-client--massive_clientpy)
9. [Factory & Configuration — `factory.py`](#9-factory--configuration--factorypy)
10. [SSE Streaming — `stream.py`](#10-sse-streaming--streampy)
11. [Ticker Tracking — `tracking.py`](#11-ticker-tracking--trackingpy)
12. [FastAPI Integration](#12-fastapi-integration)
13. [Error Handling & Edge Cases](#13-error-handling--edge-cases)
14. [Testing](#14-testing)
15. [Change Checklist vs. Current Code](#15-change-checklist-vs-current-code)

---

## 1. Requirements

Taken from `PLAN.md` §6, §8 and §10:

| # | Requirement | Where it is met |
|---|---|---|
| R1 | One abstract interface; the simulator and Massive are interchangeable; downstream code doesn't know which is running | §6, §9 |
| R2 | The source is chosen by `MASSIVE_API_KEY` (set and non-empty → Massive, otherwise the simulator) | §9 |
| R3 | Simulator: GBM, ~500 ms ticks, correlated moves, random 2–5% events, realistic seed prices, runs in-process | §7 |
| R4 | Massive: REST polling (not WebSocket), one call covers every ticker, configurable interval (free tier 15 s) | §8 |
| R5 | A shared in-memory cache holds the latest price, previous price and timestamp for each ticker | §5 |
| R6 | `GET /api/stream/prices` SSE; each event carries ticker, price, previous price, timestamp and direction; ~500 ms cadence; browser auto-reconnects | §10 |
| R7 | The watchlist panel shows **daily change %**, not just tick-to-tick change | §4, §7.3, §8.3 |
| R8 | Portfolio valuation and trade execution read prices from the cache | §12.3 |
| R9 | Watchlist add/remove (from the UI or the LLM) changes which tickers are tracked | §11 |

---

## 2. Architecture

```
                     create_market_data_source(cache)        ← reads env (§9)
                                  │
              ┌───────────────────┴───────────────────┐
              ▼                                       ▼
   SimulatorDataSource                       MassiveDataSource
   (asyncio task, 500 ms)                    (asyncio task, 15 s default)
   GBMSimulator.step()                       RESTClient.get_snapshot_all()
              │                              (in a worker thread)
              └──────────────┬────────────────────────┘
                             ▼  cache.update(ticker, price, ts, reference_price)
                   ┌───────────────────┐
                   │    PriceCache     │  thread-safe, latest PriceUpdate per ticker,
                   │  (single source   │  monotonic version counter
                   │    of truth)      │
                   └─────────┬─────────┘
       ┌────────────────┬────┴───────────┬──────────────────┐
       ▼                ▼                ▼                  ▼
  SSE /api/stream   POST /trade      GET /portfolio     POST /chat
  (push on version  (fill price)     (valuation)        (LLM context)
   change)
```

**Principles**

1. **Producers write, consumers read.** A data source never returns prices to callers; it pushes them into the `PriceCache`. Nothing downstream calls a data source to get a price.
2. **One writer at a time.** Exactly one data source exists per process. The cache still uses a lock because the Massive client calls into it from a worker thread.
3. **The interface is about tracked tickers, not prices.** `start / stop / add_ticker / remove_ticker / get_tickers`. Deciding *which* tickers to track (watchlist ∪ held positions) happens one layer up (§11).
4. **No DB knowledge inside `app/market/`.** The package depends only on numpy, `massive` and FastAPI (for the router). That keeps it unit-testable on its own.

---

## 3. File Layout

```
backend/app/market/
├── __init__.py         # Public re-exports (§12.4)
├── models.py           # PriceUpdate, normalize_ticker()
├── cache.py            # PriceCache
├── interface.py        # MarketDataSource ABC
├── seed_prices.py      # SEED_PRICES, TICKER_PARAMS, correlation constants
├── simulator.py        # GBMSimulator + SimulatorDataSource
├── massive_client.py   # parse_snapshot() + MassiveDataSource
├── factory.py          # MarketDataConfig + create_market_data_source()
├── stream.py           # create_stream_router() + SSE generator
└── tracking.py         # NEW: reconcile_tickers() — diff desired vs tracked set

backend/tests/market/
├── test_models.py  test_cache.py  test_simulator.py  test_simulator_source.py
├── test_massive.py     # rewritten to use real SDK models (§14.4)
├── test_factory.py
├── test_stream.py      # NEW (§14.5)
└── test_tracking.py    # NEW (§14.6)
```

---

## 4. Data Model — `models.py`

`PriceUpdate` is the **only** type that leaves the market data layer. It is immutable (`frozen=True, slots=True`), so readers can hold one across threads safely.

Change from the current code: a new `reference_price` field, the price daily change is measured against (the session open for the simulator, the previous close for Massive). It drives `day_change` / `day_change_percent`, which the watchlist panel needs (R7). The tick-level `change` / `change_percent` stay as they are, since the frontend uses them for the flash direction.

```python
"""Data models for market data."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field

_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


def normalize_ticker(raw: str) -> str:
    """Canonical ticker form: stripped + upper-case. Raises ValueError if invalid.

    Used at every boundary (watchlist API, trade API, LLM actions) so that
    'aapl ', 'AAPL' and 'Aapl' all refer to the same cache key.
    """
    ticker = raw.strip().upper()
    if not _TICKER_RE.match(ticker):
        raise ValueError(f"Invalid ticker symbol: {raw!r}")
    return ticker


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds
    reference_price: float | None = None  # Session open / previous close

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat' relative to the previous update."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    @property
    def day_change(self) -> float:
        """Absolute change versus the reference price (0.0 if unknown)."""
        if not self.reference_price:
            return 0.0
        return round(self.price - self.reference_price, 4)

    @property
    def day_change_percent(self) -> float:
        """Percent change versus the reference price (0.0 if unknown)."""
        if not self.reference_price:
            return 0.0
        return round((self.price - self.reference_price) / self.reference_price * 100, 4)

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
            "reference_price": self.reference_price,
            "day_change": self.day_change,
            "day_change_percent": self.day_change_percent,
        }
```

Example `to_dict()` output:

```json
{
  "ticker": "AAPL",
  "price": 191.37,
  "previous_price": 191.32,
  "timestamp": 1761431520.512,
  "change": 0.05,
  "change_percent": 0.0261,
  "direction": "up",
  "reference_price": 190.0,
  "day_change": 1.37,
  "day_change_percent": 0.7211
}
```

---

## 5. Price Cache — `cache.py`

Responsibilities:

- Store the latest `PriceUpdate` per ticker.
- Work out `previous_price` from the last stored update (on the first update, `previous_price == price`, so `direction == "flat"`).
- Keep a per-ticker `reference_price` that persists across updates: once set it sticks until a source sends a new one or the ticker is removed.
- Bump a monotonic `version` on every write, so SSE can skip unchanged ticks for free.

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every write

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        reference_price: float | None = None,
    ) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        If reference_price is None, the previously stored reference is kept.
        If this is the first update for the ticker, previous_price == price.
        """
        with self._lock:
            ts = time.time() if timestamp is None else timestamp
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price
            if reference_price is None and prev is not None:
                reference_price = prev.reference_price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
                reference_price=None if reference_price is None else round(reference_price, 2),
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices (shallow copy; values are immutable)."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Drop a ticker. Bumps version so SSE clients see the removal."""
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

Fixes compared with the current `cache.py`:

| Issue | Fix |
|---|---|
| `timestamp or time.time()` treats `0.0` as missing | `time.time() if timestamp is None else timestamp` |
| `remove()` doesn't bump `version`, so a removed ticker stays in SSE clients' view until the next price tick (which never comes on a quiet Massive feed) | bump on actual removal |
| `version` read outside the lock (fine under the GIL, not under free-threaded 3.13t) | read under the lock |

Usage:

```python
cache = PriceCache()
cache.update("AAPL", 190.00, reference_price=190.00)   # first: flat
u = cache.update("AAPL", 190.42)                         # keeps reference 190.00
assert u.direction == "up" and u.day_change == 0.42
cache.get_price("AAPL")      # 190.42
cache.get_all().keys()       # dict_keys(['AAPL'])
```

---

## 6. Unified Interface — `interface.py`

No changes to the method set. The contract is tightened in the docstrings:

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push PriceUpdates into a shared PriceCache on their own
    schedule. Downstream code never asks a source for a price — it reads the cache.

    Contract (all implementations):
      * Tickers passed in are already normalized (see models.normalize_ticker).
      * start() is called exactly once, before any other method.
      * add_ticker() / remove_ticker() are idempotent.
      * remove_ticker() also removes the ticker from the PriceCache.
      * stop() is idempotent; after it returns the source never writes again.
      * Background failures are logged and retried, never raised to callers.
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking a ticker. No-op if already tracked."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker and evict it from the cache. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers, in insertion order."""
```

Ticker normalization belongs to the **callers** (API routes, the chat action executor), not to the sources. The current `MassiveDataSource` upper-cases internally but the simulator doesn't, so the two sources disagree. Under this design, both trust their input and the boundary normalizes it.

---

## 7. Simulator

### 7.1 Seed data — `seed_prices.py`

Unchanged from the current file: `SEED_PRICES`, `TICKER_PARAMS`, `DEFAULT_PARAMS`, `CORRELATION_GROUPS`, `INTRA_TECH_CORR=0.6`, `INTRA_FINANCE_CORR=0.5`, `CROSS_GROUP_CORR=0.3`, `TSLA_CORR=0.3`.

| Ticker | Seed $ | σ (annual vol) | μ (annual drift) | Group |
|---|---|---|---|---|
| AAPL | 190 | 0.22 | 0.05 | tech |
| GOOGL | 175 | 0.25 | 0.05 | tech |
| MSFT | 420 | 0.20 | 0.05 | tech |
| AMZN | 185 | 0.28 | 0.05 | tech |
| TSLA | 250 | 0.50 | 0.03 | loner (0.3 with everyone) |
| NVDA | 800 | 0.40 | 0.08 | tech |
| META | 500 | 0.30 | 0.05 | tech |
| JPM | 195 | 0.18 | 0.04 | finance |
| V | 280 | 0.17 | 0.04 | finance |
| NFLX | 600 | 0.35 | 0.05 | tech |
| *other* | deterministic $50–300 | 0.25 | 0.05 | cross (0.3) |

**Change:** an unknown ticker added at runtime (e.g. `PYPL`) currently gets `random.uniform(50, 300)`, so its price jumps each time the app restarts. Derive it from the symbol instead, so `PYPL` always starts at the same price:

```python
import zlib

def seed_price_for(ticker: str) -> float:
    """Known seed price, else a stable pseudo-random price in [$50, $300)."""
    if ticker in SEED_PRICES:
        return SEED_PRICES[ticker]
    bucket = zlib.crc32(ticker.encode()) % 25_000  # 0..24,999 cents
    return 50.0 + bucket / 100.0
```

### 7.2 GBM math

For each tick of length `dt` (as a fraction of a trading year):

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt·Z )
dt      = 0.5 s / (252 days × 6.5 h × 3600 s) ≈ 8.48e-8
```

- `exp(·) > 0`, so prices can never go negative.
- Per-tick σ·√dt for AAPL ≈ 0.22 × 2.9e-4 ≈ 0.0064%, about 1.2¢ on $190. That's small, but it shows up as a flash on most ticks, and after rounding to cents it adds up to realistic intraday ranges.
- **Correlation:** build the correlation matrix `C` from the sector rules and take `L = cholesky(C)`. Each tick, draw `z_ind ~ N(0, I)` and use `z = L @ z_ind`. Rebuild `L` on every add or remove: O(n³) for n < 50, which is microseconds.
- **Events:** with probability `p = 0.001` per ticker per tick, multiply the price by `1 ± U(0.02, 0.05)`. With 10 tickers at 2 ticks/s, that's an event about every 50 s across the board.

### 7.3 `GBMSimulator`

Changes compared with the current code: (a) inject an RNG so tests can be deterministic, (b) use `seed_price_for`, (c) expose `get_reference_price()` (the session open, i.e. the starting price), (d) guard Cholesky against a non-positive-definite matrix.

```python
"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    TICKER_PARAMS,
    TSLA_CORR,
    seed_price_for,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices."""

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
        rng: np.random.Generator | None = None,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._rng = rng or np.random.default_rng()

        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._opens: dict[str, float] = {}  # Session open = starting price
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z = self._rng.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z

        shocks = self._rng.random(n) < self._event_prob

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]

            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            if shocks[i]:
                magnitude = self._rng.uniform(0.02, 0.05)
                sign = 1 if self._rng.random() < 0.5 else -1
                self._prices[ticker] *= 1 + sign * magnitude
                logger.debug("Random event on %s: %+.1f%%", ticker, sign * magnitude * 100)

            result[ticker] = round(self._prices[ticker], 2)
        return result

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker], self._opens[ticker], self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_reference_price(self, ticker: str) -> float | None:
        return self._opens.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        price = seed_price_for(ticker)
        self._tickers.append(ticker)
        self._prices[ticker] = price
        self._opens[ticker] = price
        self._params[ticker] = dict(TICKER_PARAMS.get(ticker, DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho

        try:
            self._cholesky = np.linalg.cholesky(corr)
        except np.linalg.LinAlgError:
            # Should not happen with the current constants, but never let a
            # bad matrix kill the price feed — fall back to independent moves.
            logger.warning("Correlation matrix not positive-definite; using independent draws")
            self._cholesky = None

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        tech, finance = CORRELATION_GROUPS["tech"], CORRELATION_GROUPS["finance"]
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

### 7.4 `SimulatorDataSource`

The asyncio wrapper. It seeds the cache straight away on `start()` and `add_ticker()`, so the first SSE frame and any trade right after an add both have a price.

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator."""

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
        rng: np.random.Generator | None = None,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._rng = rng
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers, event_probability=self._event_prob, rng=self._rng)
        for ticker in self._sim.get_tickers():
            self._seed_cache(ticker)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim and ticker not in self._sim.get_tickers():
            self._sim.add_ticker(ticker)
            self._seed_cache(ticker)
            logger.info("Simulator: added %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    def _seed_cache(self, ticker: str) -> None:
        assert self._sim is not None
        price = self._sim.get_price(ticker)
        if price is not None:
            self._cache.update(
                ticker, price, reference_price=self._sim.get_reference_price(ticker)
            )

    async def _run_loop(self) -> None:
        """Step → write → sleep. Exceptions are logged; the loop never dies."""
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker, price)  # reference is sticky
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

Note: there's no `await` between `step()` and the cache writes, so on the event loop a whole tick is written atomically. SSE readers see either the old tick or the new one, never half of each.

---

## 8. Massive API Client — `massive_client.py`

### 8.1 Endpoint & SDK

| | |
|---|---|
| Package | `massive` (Polygon.io SDK, renamed); `from massive import RESTClient` |
| Call | `client.get_snapshot_all(market_type=SnapshotMarketType.STOCKS, tickers=[...])` |
| REST | `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT,...` |
| Returns | `list[TickerSnapshot]`, **one HTTP call covers every ticker** |
| Sync/async | The SDK is synchronous, so call it through `asyncio.to_thread` |
| Rate limit | Free: 5 req/min, so poll every ≥ 12 s (default 15 s). Paid: 2–5 s |

### 8.2 SDK model fields (verified against the installed `massive` package)

```
TickerSnapshot
├── ticker: str | None
├── last_trade: LastTrade | None
│     ├── price: float | None
│     └── sip_timestamp: int | None          ← NANOSECONDS (JSON key "t")
├── day: Agg | None        (open, high, low, close, volume, vwap)
├── prev_day: Agg | None   (… close = previous session close)
├── todays_change: float | None
├── todays_change_percent: float | None
└── updated: int | None                      ← NANOSECONDS
```

> `LastTrade` has **no** `timestamp` attribute, and `Agg` has no `previous_close`. The older `planning/archive/MASSIVE_API.md` gets both wrong.

Raw JSON, for reference (the SDK maps camelCase / single-letter keys to the fields above):

```json
{
  "ticker": "AAPL",
  "todaysChange": 1.37, "todaysChangePerc": 0.72,
  "updated": 1761431520512000000,
  "day":     {"o": 190.1, "h": 191.9, "l": 189.8, "c": 191.37, "v": 41230011, "vw": 190.9},
  "prevDay": {"o": 188.0, "h": 190.4, "l": 187.6, "c": 190.0,  "v": 52011034, "vw": 189.2},
  "lastTrade": {"p": 191.37, "s": 100, "t": 1761431520512000000, "x": 4}
}
```

### 8.3 Parsing — a pure, separately testable function

Price fallback chain: `last_trade.price` → `day.close` → `prev_day.close`. Pre-market, and on plans without trade data, `last_trade` can be missing, and `day` is all zeros until the open.

```python
"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from massive import RESTClient
from massive.exceptions import BadResponse
from massive.rest.models import SnapshotMarketType, TickerSnapshot

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)

NS_PER_SECOND = 1_000_000_000


@dataclass(frozen=True, slots=True)
class ParsedQuote:
    ticker: str
    price: float
    timestamp: float  # Unix seconds
    reference_price: float | None  # Previous close


def _positive(value: float | None) -> float | None:
    return value if isinstance(value, (int, float)) and value > 0 else None


def parse_snapshot(snap: TickerSnapshot) -> ParsedQuote | None:
    """Convert one SDK snapshot to a ParsedQuote, or None if it has no usable price."""
    if not snap.ticker:
        return None

    trade, day, prev = snap.last_trade, snap.day, snap.prev_day
    price = (
        _positive(trade.price if trade else None)
        or _positive(day.close if day else None)
        or _positive(prev.close if prev else None)
    )
    if price is None:
        return None

    ts_ns = (trade.sip_timestamp if trade else None) or snap.updated
    timestamp = ts_ns / NS_PER_SECOND if ts_ns else time.time()

    return ParsedQuote(
        ticker=snap.ticker,
        price=float(price),
        timestamp=timestamp,
        reference_price=_positive(prev.close if prev else None),
    )
```

### 8.4 `MassiveDataSource`

Design points:

1. **Immediate first poll** in `start()`, so the cache isn't empty for 15 s.
2. **Copy the ticker list before going to the worker thread.** The current `_fetch_snapshots` reads `self._tickers` from the worker thread while `add_ticker` may be changing it on the event loop.
3. **Early poll on `add_ticker`.** A wake event cuts the current sleep short, but polls never run closer together than `min_poll_spacing` (12 s by default, which keeps the free tier at ≤ 5/min). A new ticker gets a price within ~12 s instead of up to 15 s, without breaking the rate limit.
4. **Backoff** on 429 / network errors: double the interval, up to 120 s; go back to normal on the first success. A 401/403 gets logged loudly once per streak; it means the key is wrong or the plan lacks snapshot access.
5. **Prune** tickers removed while a request was in flight. Without this, a ticker the user just removed would reappear in the cache when that in-flight response arrives.

```python
class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST snapshot API."""

    MAX_BACKOFF = 120.0

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
        min_poll_spacing: float = 12.0,
        client: RESTClient | None = None,  # Injected in tests
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._min_spacing = min(min_poll_spacing, poll_interval)
        self._client = client
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._last_poll = 0.0
        self._failures = 0

    # --- Interface ---

    async def start(self, tickers: list[str]) -> None:
        if self._client is None:
            self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(dict.fromkeys(tickers))  # De-dupe, keep order
        await self._poll_once()
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info("Massive poller started: %d tickers, %.1fs interval",
                    len(self._tickers), self._interval)

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            self._wake.set()  # Ask for an early (rate-limit-safe) poll
            logger.info("Massive: added %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internals ---

    def _current_delay(self) -> float:
        if self._failures == 0:
            return self._interval
        return min(self._interval * 2**self._failures, self.MAX_BACKOFF)

    async def _poll_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._current_delay())
            except TimeoutError:
                pass
            self._wake.clear()
            # Respect rate limits even when woken early.
            spacing = self._min_spacing - (time.monotonic() - self._last_poll)
            if spacing > 0:
                await asyncio.sleep(spacing)
            await self._poll_once()

    async def _poll_once(self) -> None:
        tickers = list(self._tickers)  # Copy on the event loop thread
        if not tickers or self._client is None:
            return

        self._last_poll = time.monotonic()
        try:
            snapshots = await asyncio.to_thread(self._fetch_snapshots, tickers)
        except BadResponse as e:
            self._failures += 1
            logger.error("Massive poll rejected (%s); check MASSIVE_API_KEY / plan", e)
            return
        except Exception as e:  # Network, 429, 5xx after SDK retries, …
            self._failures += 1
            logger.warning("Massive poll failed (%d in a row): %s", self._failures, e)
            return

        self._failures = 0
        tracked = set(self._tickers)  # May have changed while we were awaiting
        updated = 0
        for snap in snapshots:
            quote = parse_snapshot(snap)
            if quote is None:
                logger.debug("No usable price in snapshot for %s", snap.ticker)
                continue
            if quote.ticker not in tracked:
                continue  # Removed mid-flight — don't resurrect it
            self._cache.update(
                quote.ticker, quote.price,
                timestamp=quote.timestamp, reference_price=quote.reference_price,
            )
            updated += 1

        missing = tracked - {s.ticker for s in snapshots}
        if missing:
            logger.debug("Massive returned no snapshot for: %s", sorted(missing))
        logger.debug("Massive poll: updated %d/%d tickers", updated, len(tickers))

    def _fetch_snapshots(self, tickers: list[str]) -> list[TickerSnapshot]:
        """Blocking SDK call. Runs in a worker thread."""
        assert self._client is not None
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=tickers,
        )
```

Before implementing, check the exact exception names the installed SDK exports (`massive.exceptions`: `BadResponse`, `AuthError`). If an import fails, fall back to catching `Exception` and checking the message for the status. The SDK's urllib3 layer already retries 5xx responses.

**Unknown or invalid symbols:** Massive just leaves them out of the response, so they never get a price (logged at debug as `missing`). The watchlist still shows the row, with "—" for the price. §12.3 shows how trades handle a missing price.

**Market closed:** `last_trade.price` keeps the last print (including after-hours), so prices sit still and SSE goes quiet. The heartbeat (§10) keeps the connection alive.

---

## 9. Factory & Configuration — `factory.py`

| Env var | Default | Meaning |
|---|---|---|
| `MASSIVE_API_KEY` | *(empty)* | Non-empty → Massive; otherwise the simulator |
| `MASSIVE_POLL_INTERVAL` | `15` | Seconds between polls (use 2–5 on paid tiers) |
| `SIMULATOR_INTERVAL` | `0.5` | Seconds between simulator ticks |
| `SIMULATOR_EVENT_PROBABILITY` | `0.001` | Shock probability per ticker per tick |
| `SIMULATOR_SEED` | *(unset)* | Integer → deterministic simulator (E2E tests) |

Add the new optional variables to `.env.example`, commented out.

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    try:
        return float(raw) if raw else default
    except ValueError:
        logger.warning("Ignoring invalid %s=%r; using %s", name, raw, default)
        return default


@dataclass(frozen=True)
class MarketDataConfig:
    massive_api_key: str = ""
    massive_poll_interval: float = 15.0
    simulator_interval: float = 0.5
    simulator_event_probability: float = 0.001
    simulator_seed: int | None = None

    @classmethod
    def from_env(cls) -> MarketDataConfig:
        seed = os.environ.get("SIMULATOR_SEED", "").strip()
        return cls(
            massive_api_key=os.environ.get("MASSIVE_API_KEY", "").strip(),
            massive_poll_interval=max(_env_float("MASSIVE_POLL_INTERVAL", 15.0), 1.0),
            simulator_interval=max(_env_float("SIMULATOR_INTERVAL", 0.5), 0.05),
            simulator_event_probability=_env_float("SIMULATOR_EVENT_PROBABILITY", 0.001),
            simulator_seed=int(seed) if seed.lstrip("-").isdigit() else None,
        )


def create_market_data_source(
    price_cache: PriceCache, config: MarketDataConfig | None = None
) -> MarketDataSource:
    """Return an unstarted source. Caller must `await source.start(tickers)`."""
    cfg = config or MarketDataConfig.from_env()

    if cfg.massive_api_key:
        logger.info("Market data source: Massive API (poll every %.1fs)",
                    cfg.massive_poll_interval)
        return MassiveDataSource(
            api_key=cfg.massive_api_key,
            price_cache=price_cache,
            poll_interval=cfg.massive_poll_interval,
        )

    logger.info("Market data source: GBM simulator")
    rng = np.random.default_rng(cfg.simulator_seed) if cfg.simulator_seed is not None else None
    return SimulatorDataSource(
        price_cache=price_cache,
        update_interval=cfg.simulator_interval,
        event_probability=cfg.simulator_event_probability,
        rng=rng,
    )
```

The signature `create_market_data_source(cache)` still works as before. `config` is optional and exists for tests.

---

## 10. SSE Streaming — `stream.py`

### 10.1 Wire format

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":191.37,"previous_price":191.32,...},"GOOGL":{...}}

: ping

data: {...}
```

- One unnamed (`message`) event per change, carrying a **full snapshot** of every tracked ticker as `{ticker: PriceUpdate.to_dict()}`. Full snapshots make each frame self-contained: a reconnecting client is correct after one frame, and a ticker missing from a frame means it was removed. Ten tickers is about 2.5 KB every 500 ms, which is fine.
- Frames are sent only when `cache.version` has changed. That way the simulator pushes every ~500 ms and Massive pushes after each poll.
- `: ping` comment every 15 s with no data. It stops proxies and load balancers (App Runner, nginx) from closing an idle stream while Massive is quiet or the market is closed. `EventSource` ignores comments.
- `retry: 1000` makes the browser reconnect after 1 s.

### 10.2 Implementation

Change: create the `APIRouter` **inside** the factory. The current module-level `router` gets a duplicate `/prices` route every time the factory is called (for example, once per test app).

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",  # Disable proxy buffering
}


def create_stream_router(
    price_cache: PriceCache,
    interval: float = 0.5,
    heartbeat: float = 15.0,
) -> APIRouter:
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            generate_price_events(price_cache, request, interval, heartbeat),
            media_type="text/event-stream",
            headers=SSE_HEADERS,
        )

    return router


def format_snapshot(price_cache: PriceCache) -> str:
    data = {t: u.to_dict() for t, u in price_cache.get_all().items()}
    return f"data: {json.dumps(data, separators=(',', ':'))}\n\n"


async def generate_price_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
    heartbeat: float = 15.0,
) -> AsyncGenerator[str, None]:
    yield "retry: 1000\n\n"

    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)
    last_version = -1
    last_sent = time.monotonic()

    try:
        while not await request.is_disconnected():
            version = price_cache.version
            if version != last_version:
                last_version = version
                yield format_snapshot(price_cache)  # Empty cache → "data: {}"
                last_sent = time.monotonic()
            elif time.monotonic() - last_sent >= heartbeat:
                yield ": ping\n\n"
                last_sent = time.monotonic()
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("SSE client disconnected: %s", client)
```

Note the change from current behavior: an empty cache now sends `data: {}` instead of nothing, so when the last ticker is removed, clients clear their view.

### 10.3 Frontend consumption (contract example)

```ts
const es = new EventSource("/api/stream/prices");
es.onopen = () => setStatus("connected");
es.onerror = () => setStatus(es.readyState === EventSource.CLOSED ? "disconnected" : "reconnecting");
es.onmessage = (e) => {
  const snapshot: Record<string, PriceUpdate> = JSON.parse(e.data);
  for (const [ticker, u] of Object.entries(snapshot)) {
    if (u.timestamp !== lastTs[ticker]) {           // new tick → flash + sparkline point
      flash(ticker, u.direction);
      appendSparkline(ticker, u.timestamp, u.price);
      lastTs[ticker] = u.timestamp;
    }
  }
  setPrices(snapshot);                              // absent tickers → removed
};
```

The client deduplicates on `timestamp`. A frame can be resent because another ticker changed, and that must not add a duplicate sparkline point.

---

## 11. Ticker Tracking — `tracking.py`

**Problem:** PLAN says the source tracks "all tickers known to the system". Portfolio valuation needs live prices for **held positions** too, even after the user removes that ticker from the watchlist. If watchlist removal called `source.remove_ticker()` directly, the position would lose its price and the portfolio value would break.

**Rule:** `tracked = watchlist ∪ tickers with quantity > 0`. Every change to either set calls one reconciler. The reconciler has no DB dependency: the caller gives it the desired set.

```python
"""Keep the market data source's tracked set in sync with the desired set."""

from __future__ import annotations

import asyncio

from .interface import MarketDataSource

_lock = asyncio.Lock()


async def reconcile_tickers(source: MarketDataSource, desired: set[str]) -> None:
    """Add missing tickers, remove extras. Serialized so concurrent requests
    (e.g. a chat turn that buys and edits the watchlist) can't interleave."""
    async with _lock:
        current = set(source.get_tickers())
        for ticker in sorted(desired - current):
            await source.add_ticker(ticker)
        for ticker in sorted(current - desired):
            await source.remove_ticker(ticker)
```

The caller computes the desired set from the DB, for example in `app/services/market.py` (outside `app/market/`):

```python
async def sync_market_tickers(db, source, user_id: str = "default") -> None:
    desired = set(db.get_watchlist_tickers(user_id)) | set(db.get_position_tickers(user_id))
    await reconcile_tickers(source, desired)
```

When to call it:

| Event | Effect |
|---|---|
| App startup | `source.start(sorted(desired))` |
| `POST /api/watchlist` | adds the ticker (no-op if it's already held) |
| `DELETE /api/watchlist/{t}` | removes it **unless** a position is still held |
| Trade that opens a position in an un-watched ticker | (already tracked; see §12.3) |
| Sell to zero of an un-watched ticker | removes it |
| Chat actions | once, after all of a turn's trades and watchlist changes have run |

---

## 12. FastAPI Integration

### 12.1 App factory & lifespan (`app/main.py`)

The cache and source are created when the app is built, so the router can be wired up before startup. The source is **started** in the lifespan. Static files are mounted **last**, so the `/` catch-all doesn't swallow `/api/*`.

```python
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.db import get_db, init_db              # Built by the DB/backend agent
from app.market import PriceCache, create_market_data_source, create_stream_router


def create_app() -> FastAPI:
    price_cache = PriceCache()
    source = create_market_data_source(price_cache)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_db()                                    # Lazy schema + seed
        db = get_db()
        tickers = sorted(set(db.get_watchlist_tickers()) | set(db.get_position_tickers()))
        await source.start(tickers)
        try:
            yield
        finally:
            await source.stop()

    app = FastAPI(title="FinAlly", lifespan=lifespan)
    app.state.price_cache = price_cache
    app.state.market_source = source

    app.include_router(create_stream_router(price_cache))
    # app.include_router(portfolio_router); app.include_router(watchlist_router); ...

    static_dir = Path(__file__).parent.parent / "static"
    if static_dir.exists():
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
    return app


app = create_app()
```

Run with `uvicorn app.main:app --host 0.0.0.0 --port 8000` and **a single worker**. The cache is in-process, so more workers would mean more simulators producing different prices.

### 12.2 Dependencies (`app/deps.py`)

```python
from fastapi import Request

from app.market import MarketDataSource, PriceCache


def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache


def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

### 12.3 How consumers use the cache

**Watchlist GET:** join the DB rows with the cache. A missing price is `null`; that's normal for the first Massive poll or an unknown symbol.

```python
@router.get("/api/watchlist")
def get_watchlist(cache: PriceCache = Depends(get_price_cache), db=Depends(get_db)):
    items = []
    for row in db.get_watchlist():
        u = cache.get(row.ticker)
        items.append({"ticker": row.ticker, "added_at": row.added_at,
                      **(u.to_dict() if u else {"price": None})})
    return {"items": items}
```

**Watchlist POST / DELETE:** normalize, write to the DB, reconcile.

```python
@router.post("/api/watchlist", status_code=201)
async def add_to_watchlist(body: AddTicker, db=Depends(get_db),
                           source=Depends(get_market_source)):
    try:
        ticker = normalize_ticker(body.ticker)
    except ValueError as e:
        raise HTTPException(400, str(e))
    db.add_watchlist(ticker)                       # idempotent (UNIQUE constraint)
    await sync_market_tickers(db, source)
    return {"ticker": ticker}
```

**Trade execution:** the fill price comes from the cache. A trade in a ticker that isn't tracked yet starts tracking it first. The simulator fills it immediately; Massive returns a retryable error until the early poll arrives.

```python
async def resolve_fill_price(ticker: str, cache: PriceCache, source: MarketDataSource) -> float:
    if ticker not in source.get_tickers():
        await source.add_ticker(ticker)            # Reconciled again after the trade
    price = cache.get_price(ticker)
    if price is None:
        raise HTTPException(
            409, f"No live price for {ticker} yet — try again in a few seconds."
        )
    return price
```

After the trade: write the trade and position, take a portfolio snapshot, then `await sync_market_tickers(db, source)`. That call drops the ticker again if the trade failed or the position closed and the ticker isn't on the watchlist.

**Portfolio valuation:** `cache.get_price(t)` for each position. Fall back to `avg_cost` when the cache has no price, so the total never shows `NaN`, and flag that position as `"price_stale": true`.

**Chat context:** the system prompt includes `cache.get(t).to_dict()` for each watchlist and position ticker (price plus `day_change_percent`).

### 12.4 Public API — `__init__.py`

```python
from .cache import PriceCache
from .factory import MarketDataConfig, create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate, normalize_ticker
from .stream import create_stream_router
from .tracking import reconcile_tickers

__all__ = [
    "PriceUpdate", "PriceCache", "MarketDataSource", "MarketDataConfig",
    "create_market_data_source", "create_stream_router",
    "normalize_ticker", "reconcile_tickers",
]
```

Update `backend/CLAUDE.md` to match once this is implemented.

---

## 13. Error Handling & Edge Cases

| Situation | Behavior |
|---|---|
| Empty watchlist at startup | `start([])` works; the simulator loop steps nothing; Massive skips polls; SSE sends `data: {}` once |
| Simulator step raises | Logged with the traceback; the next tick runs as normal |
| Correlation matrix not positive-definite | Logged warning; independent draws used |
| Massive 401/403 | `logger.error` naming the key or plan; backoff up to 120 s; the cache keeps its last values |
| Massive 429 / network / 5xx | `logger.warning`; exponential backoff; reset on success |
| Snapshot missing `last_trade` | Falls back to `day.close`, then `prev_day.close`; skipped if none are > 0 |
| Unknown symbol (Massive) | Not in the response, so no price; trades return 409; the watchlist row shows "—" |
| Unknown symbol (simulator) | Gets a deterministic seed price; any well-formed symbol "exists" |
| Ticker removed mid-poll | The response for it is dropped (no resurrection) |
| Ticker on the watchlist and held, then removed from the watchlist | Stays tracked until the position closes (§11) |
| Lowercase or whitespace ticker from UI/LLM | `normalize_ticker`; an invalid format gets a 400 (API) or an error in the chat response (LLM) |
| SSE client disconnects | Generator exits within ≤ `interval`; logged |
| Idle feed (market closed, slow poll) | `: ping` every 15 s keeps the connection alive |
| Timestamp `0.0` | Kept as given (no longer replaced by "now") |
| Multiple uvicorn workers | Not supported; run a single worker (documented in §12.1) |

---

## 14. Testing

All tests live in `backend/tests/market/`, run with `uv run --extra dev pytest`, and use `asyncio_mode = "auto"`. The existing 73 tests should keep passing, apart from the deliberate updates noted below.

### 14.1 Models & cache

```python
def test_day_change_uses_reference():
    u = PriceUpdate("AAPL", price=195.0, previous_price=194.0, reference_price=190.0)
    assert u.day_change == 5.0
    assert u.day_change_percent == pytest.approx(2.6316, abs=1e-4)

def test_day_change_without_reference_is_zero():
    assert PriceUpdate("X", 10.0, 9.0).day_change_percent == 0.0

def test_reference_price_is_sticky():
    cache = PriceCache()
    cache.update("AAPL", 190.0, reference_price=188.0)
    assert cache.update("AAPL", 191.0).reference_price == 188.0

def test_zero_timestamp_preserved():
    assert PriceCache().update("AAPL", 1.0, timestamp=0.0).timestamp == 0.0

def test_remove_bumps_version():
    cache = PriceCache(); cache.update("AAPL", 1.0); v = cache.version
    cache.remove("AAPL"); assert cache.version == v + 1
    cache.remove("AAPL"); assert cache.version == v + 1   # no-op doesn't bump

def test_concurrent_writers():
    cache = PriceCache()
    def writer(t):
        for i in range(1000):
            cache.update(t, 100 + i)
    threads = [threading.Thread(target=writer, args=(f"T{i}",)) for i in range(8)]
    [t.start() for t in threads]; [t.join() for t in threads]
    assert cache.version == 8000 and len(cache) == 8

@pytest.mark.parametrize("raw,expected", [(" aapl ", "AAPL"), ("BRK.B", "BRK.B")])
def test_normalize_ticker(raw, expected):
    assert normalize_ticker(raw) == expected

@pytest.mark.parametrize("raw", ["", "123", "TOO-LONG-TICKER", "A B"])
def test_normalize_ticker_rejects(raw):
    with pytest.raises(ValueError):
        normalize_ticker(raw)
```

### 14.2 GBM simulator

```python
def test_deterministic_with_seed():
    a = GBMSimulator(["AAPL", "MSFT"], rng=np.random.default_rng(42))
    b = GBMSimulator(["AAPL", "MSFT"], rng=np.random.default_rng(42))
    assert [a.step() for _ in range(50)] == [b.step() for _ in range(50)]

def test_full_default_set_cholesky_succeeds():
    sim = GBMSimulator(list(SEED_PRICES))
    assert sim._cholesky is not None and sim._cholesky.shape == (10, 10)

def test_fifty_unknown_tickers_still_positive_definite():
    sim = GBMSimulator([f"ZZ{i}" for i in range(50)])
    assert sim._cholesky is not None

def test_prices_stay_positive_over_long_run():
    sim = GBMSimulator(["TSLA"], event_probability=0.05, rng=np.random.default_rng(1))
    for _ in range(20_000):
        assert sim.step()["TSLA"] > 0

def test_unknown_ticker_seed_is_stable():
    assert GBMSimulator(["PYPL"]).get_price("PYPL") == GBMSimulator(["PYPL"]).get_price("PYPL")
    assert 50 <= seed_price_for("PYPL") < 300

def test_empirical_volatility_matches_sigma():
    """Log-returns over many steps have std ≈ sigma * sqrt(dt)."""
    sim = GBMSimulator(["MSFT"], event_probability=0.0, rng=np.random.default_rng(7))
    sim._prices["MSFT"] = 1e6     # Avoid cent-rounding noise in the returned value
    prices = [sim.step()["MSFT"] for _ in range(20_000)]
    rets = np.diff(np.log(prices))
    expected = 0.20 * math.sqrt(GBMSimulator.DEFAULT_DT)
    assert rets.std() == pytest.approx(expected, rel=0.05)

def test_tech_correlation_higher_than_cross():
    sim = GBMSimulator(["AAPL", "MSFT", "JPM"], event_probability=0.0,
                       rng=np.random.default_rng(3))
    for t in sim.get_tickers(): sim._prices[t] = 1e6
    series = {t: [] for t in sim.get_tickers()}
    for _ in range(20_000):
        for t, p in sim.step().items(): series[t].append(p)
    r = {t: np.diff(np.log(v)) for t, v in series.items()}
    tech = np.corrcoef(r["AAPL"], r["MSFT"])[0, 1]
    cross = np.corrcoef(r["AAPL"], r["JPM"])[0, 1]
    assert tech == pytest.approx(0.6, abs=0.05) and cross == pytest.approx(0.3, abs=0.05)
```

### 14.3 `SimulatorDataSource`

```python
async def test_start_seeds_cache_with_reference():
    cache = PriceCache()
    src = SimulatorDataSource(cache, update_interval=0.01)
    await src.start(["AAPL"])
    try:
        u = cache.get("AAPL")
        assert u.price == 190.0 and u.reference_price == 190.0
        await asyncio.sleep(0.05)
        assert cache.version > 1 and cache.get("AAPL").reference_price == 190.0
    finally:
        await src.stop()

async def test_add_ticker_is_priced_immediately():
    cache = PriceCache(); src = SimulatorDataSource(cache, update_interval=10)
    await src.start([]); await src.add_ticker("PYPL")
    assert cache.get_price("PYPL") is not None
    await src.stop()
```

### 14.4 Massive: use **real SDK models**, not `MagicMock`

This is the test that would have caught the timestamp bug.

```python
from massive.rest.models import TickerSnapshot

def snap(ticker="AAPL", p=191.37, t_ns=1_761_431_520_512_000_000, prev_close=190.0, **extra):
    d = {"ticker": ticker, "prevDay": {"c": prev_close}, "day": {"c": p}, "updated": t_ns}
    if p is not None:
        d["lastTrade"] = {"p": p, "t": t_ns}
    d.update(extra)
    return TickerSnapshot.from_dict(d)

def test_parse_snapshot_real_model():
    q = parse_snapshot(snap())
    assert q.price == 191.37 and q.reference_price == 190.0
    assert q.timestamp == pytest.approx(1_761_431_520.512)   # ns → s

def test_parse_falls_back_to_day_close():
    s = TickerSnapshot.from_dict({"ticker": "AAPL", "day": {"c": 150.0}, "prevDay": {"c": 149.0}})
    assert parse_snapshot(s).price == 150.0

def test_parse_rejects_no_price():
    s = TickerSnapshot.from_dict({"ticker": "AAPL", "day": {"c": 0}})
    assert parse_snapshot(s) is None

async def test_poll_writes_cache_and_drops_removed():
    cache = PriceCache()
    client = MagicMock()
    client.get_snapshot_all.return_value = [snap("AAPL"), snap("GOOGL", p=175.0)]
    src = MassiveDataSource("k", cache, poll_interval=60, client=client)
    src._tickers = ["AAPL"]                       # GOOGL "removed mid-flight"
    await src._poll_once()
    assert cache.get_price("AAPL") == 191.37 and "GOOGL" not in cache
    client.get_snapshot_all.assert_called_once()
    assert client.get_snapshot_all.call_args.kwargs["tickers"] == ["AAPL"]

async def test_failure_backs_off_then_resets():
    client = MagicMock(); client.get_snapshot_all.side_effect = RuntimeError("429")
    src = MassiveDataSource("k", PriceCache(), poll_interval=15, client=client)
    src._tickers = ["AAPL"]
    await src._poll_once(); await src._poll_once()
    assert src._current_delay() == 60
    client.get_snapshot_all.side_effect = None; client.get_snapshot_all.return_value = []
    await src._poll_once()
    assert src._current_delay() == 15

async def test_add_ticker_wakes_early_poll():
    client = MagicMock(); client.get_snapshot_all.return_value = []
    src = MassiveDataSource("k", PriceCache(), poll_interval=60,
                            min_poll_spacing=0.0, client=client)
    await src.start(["AAPL"])
    await src.add_ticker("MSFT"); await asyncio.sleep(0.05)
    assert client.get_snapshot_all.call_count == 2
    await src.stop()
```

Replace the `MagicMock` snapshots in the existing `test_massive.py` (`_make_snapshot`, `test_timestamp_conversion`) with `TickerSnapshot.from_dict`.

### 14.5 SSE (`test_stream.py`)

Test the generator directly with a fake request. That avoids an infinite `StreamingResponse` inside `TestClient`.

```python
class FakeRequest:
    def __init__(self, disconnect_after: int):
        self.client = None; self._n = disconnect_after
    async def is_disconnected(self):
        self._n -= 1
        return self._n < 0

async def collect(gen):
    return [chunk async for chunk in gen]

async def test_first_frames():
    cache = PriceCache(); cache.update("AAPL", 190.0)
    out = await collect(generate_price_events(cache, FakeRequest(1), interval=0))
    assert out[0] == "retry: 1000\n\n"
    payload = json.loads(out[1].removeprefix("data: "))
    assert payload["AAPL"]["price"] == 190.0 and payload["AAPL"]["direction"] == "flat"

async def test_no_duplicate_frames_without_change():
    cache = PriceCache(); cache.update("AAPL", 190.0)
    out = await collect(generate_price_events(cache, FakeRequest(5), interval=0, heartbeat=999))
    assert sum(c.startswith("data:") for c in out) == 1

async def test_heartbeat_when_idle():
    cache = PriceCache(); cache.update("AAPL", 190.0)
    out = await collect(generate_price_events(cache, FakeRequest(3), interval=0, heartbeat=0))
    assert ": ping\n\n" in out

def test_router_factory_is_isolated():
    r1, r2 = create_stream_router(PriceCache()), create_stream_router(PriceCache())
    assert len(r1.routes) == len(r2.routes) == 1
```

### 14.6 Tracking (`test_tracking.py`)

```python
async def test_reconcile_adds_and_removes():
    cache = PriceCache(); src = SimulatorDataSource(cache, update_interval=10)
    await src.start(["AAPL", "GOOGL"])
    await reconcile_tickers(src, {"AAPL", "PYPL"})
    assert set(src.get_tickers()) == {"AAPL", "PYPL"}
    assert "GOOGL" not in cache and cache.get_price("PYPL") is not None
    await src.stop()
```

### 14.7 Factory

```python
def test_factory_reads_poll_interval(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "k"); monkeypatch.setenv("MASSIVE_POLL_INTERVAL", "5")
    src = create_market_data_source(PriceCache())
    assert isinstance(src, MassiveDataSource) and src._interval == 5.0

def test_factory_invalid_interval_falls_back(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "k"); monkeypatch.setenv("MASSIVE_POLL_INTERVAL", "abc")
    assert create_market_data_source(PriceCache())._interval == 15.0

def test_whitespace_key_means_simulator(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "   ")
    assert isinstance(create_market_data_source(PriceCache()), SimulatorDataSource)
```

### 14.8 Manual smoke test against the real API (optional, needs a key)

```bash
cd backend
MASSIVE_API_KEY=... uv run python -c "
import asyncio
from app.market import PriceCache, create_market_data_source
async def main():
    c = PriceCache(); s = create_market_data_source(c)
    await s.start(['AAPL','MSFT','NVDA']); await s.stop()
    for u in c.get_all().values(): print(u.to_dict())
asyncio.run(main())"
```

Each tracked ticker should print a price and a timestamp in seconds. Under the current code this prints nothing (the §8 defect).

---

## 15. Change Checklist vs. Current Code

Apply in this order; each step keeps the suite green.

**Must fix (correctness)**
- [ ] `massive_client.py`: add `parse_snapshot()` using `last_trade.sip_timestamp` (ns) with `updated` and `time.time()` fallbacks, plus the price fallback chain (§8.3)
- [ ] `massive_client.py`: copy the ticker list before `to_thread`; drop snapshots for tickers removed mid-flight (§8.4)
- [ ] `test_massive.py`: build fixtures from `TickerSnapshot.from_dict` instead of `MagicMock` (§14.4)
- [ ] `stream.py`: create the `APIRouter` inside `create_stream_router` (§10.2)
- [ ] `cache.py`: `timestamp is None` check; `remove()` bumps version; lock on the `version` read (§5)

**Platform needs**
- [ ] `models.py`: `reference_price`, `day_change`, `day_change_percent`, added to `to_dict()`; `normalize_ticker()` (§4)
- [ ] `cache.py`: `reference_price` parameter, sticky across updates (§5)
- [ ] `simulator.py`: session-open reference price; seed the cache with it (§7.3–7.4)
- [ ] `massive_client.py`: `prev_day.close` as reference (§8.3)
- [ ] `tracking.py`: `reconcile_tickers()`; app layer tracks watchlist ∪ positions (§11)
- [ ] `stream.py`: `: ping` heartbeat; send `data: {}` when the cache is empty (§10)

**Robustness / ergonomics**
- [ ] `factory.py`: `MarketDataConfig.from_env()` with `MASSIVE_POLL_INTERVAL`, `SIMULATOR_*` (§9); update `.env.example`
- [ ] `simulator.py`: injectable `np.random.Generator`; `seed_price_for()` for unknown tickers; Cholesky `LinAlgError` guard (§7)
- [ ] `massive_client.py`: early poll on `add_ticker` with minimum spacing; exponential backoff (§8.4)
- [ ] Remove internal upper-casing from `MassiveDataSource`; normalize at API boundaries instead (§6)
- [ ] New tests: §14.1–14.7. Update `backend/CLAUDE.md` and `MARKET_DATA_SUMMARY.md`
