# Quantum Breadth 360

**Q-BRAM v2 powered Market Breadth Intelligence Platform**

Real-time market breadth dashboard for **India (NIFTY 500)** and **US (S&P 500)**, powered by the proprietary Q-BRAM v2 (Quantitative Breadth & Regime Analysis Model) engine.

## Q-BRAM v2 — 7-Component Scoring Engine

| Component | Points | Description |
|-----------|--------|-------------|
| B50 (% > 50 DMA) | 20 | Primary breadth indicator |
| NH-NL Ratio | 15 | New Highs minus New Lows / Total |
| **Breadth Thrust** | **15** | Advancers / Total (3-day EMA smoothed) |
| B200 (% > 200 DMA) | 15 | Long-term trend confirmation |
| B20 Acceleration | 10 | Change in % above 20-DMA (3-day EMA smoothed) |
| Volume Thrust | 10 | Up volume vs Down volume ratio |
| **CSD (Dispersion)** | **15** | Cross-Sectional Dispersion (3-day EMA, inverse) |
| **Total** | **100** | 5 regimes: EXPANSION → ACCUMULATION → TRANSITION → DISTRIBUTION → PANIC |

**v2 Enhancements:** 3-day EMA smoothing on fast components, 2-day regime confirmation rule (PANIC is immediate), Cross-Sectional Dispersion for early crisis detection.

## Features

- **Q-BRAM v2 Regime Engine** — 5-state model with 7-component scoring, smoothing, and regime confirmation
- **Leaders Tab** — Elite Leaders, Emerging Leaders, Under Pressure & Mean Reversion — all 4 tiers always visible
- **RS Rankings** — IBD-style M2+M3 formula matching MarketSmith, full NIFTY 500 universe
- **A/D Rating** — IBD Accumulation/Distribution 11-grade system (A+ through E)
- **Sector Health** — Sector RS scores with ↑↓ trend arrows, click-to-filter
- **6 Custom Screeners** — SVRO, Qullamaggie Breakout, Episodic Pivot, Mean Reversion, Manas Arora, VCP Minervini
- **Full-View Modal** — Paginated sortable table (20/page) with PNG/Excel/PDF export
- **Market Cap Filter** — Mega / Large / Mid / Small / Micro tier filtering
- **Breadth Charts** — A/D Line, % Above DMA, NH-NL, IV Footprint
- **Screener Tab** — 16 screeners (10 built-in + 6 custom AFL translations)
- **Scanner Tab** — Top movers, volume spikes, popular scans
- **Stockbee MB** — T2108, Up/Down 4%, 5D & 10D ratios
- **Smart Metrics** — Techno-fundamental analysis per ticker
- **Charts Tab** — Lightweight-charts OHLCV with overlays (VCP, PPV, Bull Snort, FVG, RS Line)
- **Peep Into Past** — Historical breadth analysis for any date
- **Watchlist + Alerts** — Full CRUD with price/DMA alerts
- **Light/Dark Theme** — Toggle with system preference detection

## Quick Start

```bash
cd backend
pip install -r requirements.txt
python main.py
# Open http://localhost:8001
```

## Architecture

```
Quantum-Breadth-360/
├── backend/
│   ├── main.py                 # FastAPI orchestration layer
│   ├── screeners.py            # RS Rankings, Leaders, Custom Screeners
│   ├── breadth.py              # Q-BRAM compute engine
│   ├── data_store.py           # SQLite OHLCV storage
│   ├── nse_sync.py             # NIFTY 500 Yahoo v8 sync
│   ├── charts.py               # Chart data endpoints
│   ├── smart_metrics_service.py # Techno-fundamental analysis
│   └── ... (17 modules total)
└── frontend/
    ├── index.html              # App shell
    ├── css/styles.css          # Full design system
    └── js/                     # Modular JS (one file per tab)
        ├── leaders.js          # Leaders tab engine
        ├── screeners.js        # Screeners + Scanner
        ├── app.js              # Core app, routing, theme
        └── ... (14 JS modules)
```

## Data

- **India:** NIFTY 500 universe via Yahoo Finance / SQLite local DB
- **US:** S&P 500 via Yahoo Finance
- **Benchmark:** ^CRSLDX (NIFTY 500 index) for RS calculations
- **Local DB:** 10-year OHLCV history in SQLite

## About

Built on the Q-BRAM v2 engine — a proprietary 7-component quantitative breadth regime model with validated signal stack:
`BULLISH regime + Pocket Pivot + RS>85 + Stage 2 = 61.8% win rate, +2.74% mean 20-day return`

v2 adds Breadth Thrust and Cross-Sectional Dispersion for faster regime detection (1-2 days vs 3-5 days in v1), 43% max drawdown reduction in backtests, and 58% fewer false PANIC signals.
