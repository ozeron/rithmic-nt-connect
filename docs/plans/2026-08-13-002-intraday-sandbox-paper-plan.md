# Plan: Paper-trade simple NQ intraday (sandbox)

Date: 2026-08-13  
Repo: `rithmic-nt-connect`  
Status: code landed on `feat/intraday-sandbox-paper`; sandbox Lucid proof done (2026-08-13); live EXTERNAL bar Lucid proof still open (see `docs/STATUS.md`)  

## Goal

Run a simple **intraday NQ** strategy on NautilusTrader **1.231.x** `TradingNode` with:

- Live market data from Rithmic (LucidTrading, front-month NQ)
- Paper execution on Nautilus **Sandbox** (`SandboxLiveExecClientFactory`)
- No Rithmic place (`enable_trading` off; no `--live-place`)

```
TradingNode
  data:  RithmicLiveDataClientFactory   venue RITHMIC
  exec:  SandboxLiveExecClientFactory   venue="RITHMIC"
```

Sandbox `venue` must be `RITHMIC`. `trade_execution=True` (and `bar_execution=True` if using INTERNAL bars). Do not also register the Rithmic exec client on that node.

## In this change

- [x] Ticker reconnect + restore subscription intent
- [x] Book snapshot flags `F_SNAPSHOT | F_LAST`
- [x] `examples/live_nq_intraday_sandbox.py`
- [x] README / ops / STATUS notes
- [x] Live Lucid proof of the sandbox example (2026-08-13: NQU6 + INTERNAL bars)

## Out of scope (later)

Rithmic exec honesty, account auto-discovery, authorized `app_name` / live place, brackets, depth, venue EXTERNAL bars.

## Stop

- Never `RITHMIC_ENABLE_TRADING=1` in the example.
- Never `cancel_all_orders` on the Rithmic plant.
- Do not advertise INTERNAL bars as venue EXTERNAL.
