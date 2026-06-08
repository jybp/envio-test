# Envio Block Handler Demo — Daily USDC TotalSupply Snapshots

Demo project for daily on-chain snapshots using Envio [block handlers](https://docs.envio.dev/docs/HyperIndex/block-handlers). Tracks USDC `totalSupply()` on Ethereum mainnet.

## Quick start

```shell
nvm use
cp .env.example .env  # set RPC URL + Envio API token
pnpm install
pnpm codegen
pnpm dev
```

GraphQL: http://localhost:8080/console

```shell
pnpm stop
SNAPSHOT_STRATEGY=daily pnpm dev
SNAPSHOT_STRATEGY=hourly_filter pnpm dev
SNAPSHOT_STRATEGY=exact_midnight pnpm dev
```

## Problem: block intervals drift from wall-clock time

Block handlers fire on fixed block intervals (`_every: N`), not timestamps. On mainnet (~12s/block), `_every: 7200` drifts from midnight UTC due to missed slots and variable block times. Observed: **~7 min/day drift**, so ~1.5h after 2 weeks.

Additionally, `block.timestamp` is [not available](https://docs.envio.dev/docs/v2/HyperIndex/block-handlers#current-limitations) in the block handler callback — any timestamp-aware logic requires an RPC call via the Effect API.

https://www.alchemy.com/docs/chains/ethereum/ethereum-api-endpoints/eth-get-block-by-number

## Three strategies compared

| Strategy | `_every` | Accuracy | RPC calls/day
|---|---|---|---|
| `daily` | 7200 | Drifts ~7min/day (+80min after 2 weeks) | 1 (totalSupply) |
| `hourly_filter` | 300 | ±30min of midnight | 24 (timestamp) + 1 |
| `exact_midnight` | 1 | ±12s | ~7200 (timestamp) + 1 |

## What would solve this

If `block.timestamp` were exposed in the handler callback, `exact_midnight` — just check the timestamp and store on day change. Fast and exact.

## Questions

1. **`block.timestamp`** — could it be exposed in the block handler callback?
2. **Timestamp-aligned intervals** — any way to fire "at first block after midnight UTC" rather than every N blocks?

TBD proper caching strategy for historical sync.