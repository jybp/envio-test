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
SNAPSHOT_STRATEGY=hypersync_midnight pnpm dev
```

## Problem: block intervals drift from wall-clock time

Block handlers fire on fixed block intervals (`_every: N`), not timestamps. On mainnet (~12s/block), `_every: 7200` drifts from midnight UTC due to missed slots and/or variable block times. 

Observed: **~7 min/day drift**, so ~1.5h after 2 weeks.

Additionally, `block.timestamp` is [not available](https://docs.envio.dev/docs/v2/HyperIndex/block-handlers#current-limitations) in the block handler callback — any timestamp-aware logic requires an RPC call via the Effect API.

> Only block number is provided in the block object. We'll definitely add more fields in the future.

https://www.alchemy.com/docs/chains/ethereum/ethereum-api-endpoints/eth-get-block-by-number

## Four strategies compared

| Strategy | `_every` | Accuracy | Timestamp source |
|---|---|---|---|
| `daily` | 7200 | Drifts ~7min/day (+80min after 2 weeks) | RPC |
| `hourly_filter` | 300 | ±30min of midnight | RPC |
| `exact_midnight` | 1 | ±12s | RPC (slow) |
| `hypersync_midnight` | 1 | ±12s | HyperSync (fast) |

## What would solve this

If `block.timestamp` were exposed in the handler callback, `exact_midnight` — just check the timestamp and store on day change. Fast and exact.

## Questions

1. **`block.timestamp`** — could it be exposed in the block handler callback?
2. **Timestamp-aligned intervals** — any way to fire "at first block after midnight UTC" rather than every N blocks?

TBD proper caching strategy for historical sync.

## Local benchmarks (free tier HyperSync, 14 days mainnet)

- **`daily`**: ~8s/day, drifted +80min over 14 days
- **`hourly_filter`**: ~7s/day, stayed within ±30min of midnight
- **`exact_midnight`**: ~13min/day, ±11s accuracy — works but impractical (RPC per block)
- **`hypersync_midnight`**: ~15s/day, ±11s accuracy — uses HyperSync for batch timestamp lookup. Requires paid HyperSync plan or Envio Cloud (free tier rate limit makes `_every: 1` unusable on clean start)

## Considered: `Date.now()` boundary detection

Another possibility was firing on every block and using `Date.now()` to detect 24h wall-clock boundaries — no RPC needed, only fetch data when the boundary crosses. 
However, `Date.now()` is real time, not block time for historical sync. Only viable for realtime-only use cases.

## Could we precompute midnight blocks?

APIs like [Etherscan's `getblocknobytime`](https://docs.etherscan.io/api-reference/endpoint/getblocknobytime) return the block number closest to a given timestamp. We could precompute all midnight UTC blocks upfront and only snapshot those exact blocks — no drift, no per-block RPC calls.

Unsure: A [preset handler](https://docs.envio.dev/docs/HyperIndex/block-handlers#preset-handler) could call `getblocknobytime` for every midnight timestamp in the indexing range, store those block numbers as entities, then the hourly block handler checks if the current block is one of them. No drift, no per-block RPC — just one API call per day at init time?