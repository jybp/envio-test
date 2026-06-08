import { createEffect, indexer, S } from 'envio';
import { createPublicClient, http, type PublicClient } from 'viem';

// ---------------------------------------------------------------------------
// Strategy selection via SNAPSHOT_STRATEGY env var:
//
//   daily          — fire every 7200 blocks (~24h). Simple, drifts over time.
//   hourly_filter  — fire every 300 blocks (~1h), only store if timestamp is
//                    within 30 min of midnight UTC. One entry/day, low drift.
//   exact_midnight — fire every block, RPC-fetch timestamp, store only when
//                    crossing a midnight UTC boundary. Exact but slow to sync.
//
// Default: hourly_filter
// ---------------------------------------------------------------------------

const STRATEGY = (process.env.SNAPSHOT_STRATEGY || 'hourly_filter') as
    | 'daily'
    | 'hourly_filter'
    | 'exact_midnight';

console.log(`[Snapshot] Strategy: ${STRATEGY}`);

// ---------------------------------------------------------------------------
// USDC on Ethereum mainnet
// ---------------------------------------------------------------------------

const USDC_ADDRESS = '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' as const;

const USDC_ABI = [
    {
        type: 'function' as const,
        name: 'totalSupply' as const,
        inputs: [] as const,
        outputs: [{ name: '', type: 'uint256' as const }] as const,
        stateMutability: 'view' as const
    }
] as const;

// ---------------------------------------------------------------------------
// RPC client (lazily created)
// ---------------------------------------------------------------------------

let client: PublicClient | undefined;

function getClient(): PublicClient | undefined {
    if (client) return client;
    const rpcUrl = process.env.ENVIO_1_RPC_URL;
    if (!rpcUrl) return undefined;
    client = createPublicClient({ transport: http(rpcUrl, { batch: true }) });
    return client;
}

// ---------------------------------------------------------------------------
// Effects
// ---------------------------------------------------------------------------

const getTotalSupply = createEffect(
    {
        name: 'getUsdcTotalSupply',
        input: { blockNumber: S.bigint },
        output: S.bigint,
        rateLimit: { calls: 10, per: 'second' },
        cache: true
    },
    async ({ input, context }) => {
        const c = getClient();
        if (!c) {
            context.log.error('No RPC client (ENVIO_1_RPC_URL not set)');
            return 0n;
        }
        try {
            return await c.readContract({
                address: USDC_ADDRESS,
                abi: USDC_ABI,
                functionName: 'totalSupply',
                blockNumber: input.blockNumber
            });
        } catch (error) {
            context.log.error(`totalSupply call failed at block ${input.blockNumber}: ${error}`);
            return 0n;
        }
    }
);

const getBlockTimestamp = createEffect(
    {
        name: 'getBlockTimestamp',
        input: { blockNumber: S.bigint },
        output: S.number,
        rateLimit: { calls: 10, per: 'second' },
        cache: true
    },
    async ({ input, context }) => {
        const c = getClient();
        if (!c) return 0;
        try {
            const block = await c.getBlock({ blockNumber: input.blockNumber });
            return Number(block.timestamp);
        } catch (error) {
            context.log.error(`getBlock failed for ${input.blockNumber}: ${error}`);
            return 0;
        }
    }
);

// ---------------------------------------------------------------------------
// Shared
// ---------------------------------------------------------------------------

const SECONDS_PER_DAY = 86400;

function dayOf(timestamp: number): number {
    return Math.floor(timestamp / SECONDS_PER_DAY);
}

async function storeSnapshot(block: { number: number }, context: any, blockTimestamp: number) {
    const totalSupply = await context.effect(getTotalSupply, {
        blockNumber: BigInt(block.number)
    });

    context.TotalSupplySnapshot.set({
        id: `usdc_day_${dayOf(blockTimestamp)}`,
        totalSupply,
        blockNumber: block.number,
        blockTimestamp
    });

    context.log.info(
        `[${STRATEGY}] block ${block.number} (${new Date(blockTimestamp * 1000).toISOString()}): totalSupply = ${totalSupply}`
    );
}

const MIDNIGHT_WINDOW = 30 * 60; // 30 minutes

function isNearMidnight(timestamp: number): boolean {
    const secondsInDay = timestamp % SECONDS_PER_DAY;
    return secondsInDay <= MIDNIGHT_WINDOW || secondsInDay >= SECONDS_PER_DAY - MIDNIGHT_WINDOW;
}

// ---------------------------------------------------------------------------
// Strategy 1: DAILY
// ---------------------------------------------------------------------------

if (STRATEGY === 'daily') {
    indexer.onBlock(
        {
            name: 'DailySnapshot',
            where: ({ chain }) => {
                if (chain.id !== 1) return false;
                return { block: { number: { _every: 7200 } } };
            }
        },
        async ({ block, context }) => {
            const blockTimestamp = await context.effect(getBlockTimestamp, {
                blockNumber: BigInt(block.number)
            });
            await storeSnapshot(block, context, blockTimestamp);
        }
    );
}

// ---------------------------------------------------------------------------
// Strategy 2: HOURLY_FILTER
// ---------------------------------------------------------------------------

if (STRATEGY === 'hourly_filter') {
    indexer.onBlock(
        {
            name: 'HourlyFilterSnapshot',
            where: ({ chain }) => {
                if (chain.id !== 1) return false;
                return { block: { number: { _every: 300 } } };
            }
        },
        async ({ block, context }) => {
            const blockTimestamp = await context.effect(getBlockTimestamp, {
                blockNumber: BigInt(block.number)
            });
            if (!isNearMidnight(blockTimestamp)) return;
            await storeSnapshot(block, context, blockTimestamp);
        }
    );
}

// ---------------------------------------------------------------------------
// Strategy 3: EXACT_MIDNIGHT
// NOTE: _every: 1 registers but never fires — suspected Envio bug.
// The handler callback is never invoked despite where() being called.
// ---------------------------------------------------------------------------

if (STRATEGY === 'exact_midnight') {
    // WARNING: lastSeenDay may be stale after indexer restart (Envio replays
    // blocks and the in-memory variable gets set from replayed data). The
    // entity ID (usdc_day_N) handles dedup via overwrite, but lastSeenDay
    // prevents unnecessary RPC calls on every block within the same day.
    let lastSeenDay = -1;

    indexer.onBlock(
        {
            name: 'ExactMidnightSnapshot',
            where: ({ chain }) => {
                if (chain.id !== 1) return false;
                return { block: { number: { _every: 1 } } };
            }
        },
        async ({ block, context }) => {
            const blockTimestamp = await context.effect(getBlockTimestamp, {
                blockNumber: BigInt(block.number)
            });
            const day = dayOf(blockTimestamp);
            if (day === lastSeenDay) return;
            lastSeenDay = day;
            await storeSnapshot(block, context, blockTimestamp);
        }
    );
}
