import { createEffect, indexer, S } from 'envio';
import { createPublicClient, http, type PublicClient } from 'viem';
import { HypersyncClient } from '@envio-dev/hypersync-client';

// ---------------------------------------------------------------------------
// Strategy selection via SNAPSHOT  _STRATEGY env var:
//
//   daily              — fire every 7200 blocks (~24h). Simple, drifts.
//   hourly_filter      — fire every 300 blocks (~1h), store near midnight only.
//   exact_midnight     — fire every block via RPC timestamp. Exact but slow.
//   hypersync_midnight — fire every block, batch-fetch timestamps via HyperSync.
//                        Exact and fast. Recommended by Envio team.
//
// Default: hourly_filter
// ---------------------------------------------------------------------------

const STRATEGY = (process.env.SNAPSHOT_STRATEGY || 'hourly_filter') as
    | 'daily'
    | 'hourly_filter'
    | 'exact_midnight'
    | 'hypersync_midnight'
    | 'hypersync_midnight_v2';

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

// RPC-based timestamp lookup (used by daily, hourly_filter, exact_midnight)
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

function fmtTs(timestamp: number): string {
    return new Date(timestamp * 1000).toISOString();
}

function fmtDay(day: number): string {
    if (day < 0) return 'unset';
    // day * 86400 gives Unix timestamp at 00:00:00 UTC of that day.
    // Date constructor handles leap years, DST, etc.
    return new Date(day * SECONDS_PER_DAY * 1000).toISOString().slice(0, 10);
}

async function storeSnapshot(block: { number: number }, context: any, blockTimestamp: number) {
    const entityId = `usdc_${fmtDay(dayOf(blockTimestamp)).replaceAll('-', '_')}`;

    // Check if this day's snapshot already exists — skip the expensive RPC
    // totalSupply call if so. Due to FIFO microtask ordering, the first block
    // of the day should set() before subsequent blocks reach this get().
    const existing = await context.TotalSupplySnapshot.get(entityId);
    if (existing) return;

    const totalSupply = await context.effect(getTotalSupply, {
        blockNumber: BigInt(block.number)
    });

    context.TotalSupplySnapshot.set({
        id: entityId,
        totalSupply,
        blockNumber: block.number,
        blockTimestamp
    });

    context.log.info(
        `[${STRATEGY}] block ${block.number} (${fmtTs(blockTimestamp)}): totalSupply = ${totalSupply}`
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
// Strategy 3: EXACT_MIDNIGHT (RPC)
// NOTE: _every: 1 with RPC timestamp is very slow during historical sync.
// ---------------------------------------------------------------------------

if (STRATEGY === 'exact_midnight') {
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
            if (lastSeenDay === -1) {
                lastSeenDay = day;
                return;
            }
            if (day <= lastSeenDay) return;
            lastSeenDay = day;
            await storeSnapshot(block, context, blockTimestamp);
        }
    );
}

// ---------------------------------------------------------------------------
// Strategy 4: HYPERSYNC_MIDNIGHT
// Fires every block but uses HyperSync to
// batch-fetch timestamps (no RPC calls). Only stores on day boundary.
// See: https://github.com/enviodev/hyperindex/issues/748#issuecomment-3722929189
// ---------------------------------------------------------------------------


// HyperSync-based batched timestamp lookup (used by hypersync_midnight).
// Uses queueMicrotask to accumulate multiple block number requests from the
// same tick into a single HyperSync range query. Much faster than individual
// RPC calls during historical sync.
const initHypersyncTimestamp =  () => {

    const hsClient = new HypersyncClient({
        url: `https://1.hypersync.xyz`,
        apiToken: process.env.ENVIO_API_TOKEN!,
    });

    let pendingBatch: {
        blockNumbers: Set<number>;
        resolvers: Map<number, { resolve: (ts: number) => void; reject: (e: Error) => void }[]>;
        scheduled: boolean;
    } | null = null;

    return createEffect(
        {
            name: 'getBlockTimestampHypersync',
            input: S.number,
            output: S.number,
            rateLimit: false
        },
        async ({ input: blockNumber }) => {
            if (!pendingBatch) {
                pendingBatch = { blockNumbers: new Set(), resolvers: new Map(), scheduled: false };
            }

            const batch = pendingBatch;
            // console.log(`[hypersync_midnight] Queuing block ${blockNumber} for timestamp fetch (batch size: ${batch.blockNumbers.size + 1})`);
            batch.blockNumbers.add(blockNumber);

            const promise = new Promise<number>((resolve, reject) => {
                const existing = batch.resolvers.get(blockNumber) || [];
                existing.push({ resolve, reject });
                batch.resolvers.set(blockNumber, existing);
            });

            if (!batch.scheduled) {
                batch.scheduled = true;

                // console.log(`[hypersync_midnight] Scheduling batch timestamp fetch for ${batch.blockNumbers.size} blocks...`);

                queueMicrotask(async () => {
                    const currentBatch = batch;
                    pendingBatch = null;

                    const blockNumbers = Array.from(currentBatch.blockNumbers);
                    const minBlock = Math.min(...blockNumbers);
                    const maxBlock = Math.max(...blockNumbers);

                    try {
                        const timestampMap = new Map<number, number>();
                        let nextBlock = minBlock;
                        const toBlock = maxBlock + 1;

                        // console.log(`[hypersync_midnight] Fetching timestamps for blocks ${minBlock} to ${maxBlock} via HyperSync...`);

                        while (nextBlock < toBlock) {
                            const data = await hsClient.get({
                                fromBlock: nextBlock,
                                toBlock,
                                includeAllBlocks: true,
                                fieldSelection: { block: ['Number', 'Timestamp'] }
                            });

                            for (const block of data.data.blocks) {
                                if (block.number !== undefined && block.timestamp !== undefined) {
                                    // console.log(`[hypersync_midnight] Fetched block ${block.number} with timestamp ${block.timestamp}`);
                                    timestampMap.set(block.number, block.timestamp);
                                }
                            }

                            nextBlock = data.nextBlock;
                        }

                        for (const [blockNum, resolverList] of currentBatch.resolvers) {
                            const timestamp = timestampMap.get(blockNum);
                            for (const { resolve, reject } of resolverList) {
                                if (timestamp !== undefined) {
                                    resolve(timestamp);
                                } else {
                                    reject(new Error(`Timestamp not found for block ${blockNum}`));
                                }
                            }
                        }
                    } catch (error) {
                        console.log(`[hypersync_midnight] Error fetching timestamps: ${error instanceof Error ? error.message : String(error)}`);
                        for (const resolverList of currentBatch.resolvers.values()) {
                            for (const { reject } of resolverList) {
                                reject(error instanceof Error ? error : new Error(String(error)));
                            }
                        }
                    }
                });
            } 
            return promise;
        }
    );
};


// ---------------------------------------------------------------------------
// Strategy 4: HYPERSYNC_MIDNIGHT
// Uses lastSeenDay variable to detect day boundary changes.
// Relies on FIFO microtask ordering guarantee (ECMAScript spec) to ensure
// getOrCreate runs in block order. No threshold needed — stores exactly on
// day boundary change.
// ---------------------------------------------------------------------------

if (STRATEGY === 'hypersync_midnight') {
    const getTimestampHS = initHypersyncTimestamp();
    let lastSeenDay = -1;

    indexer.onBlock(
        {
            name: 'HypersyncMidnightSnapshot',
            where: ({ chain }) => {
                if (chain.id !== 1) return false;
                return { block: { number: { _every: 1 } } };
            }
        },
        async ({ block, context }) => {
            const blockTimestamp = await context.effect(getTimestampHS, block.number);
            const day = dayOf(blockTimestamp);

            if (lastSeenDay === -1) {
                context.log.info(`[hypersync_midnight] First block ${block.number} ${fmtTs(blockTimestamp)} day=${fmtDay(day)}`);
                lastSeenDay = day;
                return;
            }

            if (day <= lastSeenDay) return;

            context.log.info(`[hypersync_midnight] DAY CHANGE block ${block.number} ${fmtTs(blockTimestamp)} day=${fmtDay(day)}`);
            lastSeenDay = day;
            await storeSnapshot(block, context, blockTimestamp);
        }
    );
}

// ---------------------------------------------------------------------------
// Strategy 5: HYPERSYNC_MIDNIGHT_V2
// Uses a MIDNIGHT_THRESHOLD window instead of in-memory state.
// Concurrency-safe by design — no shared mutable variable across handlers.
// getOrCreate in storeSnapshot ensures first block of the day wins.
// ---------------------------------------------------------------------------

if (STRATEGY === 'hypersync_midnight_v2') {
    const getTimestampHS = initHypersyncTimestamp();

    // Max seconds into a UTC day for a block to count as "midnight".
    // With _every: 1 on mainnet (~12s blocks), the first block of a new day
    // is at most ~12s past 00:00:00. Use 120s for safety margin.
    // TODO mainnet specific.
    const MIDNIGHT_THRESHOLD = 120;

    indexer.onBlock(
        {
            name: 'HypersyncMidnightV2Snapshot',
            where: ({ chain }) => {
                if (chain.id !== 1) return false;
                return { block: { number: { _every: 1 } } };
            }
        },
        async ({ block, context }) => {
            const blockTimestamp = await context.effect(getTimestampHS, block.number);
            const secondsInDay = blockTimestamp % SECONDS_PER_DAY;

            // Only consider blocks in the first 2 minutes of a UTC day.
            // This is required because of concurrency safety: onBlock handlers
            // keep being invoked while previous invocations are still awaiting
            // the getTimestampHS effect. No in-memory state outside the handler
            // scope can be safely used.
            if (secondsInDay > MIDNIGHT_THRESHOLD) return;

            const day = dayOf(blockTimestamp);
            const entityId = `usdc_${fmtDay(day).replaceAll('-', '_')}`;

            // Skip if this day's snapshot already exists — avoids the expensive
            // RPC totalSupply call. Due to FIFO microtask ordering, the first
            // block of the day should set() before subsequent blocks reach here.
            const existing = await context.TotalSupplySnapshot.get(entityId);
            if (existing) return;

            const totalSupply = await context.effect(getTotalSupply, {
                blockNumber: BigInt(block.number)
            });

            context.TotalSupplySnapshot.set({
                id: entityId,
                totalSupply,
                blockNumber: block.number,
                blockTimestamp
            });

            context.log.info(
                `[hypersync_midnight_v2] block ${block.number} (${fmtTs(blockTimestamp)}): totalSupply = ${totalSupply}`
            );
        }
    );
}
