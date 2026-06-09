import { createEffect, indexer, S } from 'envio';
import { createPublicClient, http, type PublicClient } from 'viem';
import { HypersyncClient } from '@envio-dev/hypersync-client';

// ---------------------------------------------------------------------------
// Strategy selection via SNAPSHOT_STRATEGY env var:
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
    | 'hypersync_midnight';

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

// HyperSync-based batched timestamp lookup (used by hypersync_midnight).
// Uses queueMicrotask to accumulate multiple block number requests from the
// same tick into a single HyperSync range query. Much faster than individual
// RPC calls during historical sync.
const initHypersyncTimestamp = () => {
    const hsClient = new HypersyncClient({
        url: 'https://1.hypersync.xyz',
        apiToken: process.env.ENVIO_API_TOKEN!
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
            batch.blockNumbers.add(blockNumber);

            const promise = new Promise<number>((resolve, reject) => {
                const existing = batch.resolvers.get(blockNumber) || [];
                existing.push({ resolve, reject });
                batch.resolvers.set(blockNumber, existing);
            });

            if (!batch.scheduled) {
                batch.scheduled = true;
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

                        while (nextBlock < toBlock) {
                            const data = await hsClient.get({
                                fromBlock: nextBlock,
                                toBlock,
                                includeAllBlocks: true,
                                fieldSelection: { block: ['Number', 'Timestamp'] }
                            });

                            for (const block of data.data.blocks) {
                                if (block.number !== undefined && block.timestamp !== undefined) {
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
            if (day === lastSeenDay) return;
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
            if (day === lastSeenDay) return;
            lastSeenDay = day;
            await storeSnapshot(block, context, blockTimestamp);
        }
    );
}
