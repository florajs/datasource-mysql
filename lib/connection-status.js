'use strict';

function collectPoolStatus(stats, { type, host, pool }) {
    const poolStats = [
        ['_allConnections', 'open'],
        ['_freeConnections', 'sleeping'],
        ['_connectionQueue', 'waiting']
    ]
        .filter(
            ([poolProp]) =>
                Object.hasOwn(pool, poolProp) &&
                typeof pool[poolProp].toArray === 'function' &&
                pool[poolProp].toArray().length
        )
        .reduce(
            (stats, [poolProp, floraProp]) => ({
                ...stats,
                [floraProp]: pool[poolProp].toArray().length
            }),
            {
                open: 0,
                sleeping: 0,
                waiting: 0
            }
        );

    poolStats.open -= poolStats.sleeping;

    stats[type] ??= {};
    stats[type][host] = poolStats;

    return stats;
}

// a cluster contains master/slave pools
function collectClusterStatus(stats, [database, { poolCluster }]) {
    return {
        ...stats,
        [database]: Object.entries(poolCluster._nodes)
            .map(([identifier, { pool }]) => {
                const [type, host] = identifier.split('_');
                return { type: `${type.toLowerCase()}s`, host, pool };
            })
            .reduce(collectPoolStatus, {})
    };
}

module.exports = (pools) =>
    Object.entries(pools).reduce(
        (stats, [server, databases]) => ({
            ...stats,
            [server]: Object.entries(databases)
                .filter(
                    ([, { poolCluster }]) =>
                        Object.hasOwn(poolCluster, '_nodes') && typeof poolCluster._nodes === 'object'
                )
                .reduce(collectClusterStatus, {})
        }),
        {}
    );
