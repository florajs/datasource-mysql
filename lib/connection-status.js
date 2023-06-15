'use strict';

function collectPoolStatus(stats, { type, host, pool }) {
    const poolStats = [
        ['_allConnections', 'open'],
        ['_freeConnections', 'sleeping'],
        ['_acquiringConnections', 'waiting']
    ]
        .filter(([mysqljsProp]) => Object.hasOwn(pool, mysqljsProp) && Array.isArray(pool[mysqljsProp]))
        .reduce(
            (stats, [mysqljsProp, floraProp]) => ({
                ...stats,
                [floraProp]: pool[mysqljsProp].length
            }),
            {}
        );

    poolStats.open -= poolStats.sleeping;

    stats[type] = stats[type] || {};
    stats[type][host] = poolStats;

    return stats;
}

// a cluster contains master/slave pools
function collectClusterStatus(stats, [database, poolCluster]) {
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
                    ([, poolCluster]) => Object.hasOwn(poolCluster, '_nodes') && typeof poolCluster._nodes === 'object'
                )
                .reduce(collectClusterStatus, {})
        }),
        {}
    );
