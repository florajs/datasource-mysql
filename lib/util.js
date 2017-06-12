'use strict';

/**
 * Filter tree by function
 *
 * @param {Function} checkFn
 * @return {function(Object)}
 */
function filterTree(checkFn) {
    return function walk(node) {
        const items = [];

        if (checkFn(node)) {
            items.push(node);
        } else {
            Object.keys(node)
                .filter((attr) => { // remove simple types
                    const value = node[attr];
                    return value !== null && typeof value === 'object';
                })
                .forEach((attr) => {
                    items.push(...walk(node[attr]));
                });
        }

        return items;
    };
}

module.exports = { filterTree };
