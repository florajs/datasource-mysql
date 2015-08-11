'use strict';

/**
 * find column expressions in AST
 *
 * @private
 */
function getColumns(arg) {
    var res = [];

    if (Array.isArray(arg)) {
        arg.map(getColumns)
            .forEach(function (item) {
                res = res.concat(item);
            });
    } else if (arg !== null && typeof arg === 'object') {
        if (arg.type && arg.type === 'column_ref') {
            res.push(arg);
        } else {
            Object.keys(arg)
                .filter(function (key) { // we're not interested in primitive values
                    var value = arg[key];
                    return value !== null && typeof value === 'object';
                })
                .forEach(function (key) {
                    getColumns(arg[key]).forEach(function (item) {
                        if (item !== undefined) res.push(item);
                    });
                });
        }
    }

    return res;
}

/**
 * Check if columns are specified fully qualified (with tables)
 *
 * @param {Object} ast  - Abstract Syntax Tree
 */
module.exports = function (ast) {
    // only check for fully qualified column expressions in columns and from parts of the AST
    var columns = getColumns({ columns: ast.columns, from: ast.from });

    if (ast.from.length > 1) { // if query contains more than one table, columns must be specified fully qualified
        for (var i = 0, l = columns.length; i < l; ++i) {
            if (columns[i].table === null) throw new Error('Column "' + columns[i].column + '" must be fully qualified');
        }
    }
};
