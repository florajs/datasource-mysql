'use strict';

/**
 * Check AST for column expressions without reference table
 *
 * @private
 */
function checkColumns(arg) {
    if (arg.type && arg.type === 'column_ref') {
        if (arg.table === null) throw new Error('Column "' + arg.column + '" must be fully qualified');
        return;
    }

    if (Array.isArray(arg)) return arg.forEach(checkColumns);

    Object.keys(arg)
        .filter(function (key) { // we're not interested in primitive values
            var value = arg[key];
            return value !== null && typeof value === 'object';
        })
        .forEach(function (key) {
            checkColumns(arg[key]);
        });
}

/**
 * Check if columns are specified fully qualified (with tables)
 *
 * @param {Object} ast  - Abstract Syntax Tree
 */
module.exports = function (ast) {
    if (ast.from.length < 2) return; // if query contains more than one table, columns must be specified fully qualified

    // only check for fully qualified column expressions in columns and from parts of the AST
    checkColumns([ast.columns, ast.from]);
};
