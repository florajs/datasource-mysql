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

    if (Array.isArray(arg)) {
        arg.forEach(checkColumns);
        return;
    }

    Object.keys(arg)
        .filter((key) => {
            // we're not interested in primitive values
            const value = arg[key];
            return value !== null && typeof value === 'object';
        })
        .forEach((key) => checkColumns(arg[key]));
}

/**
 * Check if columns are specified fully qualified (with tables)
 *
 * @param {Object} ast  - Abstract Syntax Tree
 */
module.exports = function checkAST(ast) {
    // if query contains more than one table, columns must be specified fully qualified
    if (ast.from.length < 2) return;

    // only check for fully qualified column expressions in columns and from parts of the AST
    checkColumns([ast.columns, ast.from]);
};
