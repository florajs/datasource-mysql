'use strict';

const has = require('has');

function isColumn(expr) {
    return has(expr, 'type') && expr.type === 'column_ref';
}

function checkColumn(expr) {
    if (expr.table !== null) return;
    throw new Error(`Column "${expr.column}" must be fully qualified`);
}

/**
 * Check AST for column expressions without reference table
 *
 * @private
 */
function checkColumns(arg) {
    if (isColumn(arg)) return checkColumn(arg);
    if (Array.isArray(arg)) return arg.forEach(checkColumns);

    Object.entries(arg)
        .filter(([, value]) => value !== null && typeof value === 'object') // remove in primitive values
        .forEach(([, value]) => checkColumns(value));
}

function checkWhere(expr) {
    if (expr === null) return;
    if (isColumn(expr)) return checkColumn(expr);

    ['left', 'right']
        .map((property) => expr[property])
        .filter((node) => typeof node === 'object')
        .forEach(checkWhere);
}

/**
 * Check if columns are specified fully qualified (with tables)
 *
 * @param {Object} ast  - Abstract Syntax Tree
 */
module.exports = function checkAST(ast) {
    // with only one table columns must not be fully qualified
    if (ast.from.length === 1) return;

    // only check for fully qualified column expressions in columns and from parts of the AST
    checkColumns([ast.columns, ast.from]);
    checkWhere(ast.where);
};
