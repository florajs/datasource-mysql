'use strict';

const { filterTree } = require('../lib/util');
const unique = (array) => Array.from(new Set(array));

/**
 * Get used table names from AST
 *
 * @type {Function}
 * @param {Object} node
 * @return {Array.<string>} Array of table names
 * @private
 */
const getTableExpressions = filterTree((node) => node?.type === 'column_ref');

/**
 * Get attributes/aliases used in AST
 *
 * @type {Function}
 * @param {Object} node
 * @return {Array.<string>} Array of table names
 * @private
 */
const getAttributeExpressions = filterTree(
    (node) => Object.hasOwn(node, 'table') && node.table === null && node?.type === 'column_ref'
);

/**
 * Extract required tables
 *
 * @param {Object} ast Abstract Syntax Tree
 * @return {Array.<string>} Array of used tables
 * @private
 */
function getRequiredTables(ast) {
    const tableExpressions = ['columns', 'where', 'groupby', 'orderby']
        .filter((clause) => Object.hasOwn(ast, clause) && ast[clause] !== null)
        .flatMap((clause) => getTableExpressions(ast[clause]));

    if (ast.from.length > 1) {
        ast.from
            // extract joined tables that are not marked as required
            .filter((expr) => {
                const table = expr.as !== null ? expr.as : expr.table;
                return expr.join !== undefined && tableExpressions.some((tblExpr) => tblExpr.table === table);
            })
            // get tables used in joins
            .forEach((join) => tableExpressions.push(...getTableExpressions(join.on)));
    }

    const requiredTables = tableExpressions.map(({ table }) => table);

    return unique(requiredTables);
}

/**
 * Extract required aliases
 *
 * @param {Object} ast Abstract Syntax Tree
 * @return {Array.<string>} Array of used attributes/aliases
 */
function getRequiredAliases(ast) {
    const requiredAliases = ['groupby', 'orderby']
        .filter((clause) => Object.hasOwn(ast, clause) && ast[clause] !== null)
        .map((clause) => ast[clause])
        .reduce((aliasExpressions, expr) => {
            aliasExpressions.push(...getAttributeExpressions(expr));
            return aliasExpressions;
        }, [])
        .map((expr) => expr.column);

    return unique(requiredAliases);
}

/**
 * Remove unnecessary expressions (attributes/columns or LEFT JOINs) from AST/query.
 *
 * @param {Object} ast Abstract Syntax Tree
 * @param {Array.<string>} requestedAttrs Attributes from Flora request
 * @param {boolean=} isLimitPer
 * @return {Object}
 */
function optimizeAST(ast, requestedAttrs, isLimitPer = false) {
    if (!ast.type || ast.type !== 'select') {
        return ast;
    }

    // don't touch union selects
    if (Object.hasOwn(ast, '_next') && typeof ast._next === 'object') {
        return ast;
    }

    if (isLimitPer) {
        const subSelect = ast.from[1].expr;
        ast.from[1].expr = optimizeAST(subSelect, requestedAttrs);
        return ast;
    }

    const requiredAliases = getRequiredAliases(ast);
    const requiredAttrs = requiredAliases ? [...requestedAttrs, ...requiredAliases] : [];

    ast.columns = ast.columns.filter((column) => {
        // remove attributes that are not queried
        const attribute = column.as !== null ? column.as : column.expr.column;
        return requiredAttrs.includes(attribute);
    });

    const requiredTables = getRequiredTables(ast);
    ast.from = ast.from.filter((expr) => {
        const joinTable = expr.as !== null ? expr.as : expr.table;
        if (!expr.join) return true; // FROM clause
        if (expr.join !== 'LEFT JOIN') return true; // do not remove other types than LEFT JOIN
        return requiredTables.includes(joinTable);
    });

    return ast;
}

module.exports = optimizeAST;
