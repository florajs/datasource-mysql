'use strict';

const _ = require('lodash');
const has = require('has');
const { filterTree } = require('../lib/util');

/**
 * Get used table names from AST
 *
 * @type {Function}
 * @param {Object} node
 * @return {Array.<string>} Array of table names
 * @private
 */
const getTableExpressions = filterTree(node =>
    (node.type && node.type === 'column_ref'));

/**
 * Get attributes/aliases used in AST
 *
 * @type {Function}
 * @param {Object} node
 * @return {Array.<string>} Array of table names
 * @private
 */
const getAttributeExpressions = filterTree(node =>
    // filter alias expressions from AST
    (has(node, 'table') && node.table === null && node.type && node.type === 'column_ref'));

/**
 * Extract required tables
 *
 * @param {Object} ast Abstract Syntax Tree
 * @return {Array.<string>} Array of used tables
 * @private
 */
function getRequiredTables(ast) {
    const tableExpressions = [];

    ['columns', 'where', 'groupby', 'orderby'].forEach((clause) => { // extract tables used in given clauses
        if (ast[clause] !== null) {
            Array.prototype.push.apply(tableExpressions, getTableExpressions(ast[clause]));
        }
    });

    ast.from
        .filter((expr) => { // extract joined tables that are not marked as required
            const table = expr.as !== '' ? expr.as : expr.table;
            return expr.join !== undefined
                && tableExpressions.some(tblExpr => (tblExpr.table === table));
        })
        .forEach((join) => { // get tables used in joins
            Array.prototype.push.apply(tableExpressions, getTableExpressions(join.on));
        });

    const requiredTables = tableExpressions.map(expr => expr.table);

    return _.uniq(requiredTables);
}

/**
 * Extract required aliases
 *
 * @param {Object} ast Abstract Syntax Tree
 * @return {Array.<string>} Array of used attributes/aliases
 */
function getRequiredAliases(ast) {
    const aliasExpressions = [];

    // extract alias expressions used in given clauses
    ['groupby', 'orderby'].forEach((clause) => {
        if (ast[clause]) {
            Array.prototype.push.apply(
                aliasExpressions,
                getAttributeExpressions(ast[clause])
            );
        }
    });

    const requiredAliases = aliasExpressions.map(expr => expr.column);

    return _.uniq(requiredAliases);
}

/**
 * Remove unnecessary expressions (attributes/columns or LEFT JOINs) from AST/query.
 *
 * @param {Object} ast Abstract Syntax Tree
 * @param {Array.<string>} requestedAttrs Attributes from Flora request
 */
function optimizeAST(ast, requestedAttrs) {
    const requiredAttrs = requestedAttrs;
    const requiredAliases = getRequiredAliases(ast);

    if (!ast.type || ast.type !== 'select') return ast;

    if (requiredAliases) Array.prototype.push.apply(requiredAttrs, requiredAliases);

    ast.columns = ast.columns.filter((column) => { // remove attributes that are not queried
        const attribute = column.as !== null ? column.as : column.expr.column;
        return requiredAttrs.indexOf(attribute) !== -1;
    });

    const requiredTables = getRequiredTables(ast);
    if (requiredTables.length < ast.from.length) {
        ast.from = ast.from.filter((expr) => {
            const joinTable = expr.as !== null ? expr.as : expr.table;
            if (!expr.join) return true; // FROM clause
            if (expr.join !== 'LEFT JOIN') return true; // do not remove other types than LEFT JOIN
            return requiredTables.indexOf(joinTable) !== -1;
        });
    }

    return null;
}

module.exports = optimizeAST;
