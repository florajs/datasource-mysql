'use strict';

var _ = require('lodash');

/**
 * Extract tables or aliases by recursively walking the tree/clause
 *
 * @param {Function} checkFn
 * @return {function(Object)}
 * @private
 */
function filter(checkFn) {
    return function walk(node) {
        var items = [];

        if (checkFn(node)) {
            items.push(node);
        } else {
            Object.keys(node)
                .filter(function (attr) { // remove simple types
                    var value = node[attr];
                    return value !== null && typeof value === 'object';
                })
                .forEach(function (attr) {
                    Array.prototype.push.apply(items, walk(node[attr]));
                });
        }

        return items;
    };
}

/**
 * Get used table names from AST
 *
 * @type {Function}
 * @param {Object} node
 * @return {Array.<string>} Array of table names
 * @private
 */
var getTableExpressions = filter(function (node) {
    return node.type && node.type === 'column_ref';
});

/**
 * Get attributes/aliases used in AST
 *
 * @type {Function}
 * @param {Object} node
 * @return {Array.<string>} Array of table names
 * @private
 */
var getAttributeExpressions = filter(function (node) { // filter alias expressions from AST
    return node.hasOwnProperty('table') && node.table === null && node.type && node.type === 'column_ref';
});

/**
 * Extract required tables
 *
 * @param {Object} ast Abstract Syntax Tree
 * @return {Array.<string>} Array of used tables
 * @private
 */
function getRequiredTables(ast) {
    var tableExpressions = [],
        requiredTables;

    ['columns', 'where', 'groupby', 'orderby'].forEach(function (clause) { // extract tables used in given clauses
        if (ast[clause] !== null) Array.prototype.push.apply(tableExpressions, getTableExpressions(ast[clause]));
    });

    ast.from
        .filter(function (expr) { // extract joined tables that are not marked as required
            var table = expr.as !== '' ? expr.as : expr.table;
            return expr.join !== undefined && tableExpressions.some(function (tblExpr) {
                return tblExpr.table === table;
            });
        })
        .forEach(function (join) {  // get tables used in joins
            Array.prototype.push.apply(tableExpressions, getTableExpressions(join.on));
        });

    requiredTables = tableExpressions.map(function (expr) {
        return expr.table;
    });

    return _.uniq(requiredTables);
}

/**
 * Extract required aliases
 *
 * @param {Object} ast Abstract Syntax Tree
 * @return {Array.<string>} Array of used attributes/aliases
 */
function getRequiredAliases(ast) {
    var aliasExpressions = [],
        requiredAliases;

    ['groupby', 'orderby'].forEach(function (clause) { // extract alias expressions used in given clauses
        if (ast[clause]) Array.prototype.push.apply(aliasExpressions, getAttributeExpressions(ast[clause]));
    });

    requiredAliases = aliasExpressions.map(function (expr) {
        return expr.column;
    });

    return _.uniq(requiredAliases);
}

/**
 * Remove unnecessary expressions (attributes/columns or LEFT JOINs) from AST/query.
 *
 * @param {Object} ast Abstract Syntax Tree
 * @param {Array.<string>} requestedAttrs Attributes from Flora request
 */
function optimizeAST(ast, requestedAttrs) {
    var requiredAttrs = requestedAttrs,
        requiredAliases = getRequiredAliases(ast),
        requiredTables;

    if (!ast.type || ast.type !== 'select') return ast;

    if (requiredAliases) Array.prototype.push.apply(requiredAttrs, requiredAliases);

    ast.columns = ast.columns.filter(function (column) { // remove attributes that are not queried
        var attribute = column.as !== null ? column.as : column.expr.column;
        return requiredAttrs.indexOf(attribute) !== -1;
    });

    requiredTables = getRequiredTables(ast);
    if (requiredTables.length < ast.from.length) {
        ast.from = ast.from.filter(function (expr) {
            var joinTable = expr.as !== null ? expr.as : expr.table;
            if (! expr.join) return true;                   // FROM clause
            if (expr.join !== 'LEFT JOIN') return true;     // do not remove other types than LEFT JOIN
            return requiredTables.indexOf(joinTable) !== -1;
        });
    }
}

module.exports = optimizeAST;
