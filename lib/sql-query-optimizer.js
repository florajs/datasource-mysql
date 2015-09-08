'use strict';

var _ = require('lodash');

/**
 * Extract tables or columns/aliases by recursively walking the tree/clause
 *
 * @param {string} key
 * @return {function(Object)}
 * @private
 */
function filter(key) {
    return function walk(node) {
        var items = [];

        if (node.type && node.type === 'column_ref') {
            items.push(node[key]);
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
var getTables = filter('table');

/**
 * Get attributes/aliases used in AST
 *
 * @type {Function}
 * @param {Object} node
 * @return {Array.<string>} Array of table names
 * @private
 */
var getAttributes = filter('column');

/**
 * Extract required tables
 *
 * @param {Object} ast Abstract Syntax Tree
 * @return {Array.<string>} Array of used tables
 * @private
 */
function getRequiredTables(ast) {
    var requiredTables = [];

    ['columns', 'where', 'groupby', 'orderby'].forEach(function (clause) { // extract tables used in given clauses
        if (ast[clause] !== null) Array.prototype.push.apply(requiredTables, getTables(ast[clause]));
    });

    ast.from
        .filter(function (expr) { // extraxt joined tables that are not marked as required
            var table = expr.as !== '' ? expr.as : expr.table;
            return expr.join !== undefined && requiredTables.indexOf(table) !== -1;
        })
        .forEach(function (join) {  // get tables used in joins
            Array.prototype.push.apply(requiredTables, getTables(join.on));
        });

    return _.unique(requiredTables);
}

/**
 * Extract required attributes/aliases
 *
 * @param {Object} ast Abstract Syntax Tree
 * @return {Array.<string>} Array of used attributes/aliases
 */
function getRequiredAttributes(ast) {
    var requiredAttrs = [];

    ['where', 'groupby', 'orderby'].forEach(function (clause) { // extract attributes/aliases used in given clauses
        if (ast[clause]) Array.prototype.push.apply(requiredAttrs, getAttributes(ast[clause]));
    });

    return _.unique(requiredAttrs);
}

/**
 * Remove unnecessary expressions (attributes/columns or LEFT JOINS) from AST/query.
 *
 * @param {Object} ast Abstract Syntax Tree
 * @param {Array.<string>} requestedAttrs Attributes from Flora request
 */
function optimizeAST(ast, requestedAttrs) {
    var requiredTables = [],
        requiredAttrs = requestedAttrs;

    if (! ast.type) return ast;
    if (ast.type !== 'select') return ast;

    Array.prototype.push.apply(requiredAttrs, getRequiredAttributes(ast));

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
