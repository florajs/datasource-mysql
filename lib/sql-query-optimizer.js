'use strict';

var _ = require('lodash');

/**
 * Get table names from AST.
 *
 * @param {Object} node
 * @return {Array.<string>} Array of table names
 * @private
 */
function walk(node) {
    var tables = [];

    if (node.type && node.type === 'column_ref') {
        tables.push(node.table);
    } else {
        Object.keys(node)
            .filter(function (attr) { // remove simple types
                return typeof node[attr] === 'object';
            })
            .forEach(function (attr) {
                tables = tables.concat(walk(node[attr]));
            });
    }

    return tables;
}

/**
 * Extract required tables .
 *
 * @param {Object} ast Abstract Syntax Tree
 * @return {Array.<string>} Array of required tables
 * @private
 */
function getRequiredTables(ast) {
    var requiredTables = [];

    ['columns', 'where', 'groupby', 'orderby']  // extract tables used in given clauses
        .forEach(function (clause) {
            if (ast[clause] !== '') requiredTables = requiredTables.concat(walk(ast[clause]));
        });

    ast.from
        .filter(function (expr) {   // extraxt joined tables that are not marked as required
            var table = expr.as !== '' ? expr.as : expr.table;
            return expr.join !== undefined && requiredTables.indexOf(table) !== -1;
        })
        .forEach(function (join) {  // get tables used in joins
            requiredTables = requiredTables.concat(walk(join.on));
        });

    return _.unique(requiredTables);
}

/**
 * Remove unnecessary expressions (attributes/columns or LEFT JOINS) from AST/query.
 *
 * @param {Object} ast Abstract Syntax Tree
 * @param {Array.<string>} requiredAttrs Required attributes
 * @return {Object}
 */
function optimizeAST(ast, requiredAttrs) {
    var requiredTables = [];

    if (! ast.type) return ast;
    if (ast.type !== 'select') return ast;

    ast = _.cloneDeep(ast);

    ast.columns = ast.columns.filter(function (column) { // remove attributes that are not queried
        var attribute = column.as !== '' ? column.as : column.expr.column;
        return requiredAttrs.indexOf(attribute) !== -1;
    });

    requiredTables = getRequiredTables(ast);
    if (requiredTables.length < ast.from.length) {
        ast.from = ast.from.filter(function (expr) {
            var joinTable = expr.as !== '' ? expr.as : expr.table;
            if (! expr.join) return true;                   // FROM clause
            if (expr.join !== 'LEFT JOIN') return true;     // do not remove other types than LEFT JOIN
            return requiredTables.indexOf(joinTable) !== -1;
        });
    }

    return ast;
}

module.exports = optimizeAST;
