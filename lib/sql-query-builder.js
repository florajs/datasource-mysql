'use strict';

const { ImplementationError } = require('flora-errors');
const { createBinaryExpr } = require('flora-sql-parser').util;
const { filterTree } = require('./util');

const OPERATOR_MAPPING = {
    equal: '=',
    notEqual: '!=',
    greater: '>',
    greaterOrEqual: '>=',
    less: '<',
    lessOrEqual: '<=',
    like: 'LIKE',
    between: 'BETWEEN',
    notBetween: 'NOT BETWEEN'
};
// if request doesn't have any filters, replace placeholder by "1 = 1"
const FILTER_PLACEHOLDER_FALLBACK = {
    type: 'binary_expr',
    operator: '=',
    left: { type: 'number', value: 1 },
    right: { type: 'number', value: 1 }
};

/**
 * @param {Object} left
 * @param {Object} right
 * @return {Object}
 * @private
 */
function createBinaryAndExpr(left, right) {
    return createBinaryExpr('AND', left, right);
}

/**
 * @param {Object} left
 * @param {Object} right
 * @return {Object}
 * @private
 */
function createBinaryOrExpr(left, right) {
    return createBinaryExpr('OR', left, right);
}

/**
 * Map single attribute filter to SQL AST expression.
 *
 * @param {Object} filter
 * @param {string} operator
 * @param {Array.<string>} columns
 * @return {Object}
 * @private
 */
function mapSingleAttributeFilter(filter, operator, columns) {
    return createBinaryExpr(
        operator,
        columns[filter.attribute],
        operator === 'LIKE' ? `%${filter.value}%` : filter.value
    );
}

/**
 * Map composite key filter to SQL AST expression.
 *
 * Due to a bug (http://bugs.mysql.com/bug.php?id=31188) in MySQL query optimizer,
 * a sample filter:
 * {
 *      attribute: ['key1', 'key2', ..., 'keyX'],
 *      operator: 'equal',
 *      value: [[1, 2, ..., valX], [3, 4, ..., valY]]
 * }
 *
 * will be transformed to this SQL fragment:
 *
 * "... (key1 = 1 AND key2 = 2 AND ... AND keyX = valX)
 *  OR (key1 = 3 AND key2 = 4 AND ... AND keyX = valY) ..."
 *
 * @param {Object} filter
 * @param {string} operator
 * @param {Array.<string>} columns
 * @return {Object}
 * @private
 */
function mapMultiAttributeFilter(filter, operator, columns) {
    const expr = filter.value
        // create AND-expressions from attribute/value pairs
        .map((values) =>
            values
                .map((value, index) => createBinaryExpr(operator, columns[filter.attribute[index]], value))
                .reduce(createBinaryAndExpr)
        )
        .reduce(createBinaryOrExpr); // concatenate AND- with OR-expressions

    expr.parentheses = true; // parenthesize complete expression
    return expr;
}

function mapOperator(filter) {
    let operator = OPERATOR_MAPPING[filter.operator];

    if (filter.value === null && (operator === '=' || operator === '!=')) {
        operator = operator === '=' ? 'IS' : 'IS NOT';
    }

    return operator;
}

/**
 * Map Flora request filters to SQL conditions
 *
 * @param {Array.<Array.<Object>>} filters Flora request filters
 * @param {Array.<Object>} columns
 * @return {Object}
 */
function mapFilters(filters, columns) {
    const orExpressions = filters.map((andFilters) => {
        const andFilterExpressions = andFilters.map((filter) => {
            const operator = mapOperator(filter);
            return Array.isArray(filter.attribute)
                ? mapMultiAttributeFilter(filter, operator, columns)
                : mapSingleAttributeFilter(filter, operator, columns);
        });
        // combine expressions with AND operator
        return andFilterExpressions.reduce(createBinaryAndExpr);
    });

    // only combine multiple 'or' expressions with OR operator
    return orExpressions.length === 1 ? orExpressions[0] : orExpressions.reduce(createBinaryOrExpr);
}

function where(ast, filters, columns) {
    const conditions =
        Array.isArray(filters) && filters.length ? mapFilters(filters, columns) : FILTER_PLACEHOLDER_FALLBACK;

    if (!ast._meta.hasFilterPlaceholders) {
        // if query from resource config contains WHERE clause,
        // combine it with filter conditions using AND operator
        ast.where =
            ast.where === null
                ? conditions
                : createBinaryAndExpr({ ...ast.where, parentheses: true }, { ...conditions, parentheses: true });
        return ast;
    }

    ast.where = JSON.parse(JSON.stringify(ast.where), (key, value) => {
        if (typeof value !== 'object') return value;
        if (value === null) return value;
        if (Array.isArray(value)) return value;
        if (value.type !== 'column_ref') return value;
        if (value.column !== '__floraFilterPlaceholder__') return;

        ['table', 'column'].forEach((key) => delete value[key]);

        return { ...value, ...conditions };
    });

    if (ast._next && ast._next._meta.hasFilterPlaceholders) {
        ast._next = where(ast._next, filters, columns);
    }

    return ast;
}

function orderBy(attribute, direction, columns) {
    const dir = direction.toUpperCase();

    if (dir === 'ASC' || dir === 'DESC') {
        return {
            expr: columns[attribute],
            type: dir
        };
    }

    if (dir === 'RANDOM') {
        return {
            expr: {
                type: 'function',
                name: 'RAND',
                args: { type: 'expr_list', value: [] }
            },
            type: ''
        };
    }

    throw new Error(`Invalid order direction "${direction}"`);
}

function search(where, attributes, term, columns) {
    const searchExpressions = attributes
        .map((attribute) => ({
            type: 'binary_expr',
            operator: 'LIKE',
            left: columns[attribute],
            right: {
                type: 'string',
                // escape LIKE pattern characters
                value: '%' + term.replace(/([%_])/g, '\\$1') + '%'
            }
        }))
        // create sub-tree of search expressions
        .reduce(createBinaryOrExpr);

    return where !== null && typeof where === 'object'
        ? createBinaryAndExpr(where, { ...searchExpressions, parentheses: true })
        : searchExpressions;
}

module.exports = function generateAST({
    filter,
    limitPer,
    queryAst,
    searchable: searchAttributes,
    search: searchTerm,
    order,
    limit,
    page
}) {
    if (limitPer) throw new ImplementationError('flora-mysql does not support "limitPer" yet');

    const columns = queryAst.columns.reduce((cols, col) => {
        cols[col.as !== null ? col.as : col.expr.column] = col.expr;
        return cols;
    }, {});

    if ((filter && filter.length) || (queryAst._meta && queryAst._meta.hasFilterPlaceholders)) {
        queryAst = where(queryAst, filter, columns);
    }

    if (searchTerm && searchTerm.length) {
        queryAst.where = search(queryAst.where, searchAttributes, searchTerm, columns);
    }

    if (order && order.length) {
        queryAst.orderby = [
            ...(queryAst.orderby || []),
            ...order.map((spec) => orderBy(spec.attribute, spec.direction, columns))
        ];
    }

    if (limit) {
        const offset = page ? (page - 1) * limit : 0;
        queryAst.limit = [
            { type: 'number', value: offset },
            { type: 'number', value: limit }
        ];
    }

    return queryAst;
};
