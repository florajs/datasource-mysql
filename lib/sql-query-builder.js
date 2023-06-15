/* eslint-disable no-unused-vars */

'use strict';

const { ImplementationError } = require('@florajs/errors');
const { createBinaryExpr } = require('@florajs/sql-parser').util;

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
 * @param {Array.<Object>} columns
 * @return {Object}
 * @private
 */
function mapSingleAttributeFilter(filter, operator, columns) {
    return createBinaryExpr(
        operator,
        Object.hasOwn(columns, filter.attribute) ? columns[filter.attribute] : filter.attribute,
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
 * @param {Array.<Object>} columns
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
 * @return {Object|null}
 */
function mapFilters(filters, columns) {
    const orExpressions = filters
        .map((andFilters) => {
            const andFilterExpressions = andFilters.map((filter) => {
                if (Object.hasOwn(filter, 'limitPerCondition') && filter.limitPerCondition !== null) {
                    return filter.limitPerCondition;
                }

                const operator = mapOperator(filter);
                return Array.isArray(filter.attribute)
                    ? mapMultiAttributeFilter(filter, operator, columns)
                    : mapSingleAttributeFilter(filter, operator, columns);
            });

            if (!andFilterExpressions.length) {
                return null;
            }

            // combine expressions with AND operator
            return andFilterExpressions.reduce(createBinaryAndExpr);
        })
        .filter((andExpressions) => andExpressions !== null);

    if (orExpressions.length === 0) {
        return null;
    }

    // only combine multiple 'or' expressions with OR operator
    return orExpressions.length === 1 ? orExpressions[0] : orExpressions.reduce(createBinaryOrExpr);
}

function where(ast, filters, columns) {
    const conditions = mapFilters(filters, columns);

    // if query from resource config contains WHERE clause,
    // combine it with filter conditions using AND operator
    return ast.where === null
        ? conditions
        : createBinaryAndExpr({ ...ast.where, parentheses: true }, { ...conditions, parentheses: true });
}

function orderBy(spec, columns) {
    const { attribute, direction } = spec;
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

function limit(count, page) {
    if (typeof count !== 'number') return null;

    const offset = page ? (page - 1) * count : 0;
    return [
        { type: 'number', value: offset },
        { type: 'number', value: count }
    ];
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
        ? createBinaryAndExpr({ ...where, parentheses: true }, { ...searchExpressions, parentheses: true })
        : searchExpressions;
}

function generateLimitPerAst(floraRequest, columns) {
    const isLimitPerFilter = (f) => f.attribute === floraRequest.limitPer && f.valueFromParentKey === true;
    const { queryAst, filter, order, limit: count, page } = floraRequest;
    const idFilter = filter[0].find(isLimitPerFilter);
    const limitPerCondition = {
        type: 'binary_expr',
        operator: '=',
        left: { type: 'column_ref', table: 'limitper_ids', column: 'id' },
        right: columns[idFilter.attribute]
    };
    const limitPerFilters = filter.map((andFilters) =>
        andFilters.map((andFilter) => ({
            ...andFilter,
            limitPerCondition: isLimitPerFilter(andFilter) ? limitPerCondition : null
        }))
    );

    queryAst.where = where(queryAst, limitPerFilters, columns);

    const { search: searchTerm, searchable } = floraRequest;
    if (searchTerm && searchTerm.length) {
        queryAst.where = search(queryAst.where, searchable, searchTerm, columns);
    }

    return {
        with: null,
        type: 'select',
        options: null,
        distinct: null,
        columns: floraRequest.attributes.map((attribute) => ({
            expr: { type: 'column_ref', table: 'limitper', column: attribute },
            as: null
        })),
        from: [
            // VALUES expression
            {
                expr: {
                    type: 'values',
                    value: idFilter.value.map((id) => ({
                        type: 'row_value',
                        keyword: true,
                        value: [{ type: 'number', value: id }]
                    }))
                },
                as: 'limitper_ids',
                columns: ['id']
            },
            // lateral join subquery
            {
                expr: {
                    ...queryAst,
                    orderby: order && order.length ? order.map((spec) => orderBy(spec, columns)) : null,
                    limit: limit(count, page),
                    parentheses: true
                },
                join: 'JOIN',
                lateral: true,
                on: { type: 'boolean', value: true },
                as: 'limitper'
            }
        ],
        where: null,
        groupby: null,
        having: null,
        orderby: null,
        limit: null
    };
}

module.exports = function generateAST(floraRequest) {
    const { queryAst } = floraRequest;
    const columns = queryAst.columns.reduce((cols, col) => {
        cols[col.as !== null ? col.as : col.expr.column] = col.expr;
        return cols;
    }, {});

    if (floraRequest.limitPer) {
        throw new ImplementationError('datasource-mysql does not support "limitPer" yet');
        // return generateLimitPerAst(floraRequest, columns);
    }

    const { filter } = floraRequest;
    if (filter && filter.length) {
        queryAst.where = where(queryAst, filter, columns);
    }

    const { search: searchTerm, searchable } = floraRequest;
    if (searchTerm && searchTerm.length) {
        queryAst.where = search(queryAst.where, searchable, searchTerm, columns);
    }

    const { order } = floraRequest;
    if (order && order.length) {
        queryAst.orderby = order.map((spec) => orderBy(spec, columns));
    }

    const { limit: count, page } = floraRequest;
    queryAst.limit = limit(count, page);

    return queryAst;
};
