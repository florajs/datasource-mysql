'use strict';

const { ImplementationError } = require('@florajs/errors');
const { createBinaryExpr, createValueExpr } = require('@florajs/sql-parser').util;

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
 * A filter like
 * {
 *      attribute: ['key1', 'key2'],
 *      operator: 'equal',
 *      value: [[1,2],[3,4]]
 * }
 *
 * will be transformed to
 *
 * (key1, key2) = ((1,2),(3,4))
 *
 * @param {Object} filter
 * @param {string} operator
 * @param {Object} columns
 * @return {Object}
 */
function mapMultiAttributeFilter(filter, operator, columns) {
    return createBinaryExpr(
        operator,
        {
            type: 'expr_list',
            value: filter.attribute.map((attribute) => columns[attribute]),
            parentheses: true
        },
        {
            type: 'expr_list',
            value: filter.value.map((value) => ({ ...createValueExpr(value), parentheses: true })),
            parentheses: true
        }
    );
}

function mapOperator(filter) {
    if (!Array.isArray(filter.attribute)) {
        const operator = OPERATOR_MAPPING[filter.operator];

        // Handle NULL value comparison - convert = to IS and != to IS NOT
        if (filter.value === null && (operator === '=' || operator === '!=')) {
            return operator === '=' ? 'IS' : 'IS NOT';
        }

        return operator;
    }

    if (filter.value === null) {
        throw new ImplementationError('NULL value comparisons are not supported for multi-attribute filters');
    }

    if (filter.operator === 'equal') {
        return 'IN';
    }

    throw new ImplementationError(`Operator "${filter.operator}" is not supported for multi-attribute filters`);
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
    if (searchTerm?.length) {
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
                    orderby: order?.length ? order.map((spec) => orderBy(spec, columns)) : null,
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
    if (filter?.length) {
        queryAst.where = where(queryAst, filter, columns);
    }

    const { search: searchTerm, searchable } = floraRequest;
    if (searchTerm?.length) {
        queryAst.where = search(queryAst.where, searchable, searchTerm, columns);
    }

    const { order } = floraRequest;
    if (order?.length) {
        queryAst.orderby = order.map((spec) => orderBy(spec, columns));
    }

    const { limit: count, page } = floraRequest;
    queryAst.limit = limit(count, page);

    return queryAst;
};
