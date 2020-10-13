'use strict';

const has = require('has');
const { ImplementationError } = require('flora-errors');
const { createBinaryExpr } = require('flora-sql-parser').util;

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
 * @param {Array.<Object>} columns
 * @return {Object}
 * @private
 */
function mapSingleAttributeFilter(filter, operator, columns) {
    return createBinaryExpr(
        operator,
        has(columns, filter.attribute) ? columns[filter.attribute] : filter.attribute,
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

function orderBy(ast, order, columns) {
    if (!Array.isArray(order)) return null;

    return [
        ...(ast.orderby || []),
        ...order.map((spec) => {
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
        })
    ];
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
        ? createBinaryAndExpr(where, { ...searchExpressions, parentheses: true })
        : searchExpressions;
}

function limitPerAst({ attributes, queryAst, filter, limitPer, order, count, page, columns }) {
    const [andFilters] = filter;
    const idValues = andFilters.find((f) => f.attribute === limitPer && f.valueFromParentKey === true).value;
    const limitPerifiedFilters = filter.map((andFilters) => {
        return andFilters.map((filter) => {
            if (filter.attribute !== limitPer) return filter;
            if (!filter.valueFromParentKey) return filter;
            return { attribute: '__limitPer__', operator: 'equal', value: 'id' };
        });
    });

    queryAst = JSON.parse(JSON.stringify(where(queryAst, limitPerifiedFilters, columns)), (key, value) => {
        if (typeof value !== 'object') return value;
        if (value === null) return value;
        if (value.type !== 'binary_expr') return value;
        if (value.operator !== '=') return value;
        if (value.left.value !== '__limitPer__') return value;

        return {
            type: 'binary_expr',
            operator: '=',
            left: { type: 'column_ref', table: 'limitper_ids', column: 'id' },
            right: columns[limitPer]
        };
    });

    return {
        with: null,
        type: 'select',
        options: null,
        distinct: null,
        from: [
            // VALUES expression
            {
                expr: {
                    type: 'values',
                    value: idValues.map((value) => ({
                        type: 'row_value',
                        keyword: true,
                        value: [{ type: typeof value, value }]
                    }))
                },
                as: 'limitper_ids',
                columns: ['id']
            },
            // lateral join subquery
            {
                expr: {
                    ...queryAst,
                    orderby: orderBy(queryAst, order, columns),
                    limit: limit(count, page),
                    parentheses: true
                },
                join: 'JOIN',
                lateral: true,
                on: { type: 'boolean', value: true },
                as: 'limitper'
            }
        ],
        columns: attributes.map((attribute) => ({
            expr: { type: 'column_ref', table: 'limitper', column: attribute },
            as: null
        })),
        where: null,
        groupby: null,
        having: null,
        orderby: null,
        limit: null
    };
}

module.exports = function generateAST({
    attributes,
    filter,
    limitPer,
    queryAst,
    searchable: searchAttributes,
    search: searchTerm,
    order,
    limit: count,
    page
}) {
    const columns = queryAst.columns.reduce((cols, col) => {
        cols[col.as !== null ? col.as : col.expr.column] = col.expr;
        return cols;
    }, {});

    if (limitPer) {
        // throw new ImplementationError('flora-mysql does not support "limitPer" yet');
        return limitPerAst({ attributes, filter, queryAst, order, count, page, limitPer, columns });
    }

    if ((filter && filter.length) || (queryAst._meta && queryAst._meta.hasFilterPlaceholders)) {
        queryAst = where(queryAst, filter, columns);
    }

    if (searchTerm && searchTerm.length) {
        queryAst.where = search(queryAst.where, searchAttributes, searchTerm, columns);
    }

    queryAst.orderby = orderBy(queryAst, order, columns);
    queryAst.limit = limit(count, page);

    return queryAst;
};
