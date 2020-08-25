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

/**
 * @param {Object} node
 * @return {boolean}
 */
function isPlaceholder(node) {
    return node.type === 'column_ref' && node.column === '__floraFilterPlaceholder__';
}

class SqlQueryBuilder {
    /**
     * @param {Object} ast  - Abstract syntax tree representation of SQL query
     */
    constructor(ast) {
        this.ast = ast;
        this.columns = this._getColumns();
    }

    /**
     * @return {Object}
     */
    getAST() {
        return this.ast;
    }

    /**
     * Convert array of filters to expressions in where part of AST.
     *
     * @param {Array.<Array>} filters
     */
    filter(filters) {
        const hasFilters = Array.isArray(filters) && filters.length;
        let conditions;

        if (hasFilters) conditions = mapFilters(filters, this.columns);

        if (!this.ast._meta || !this.ast._meta.hasFilterPlaceholders) {
            // if query from resource config contains WHERE clause,
            // combine it with filter conditions using AND operator
            this.ast.where =
                this.ast.where === null
                    ? conditions
                    : createBinaryAndExpr(
                          { ...this.ast.where, ...{ parentheses: true } },
                          { ...conditions, ...{ parentheses: true } }
                      );
        } else {
            if (!hasFilters) conditions = FILTER_PLACEHOLDER_FALLBACK;
            /** @type {Array} */
            const placeholders = filterTree(isPlaceholder)(this.ast);
            placeholders.forEach((placeholder) => {
                ['table', 'column'].forEach((key) => delete placeholder[key]);
                Object.assign(placeholder, conditions);
            });
        }
    }

    /**
     * Full-text search using LIKE operator.
     *
     * @param {Array.<string>} attributes
     * @param {string} term
     */
    search(attributes, term) {
        term = term.replace(/([%_])/g, '\\$1'); // escape LIKE pattern characters

        const searchExpressions = attributes
            // create LIKE expression for every attribute
            .map((attribute) => ({
                type: 'binary_expr',
                operator: 'LIKE',
                left: this.columns[attribute],
                right: { type: 'string', value: '%' + term + '%' }
            }))
            // create sub-tree of search expressions
            .reduce(createBinaryOrExpr);

        if (this.ast.where !== null && typeof this.ast.where === 'object') {
            searchExpressions.parentheses = true; // parenthesize search expressions
            this.ast.where = createBinaryAndExpr(this.ast.where, searchExpressions);
        } else {
            this.ast.where = searchExpressions;
        }
    }

    /**
     * Add order specifiction to AST/query.
     *
     * @param {string} attr
     * @param {string} direction
     */
    orderBy(attr, direction) {
        if (!Array.isArray(this.ast.orderby)) this.ast.orderby = [];

        const dir = direction.toUpperCase();

        if (dir === 'ASC' || dir === 'DESC') {
            this.ast.orderby.push({
                expr: this.columns[attr],
                type: dir
            });
            return;
        }

        if (dir === 'RANDOM') {
            this.ast.orderby.push({
                expr: {
                    type: 'function',
                    name: 'RAND',
                    args: { type: 'expr_list', value: [] }
                },
                type: ''
            });
            return;
        }

        throw new Error(`Invalid order direction "${direction}"`);
    }

    /**
     * Limit result set of the query.
     *
     * @param {number} count
     * @param {number} offset
     */
    limit(count, offset) {
        this.ast.limit = [
            { type: 'number', value: offset },
            { type: 'number', value: count }
        ];
    }

    /**
     * Index/hash query columns by alias or column name.
     * @return {Object}
     */
    _getColumns() {
        const columns = {};
        this.ast.columns.forEach((column) => {
            columns[column.as !== null ? column.as : column.expr.column] = column.expr;
        });
        return columns;
    }
}

module.exports = function generateAST({ filter, limitPer, queryAst, searchable, search, order, limit, page }) {
    if (limitPer) throw new ImplementationError('flora-mysql does not support "limitPer" yet');

    const qb = new SqlQueryBuilder(queryAst);

    if ((filter && filter.length) || (queryAst._meta && queryAst._meta.hasFilterPlaceholders)) qb.filter(filter);

    if (search) qb.search(searchable, search);

    if (order && order.length) {
        order.forEach((sortSpec) => qb.orderBy(sortSpec.attribute, sortSpec.direction));
    }

    if (limit) {
        const offset = page ? (page - 1) * limit : 0;
        qb.limit(limit, offset);
    }

    return qb.getAST();
};
