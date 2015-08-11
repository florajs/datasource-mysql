'use strict';

var _ = require('lodash'),
    createBinaryExpr = require('flora-sql-parser').util.createBinaryExpr;

var OPERATOR_MAPPING = {
    equal: '=',
    notEqual: '!=',
    greater: '>',
    greaterOrEqual: '>=',
    less: '<',
    lessOrEqual: '<='
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
 * @param {Object} ast  - Abstract syntax tree representation of SQL query
 * @constructor
 */
function SqlQueryBuilder(ast) {
    this.ast = ast;
    this.columns = this._getColumns();
}

/**
 * @return {Object}
 */
SqlQueryBuilder.prototype.getAST = function () {
    return this.ast;
};

/**
 * Convert array of filters to expressions in where part of AST.
 *
 * @param {Array.<Array>} filters
 */
SqlQueryBuilder.prototype.filter = function (filters) {
    var self = this,
        orExpressions,
        conditions;

    orExpressions = filters.map(function processFilters(andFilters) {
        var andFilterExpressions = andFilters.map(function mapFiltersToSqlExpression(filter) {
            var operator = OPERATOR_MAPPING[filter.operator];

            if (! Array.isArray(filter.attribute)) return mapSingleAttributeFilter(filter, operator, self.columns);
            else return mapMultiAttributeFilter(filter, operator, self.columns);
        });
        return andFilterExpressions.reduce(createBinaryAndExpr); // combine expressions with AND operator
    });

    // only combine multiple 'or' expressions with OR operator
    conditions = orExpressions.length === 1 ? orExpressions[0] : orExpressions.reduce(createBinaryOrExpr);
    // if query from resource config contains WHERE clause, combine it with filter conditions using AND operator
    self.ast.where = self.ast.where === null ? conditions : createBinaryAndExpr(self.ast.where, conditions);
};

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
    return createBinaryExpr(operator, columns[filter.attribute], filter.value);
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
 * "... (key1 = 1 AND key2 = 2 AND ... AND keyX = valX) OR (key1 = 3 AND key2 = 4 AND ... AND keyX = valY) ..."
 *
 * @param {Object} filter
 * @param {string} operator
 * @param {Array.<string>} columns
 * @return {Object}
 * @private
 */
function mapMultiAttributeFilter(filter, operator, columns) {
    var expr;

    expr = filter.value
        .map(function (values) { // create AND-expressions from attribute/value pairs
            return values
                .map(function (value, index) {
                    return createBinaryExpr(operator, columns[filter.attribute[index]], value);
                })
                .reduce(createBinaryAndExpr);
        })
        .reduce(createBinaryOrExpr); // concatenate AND- with OR-expressions

    expr.parentheses = true; // parenthesize complete expression
    return expr;
}

/**
 * Full-text search using LIKE operator.
 *
 * @param {Array.<string>} attributes
 * @param {string} term
 */
SqlQueryBuilder.prototype.search = function (attributes, term) {
    var self = this,
        searchExpressions;

    term = term.replace(/(%|_)/g, '\\$1'); // escape LIKE pattern characters

    searchExpressions = attributes
        .map(function (attribute) { // create LIKE expression for every attribute
            return {
                type: 'binary_expr',
                operator: 'LIKE',
                left: self.columns[attribute],
                right: { type: 'string', value: '%' + term + '%' }
            };
        })
        .reduce(createBinaryOrExpr); // create sub-tree of search expressions

    if (this.ast.where !== null && typeof this.ast.where === 'object') {
        searchExpressions.paren = true; // parenthesize search expressions
        this.ast.where = createBinaryAndExpr(this.ast.where, searchExpressions);
    } else {
        this.ast.where = searchExpressions;
    }
};

/**
 * Add order specifiction to AST/query.
 *
 * @param {string} attr
 * @param {string} direction
 */
SqlQueryBuilder.prototype.orderBy = function (attr, direction) {
    if (! Array.isArray(this.ast.orderby)) this.ast.orderby = [];
    this.ast.orderby.push({
        expr: this.columns[attr],
        type: direction.toUpperCase()
    });
};

/**
 * Limit result set of the query.
 *
 * @param {number} count
 * @param {number} offset
 */
SqlQueryBuilder.prototype.limit = function (count, offset) {
    this.ast.limit = [
        { type: 'number', value: offset },
        { type: 'number', value: count }
    ];
};

/**
 * Index/hash query columns by alias or column name.
 * @return {Object}
 */
SqlQueryBuilder.prototype._getColumns = function () {
    var columns = {};
    this.ast.columns.forEach(function (column) {
        columns[column.as !== null ? column.as : column.expr.column] = column.expr;
    });
    return columns;
};

module.exports = function (req) {
    var qb = new SqlQueryBuilder(req.queryAST),
        offset;

    if (req.filter && req.filter.length) qb.filter(req.filter);
    if (req.search) qb.search(req.searchable, req.search);

    if (req.order && req.order.length) {
        req.order.forEach(function (sortSpec) {
            qb.orderBy(sortSpec.attribute, sortSpec.direction);
        });
    }

    if (req.limit) {
        offset = req.page ? (req.page - 1) * req.limit : 0;
        qb.limit(req.limit, offset);
    }

    return qb.getAST();
};
