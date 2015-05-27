'use strict';

var queryBuilder = require('../lib/sql-query-builder'),
    _ = require('lodash'),
    expect = require('chai').expect,
    astFixture = {
        type: 'select',
        distinct: null,
        columns: [
            { expr: { type: 'column_ref', table: 't', column: 'col1' }, as: null },
            { expr: { type: 'column_ref', table: 't', column: 'col2' }, as: null },
            { expr: { type: 'column_ref', table: 't', column: 'col3' }, as: 'columnAlias' }
        ],
        from: [{ db: null, table: 't', as: null }],
        where: null,
        groupby: null,
        orderby: null,
        limit: null
    };

describe('SQL query builder', function () {
    var ast;

    it('should work on cloned AST', function () {
        ast = queryBuilder({ queryAST: astFixture });
        expect(ast).to.not.equal(astFixture);
    });

    describe('order', function () {
        it('should order by one attribute', function () {
            ast = queryBuilder({
                order: [{ attribute: 'col1', direction: 'asc' }],
                queryAST: astFixture
            });
            expect(ast.orderby).to.eql([{
                expr: { type: 'column_ref', table: 't', column: 'col1' },
                type: 'ASC'
            }]);
        });

        it('should resolve alias to column', function () {
            ast = queryBuilder({
                order: [{ attribute: 'columnAlias', direction: 'desc' }],
                queryAST: astFixture
            });
            expect(ast.orderby).to.eql([{
                expr: { type: 'column_ref', table: 't', column: 'col3' },
                type: 'DESC'
            }]);
        });

        it('should order by multiple attributes', function () {
            ast = queryBuilder({
                order: [
                    { attribute: 'col1', direction: 'asc' },
                    { attribute: 'col2', direction: 'desc' }
                ],
                queryAST: astFixture
            });
            expect(ast.orderby).to.eql([
                { expr: { type: 'column_ref', table: 't', column: 'col1' }, type: 'ASC' },
                { expr: { type: 'column_ref', table: 't', column: 'col2' }, type: 'DESC' }
            ]);
        });
    });

    describe('limit', function () {
        it('should set limit', function () {
            ast = queryBuilder({
                limit: 17,
                queryAST: astFixture
            });
            expect(ast.limit).to.eql([
                { type: 'number', value: 0 },
                { type: 'number', value: 17 }
            ]);
        });

        it('should set limit with offset', function () {
            ast = queryBuilder({
                limit: 10,
                page: 3,
                queryAST: astFixture
            });
            expect(ast.limit).to.eql([
                { type: 'number', value: 20 },
                { type: 'number', value: 10 }
            ]);
        });
    });

    describe('filter', function () {
        it('should add single "AND" condition', function () {
            ast = queryBuilder({
                filter: [
                    [{ attribute: 'col1', operator: 'equal', value: 0 }]
                ],
                queryAST: astFixture
            });
            expect(ast.where).to.be.eql({
                type: 'binary_expr',
                operator: '=',
                left: { type: 'column_ref', table: 't', column: 'col1' },
                right: { type: 'number', value: 0 }
            });
        });

        it('should add multiple "AND" conditions', function () {
            ast = queryBuilder({
                filter: [
                    [
                        { attribute: 'col1', operator: 'greater', value: 10 },
                        { attribute: 'col1', operator: 'less', value: 20 }
                    ]
                ],
                queryAST: astFixture
            });
            expect(ast.where).to.be.eql({
                type: 'binary_expr',
                operator: 'AND',
                left: {
                    type: 'binary_expr',
                    operator: '>',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 10 }
                },
                right: {
                    type: 'binary_expr',
                    operator: '<',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 20 }
                }
            });
        });

        it('should add mulitple "OR" conditions', function () {
            ast = queryBuilder({
                filter: [
                    [{ attribute: 'col1', operator: 'greater', value: 10 }],
                    [{ attribute: 'col1', operator: 'less', value: 20 }]
                ],
                queryAST: astFixture
            });
            expect(ast.where).to.be.eql({
                type: 'binary_expr',
                operator: 'OR',
                left: {
                    type: 'binary_expr',
                    operator: '>',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 10 }
                },
                right: {
                    type: 'binary_expr',
                    operator: '<',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 20 }
                }
            });
        });

        it('should not overwrite existing where conditions (single filter)', function () {
            ast = _.assign({}, astFixture, {
                where: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 0 }
                }
            });

            ast = queryBuilder({
                filter: [
                    [{ attribute: 'col2', operator: 'greater', value: 100 }]
                ],
                queryAST: ast
            });

            expect(ast.where).to.be.eql({
                type: 'binary_expr',
                operator: 'AND',
                left: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 0 }
                },
                right: {
                    type: 'binary_expr',
                    operator: '>',
                    left: { type: 'column_ref', table: 't', column: 'col2' },
                    right: { type: 'number', value: 100 }
                }
            });
        });

        it('should not overwrite existing where conditions (multiple filters)', function () {
            ast = _.assign({}, astFixture, {
                where: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 0 }
                }
            });

            ast = queryBuilder({
                filter: [
                    [{ attribute: 'col2', operator: 'greater', value: 100 }],
                    [{ attribute: 'columnAlias', operator: 'lessOrEqual', value: 100 }]
                ],
                queryAST: ast
            });

            expect(ast.where).to.be.eql({
                type: 'binary_expr',
                operator: 'AND',
                left: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 0 }
                },
                right: {
                    type: 'binary_expr',
                    operator: 'OR',
                    left: {
                        type: 'binary_expr',
                        operator: '>',
                        left: { type: 'column_ref', table: 't', column: 'col2' },
                        right: { type: 'number', value: 100 }
                    },
                    right: {
                        type: 'binary_expr',
                        operator: '<=',
                        left: { type: 'column_ref', table: 't', column: 'col3' },
                        right: { type: 'number', value: 100 }
                    }
                }
            });
        });

        var operators = { equal: 'IN', notEqual: 'NOT IN' };
        Object.keys(operators).forEach(function (operator) {
            var apiOperator = operator,
                sqlOperator = operators[operator];

            it('should map "' + apiOperator + '" operator and array values to "' + sqlOperator + '" expr', function () {
                ast = queryBuilder({
                    filter: [
                        [{ attribute: 'col1', operator: apiOperator, value: [1, 3, 5] }]
                    ],
                    queryAST: astFixture
                });
                expect(ast.where).to.eql({
                    type: 'binary_expr',
                    operator: sqlOperator,
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: {
                        type: 'expr_list',
                        value: [
                            { type: 'number', value: 1 },
                            { type: 'number', value: 3 },
                            { type: 'number', value: 5 }
                        ]
                    }
                });
            });
        });

        it('should support arrays as attribute filters', function () {
            ast = queryBuilder({
                filter: [
                    [{
                        attribute: ['instrumentId', 'exchangeId'],
                        operator: 'equal',
                        value: [[133962, 4], [133962, 22]]
                    }]
                ],
                queryAST: {
                    type: 'select',
                    distinct: null,
                    columns: [
                        { expr: { type: 'column_ref', table: 't', column: 'instrumentId' }, as: null },
                        { expr: { type: 'column_ref', table: 't', column: 'exchangeId' }, as: null }
                    ],
                    from: [{ db: null, table: 't', as: null }],
                    where: null,
                    groupby: null,
                    orderby: null,
                    limit: null
                }
            });

            expect(ast.where).to.eql({
                type: 'binary_expr',
                operator: 'OR',
                left: {
                    type: 'binary_expr',
                    operator: 'AND',
                    left: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 't', column: 'instrumentId' },
                        right: { type: 'number', value: 133962 }
                    },
                    right: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 't', column: 'exchangeId' },
                        right: { type: 'number', value: 4 }
                    }
                },
                right: {
                    type: 'binary_expr',
                    operator: 'AND',
                    left: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 't', column: 'instrumentId' },
                        right: { type: 'number', value: 133962 }
                    },
                    right: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 't', column: 'exchangeId' },
                        right: { type: 'number', value: 22 }
                    }
                },
                paren: true
            });
        });
    });

    describe('full-text search', function () {
        it('should support single attribute', function () {
            ast = queryBuilder({
                searchable: ['col1'],
                search: 'foobar',
                queryAST: astFixture
            });

            expect(ast.where).to.eql({
                type: 'binary_expr',
                operator: 'LIKE',
                left: { type: 'column_ref', table: 't', column: 'col1' },
                right: { type: 'string', value: '%foobar%' }
            });
        });

        it('should support multiple attributes', function () {
            ast = queryBuilder({
                searchable: ['col1', 'columnAlias'],
                search: 'foobar',
                queryAST: astFixture
            });

            expect(ast.where).to.eql({
                type: 'binary_expr',
                operator: 'OR',
                left: {
                    type: 'binary_expr',
                    operator: 'LIKE',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'string', value: '%foobar%' }
                },
                right: {
                    type: 'binary_expr',
                    operator: 'LIKE',
                    left: { type: 'column_ref', table: 't', column: 'col3' },
                    right: { type: 'string', value: '%foobar%' }
                }
            });
        });

        it('should escape special pattern characters "%" and "_"', function () {
            ast = queryBuilder({
                searchable: ['col1'],
                search: 'f%o_o%b_ar',
                queryAST: astFixture
            });

            expect(ast.where).to.eql({
                type: 'binary_expr',
                operator: 'LIKE',
                left: { type: 'column_ref', table: 't', column: 'col1' },
                right: { type: 'string', value: '%f\\%o\\_o\\%b\\_ar%' }
            });
        });

        it('should support multiple attributes and non-empty where clause', function () {
            ast = queryBuilder({
                filter: [
                    [{ attribute: 'col2', operator: 'equal', value: 5 }]
                ],
                searchable: ['col1', 'columnAlias'],
                search: 'foobar',
                queryAST: astFixture
            });

            expect(ast.where).to.eql({
                type: 'binary_expr',
                operator: 'AND',
                left: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'col2' },
                    right: { type: 'number', value: 5 }
                },
                right: {
                    type: 'binary_expr',
                    operator: 'OR',
                    left: {
                        type: 'binary_expr',
                        operator: 'LIKE',
                        left: { type: 'column_ref', table: 't', column: 'col1' },
                        right: { type: 'string', value: '%foobar%' }
                    },
                    right: {
                        type: 'binary_expr',
                        operator: 'LIKE',
                        left: { type: 'column_ref', table: 't', column: 'col3' },
                        right: { type: 'string', value: '%foobar%' }
                    },
                    paren: true
                }
            });
        });
    });
});
