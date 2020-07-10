'use strict';

const queryBuilder = require('../../lib/sql-query-builder');
const { expect } = require('chai');
const astFixture = {
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
const { ImplementationError } = require('flora-errors');

describe('SQL query builder', () => {
    let ast;

    beforeEach(() => {
        ast = JSON.parse(JSON.stringify(astFixture));
    });

    afterEach(() => {
        ast = null;
    });

    describe('order', () => {
        it('should order by one attribute', () => {
            queryBuilder({
                order: [{ attribute: 'col1', direction: 'asc' }],
                queryAst: ast
            });
            expect(ast.orderby).to.eql([{
                expr: { type: 'column_ref', table: 't', column: 'col1' },
                type: 'ASC'
            }]);
        });

        it('should resolve alias to column', () => {
            queryBuilder({
                order: [{ attribute: 'columnAlias', direction: 'desc' }],
                queryAst: ast
            });
            expect(ast.orderby).to.eql([{
                expr: { type: 'column_ref', table: 't', column: 'col3' },
                type: 'DESC'
            }]);
        });

        it('should order by multiple attributes', () => {
            queryBuilder({
                order: [
                    { attribute: 'col1', direction: 'asc' },
                    { attribute: 'col2', direction: 'desc' }
                ],
                queryAst: ast
            });
            expect(ast.orderby).to.eql([
                { expr: { type: 'column_ref', table: 't', column: 'col1' }, type: 'ASC' },
                { expr: { type: 'column_ref', table: 't', column: 'col2' }, type: 'DESC' }
            ]);
        });
    });

    describe('limit', () => {
        it('should set limit', () => {
            queryBuilder({
                limit: 17,
                queryAst: ast
            });
            expect(ast.limit).to.eql([
                { type: 'number', value: 0 },
                { type: 'number', value: 17 }
            ]);
        });

        it('should set limit with offset', () => {
            queryBuilder({
                limit: 10,
                page: 3,
                queryAst: ast
            });
            expect(ast.limit).to.eql([
                { type: 'number', value: 20 },
                { type: 'number', value: 10 }
            ]);
        });
    });

    describe('filter', () => {
        it('should add single "AND" condition', () => {
            queryBuilder({
                filter: [
                    [{ attribute: 'col1', operator: 'equal', value: 0 }]
                ],
                queryAst: ast
            });
            expect(ast.where).to.be.eql({
                type: 'binary_expr',
                operator: '=',
                left: { type: 'column_ref', table: 't', column: 'col1' },
                right: { type: 'number', value: 0 }
            });
        });

        it('should add multiple "AND" conditions', () => {
            queryBuilder({
                filter: [
                    [
                        { attribute: 'col1', operator: 'greater', value: 10 },
                        { attribute: 'col1', operator: 'less', value: 20 }
                    ]
                ],
                queryAst: ast
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

        it('should add mulitple "OR" conditions', () => {
            queryBuilder({
                filter: [
                    [{ attribute: 'col1', operator: 'greater', value: 10 }],
                    [{ attribute: 'col1', operator: 'less', value: 20 }]
                ],
                queryAst: ast
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

        it('should not overwrite existing where conditions (single filter)', () => {
            ast = {...ast, ...{
                where: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 0 }
                }
            }};

            queryBuilder({
                filter: [
                    [{ attribute: 'col2', operator: 'greater', value: 100 }]
                ],
                queryAst: ast
            });

            expect(ast.where).to.be.eql({
                type: 'binary_expr',
                operator: 'AND',
                left: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 0 },
                    parentheses: true
                },
                right: {
                    type: 'binary_expr',
                    operator: '>',
                    left: { type: 'column_ref', table: 't', column: 'col2' },
                    right: { type: 'number', value: 100 },
                    parentheses: true
                }
            });
        });

        it('should not overwrite existing where conditions (multiple filters)', () => {
            ast = {...astFixture, ...{
                where: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 0 }
                }
            }};

            queryBuilder({
                filter: [
                    [{ attribute: 'col2', operator: 'greater', value: 100 }],
                    [{ attribute: 'columnAlias', operator: 'lessOrEqual', value: 100 }]
                ],
                queryAst: ast
            });

            expect(ast.where).to.be.eql({
                type: 'binary_expr',
                operator: 'AND',
                left: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 0 },
                    parentheses: true
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
                    },
                    parentheses: true
                }
            });
        });

        it('should use parentheses to group conditions', () => {
            ast = {...ast, ...{
                where: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 0 }
                }
            }};

            queryBuilder({
                filter: [
                    [{ attribute: 'col2', operator: 'greater', value: 100 }]
                ],
                queryAst: ast
            });

            expect(ast.where.left).to.have.property('parentheses', true);
            expect(ast.where.right).to.have.property('parentheses', true);
        });

        it('should support arrays as attribute filters', () => {
            const ast = {
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
            };

            queryBuilder({
                filter: [
                    [{
                        attribute: ['instrumentId', 'exchangeId'],
                        operator: 'equal',
                        value: [[133962, 4], [133962, 22]]
                    }]
                ],
                queryAst: ast
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
                parentheses: true
            });
        });

        Object.entries({ equal: 'IS', notEqual: 'IS NOT' }).forEach(([filterOperator, sqlOperator]) => {
            it(`should support ${filterOperator} operator and null values`, () => {
                const ast = { ...astFixture };

                queryBuilder({
                    filter: [[{ attribute: 'col1', operator: filterOperator, value: null }]],
                    queryAst: ast
                });

                expect(ast.where).to.eql({
                    type: 'binary_expr',
                    operator: sqlOperator,
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'null', value: null }
                });
            });
        });

        describe('placeholder', () => {
            const EMPTY_FILTER_FALLBACK = {
                type: 'binary_expr',
                operator: '=',
                left: { type: 'number', value: 1 },
                right: { type: 'number', value: 1 }
            };
            let floraFilterPlaceholder;

            beforeEach(() => {
                floraFilterPlaceholder = { type: 'column_ref', table: null, column: '__floraFilterPlaceholder__' };
            });

            it('should be replaced by "1 = 1" for empty request filters', () => {
                const ast = {...astFixture, ...{ _meta: { hasFilterPlaceholders: true }, where: floraFilterPlaceholder }};

                queryBuilder({ queryAst: ast });

                expect(ast.where).to.eql(EMPTY_FILTER_FALLBACK);
            });

            it('should be replaced by request filter(s)', () => {
                const ast = {...astFixture, ...{ _meta: { hasFilterPlaceholders: true }, where: floraFilterPlaceholder }};

                queryBuilder({
                    filter: [
                        [{ attribute: 'col1', operator: 'equal', value: 1 }]
                    ],
                    queryAst: ast
                });

                expect(ast.where).to.eql({
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 1 }
                });
            });

            it('should be replaced multiple times', () => {
                const unionFloraFilterPlaceholder = { ...floraFilterPlaceholder };
                /*
                    SELECT col
                    FROM t
                    WHERE __floraFilterPlaceholder__

                    UNION

                    SELECT col
                    FROM t2
                    WHERE __floraFilterPlaceholder__
                 */
                const ast = {
                    _meta: { hasFilterPlaceholders: true },
                    type: 'select',
                    distinct: null,
                    columns: [{ expr: { type: 'column_ref', table: null, column: 'col' }, as: null }],
                    from: [{ db: null, table: 't', as: null }],
                    where: floraFilterPlaceholder,
                    groupby: null,
                    orderby: null,
                    limit: null,
                    _next: {
                        type: 'select',
                        distinct: null,
                        columns: [{ expr: { type: 'column_ref', table: null, column: 'col' }, as: null }],
                        from: [{ db: null, table: 't2', as: null }],
                        where: unionFloraFilterPlaceholder,
                        groupby: null,
                        having: null,
                        orderby: null,
                        limit: null
                    }
                };

                queryBuilder({ queryAst: ast });

                expect(ast.where).to.eql(EMPTY_FILTER_FALLBACK);
                expect(ast._next.where).to.eql(EMPTY_FILTER_FALLBACK);
            });
        });
    });

    describe('full-text search', () => {
        it('should support single attribute', () => {
            queryBuilder({
                searchable: ['col1'],
                search: 'foobar',
                queryAst: ast
            });

            expect(ast.where).to.eql({
                type: 'binary_expr',
                operator: 'LIKE',
                left: { type: 'column_ref', table: 't', column: 'col1' },
                right: { type: 'string', value: '%foobar%' }
            });
        });

        it('should support multiple attributes', () => {
            queryBuilder({
                searchable: ['col1', 'columnAlias'],
                search: 'foobar',
                queryAst: ast
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

        it('should escape special pattern characters "%" and "_"', () => {
            queryBuilder({
                searchable: ['col1'],
                search: 'f%o_o%b_ar',
                queryAst: ast
            });

            expect(ast.where).to.eql({
                type: 'binary_expr',
                operator: 'LIKE',
                left: { type: 'column_ref', table: 't', column: 'col1' },
                right: { type: 'string', value: '%f\\%o\\_o\\%b\\_ar%' }
            });
        });

        it('should support multiple attributes and non-empty where clause', () => {
            queryBuilder({
                filter: [
                    [{ attribute: 'col2', operator: 'equal', value: 5 }]
                ],
                searchable: ['col1', 'columnAlias'],
                search: 'foobar',
                queryAst: ast
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
                    parentheses: true
                }
            });
        });
    });

    describe('limitPer', () => {
        it('should throw error if "limitPer" key is set', () => {
            expect(() => {
                queryBuilder({
                    limitPer: 'someId',
                    queryAst: ast
                });
            }).to.throw(ImplementationError, /does not support "limitPer"/);
        });
    });
});
