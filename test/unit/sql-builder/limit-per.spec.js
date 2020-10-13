'use strict';

const { expect } = require('chai');

const queryBuilder = require('../../../lib/sql-query-builder');
const astFixture = require('./fixture');

// TODO check parentheses
describe('query-builder (limit-per)', () => {
    let queryAst;

    beforeEach(() => {
        queryAst = JSON.parse(JSON.stringify(astFixture));
    });

    afterEach(() => {
        queryAst = null;
    });

    it('should use filtered ids in VALUES expression', () => {
        const ast = queryBuilder({
            attributes: ['col2'],
            queryAst,
            limitPer: 'col1',
            limit: 5,
            filter: [[{ attribute: 'col1', operator: 'equal', valueFromParentKey: true, value: [1, 3] }]]
        });

        const [valuesExpr] = ast.from;
        expect(valuesExpr).to.be.eql({
            expr: {
                type: 'values',
                value: [
                    { type: 'row_value', keyword: true, value: [{ type: 'number', value: 1 }] },
                    { type: 'row_value', keyword: true, value: [{ type: 'number', value: 3 }] }
                ]
            },
            as: 'limitper_ids',
            columns: ['id']
        });
    });

    it('should use original query in lateral join', () => {
        const ast = queryBuilder({
            attributes: ['col2'],
            queryAst,
            limitPer: 'col1',
            limit: 5,
            filter: [[{ attribute: 'col1', operator: 'equal', valueFromParentKey: true, value: [1, 3] }]]
        });

        const [, laterlJoin] = ast.from;
        const { expr: originalQuery } = laterlJoin;
        expect(originalQuery).to.have.property('type', 'select');
        expect(originalQuery).to.have.property('from').and.to.eql(queryAst.from);
        expect(originalQuery).to.have.property('parentheses', true);
        expect(laterlJoin).to.have.property('join', 'JOIN');
        expect(laterlJoin).to.have.property('lateral', true);
        expect(laterlJoin).to.have.property('on').and.to.eql({ type: 'boolean', value: true });
        expect(laterlJoin).to.have.property('as', 'limitper');
    });

    it('should select attributes from derived table alias', () => {
        const ast = queryBuilder({
            attributes: ['col2'],
            queryAst,
            limitPer: 'col1',
            limit: 5,
            filter: [[{ attribute: 'col1', operator: 'equal', valueFromParentKey: true, value: [1, 3] }]]
        });

        expect(ast.columns).to.eql([
            {
                expr: { type: 'column_ref', table: 'limitper', column: 'col2' },
                as: null
            }
        ]);
    });

    describe('filters', () => {
        it('should generate WHERE clause for correlated subquery', () => {
            const ast = queryBuilder({
                attributes: ['col2'],
                queryAst,
                limitPer: 'col1',
                limit: 5,
                filter: [[{ attribute: 'col1', operator: 'equal', valueFromParentKey: true, value: [1, 3] }]]
            });

            const [, { expr: subSelect }] = ast.from;
            expect(subSelect)
                .to.have.property('where')
                .and.to.eql({
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 'limitper_ids', column: 'id' },
                    right: { type: 'column_ref', table: 't', column: 'col1' }
                });
        });

        it('should generate WHERE clause for correlated subquery for existing conditions', () => {
            const ast = queryBuilder({
                attributes: ['col2'],
                queryAst: {
                    ...queryAst,
                    where: {
                        type: 'binary_expr',
                        operator: 'IS',
                        left: { type: 'column_ref', table: 't', column: 'deleted_at' },
                        right: { type: 'null', value: null }
                    }
                },
                limitPer: 'col1',
                limit: 5,
                filter: [[{ attribute: 'col1', operator: 'equal', valueFromParentKey: true, value: [1, 3] }]]
            });

            const [, { expr: subSelect }] = ast.from;
            expect(subSelect)
                .to.have.property('where')
                .and.to.eql({
                    type: 'binary_expr',
                    operator: 'AND',
                    left: {
                        type: 'binary_expr',
                        operator: 'IS',
                        left: { type: 'column_ref', table: 't', column: 'deleted_at' },
                        right: { type: 'null', value: null },
                        parentheses: true
                    },
                    right: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 'limitper_ids', column: 'id' },
                        right: { type: 'column_ref', table: 't', column: 'col1' }
                    }
                });
        });

        it('should generate WHERE clause for correlated subquery for OR filters', () => {
            const ast = queryBuilder({
                attributes: ['col2'],
                queryAst: {
                    ...queryAst,
                    where: {
                        type: 'binary_expr',
                        operator: 'IS',
                        left: { type: 'column_ref', table: 't', column: 'deleted_at' },
                        right: { type: 'null', value: null }
                    }
                },
                limitPer: 'col1',
                limit: 5,
                filter: [
                    [
                        { attribute: 'col2', operator: 'lessOrEqual', value: 10 },
                        { attribute: 'col1', operator: 'equal', valueFromParentKey: true, value: [1, 3] }
                    ],
                    [
                        { attribute: 'col2', operator: 'greaterOrEqual', value: 5 },
                        { attribute: 'col1', operator: 'equal', valueFromParentKey: true, value: [1, 3] }
                    ]
                ]
            });

            const [, { expr: subSelect }] = ast.from;
            expect(subSelect)
                .to.have.property('where')
                .and.to.eql({
                    type: 'binary_expr',
                    operator: 'AND',
                    left: {
                        type: 'binary_expr',
                        operator: 'IS',
                        left: { type: 'column_ref', table: 't', column: 'deleted_at' },
                        right: { type: 'null', value: null },
                        parentheses: true
                    },
                    right: {
                        type: 'binary_expr',
                        operator: 'OR',
                        left: {
                            type: 'binary_expr',
                            operator: 'AND',
                            left: {
                                type: 'binary_expr',
                                operator: '<=',
                                left: {
                                    column: 'col2',
                                    table: 't',
                                    type: 'column_ref'
                                },
                                right: {
                                    type: 'number',
                                    value: 10
                                }
                            },
                            right: {
                                type: 'binary_expr',
                                operator: '=',
                                left: {
                                    column: 'id',
                                    table: 'limitper_ids',
                                    type: 'column_ref'
                                },
                                right: {
                                    column: 'col1',
                                    table: 't',
                                    type: 'column_ref'
                                }
                            }
                        },
                        right: {
                            type: 'binary_expr',
                            operator: 'AND',
                            left: {
                                type: 'binary_expr',
                                operator: '>=',
                                left: {
                                    column: 'col2',
                                    table: 't',
                                    type: 'column_ref'
                                },
                                right: {
                                    type: 'number',
                                    value: 5
                                }
                            },
                            right: {
                                type: 'binary_expr',
                                operator: '=',
                                left: {
                                    column: 'id',
                                    table: 'limitper_ids',
                                    type: 'column_ref'
                                },
                                right: {
                                    column: 'col1',
                                    table: 't',
                                    type: 'column_ref'
                                }
                            }
                        },
                        parentheses: true
                    }
                });
        });

        xit('should support search?');
    });

    it('should apply limit to lateral join', () => {
        const ast = queryBuilder({
            attributes: ['col2'],
            queryAst,
            limitPer: 'col1',
            limit: 5,
            filter: [[{ attribute: 'col1', operator: 'equal', valueFromParentKey: true, value: [1, 3] }]]
        });

        const [, { expr: subSelect }] = ast.from;
        expect(subSelect)
            .to.have.property('limit')
            .and.to.eql([
                { type: 'number', value: 0 },
                { type: 'number', value: 5 }
            ]);
    });

    it('should apply page to lateral join', () => {
        const ast = queryBuilder({
            attributes: ['col2'],
            queryAst,
            limitPer: 'col1',
            filter: [[{ attribute: 'col1', operator: 'equal', valueFromParentKey: true, value: [1, 3] }]],
            order: [{ attribute: 'col2', direction: 'desc' }],
            limit: 10,
            page: 5
        });

        const [, { expr: subSelect }] = ast.from;
        expect(subSelect)
            .to.have.property('limit')
            .and.to.eql([
                { type: 'number', value: 40 },
                { type: 'number', value: 10 }
            ]);
    });

    it('should apply order to lateral join', () => {
        const ast = queryBuilder({
            attributes: ['col2'],
            queryAst,
            limitPer: 'col1',
            filter: [[{ attribute: 'col1', operator: 'equal', valueFromParentKey: true, value: [1, 3] }]],
            order: [{ attribute: 'col2', direction: 'desc' }]
        });

        const [, { expr: subSelect }] = ast.from;
        expect(subSelect)
            .to.have.property('orderby')
            .and.to.eql([{ expr: { type: 'column_ref', table: 't', column: 'col2' }, type: 'DESC' }]);
    });

    xit('should optimize lateraled/original query'); // remove unused columns and joins
});
