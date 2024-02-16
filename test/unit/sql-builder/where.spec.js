'use strict';

const assert = require('node:assert/strict');
const { beforeEach, describe, it } = require('node:test');

const queryBuilder = require('../../../lib/sql-query-builder');
const astFixture = require('./fixture');

describe('query-builder (where)', () => {
    let queryAst;

    beforeEach(() => (queryAst = structuredClone(astFixture)));

    it('should add single "AND" condition', () => {
        const ast = queryBuilder({
            queryAst,
            filter: [[{ attribute: 'col1', operator: 'equal', value: 0 }]]
        });

        assert.deepEqual(ast.where, {
            type: 'binary_expr',
            operator: '=',
            left: { type: 'column_ref', table: 't', column: 'col1' },
            right: { type: 'number', value: 0 }
        });
    });

    it('should add multiple "AND" conditions', () => {
        const ast = queryBuilder({
            queryAst,
            filter: [
                [
                    { attribute: 'col1', operator: 'greater', value: 10 },
                    { attribute: 'col1', operator: 'less', value: 20 }
                ]
            ]
        });

        assert.deepEqual(ast.where, {
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

    it('should remove empty "AND"/"OR" conditions', () => {
        const ast = queryBuilder({ queryAst, filter: [[]] });

        assert.equal(ast.where, null);
    });

    it('should add mulitple "OR" conditions', () => {
        const ast = queryBuilder({
            queryAst,
            filter: [
                [{ attribute: 'col1', operator: 'greater', value: 10 }],
                [{ attribute: 'col1', operator: 'less', value: 20 }]
            ]
        });

        assert.deepEqual(ast.where, {
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
        queryAst = {
            ...queryAst,
            ...{
                where: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 0 }
                }
            }
        };

        const ast = queryBuilder({
            queryAst,
            filter: [[{ attribute: 'col2', operator: 'greater', value: 100 }]]
        });

        assert.deepEqual(ast.where, {
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
        queryAst = {
            ...astFixture,
            ...{
                where: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 0 }
                }
            }
        };

        const ast = queryBuilder({
            queryAst,
            filter: [
                [{ attribute: 'col2', operator: 'greater', value: 100 }],
                [{ attribute: 'columnAlias', operator: 'lessOrEqual', value: 100 }]
            ]
        });

        assert.deepEqual(ast.where, {
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
        queryAst = {
            ...queryAst,
            ...{
                where: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'col1' },
                    right: { type: 'number', value: 0 }
                }
            }
        };

        const ast = queryBuilder({
            queryAst,
            filter: [[{ attribute: 'col2', operator: 'greater', value: 100 }]]
        });

        assert.ok(Object.hasOwn(ast.where.left, 'parentheses'));
        assert.equal(ast.where.left.parentheses, true);
        assert.ok(Object.hasOwn(ast.where.right, 'parentheses'));
        assert.equal(ast.where.right.parentheses, true);
    });

    it('should support arrays as attribute filters', () => {
        queryAst = {
            with: null,
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

        const ast = queryBuilder({
            queryAst,
            filter: [
                [
                    {
                        attribute: ['instrumentId', 'exchangeId'],
                        operator: 'equal',
                        value: [
                            [133962, 4],
                            [133962, 22]
                        ]
                    }
                ]
            ]
        });

        assert.deepEqual(ast.where, {
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
            const ast = queryBuilder({
                queryAst: { ...astFixture },
                filter: [[{ attribute: 'col1', operator: filterOperator, value: null }]]
            });

            assert.deepEqual(ast.where, {
                type: 'binary_expr',
                operator: sqlOperator,
                left: { type: 'column_ref', table: 't', column: 'col1' },
                right: { type: 'null', value: null }
            });
        });
    });
});
