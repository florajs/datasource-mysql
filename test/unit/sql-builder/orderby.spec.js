'use strict';

const assert = require('node:assert/strict');
const { beforeEach, describe, it } = require('node:test');

const queryBuilder = require('../../../lib/sql-query-builder');
const astFixture = require('./fixture');

describe('query-builder (order)', () => {
    let queryAst;

    beforeEach(() => (queryAst = structuredClone(astFixture)));

    it('should order by one attribute', () => {
        const ast = queryBuilder({
            queryAst,
            order: [{ attribute: 'col1', direction: 'asc' }]
        });

        assert.deepEqual(ast.orderby, [
            {
                expr: { type: 'column_ref', table: 't', column: 'col1' },
                type: 'ASC'
            }
        ]);
    });

    it('should order by one attribute (case insensitive direction)', () => {
        const ast = queryBuilder({
            queryAst,
            order: [{ attribute: 'col1', direction: 'ASC' }]
        });

        assert.deepEqual(ast.orderby, [
            {
                expr: { type: 'column_ref', table: 't', column: 'col1' },
                type: 'ASC'
            }
        ]);
    });

    it('should throw on invalid direction', () => {
        assert.throws(
            () =>
                queryBuilder({
                    queryAst,
                    order: [{ attribute: 'col1', direction: 'invalid' }]
                }),
            {
                name: 'Error',
                message: /Invalid order direction/
            }
        );
    });

    it('should order by one attribute (random)', () => {
        const ast = queryBuilder({
            queryAst,
            order: [{ attribute: 'col1', direction: 'random' }]
        });

        assert.deepEqual(ast.orderby, [
            {
                expr: { type: 'function', name: 'RAND', args: { type: 'expr_list', value: [] } },
                type: ''
            }
        ]);
    });

    it('should order by one attribute (rAnDoM, case insensitive)', () => {
        const ast = queryBuilder({
            queryAst,
            order: [{ attribute: 'col1', direction: 'rAnDoM' }]
        });

        assert.deepEqual(ast.orderby, [
            {
                expr: { type: 'function', name: 'RAND', args: { type: 'expr_list', value: [] } },
                type: ''
            }
        ]);
    });

    it('should resolve alias to column', () => {
        const ast = queryBuilder({
            queryAst,
            order: [{ attribute: 'columnAlias', direction: 'desc' }]
        });

        assert.deepEqual(ast.orderby, [
            {
                expr: { type: 'column_ref', table: 't', column: 'col3' },
                type: 'DESC'
            }
        ]);
    });

    it('should order by multiple attributes', () => {
        const ast = queryBuilder({
            queryAst,
            order: [
                { attribute: 'col1', direction: 'asc' },
                { attribute: 'col2', direction: 'desc' }
            ]
        });

        assert.deepEqual(ast.orderby, [
            { expr: { type: 'column_ref', table: 't', column: 'col1' }, type: 'ASC' },
            { expr: { type: 'column_ref', table: 't', column: 'col2' }, type: 'DESC' }
        ]);
    });
});
