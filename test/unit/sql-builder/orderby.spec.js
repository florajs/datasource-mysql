'use strict';

const { expect } = require('chai');

const queryBuilder = require('../../../lib/sql-query-builder');
const astFixture = require('./fixture');

describe('query-builder (order)', () => {
    let queryAst;

    beforeEach(() => {
        queryAst = JSON.parse(JSON.stringify(astFixture));
    });

    afterEach(() => {
        queryAst = null;
    });

    it('should order by one attribute', () => {
        const ast = queryBuilder({
            queryAst,
            order: [{ attribute: 'col1', direction: 'asc' }]
        });

        expect(ast.orderby).to.eql([
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

        expect(ast.orderby).to.eql([
            {
                expr: { type: 'column_ref', table: 't', column: 'col1' },
                type: 'ASC'
            }
        ]);
    });

    it('should throw on invalid direction', () => {
        expect(() => {
            queryBuilder({
                queryAst,
                order: [{ attribute: 'col1', direction: 'invalid' }]
            });
        }).to.throw(Error, /Invalid order direction/);
    });

    it('should order by one attribute (random)', () => {
        const ast = queryBuilder({
            queryAst,
            order: [{ attribute: 'col1', direction: 'random' }]
        });

        expect(ast.orderby).to.eql([
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

        expect(ast.orderby).to.eql([
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

        expect(ast.orderby).to.eql([
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

        expect(ast.orderby).to.eql([
            { expr: { type: 'column_ref', table: 't', column: 'col1' }, type: 'ASC' },
            { expr: { type: 'column_ref', table: 't', column: 'col2' }, type: 'DESC' }
        ]);
    });
});
