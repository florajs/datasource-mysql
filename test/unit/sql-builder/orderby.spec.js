'use strict';

const { expect } = require('chai');

const queryBuilder = require('../../../lib/sql-query-builder');
const astFixture = require('./fixture');

describe('query-builder (order)', () => {
    let ast;

    beforeEach(() => {
        ast = JSON.parse(JSON.stringify(astFixture));
    });

    afterEach(() => {
        ast = null;
    });

    it('should order by one attribute', () => {
        queryBuilder({
            order: [{ attribute: 'col1', direction: 'asc' }],
            queryAst: ast
        });

        expect(ast.orderby).to.eql([
            {
                expr: { type: 'column_ref', table: 't', column: 'col1' },
                type: 'ASC'
            }
        ]);
    });

    it('should order by one attribute (case insensitive direction)', () => {
        queryBuilder({
            order: [{ attribute: 'col1', direction: 'ASC' }],
            queryAst: ast
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
                order: [{ attribute: 'col1', direction: 'invalid' }],
                queryAst: ast
            });
        }).to.throw(Error, /Invalid order direction/);
    });

    it('should order by one attribute (random)', () => {
        queryBuilder({
            order: [{ attribute: 'col1', direction: 'random' }],
            queryAst: ast
        });

        expect(ast.orderby).to.eql([
            {
                expr: { type: 'function', name: 'RAND', args: { type: 'expr_list', value: [] } },
                type: ''
            }
        ]);
    });

    it('should order by one attribute (rAnDoM, case insensitive)', () => {
        queryBuilder({
            order: [{ attribute: 'col1', direction: 'rAnDoM' }],
            queryAst: ast
        });

        expect(ast.orderby).to.eql([
            {
                expr: { type: 'function', name: 'RAND', args: { type: 'expr_list', value: [] } },
                type: ''
            }
        ]);
    });

    it('should resolve alias to column', () => {
        queryBuilder({
            order: [{ attribute: 'columnAlias', direction: 'desc' }],
            queryAst: ast
        });

        expect(ast.orderby).to.eql([
            {
                expr: { type: 'column_ref', table: 't', column: 'col3' },
                type: 'DESC'
            }
        ]);
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
