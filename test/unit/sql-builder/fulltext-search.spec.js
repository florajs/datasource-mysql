'use strict';

const { expect } = require('chai');

const queryBuilder = require('../../../lib/sql-query-builder');
const astFixture = require('./fixture');

describe('query-builder (fulltext search)', () => {
    let queryAst;

    beforeEach(() => {
        queryAst = JSON.parse(JSON.stringify(astFixture));
    });

    afterEach(() => {
        queryAst = null;
    });

    it('should support single attribute', () => {
        const ast = queryBuilder({
            queryAst,
            searchable: ['col1'],
            search: 'foobar'
        });

        expect(ast.where).to.eql({
            type: 'binary_expr',
            operator: 'LIKE',
            left: { type: 'column_ref', table: 't', column: 'col1' },
            right: { type: 'string', value: '%foobar%' }
        });
    });

    it('should support multiple attributes', () => {
        const ast = queryBuilder({
            queryAst,
            searchable: ['col1', 'columnAlias'],
            search: 'foobar'
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
        const ast = queryBuilder({
            queryAst,
            searchable: ['col1'],
            search: 'f%o_o%b_ar'
        });

        expect(ast.where).to.eql({
            type: 'binary_expr',
            operator: 'LIKE',
            left: { type: 'column_ref', table: 't', column: 'col1' },
            right: { type: 'string', value: '%f\\%o\\_o\\%b\\_ar%' }
        });
    });

    it('should support multiple attributes and non-empty where clause', () => {
        const ast = queryBuilder({
            queryAst,
            filter: [[{ attribute: 'col2', operator: 'equal', value: 5 }]],
            searchable: ['col1', 'columnAlias'],
            search: 'foobar'
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
