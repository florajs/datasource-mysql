'use strict';

const { expect } = require('chai');

const queryBuilder = require('../../../lib/sql-query-builder');
const astFixture = require('./fixture');

describe('query-builder (fulltext search)', () => {
    let ast;

    beforeEach(() => {
        ast = JSON.parse(JSON.stringify(astFixture));
    });

    afterEach(() => {
        ast = null;
    });

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
            filter: [[{ attribute: 'col2', operator: 'equal', value: 5 }]],
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
