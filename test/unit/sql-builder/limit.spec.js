'use strict';

const { expect } = require('chai');

const queryBuilder = require('../../../lib/sql-query-builder');
const astFixture = require('./fixture');

describe('query-builder (limit)', () => {
    let ast;

    beforeEach(() => {
        ast = JSON.parse(JSON.stringify(astFixture));
    });

    afterEach(() => {
        ast = null;
    });

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
