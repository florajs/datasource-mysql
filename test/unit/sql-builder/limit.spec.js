'use strict';

const { expect } = require('chai');

const queryBuilder = require('../../../lib/sql-query-builder');
const astFixture = require('./fixture');

describe('query-builder (limit)', () => {
    let queryAst;

    beforeEach(() => {
        queryAst = JSON.parse(JSON.stringify(astFixture));
    });

    afterEach(() => {
        queryAst = null;
    });

    it('should set limit', () => {
        const ast = queryBuilder({
            queryAst,
            limit: 17
        });

        expect(ast.limit).to.eql([
            { type: 'number', value: 0 },
            { type: 'number', value: 17 }
        ]);
    });

    it('should set limit with offset', () => {
        const ast = queryBuilder({
            queryAst,
            limit: 10,
            page: 3
        });

        expect(ast.limit).to.eql([
            { type: 'number', value: 20 },
            { type: 'number', value: 10 }
        ]);
    });
});
