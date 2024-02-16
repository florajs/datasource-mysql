'use strict';

const assert = require('node:assert/strict');
const { beforeEach, describe, it } = require('node:test');

const queryBuilder = require('../../../lib/sql-query-builder');
const astFixture = require('./fixture');

describe('query-builder (limit)', () => {
    let queryAst;

    beforeEach(() => (queryAst = structuredClone(astFixture)));

    it('should set limit', () => {
        const ast = queryBuilder({
            queryAst,
            limit: 17
        });

        assert.deepEqual(ast.limit, [
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

        assert.deepEqual(ast.limit, [
            { type: 'number', value: 20 },
            { type: 'number', value: 10 }
        ]);
    });
});
