'use strict';

const assert = require('node:assert/strict');
const { describe, it } = require('node:test');
const Transaction = require('../../lib/transaction');

describe('transaction', () => {
    describe('interface', () => {
        [
            'begin',
            'commit',
            'insert',
            'delete',
            'queryCol',
            'queryOne',
            'queryRow',
            'rollback',
            'update',
            'upsert',
            'raw',
            'quote',
            'quoteIdentifier'
        ].forEach((method) => {
            it(`should have ${method} method`, () => {
                assert.ok(typeof Transaction.prototype[method] === 'function');
            });
        });
    });

    describe('#raw', () => {
        it('should pass through value', () => {
            const trx = new Transaction({});
            const expr = trx.raw('NOW()');

            assert.ok(typeof expr === 'object');
            assert.ok(typeof expr.toSqlString === 'function');
            assert.equal(expr.toSqlString(), 'NOW()');
        });
    });

    describe('#quote', () => {
        it('should quote values', () => {
            const trx = new Transaction({});

            assert.equal(trx.quote(`foo\\b'ar`), `'foo\\\\b\\'ar'`);
        });

        describe('#quoteIdentifier', () => {
            it('should quote identifiers', () => {
                const trx = new Transaction({});

                assert.equal(trx.quoteIdentifier('table'), '`table`');
            });
        });
    });
});
