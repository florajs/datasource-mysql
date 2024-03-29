'use strict';

const { expect } = require('chai');
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
                expect(Transaction.prototype[method]).to.be.a('function');
            });
        });
    });

    describe('#raw', () => {
        it('should pass through value', () => {
            const trx = new Transaction({});
            const expr = trx.raw('NOW()');

            expect(expr).to.be.an('object');
            expect(expr.toSqlString).to.be.a('function');
            expect(expr.toSqlString()).to.equal('NOW()');
        });
    });

    describe('#quote', () => {
        it('should quote values', () => {
            const trx = new Transaction({});
            expect(trx.quote(`foo\\b'ar`)).to.equal(`'foo\\\\b\\'ar'`);
        });

        describe('#quoteIdentifier', () => {
            it('should quote identifiers', () => {
                const trx = new Transaction({});
                expect(trx.quoteIdentifier('table')).to.equal('`table`');
            });
        });
    });
});
