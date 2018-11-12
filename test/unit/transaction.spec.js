'use strict';

const { expect } = require('chai');
const Transaction = require('../../lib/transaction');

describe('transaction', () => {
    describe('interface', () => {
        ['begin', 'commit', 'rollback', 'insert', 'update', 'delete', 'upsert', 'queryCol'].forEach(method => {
            it(`should have ${method} method`, () => {
                expect(Transaction.prototype[method]).to.be.a('function');
            });
        });
    });
});
