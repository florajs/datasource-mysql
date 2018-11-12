'use strict';

const { expect } = require('chai');
const Transaction = require('../../lib/transaction');

describe('transaction', () => {
    describe('interface', () => {
        const methods = [
            'begin',
            'commit',
            'insert',
            'delete',
            'queryCol',
            'queryOne',
            'rollback',
            'update',
            'upsert'
        ];

        methods.forEach(method => {
            it(`should have ${method} method`, () => {
                expect(Transaction.prototype[method]).to.be.a('function');
            });
        });
    });
});
