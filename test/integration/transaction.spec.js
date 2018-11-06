'use strict';

const { expect } = require('chai');
const sinon = require('sinon');

const PoolConnection = require('../../node_modules/mysql/lib/PoolConnection');
const Transaction = require('../../lib/transaction');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');

describe('transaction', () => {
    const ds = FloraMysqlFactory.create();
    const ctx = ds.getContext({ db: 'flora_mysql_testdb' });
    let queryFnSpy;

    beforeEach(() => {
        queryFnSpy = sinon.spy(PoolConnection.prototype, 'query');
    });

    afterEach(() => queryFnSpy.restore());

    it('should return a transaction', async () => {
        const trx = await ctx.transaction();

        expect(trx).to.be.instanceOf(Transaction);
        await trx.rollback();
    });

    it('should acquire a connection and start the transaction', async () => {
        const trx = await ctx.transaction();

        expect(queryFnSpy).to.have.been.calledWith('START TRANSACTION');
        await trx.rollback();
    });

    it('should send COMMIT on commit()', async () => {
        const trx = await ctx.transaction();

        await trx.commit();
        expect(queryFnSpy).to.have.been.calledWith('COMMIT');
    });

    it('should send ROLLBACK on rollback()', async () => {
        const trx = await ctx.transaction();

        await trx.rollback();
        expect(queryFnSpy).to.have.been.calledWith('ROLLBACK');
    });

    describe('#query', () => {
        it('should support parameters', async () => {
            const trx = await ctx.transaction();
            await trx.query('SELECT "id" FROM "t" WHERE "col1" = ?', ['foo']);
            await trx.rollback();

            expect(queryFnSpy).to.have.been.calledWith(`SELECT "id" FROM "t" WHERE "col1" = 'foo'`);
        });

        it('should support named parameters', async () => {
            const trx = await ctx.transaction();
            await trx.query('SELECT "id" FROM "t" WHERE "col1" = :col1', { col1: 'foo' });
            await trx.rollback();

            expect(queryFnSpy).to.have.been.calledWith(`SELECT "id" FROM "t" WHERE "col1" = 'foo'`);
        });
    });

    describe('#exec', () => {
        it('should support parameters', async () => {
            const trx = await ctx.transaction();
            await trx.query('SELECT "id" FROM "t" WHERE "col1" = ?', ['foo']);
            await trx.rollback();

            expect(queryFnSpy).to.have.been.calledWith(`SELECT "id" FROM "t" WHERE "col1" = 'foo'`);
        });

        it('should support named parameters', async () => {
            const trx = await ctx.transaction();
            await trx.query('SELECT "id" FROM "t" WHERE "col1" = :col1', { col1: 'foo' });
            await trx.rollback();

            expect(queryFnSpy).to.have.been.calledWith(`SELECT "id" FROM "t" WHERE "col1" = 'foo'`);
        });
    });
});
