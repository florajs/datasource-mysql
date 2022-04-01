'use strict';

const { expect } = require('chai');
const sinon = require('sinon');

const PoolConnection = require('../../node_modules/mysql/lib/PoolConnection');
const Transaction = require('../../lib/transaction');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');
const tableWithAutoIncrement = require('./table-with-auto-increment');
const ciCfg = require('./ci-config');

describe('transaction', () => {
    const ds = FloraMysqlFactory.create(ciCfg);
    const db = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';
    const ctx = ds.getContext({ db });

    after(() => ds.close());

    it('should return a transaction', async () => {
        const trx = await ctx.transaction();
        await trx.rollback();

        expect(trx).to.be.instanceOf(Transaction);
    });

    describe('transaction handling', () => {
        let queryFnSpy;

        beforeEach(() => {
            queryFnSpy = sinon.spy(PoolConnection.prototype, 'query');
        });

        afterEach(() => queryFnSpy.restore());

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
    });

    describe('#insert', () => {
        it('should return last inserted id', async () => {
            const { insertId } = await tableWithAutoIncrement(ctx, 't1', async () => {
                const trx = await ctx.transaction();
                const result = await trx.insert('t1', { col1: 'foo' });
                await trx.rollback();

                return result;
            });

            expect(insertId).to.equal(1);
        });

        it('should return number of affected rows', async () => {
            const trx = await ctx.transaction();
            const { affectedRows } = await trx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            await trx.rollback();

            expect(affectedRows).to.equal(2);
        });
    });

    describe('#update', () => {
        it('should return number of changed rows', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const { changedRows } = await trx.update('t', { col1: 'foobar' }, { id: 1 });
            await trx.rollback();

            expect(changedRows).to.equal(1);
        });

        it('should return number of affected rows', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const { affectedRows } = await trx.update('t', { col1: 'test' }, '1 = 1');
            await trx.rollback();

            expect(affectedRows).to.equal(2);
        });
    });

    describe('#delete', () => {
        it('should return number of affected rows', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const { affectedRows } = await trx.delete('t', { id: 1 });
            await trx.rollback();

            expect(affectedRows).to.equal(1);
        });
    });

    describe('#upsert', () => {
        it('should return number of affected rows', async () => {
            const { affectedRows } = await tableWithAutoIncrement(ctx, 't1', async () => {
                const trx = await ctx.transaction();
                const result = await trx.upsert('t1', { id: 1, col1: 'foobar' }, ['col1']);
                await trx.rollback();

                return result;
            });

            expect(affectedRows).to.equal(1);
        });

        it('should return number of changed rows', async () => {
            const { changedRows } = await tableWithAutoIncrement(ctx, 't1', async () => {
                const trx = await ctx.transaction();
                const result = await trx.upsert('t1', { id: 1, col1: 'foobar' }, ['col1']);
                await trx.rollback();

                return result;
            });

            expect(changedRows).to.equal(0);
        });

        it('should accept data as an object', async () => {
            const trx = await ctx.transaction();
            const result = await trx.upsert('t', { id: 1, col1: 'foo' }, ['col1']);
            await trx.rollback();

            expect(result).to.be.an('object');
        });

        it('should accept data as an array of objects', async () => {
            const trx = await ctx.transaction();
            const result = await trx.upsert(
                't',
                [
                    { id: 1, col1: 'foo' },
                    { id: 2, col1: 'bar' }
                ],
                ['col1']
            );
            await trx.rollback();

            expect(result).to.be.an('object');
        });

        it('should accept updates as an object', async () => {
            const trx = await ctx.transaction();
            const result = await trx.upsert('t', { id: 1, col1: 'foo' }, { col1: ctx.raw('MD5(col1)') });
            await trx.rollback();

            expect(result).to.be.an('object');
        });

        it('should accept updates as an array', async () => {
            const trx = await ctx.transaction();
            const result = await trx.upsert('t', { id: 1, col1: 'foo' }, ['col1']);
            await trx.rollback();

            expect(result).to.be.an('object');
        });
    });

    describe('#query', () => {
        it('should support parameters as an array', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const result = await trx.query('SELECT "id", "col1" FROM "t" WHERE "id" = ?', [1]);
            await trx.rollback();

            expect(result).to.eql([{ id: 1, col1: 'foo' }]);
        });

        it('should support named parameters', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            const result = await trx.query('SELECT "id", "col1" FROM "t" WHERE "id" = :id', { id: 1 });
            await trx.rollback();

            expect(result).to.eql([{ id: 1, col1: 'foo' }]);
        });

        it('should return typecasted result', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            const [item] = await trx.query('SELECT "id" FROM "t" WHERE "id" = 1');
            await trx.rollback();

            expect(item).to.have.property('id', 1);
        });
    });

    describe('#queryRow', () => {
        it('should return an object', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            const result = await trx.queryRow('SELECT "id", "col1" FROM "t" WHERE "id" = ?', [1]);
            await trx.rollback();

            expect(result).to.eql({ id: 1, col1: 'foo' });
        });

        it('should return typecasted result', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            const row = await trx.queryRow('SELECT "id", "col1" FROM "t" WHERE "id" = 1');
            await trx.rollback();

            expect(row).to.have.property('id', 1);
        });
    });

    describe('#queryOne', () => {
        it('should return array of values', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            const result = await trx.queryOne('SELECT "col1" FROM "t" WHERE "id" = ?', [1]);
            await trx.rollback();

            expect(result).to.equal('foo');
        });

        it('should return typecasted result', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            const id = await trx.queryOne('SELECT "id" FROM "t" WHERE "id" = 1');
            await trx.rollback();

            expect(id).to.equal(1);
        });
    });

    describe('#queryCol', () => {
        it('should return array of values', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const result = await trx.queryCol('SELECT "col1" FROM "t" WHERE "id" = ?', [1]);
            await trx.rollback();

            expect(result).to.eql(['foo']);
        });

        it('should return typecasted result', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const [id] = await trx.queryCol('SELECT "id" FROM "t"');
            await trx.rollback();

            expect(id).to.equal(1);
        });
    });

    describe('#exec', () => {
        it('should support parameters', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            const result = await trx.query('SELECT "id", "col1" FROM "t" WHERE "id" = ?', [1]);
            await trx.rollback();

            expect(result).to.eql([{ id: 1, col1: 'foo' }]);
        });

        it('should support named parameters', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            const result = await trx.query('SELECT "id", "col1" FROM "t" WHERE "id" = :id', { id: 1 });
            await trx.rollback();

            expect(result).to.eql([{ id: 1, col1: 'foo' }]);
        });

        it(`should resolve/return with insertId property`, async () => {
            const { insertId } = await tableWithAutoIncrement(ctx, 't1', async () => {
                const trx = await ctx.transaction();
                const result = await trx.exec(`INSERT INTO t1 (col1) VALUES ('insertId')`);
                await trx.rollback();

                return result;
            });

            expect(insertId).to.equal(1);
        });

        Object.entries({
            affectedRows: 1,
            changedRows: 1,
            insertId: 0
        }).forEach(([property, value]) => {
            it(`should resolve/return with ${property} property`, async () => {
                const trx = await ctx.transaction();
                await trx.insert('t', [{ id: 1, col1: 'foo' }]);
                const result = await trx.exec(`UPDATE t SET col1 = 'affectedRows' WHERE id = 1`);
                await trx.rollback();

                expect(result).to.have.property(property, value);
            });
        });
    });
});
