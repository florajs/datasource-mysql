'use strict';

const assert = require('node:assert/strict');
const { after, describe, it } = require('node:test');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');
const tableWithAutoIncrement = require('./table-with-auto-increment');
const ciCfg = require('./ci-config');

describe('transaction', () => {
    const ds = FloraMysqlFactory.create(ciCfg);
    const db = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';
    const ctx = ds.getContext({ db });

    after(() => ds.close());

    describe('transaction handling', () => {
        ['should start a transaction', 'should abort a transaction'].forEach((description) =>
            it(description, async () => {
                const trx = await ctx.transaction();
                await trx.insert('t', { col1: 'transaction' });
                await trx.rollback();

                const result = await ctx.queryOne(`SELECT id FROM "t" WHERE col1 = 'transaction'`);
                assert.equal(result, null);
            })
        );

        it('should send COMMIT on commit()', async () => {
            const trx1 = await ctx.transaction();

            await trx1.insert('t', { col1: 'transaction' });
            const trxRunningResult = await ctx.queryOne(`SELECT id FROM "t" WHERE col1 = 'transaction'`);

            assert.equal(trxRunningResult, null);

            await trx1.commit();

            const trxFinishResult = await ctx.queryOne(`SELECT id FROM "t" WHERE col1 = 'transaction'`);
            assert.notEqual(trxFinishResult, null);
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

            assert.equal(insertId, 1);
        });

        it('should return number of affected rows', async () => {
            const trx = await ctx.transaction();
            const { affectedRows } = await trx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            await trx.rollback();

            assert.equal(affectedRows, 2);
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

            assert.equal(changedRows, 1);
        });

        it('should return number of affected rows', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const { affectedRows } = await trx.update('t', { col1: 'test' }, '1 = 1');
            await trx.rollback();

            assert.ok(affectedRows > 1);
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

            assert.equal(affectedRows, 1);
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

            assert.equal(affectedRows, 1);
        });

        it('should return number of changed rows', async () => {
            const { changedRows } = await tableWithAutoIncrement(ctx, 't1', async () => {
                const trx = await ctx.transaction();
                const result = await trx.upsert('t1', { id: 1, col1: 'foobar' }, ['col1']);
                await trx.rollback();

                return result;
            });

            assert.equal(changedRows, 0);
        });

        it('should accept data as an object', async () => {
            const trx = await ctx.transaction();
            await assert.doesNotReject(async () => await trx.upsert('t', { id: 1, col1: 'foo' }, ['col1']));
            await trx.rollback();
        });

        it('should accept data as an array of objects', async () => {
            const trx = await ctx.transaction();
            await assert.doesNotReject(
                async () =>
                    await trx.upsert(
                        't',
                        [
                            { id: 1, col1: 'foo' },
                            { id: 2, col1: 'bar' }
                        ],
                        ['col1']
                    )
            );
            await trx.rollback();
        });

        it('should accept updates as an object', async () => {
            const trx = await ctx.transaction();
            await assert.doesNotReject(
                async () => await trx.upsert('t', { id: 1, col1: 'foo' }, { col1: ctx.raw('MD5(col1)') })
            );
            await trx.rollback();
        });

        it('should accept updates as an array', async () => {
            const trx = await ctx.transaction();
            assert.doesNotReject(async () => await trx.upsert('t', { id: 1, col1: 'foo' }, ['col1']));
            await trx.rollback();
        });
    });

    describe('#query', () => {
        it('should support parameters as an array', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            await assert.doesNotReject(async () => await trx.query('SELECT "id", "col1" FROM "t" WHERE "id" = ?', [1]));
            await trx.rollback();
        });

        it('should support named parameters', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            await assert.doesNotReject(
                async () => await trx.query('SELECT "id", "col1" FROM "t" WHERE "id" = :id', { id: 1 })
            );
            await trx.rollback();
        });

        it('should return typecasted result', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            const [item] = await trx.query('SELECT "id" FROM "t" WHERE "id" = 1');
            await trx.rollback();

            assert.ok(Object.hasOwn(item, 'id'));
            assert.ok(typeof item.id === 'number');
        });
    });

    describe('#queryRow', () => {
        it('should return an object', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            const result = await trx.queryRow('SELECT "id", "col1" FROM "t" WHERE "id" = ?', [1]);
            await trx.rollback();

            assert.deepEqual({ ...result }, { id: 1, col1: 'foo' });
        });

        it('should return typecasted result', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            const row = await trx.queryRow('SELECT "id", "col1" FROM "t" WHERE "id" = 1');
            await trx.rollback();

            assert.ok(Object.hasOwn(row, 'id'));
            assert.ok(typeof row.id === 'number');
        });
    });

    describe('#queryOne', () => {
        it('should return single value', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            const result = await trx.queryOne('SELECT "col1" FROM "t" WHERE "id" = ?', [1]);
            await trx.rollback();

            assert.equal(result, 'foo');
        });

        it('should return typecasted result', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            const id = await trx.queryOne('SELECT "id" FROM "t" WHERE "id" = 1');
            await trx.rollback();

            assert.equal(id, 1);
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

            assert.ok(Array.isArray(result));
            assert.deepEqual(result, ['foo']);
        });

        it('should return typecasted result', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const result = await trx.queryCol('SELECT "id" FROM "t"');
            await trx.rollback();

            assert.ok(Array.isArray(result));
            assert.ok(result.length >= 2);
        });
    });

    describe('#exec', () => {
        it('should support parameters', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            await assert.doesNotReject(async () => await trx.query('SELECT "id", "col1" FROM "t" WHERE "id" = ?', [1]));
            await trx.rollback();
        });

        it('should support named parameters', async () => {
            const trx = await ctx.transaction();
            await trx.insert('t', [{ id: 1, col1: 'foo' }]);
            await assert.doesNotReject(
                async () => await trx.query('SELECT "id", "col1" FROM "t" WHERE "id" = :id', { id: 1 })
            );
            await trx.rollback();
        });

        it(`should resolve/return with insertId property`, async () => {
            const { insertId } = await tableWithAutoIncrement(ctx, 't1', async () => {
                const trx = await ctx.transaction();
                const result = await trx.exec(`INSERT INTO t1 (col1) VALUES ('insertId')`);
                await trx.rollback();

                return result;
            });

            assert.ok(typeof insertId === 'number');
            assert.equal(insertId, 1);
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

                assert.ok(Object.hasOwn(result, property));
                assert.equal(result[property], value);
            });
        });
    });
});
