'use strict';

const assert = require('node:assert/strict');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');
const tableWithAutoIncrement = require('./table-with-auto-increment');
const ciCfg = require('./ci-config');

describe('context', () => {
    const ds = FloraMysqlFactory.create(ciCfg);
    const db = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';
    const ctx = ds.getContext({ db });

    afterEach(() => ctx.exec('TRUNCATE TABLE "t"'));
    after(() => ds.close());

    describe('#query', () => {
        it('should return an array for empty results', async () => {
            const result = await ctx.query('SELECT "col1" FROM "t"');

            assert.ok(Array.isArray(result));
            assert.equal(result.length, 0);
        });

        it('should accept query params as an array', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const [item] = await ctx.query('SELECT "id", "col1" FROM "t" WHERE "id" = ?', [1]);

            assert.deepEqual({ ...item }, { id: 1, col1: 'foo' });
        });

        it('should accept query params as an object', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const [item] = await ctx.query('SELECT "id", "col1" FROM "t" WHERE "id" = :id', { id: 1 });

            assert.deepEqual({ ...item }, { id: 1, col1: 'foo' });
        });

        it('should return typecasted result', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const [item] = await ctx.query('SELECT "id" FROM "t" WHERE "id" = 1');

            assert.ok(Object.hasOwn(item, 'id'));
            assert.equal(item.id, 1);
        });
    });

    describe('#queryRow', () => {
        it('should return null for empty results', async () => {
            const row = await ctx.queryRow('SELECT "col1" FROM "t" WHERE "id" = 1337');

            assert.equal(row, null);
        });

        [
            ['should resolve to an object', 'SELECT "id", "col1" FROM "t" WHERE "id" = 1'],
            ['should handle multiple rows', 'SELECT "id", "col1" FROM "t" ORDER BY "id"']
        ].forEach(([description, sql]) => {
            it(description, async () => {
                await ctx.insert('t', [
                    { id: 1, col1: 'foo' },
                    { id: 2, col1: 'bar' }
                ]);
                const row = await ctx.queryRow(sql);

                assert.deepEqual({ ...row }, { id: 1, col1: 'foo' });
            });
        });

        it('should accept query params as an array', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const row = await ctx.queryRow('SELECT "id", "col1" FROM "t" WHERE "id" = ?', [1]);

            assert.deepEqual({ ...row }, { id: 1, col1: 'foo' });
        });

        it('should accept query params as an object', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const row = await ctx.queryRow('SELECT "id", "col1" FROM "t" WHERE "id" = :id', { id: 1 });

            assert.deepEqual({ ...row }, { id: 1, col1: 'foo' });
        });

        it('should return typecasted result', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const row = await ctx.queryRow('SELECT "id", "col1" FROM "t" WHERE "id" = 1');

            assert.equal(row.id, 1);
        });
    });

    describe('#queryOne', () => {
        it('should return null for empty results', async () => {
            const result = await ctx.queryOne('SELECT "col1" FROM "t" WHERE "id" = 1337');

            assert.equal(result, null);
        });

        [
            ['should resolve to single of value', 'SELECT "col1" FROM "t" WHERE "id" = 1'],
            ['should handle aliases', 'SELECT "col1" AS "funkyAlias" FROM "t" WHERE "id" = 1'],
            ['should handle multiple columns', 'SELECT "col1", "id" FROM "t" WHERE "id" = 1'],
            ['should handle multiple rows', 'SELECT "col1" FROM "t" ORDER BY "id"']
        ].forEach(([description, sql]) => {
            it(description, async () => {
                await ctx.insert('t', [
                    { id: 1, col1: 'foo' },
                    { id: 2, col1: 'bar' }
                ]);
                const result = await ctx.queryOne(sql);

                assert.equal(result, 'foo');
            });
        });

        it('should accept query params as an array', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const result = await ctx.queryOne('SELECT "col1" FROM "t" WHERE "id" = ?', [1]);

            assert.equal(result, 'foo');
        });

        it('should accept query params as an object', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const result = await ctx.queryOne('SELECT "col1" FROM "t" WHERE "id" = :id', { id: 1 });

            assert.equal(result, 'foo');
        });

        it('should return typecasted result', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const id = await ctx.queryOne('SELECT "id" FROM "t" WHERE "id" = 1');

            assert.equal(id, 1);
        });
    });

    describe('#queryCol', () => {
        [
            ['should resolve to an array of values', 'SELECT "col1" FROM "t"'],
            ['should handle aliases', 'SELECT "col1" AS "funkyAlias" FROM "t"'],
            ['should handle multiple columns', 'SELECT "col1", "id" FROM "t"']
        ].forEach(([description, sql]) => {
            it(description, async () => {
                await ctx.insert('t', [
                    { id: 1, col1: 'foo' },
                    { id: 2, col1: 'bar' }
                ]);
                const result = await ctx.queryCol(sql);

                assert.deepEqual(result, ['foo', 'bar']);
            });
        });

        it('should accept query params as an array', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const result = await ctx.queryCol('SELECT "col1" FROM "t" WHERE "id" = ?', [1]);

            assert.deepEqual(result, ['foo']);
        });

        it('should accept query params as an object', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const result = await ctx.queryCol('SELECT "col1" FROM "t" WHERE "id" = :id', { id: 1 });

            assert.deepEqual(result, ['foo']);
        });

        it('should return typecasted result', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const [id] = await ctx.queryCol('SELECT "id" FROM "t" WHERE "id" = 1');

            assert.equal(id, 1);
        });
    });

    describe('DML statements', () => {
        describe('#insert', () => {
            it('should return last inserted id', async () => {
                const { insertId } = await tableWithAutoIncrement(
                    ctx,
                    't1',
                    async () => await ctx.insert('t1', { col1: 'foo' })
                );

                assert.equal(insertId, 1);
            });

            it('should return number of affected rows', async () => {
                const { affectedRows } = await ctx.insert('t', [
                    { id: 1, col1: 'foo' },
                    { id: 2, col1: 'bar' }
                ]);

                assert.equal(affectedRows, 2);
            });
        });

        describe('#update', () => {
            it('should return number of changed rows', async () => {
                await ctx.insert('t', [
                    { id: 1, col1: 'foo' },
                    { id: 2, col1: 'bar' }
                ]);

                const { changedRows } = await ctx.update('t', { col1: 'test' }, { id: 1 });

                assert.equal(changedRows, 1);
            });

            it('should return number of affected rows', async () => {
                await ctx.insert('t', [
                    { id: 1, col1: 'foo' },
                    { id: 2, col1: 'bar' }
                ]);

                const { affectedRows } = await ctx.update('t', { col1: 'test' }, '1 = 1');

                assert.equal(affectedRows, 2);
            });
        });

        describe('#delete', () => {
            it('should return number of affected rows', async () => {
                await ctx.insert('t', [
                    { id: 1, col1: 'foo' },
                    { id: 2, col1: 'bar' }
                ]);

                const { affectedRows } = await ctx.delete('t', { id: 1 });

                assert.equal(affectedRows, 1);
            });
        });

        describe('#exec', () => {
            it(`should resolve/return with insertId property`, async () => {
                const { insertId } = await tableWithAutoIncrement(
                    ctx,
                    't1',
                    async () => await ctx.exec(`INSERT INTO t1 (col1) VALUES ('insertId')`)
                );

                assert.equal(insertId, 1);
            });

            Object.entries({
                affectedRows: 1,
                changedRows: 1,
                insertId: 0
            }).forEach(([property, value]) => {
                it(`should resolve/return with ${property} property`, async () => {
                    await ctx.insert('t', [{ id: 1, col1: 'foo' }]);
                    const result = await ctx.exec(`UPDATE t SET col1 = 'changedRows' WHERE id = 1`);

                    assert.ok(Object.hasOwn(result, property));
                    assert.equal(result[property], value);
                });
            });
        });
    });

    describe('#transaction', () => {
        it('should support callback parameter', async () => {
            await ctx.transaction(async (trx) => {
                await trx.insert('t', { id: 1, col1: 'foobar' });
            });
            const row = await ctx.queryRow('SELECT id, col1 FROM t');

            assert.deepEqual({ ...row }, { id: 1, col1: 'foobar' });
        });

        it('should automatically rollback on errors', async () => {
            await assert.rejects(
                async () =>
                    await ctx.transaction(async (trx) => {
                        await trx.insert('t', { id: 1, col1: 'foo' });
                        await trx.insert('nonexistent_table', { id: 1, col1: 'bar' });
                    }),
                { code: 'ER_NO_SUCH_TABLE' }
            );

            const row = await ctx.queryRow('SELECT id, col1 FROM t');
            assert.equal(row, null);
        });

        it('should rethrow errors', async () => {
            await assert.rejects(
                async () =>
                    await ctx.transaction(async (trx) => {
                        await trx.insert('nonexistent_table', { col1: 'blablub' });
                    }),
                {
                    name: 'Error',
                    message: /Table 'flora_mysql_testdb.nonexistent_table' doesn't exist/
                }
            );
        });
    });
});
