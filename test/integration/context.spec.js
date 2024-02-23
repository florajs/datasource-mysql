'use strict';

const assert = require('node:assert/strict');
const { after, afterEach, describe, it } = require('node:test');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');
const tableWithAutoIncrement = require('./table-with-auto-increment');
const ciCfg = require('./ci-config');

describe('context', () => {
    const ds = FloraMysqlFactory.create(ciCfg);
    const db = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';
    const ctx = ds.getContext({ db });

    afterEach(async () => await ctx.exec('TRUNCATE TABLE "t"'));
    after(async () => await ds.close());

    describe('#query', () => {
        it('should return an array for empty results', async () => {
            const table = 'query_empty_results';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            const result = await ctx.query(`SELECT id FROM ${table}`);

            assert.ok(Array.isArray(result));
            assert.equal(result.length, 0);
        });

        it('should accept query params as an array', async () => {
            const table = 'query_params_array';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.insert(table, [{ id: 1 }, { id: 2 }]);
            const result = await ctx.query(`SELECT id FROM ${table} WHERE id = ?`, [1]);

            assert.equal(result.length, 1);
            assert.deepEqual({ ...result[0] }, { id: 1 });
        });

        it('should accept query params as an object', async () => {
            const table = 'query_params_object';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.insert(table, [{ id: 1 }, { id: 2 }]);
            const [item] = await ctx.query(`SELECT id FROM ${table} WHERE id = :id`, { id: 1 });

            assert.deepEqual({ ...item }, { id: 1 });
        });

        it('should return typecasted result', async () => {
            const table = 'query_typecasted_result';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.insert(table, [{ id: 1 }, { id: 2 }]);
            const [item] = await ctx.query(`SELECT id FROM ${table} WHERE id = 1`);

            assert.ok(Object.hasOwn(item, 'id'));
            assert.equal(item.id, 1);
        });
    });

    describe('#queryRow', () => {
        it('should return null for empty results', async () => {
            const table = 'queryrow_empty_results';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            const row = await ctx.queryRow(`SELECT id FROM ${table} WHERE id = 1337`);

            assert.equal(row, null);
        });

        it('should resolve to an object for single result', async () => {
            const table = 'queryrow_single_row';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.insert(table, [{ id: 1 }, { id: 2 }]);
            const row = await ctx.queryRow(`SELECT id FROM ${table} WHERE id = 1`);

            assert.deepEqual({ ...row }, { id: 1 });
        });

        it('should resolve to an object for single result', async () => {
            const table = 'queryrow_multiple_rows';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.insert(table, [{ id: 1 }, { id: 2, txt: 'bar' }]);
            const row = await ctx.queryRow(`SELECT id FROM ${table} ORDER BY id`);

            assert.deepEqual({ ...row }, { id: 1 });
        });

        it('should accept query params as an array', async () => {
            const table = 'queryrow_params_array';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.insert(table, [{ id: 1 }, { id: 2 }]);
            const row = await ctx.queryRow(`SELECT id FROM ${table} WHERE id = ?`, [1]);

            assert.deepEqual({ ...row }, { id: 1 });
        });

        it('should accept query params as an object', async () => {
            const table = 'queryrow_params_object';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.insert(table, [{ id: 1 }, { id: 2, col: 'bar' }]);
            const row = await ctx.queryRow(`SELECT id FROM ${table} WHERE id = :id`, { id: 1 });

            assert.deepEqual({ ...row }, { id: 1 });
        });

        it('should return typecasted result', async () => {
            const table = 'queryrow_typecasted_result';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.insert(table, [{ id: 1 }, { id: 2, col1: 'bar' }]);
            const row = await ctx.queryRow(`SELECT id FROM ${table} WHERE id = 1`);

            assert.equal(row.id, 1);
        });
    });

    describe('#queryOne', () => {
        it('should return null for empty results', async () => {
            const table = 'queryone_empty_results';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            const result = await ctx.queryOne(`SELECT id FROM ${table} WHERE id = 1337`);

            assert.equal(result, null);
        });

        [
            [
                'should resolve to single of value',
                'queryone_single_value_0',
                'SELECT id FROM queryone_single_value_0 WHERE id = 1'
            ],
            [
                'should handle aliases',
                'queryone_single_value_1',
                'SELECT id AS funkyAlias FROM queryone_single_value_1 WHERE id = 1'
            ]
        ].forEach(([description, table, sql]) => {
            it(description, async () => {
                await ctx.exec(`CREATE TABLE ${table} (id INT)`);
                await ctx.insert(table, [{ id: 1 }]);
                const result = await ctx.queryOne(sql);

                assert.equal(result, 1);
            });
        });

        it('should handle multiple columns', async () => {
            const table = 'queryone_multiple_columns';

            await ctx.exec(`CREATE TABLE ${table} (id INT, txt CHAR(3))`);
            await ctx.insert(table, [{ id: 1, txt: 'foo' }]);
            const result = await ctx.queryOne(`SELECT id, txt FROM ${table} WHERE id = 1`);

            assert.equal(result, 1);
        });

        it('should handle multiple rows', async () => {
            const table = 'queryone_multiple_rows';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.insert(table, [{ id: 1 }, { id: 2 }]);
            const result = await ctx.queryOne(`SELECT id FROM ${table} ORDER BY id`);

            assert.equal(result, 1);
        });

        it('should accept query params as an array', async () => {
            const table = 'queryone_params_array';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.insert(table, [{ id: 1 }, { id: 2 }]);
            const result = await ctx.queryOne(`SELECT id FROM ${table} WHERE id = ?`, [1]);

            assert.equal(result, 1);
        });

        it('should accept query params as an object', async () => {
            const table = 'queryone_params_object';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.insert(table, [{ id: 1 }, { id: 2 }]);
            const result = await ctx.queryOne(`SELECT id FROM ${table} WHERE id = :id`, { id: 1 });

            assert.equal(result, 1);
        });

        it('should return typecasted result', async () => {
            const table = 'queryone_typecasted_result';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.insert(table, [{ id: 1 }, { id: 2 }]);
            const id = await ctx.queryOne(`SELECT id FROM ${table} WHERE id = 1`);

            assert.equal(id, 1);
        });
    });

    describe('#queryCol', () => {
        [
            ['should resolve to an array of values', 'querycol_array_values', 'SELECT id FROM querycol_array_values'],
            ['should handle aliases', 'querycol_aliases', 'SELECT id AS funkyAlias FROM querycol_aliases']
        ].forEach(([description, table, sql]) => {
            it(description, async () => {
                await ctx.exec(`CREATE TABLE ${table} (id INT)`);
                await ctx.insert(table, [{ id: 1 }, { id: 2 }]);
                const result = await ctx.queryCol(sql);

                assert.deepEqual(result, [1, 2]);
            });
        });

        it('should handle multiple columns', async () => {
            const table = 'querycol_multiple_columns';

            await ctx.exec(`CREATE TABLE ${table} (id INT, txt CHAR(3))`);
            await ctx.insert(table, [
                { id: 1, txt: 'foo' },
                { id: 2, txt: 'bar' }
            ]);
            const result = await ctx.queryCol(`SELECT id, txt FROM ${table} ORDER BY id`);

            assert.deepEqual(result, [1, 2]);
        });

        it('should accept query params as an array', async () => {
            const table = 'querycol_params_array';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.insert(table, [{ id: 1 }]);
            const result = await ctx.queryCol(`SELECT id FROM ${table} WHERE id = ?`, [1]);

            assert.deepEqual(result, [1]);
        });

        it('should accept query params as an object', async () => {
            const table = 'querycol_params_object';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.insert(table, [{ id: 1 }]);
            const result = await ctx.queryCol(`SELECT id FROM ${table} WHERE id = :id`, { id: 1 });

            assert.deepEqual(result, [1]);
        });

        it('should return typecasted result', async () => {
            const table = 'querycol_typecasted_result';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.insert(table, [{ id: 1 }]);
            const id = await ctx.queryCol(`SELECT id FROM ${table} WHERE id = 1`);

            assert.deepEqual(id, [1]);
        });
    });

    describe.skip('DML statements', () => {
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
