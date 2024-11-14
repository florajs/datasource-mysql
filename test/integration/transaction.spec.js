'use strict';

const assert = require('node:assert/strict');
const { after, describe, it } = require('node:test');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');
const ciCfg = require('./ci-config');

describe('transaction', () => {
    const ds = FloraMysqlFactory.create(ciCfg);
    const db = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';
    const ctx = ds.getContext({ db });

    after(() => ds.close());

    describe('transaction handling', () => {
        it('should send COMMIT on commit()', async () => {
            const table = 'trx_commit';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);

            const trx1 = await ctx.transaction();
            await trx1.insert(table, { id: 1 });
            const trxRunningCount = await ctx.queryOne(`SELECT COUNT(*) FROM ${table}`);
            await trx1.commit();

            const trxFinishCount = await ctx.queryOne(`SELECT COUNT(*) FROM ${table}`);

            assert.equal(trxRunningCount, 0);
            assert.equal(trxFinishCount, 1);
        });

        it('should send ROLLBACK on rollback()', async () => {
            const table = 'trx_rollback';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            const trx = await ctx.transaction();
            await trx.insert(table, { id: 1 });
            await trx.rollback();

            const result = await ctx.queryOne(`SELECT COUNT(*) FROM ${table}`);
            assert.equal(result, 0);
        });
    });

    describe('#insert', () => {
        it('should return last inserted id', async () => {
            const table = 'trx_insert_insert_id';

            await ctx.exec(`CREATE TABLE ${table} (id INT PRIMARY KEY AUTO_INCREMENT, col1 VARCHAR(10))`);
            const trx = await ctx.transaction();
            const { insertId } = await trx.insert(table, { col1: 'foo' });
            await trx.commit();

            assert.equal(insertId, 1);
        });

        it('should return number of affected rows', async () => {
            const table = 'trx_insert_affected_rows';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            const trx = await ctx.transaction();
            const { affectedRows } = await trx.insert(table, [{ id: 1 }, { id: 2 }]);
            await trx.commit();

            assert.equal(affectedRows, 2);
        });
    });

    describe('#update', () => {
        it('should return number of changed rows', async () => {
            const table = 'trx_update_changed_rows';

            await ctx.exec(`CREATE TABLE ${table} (id INT, col1 VARCHAR(10))`);
            const trx = await ctx.transaction();
            await trx.insert(table, [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const { changedRows } = await trx.update(table, { col1: 'foobar' }, { id: 1 });
            await trx.commit();

            assert.equal(changedRows, 1);
        });

        it('should return number of affected rows', async () => {
            const table = 'trx_update_affected_rows';

            await ctx.exec(`CREATE TABLE ${table} (id INT, col1 VARCHAR(10))`);
            const trx = await ctx.transaction();
            await trx.insert(table, [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const { affectedRows } = await trx.update(table, { col1: 'test' }, '1 = 1');
            await trx.commit();

            assert.ok(affectedRows > 1);
        });
    });

    describe('#delete', () => {
        it('should return number of affected rows', async () => {
            const table = 'trx_delete_affected_rows';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            const trx = await ctx.transaction();
            await trx.insert(table, [{ id: 1 }, { id: 2 }]);
            const { affectedRows } = await trx.delete(table, { id: 1 });
            await trx.commit();

            assert.equal(affectedRows, 1);
        });
    });

    describe('#upsert', () => {
        it('should return number of affected rows', async () => {
            const table = 'trx_upsert_affected_rows';

            await ctx.exec(`CREATE TABLE ${table} (id INT, col1 VARCHAR(10))`);
            const trx = await ctx.transaction();
            const { affectedRows } = await trx.upsert(table, { id: 1, col1: 'foobar' }, ['col1']);
            await trx.commit();

            assert.equal(affectedRows, 1);
        });

        it('should return number of changed rows', async () => {
            const table = 'trx_upsert_changed_rows';

            await ctx.exec(`CREATE TABLE ${table} (id INT, col1 VARCHAR(10))`);
            const trx = await ctx.transaction();
            const { changedRows } = await trx.upsert(table, { id: 1, col1: 'foobar' }, ['col1']);
            await trx.commit();

            assert.equal(changedRows, 0);
        });

        it('should accept data as an object', async () => {
            const table = 'trx_upsert_data_object';

            await ctx.exec(`CREATE TABLE ${table} (id INT, col1 VARCHAR(10))`);
            await ctx.transaction(async (trx) => await trx.upsert(table, { id: 1, col1: 'foo' }, ['col1']));

            const result = await ctx.query(`SELECT id, col1 FROM ${table}`);
            assert.equal(result.length, 1);
            assert.deepEqual({ ...result[0] }, { id: 1, col1: 'foo' });
        });

        it('should accept data as an array of objects', async () => {
            const table = 'trx_upsert_data_array';

            await ctx.exec(`CREATE TABLE ${table} (id INT PRIMARY KEY, col1 VARCHAR(10))`);
            await ctx.transaction(async (trx) => {
                await trx.insert(table, { id: 1, col1: 'foobar' });
                await trx.upsert(
                    table,
                    [
                        { id: 1, col1: 'foo' },
                        { id: 2, col1: 'bar' }
                    ],
                    ['col1']
                );
            });

            const result = await ctx.query(`SELECT id, col1 FROM ${table} ORDER BY id`);
            assert.equal(result.length, 2);
            assert.deepEqual({ ...result[0] }, { id: 1, col1: 'foo' });
            assert.deepEqual({ ...result[1] }, { id: 2, col1: 'bar' });
        });

        it('should accept updates as an object', async () => {
            const table = 'trx_upsert_update_object';

            await ctx.exec(`CREATE TABLE ${table} (id INT PRIMARY KEY, col1 VARCHAR(32))`);
            await ctx.transaction(async (trx) => {
                await trx.insert(table, { id: 1, col1: 'foo' });
                await trx.upsert(
                    table,
                    { id: 1, col1: 'bar' },
                    { col1: ctx.raw(`CONCAT(${table}.col1, \`new_values\`.col1)`) }
                );
            });

            const value = await ctx.queryOne(`SELECT col1 FROM ${table} WHERE id = 1`);
            assert.equal(value, 'foobar');
        });

        it('should accept updates as an array', async () => {
            const table = 'trx_upsert_update_array';

            await ctx.exec(`CREATE TABLE ${table} (id INT PRIMARY KEY, col1 VARCHAR(10))`);
            await ctx.transaction(async (trx) => {
                await trx.insert(table, { id: 1, col1: 'foo' });
                await trx.upsert(table, { id: 1, col1: 'bar' }, ['col1']);
            });

            const result = await ctx.queryOne(`SELECT col1 FROM ${table} WHERE id = 1`);
            assert.equal(result, 'bar');
        });

        it('should use alias parameter', async () => {
            const table = 'trx_upsert_update_object_custom_alias';

            await ctx.exec(`CREATE TABLE ${table} (id INT PRIMARY KEY, col1 VARCHAR(32))`);
            await ctx.transaction(async (trx) => {
                await trx.insert(table, { id: 1, col1: 'foo' });
                await trx.upsert(
                    table,
                    { id: 1, col1: 'bar' },
                    { col1: ctx.raw(`CONCAT(${table}.col1, \`customAlias\`.col1)`) },
                    'customAlias'
                );
            });

            const value = await ctx.queryOne(`SELECT col1 FROM ${table} WHERE id = 1`);
            assert.equal(value, 'foobar');
        });
    });

    describe('#query', () => {
        it('should support parameters as an array', async () => {
            const table = 'trx_query_array';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            const trx = await ctx.transaction();
            await trx.insert(table, [{ id: 1 }]);
            await assert.doesNotReject(async () => await trx.query(`SELECT id FROM ${table} WHERE id = ?`, [1]));
            await trx.commit();
        });

        it('should support named parameters', async () => {
            const table = 'trx_query_object';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            const trx = await ctx.transaction();
            await trx.insert(table, [{ id: 1 }]);
            await assert.doesNotReject(
                async () => await trx.query(`SELECT id FROM ${table} WHERE id = :id`, { id: 1 })
            );
            await trx.commit();
        });

        it('should return typecasted result', async () => {
            const table = 'trx_query_typecast';

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            const trx = await ctx.transaction();
            await trx.insert(table, [{ id: 1 }]);
            const [item] = await trx.query(`SELECT id FROM ${table} WHERE id = 1`);
            await trx.commit();

            assert.ok(Object.hasOwn(item, 'id'));
            assert.equal(item.id, 1);
        });
    });

    describe('#queryRow', () => {
        it('should return an object', async () => {
            const table = 'trx_queryrow_return_object';

            await ctx.exec(`CREATE TABLE ${table} (id INT, col1 VARCHAR(10))`);
            const trx = await ctx.transaction();
            await trx.insert(table, [{ id: 1, col1: 'foo' }]);
            const result = await trx.queryRow(`SELECT id, col1 FROM ${table} WHERE id = ?`, [1]);
            await trx.commit();

            assert.deepEqual({ ...result }, { id: 1, col1: 'foo' });
        });

        it('should return typecasted result', async () => {
            const table = 'trx_queryrow_typecast';
            let row;

            await ctx.exec(`CREATE TABLE ${table} (id INT, col1 VARCHAR(10))`);
            await ctx.transaction(async (trx) => {
                await trx.insert(table, [{ id: 1, col1: 'foo' }]);
                row = await trx.queryRow(`SELECT id, col1 FROM ${table} WHERE id = 1`);
            });

            assert.ok(Object.hasOwn(row, 'id'));
            assert.equal(row.id, 1);
        });
    });

    describe('#queryOne', () => {
        it('should return single value', async () => {
            const table = 'trx_queryone_return_value';
            let value;

            await ctx.exec(`CREATE TABLE ${table} (id INT, col1 VARCHAR(10))`);
            await ctx.transaction(async (trx) => {
                await trx.insert(table, [{ id: 1, col1: 'foo' }]);
                value = await trx.queryOne(`SELECT col1 FROM ${table} WHERE id = ?`, [1]);
            });

            assert.equal(value, 'foo');
        });

        it('should return typecasted result', async () => {
            const table = 'trx_queryone_return_typecast';
            let id;

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.transaction(async (trx) => {
                await trx.insert(table, [{ id: 1 }]);
                id = await trx.queryOne(`SELECT id FROM ${table} WHERE id = 1`);
            });

            assert.equal(id, 1);
        });
    });

    describe('#queryCol', () => {
        it('should return array of (typecasted) values', async () => {
            const table = 'trx_querycol_return_value';
            let result;

            await ctx.exec(`CREATE TABLE ${table} (id INT)`);
            await ctx.transaction(async (trx) => {
                await trx.insert(table, [{ id: 1 }, { id: 2 }]);
                result = await trx.queryCol(`SELECT id FROM ${table}`);
            });

            assert.ok(Array.isArray(result));
            assert.deepEqual(result, [1, 2]);
        });
    });

    describe('#exec', () => {
        it('should support parameters', async () => {
            const trx = await ctx.transaction();
            await assert.doesNotReject(async () => await trx.exec('SHOW TABLES LIKE ?', ['foo']));
            await trx.rollback();
        });

        it('should support named parameters', async () => {
            const trx = await ctx.transaction();
            await assert.doesNotReject(async () => await trx.exec('SHOW TABLES LIKE :name', { name: 'foo' }));
            await trx.rollback();
        });

        it(`should resolve/return with insertId property`, async () => {
            const table = 'trx_exec_insert_id';
            let result;

            await ctx.exec(`CREATE TABLE ${table} (id INT PRIMARY KEY AUTO_INCREMENT, col1 VARCHAR(10))`);
            await ctx.transaction(async (trx) => {
                result = await trx.exec(`INSERT INTO ${table} (col1) VALUES ('insertId')`);
            });

            assert.ok(Object.hasOwn(result, 'insertId'));
            assert.equal(result.insertId, 1);
        });

        Object.entries({
            affectedRows: 1,
            changedRows: 1,
            insertId: 0
        }).forEach(([property, value]) => {
            it(`should resolve/return with ${property} property`, async () => {
                const table = `trx_return_property_${property.toLowerCase()}`;
                let result;

                await ctx.exec(`CREATE TABLE ${table} (id INT PRIMARY KEY, col1 VARCHAR(10))`);
                await ctx.transaction(async (trx) => {
                    await trx.insert(table, [{ id: 1, col1: 'foo' }]);
                    result = await trx.exec(`UPDATE ${table} SET col1 = 'affectedRows' WHERE id = 1`);
                });

                assert.ok(Object.hasOwn(result, property));
                assert.equal(result[property], value);
            });
        });
    });
});
