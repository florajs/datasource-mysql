'use strict';

const assert = require('node:assert/strict');
const { afterEach, beforeEach, describe, it, mock } = require('node:test');

const { FloraMysqlFactory, defaultCfg } = require('../FloraMysqlFactory');

describe('context', () => {
    const testCfg = {
        ...defaultCfg,
        servers: {
            default: { masters: [{ host: 'mysql-master.example.com' }], slaves: [{ host: 'mysql-slave.example.com' }] }
        }
    };
    const ds = FloraMysqlFactory.create(testCfg);
    const db = 'db';
    const ctx = ds.getContext({ db });

    describe('interface', () => {
        [
            'delete',
            'exec',
            'insert',
            'query',
            'queryCol',
            'queryOne',
            'queryRow',
            'quote',
            'quoteIdentifier',
            'raw',
            'update',
            'upsert',
            'transaction'
        ].forEach((method) => {
            it(`should export "${method}" method`, () => {
                assert.ok(typeof ctx[method] === 'function');
            });
        });
    });

    describe('#constructor', () => {
        it('should throw an error when database setting is missing', () => {
            assert.throws(() => ds.getContext({}), {
                name: 'ImplementationError',
                message: 'Context requires a db (database) property'
            });
        });

        it('should throw an error when database setting is not a string', () => {
            assert.throws(() => ds.getContext({ db: undefined }), {
                name: 'ImplementationError',
                message: 'Invalid value for db (database) property'
            });
        });

        it('should throw an error when database setting is an empty string', () => {
            assert.throws(() => ds.getContext({ db: ' ' }), {
                name: 'ImplementationError',
                message: 'Invalid value for db (database) property'
            });
        });
    });

    describe('#query', () => {
        beforeEach(() => mock.method(ds, '_query', async () => Promise.resolve({ results: [] })));
        afterEach(() => ds._query.mock.restore());

        it('should use a slave connection', async () => {
            const sql = 'SELECT 1 FROM dual';
            await ctx.query(sql);

            const [call] = ds._query.mock.calls;
            assert.deepEqual(call.arguments, [{ type: 'SLAVE', db, server: 'default' }, 'SELECT 1 FROM dual']);
        });

        it('should handle placeholders', async () => {
            await ctx.query('SELECT id FROM "t" WHERE col1 = ?', ['val1']);

            const [call] = ds._query.mock.calls;
            assert.deepEqual(call.arguments, [
                { type: 'SLAVE', db, server: 'default' },
                `SELECT id FROM "t" WHERE col1 = 'val1'`
            ]);
        });

        it('should handle named placeholders', async () => {
            await ctx.query('SELECT id FROM "t" WHERE col1 = :col1', { col1: 'val1' });

            const [call] = ds._query.mock.calls;
            assert.deepEqual(call.arguments, [
                { type: 'SLAVE', db, server: 'default' },
                `SELECT id FROM "t" WHERE col1 = 'val1'`
            ]);
        });
    });

    describe('#exec', () => {
        beforeEach(() => mock.method(ds, '_query', async () => Promise.resolve({ results: [] })));
        afterEach(() => ds._query.mock.restore());

        it('should use a master connection', async () => {
            await ctx.exec('SELECT 1 FROM dual');

            const [call] = ds._query.mock.calls;
            assert.deepEqual(call.arguments, [{ type: 'MASTER', db, server: 'default' }, 'SELECT 1 FROM dual']);
        });

        it('should handle placeholders', async () => {
            await ctx.exec('SELECT id FROM "t" WHERE col1 = ?', ['val1']);

            const [call] = ds._query.mock.calls;
            assert.deepEqual(call.arguments, [
                { type: 'MASTER', db, server: 'default' },
                `SELECT id FROM "t" WHERE col1 = 'val1'`
            ]);
        });

        it('should handle named placeholders', async () => {
            await ctx.exec('SELECT id FROM "t" WHERE col1 = :col1', { col1: 'val1' });

            const [call] = ds._query.mock.calls;
            assert.deepEqual(call.arguments, [
                { type: 'MASTER', db, server: 'default' },
                `SELECT id FROM "t" WHERE col1 = 'val1'`
            ]);
        });
    });

    describe('#raw', () => {
        it('should pass through value', () => {
            const expr = ctx.raw('NOW()');

            assert.ok(typeof expr === 'object');
            assert.ok(typeof expr.toSqlString === 'function');
            assert.equal(expr.toSqlString(), 'NOW()');
        });
    });

    describe('#quote', () => {
        it('should quote values', () => {
            assert.equal(ctx.quote(`foo\\b'ar`), `'foo\\\\b\\'ar'`);
        });
    });

    describe('#quoteIdentifier', () => {
        it('should quote identifiers', () => {
            assert.equal(ctx.quoteIdentifier('table'), '`table`');
        });
    });

    describe('parameter placeholders', () => {
        beforeEach(() => mock.method(ds, '_query', async () => Promise.resolve({ results: [] })));
        afterEach(() => ds._query.mock.restore());

        [
            ['strings', 'val', 'SELECT "id" FROM "t" WHERE "col1" = ?', `SELECT "id" FROM "t" WHERE "col1" = 'val'`],
            ['numbers', 1337, 'SELECT "id" FROM "t" WHERE "col1" = ?', 'SELECT "id" FROM "t" WHERE "col1" = 1337'],
            ['booleans', true, 'SELECT "id" FROM "t" WHERE "col1" = ?', 'SELECT "id" FROM "t" WHERE "col1" = true'],
            [
                'dates',
                new Date(2019, 0, 1, 0, 0, 0, 0),
                'SELECT "id" FROM "t" WHERE "col1" = ?',
                `SELECT "id" FROM "t" WHERE "col1" = '2019-01-01 00:00:00.000'`
            ],
            [
                'arrays',
                [1, 3, 3, 7],
                'SELECT "id" FROM "t" WHERE "col1" IN (?)',
                `SELECT "id" FROM "t" WHERE "col1" IN (1, 3, 3, 7)`
            ],
            [
                'sqlstringifiable objects',
                ctx.raw('CURDATE()'),
                'SELECT "id" FROM "t" WHERE "col1" = ?',
                `SELECT "id" FROM "t" WHERE "col1" = CURDATE()`
            ],
            ['null', null, 'SELECT "id" FROM "t" WHERE "col1" = ?', `SELECT "id" FROM "t" WHERE "col1" = NULL`]
        ].forEach(([type, value, query, sql]) => {
            it(`should support ${type}`, async () => {
                await ctx.exec(query, [value]);

                const [call] = ds._query.mock.calls;
                assert.equal(call.arguments[1], sql);
            });
        });

        [
            ['objects', {}, '"values" must not be an empty object'],
            ['arrays', [], '"values" must not be an empty array']
        ].forEach(([type, params, message]) => {
            it(`throw an error for empty ${type}`, async () => {
                await assert.rejects(
                    async () => await ctx.exec('SELECT "id" FROM "t" WHERE "col1" IN (:col1)', params),
                    {
                        name: 'ImplementationError',
                        message
                    }
                );
                assert.equal(ds._query.mock.callCount(), 0);
            });
        });

        it('throw an error for non-object or non-array values', async () => {
            await assert.rejects(async () => await ctx.exec('SELECT "id" FROM "t" WHERE "col1" = ?', 'foo'), {
                name: 'ImplementationError',
                message: '"values" must be an object or an array'
            });
            assert.equal(ds._query.mock.callCount(), 0);
        });
    });

    describe('#insert', () => {
        beforeEach(() => mock.method(ctx, 'exec', async () => Promise.resolve({ insertId: 1, affectedRow: 1 })));
        afterEach(() => ctx.exec.mock.restore());

        it('should accept data as an object', async () => {
            await ctx.insert('t', { col1: 'val1', col2: 1, col3: ctx.raw('NOW()') });

            const [call] = ctx.exec.mock.calls;
            assert.deepEqual(call.arguments, ["INSERT INTO `t` (`col1`, `col2`, `col3`) VALUES ('val1', 1, NOW())"]);
        });

        it('should accept data as an array of objects', async () => {
            const date = new Date(2018, 10, 16, 15, 24);
            const dateStr = '2018-11-16 15:24:00.000';
            await ctx.insert('t', [
                { col1: 'val1', col2: 1, col3: date },
                { col1: 'val2', col2: 2, col3: date }
            ]);

            const [call] = ctx.exec.mock.calls;
            assert.deepEqual(call.arguments, [
                `INSERT INTO \`t\` (\`col1\`, \`col2\`, \`col3\`) VALUES ('val1', 1, '${dateStr}'), ('val2', 2, '${dateStr}')`
            ]);
        });

        it('should reject with an error if data is not set', async () => {
            await assert.rejects(async () => await ctx.insert('t'), {
                name: 'ImplementationError',
                message: 'data parameter is required'
            });
        });

        it('should reject with an error if data neither an object nor an array', async () => {
            await assert.rejects(async () => await ctx.insert('t', 'foo'), {
                name: 'ImplementationError',
                message: 'data is neither an object nor an array of objects'
            });
            assert.equal(ctx.exec.mock.callCount(), 0);
        });
    });

    describe('#update', () => {
        beforeEach(() => mock.method(ctx, 'exec', async () => Promise.resolve({ insertId: 1, affectedRow: 1 })));
        afterEach(() => ctx.exec.mock.restore());

        it('should accept data as an object', async () => {
            await ctx.update('t', { col1: 'val1', col2: 1, col3: ctx.raw('NOW()') }, '1 = 1');

            const [call] = ctx.exec.mock.calls;
            assert.deepEqual(call.arguments, [
                `UPDATE \`t\` SET \`col1\` = 'val1', \`col2\` = 1, \`col3\` = NOW() WHERE 1 = 1`
            ]);
        });

        it('should accept where as an object', async () => {
            await ctx.update('t', { col1: 'val1' }, { col2: 1, col3: ctx.raw('CURDATE()') });

            const [call] = ctx.exec.mock.calls;
            assert.deepEqual(call.arguments, [
                `UPDATE \`t\` SET \`col1\` = 'val1' WHERE \`col2\` = 1 AND \`col3\` = CURDATE()`
            ]);
        });

        it('should reject with an error if data is not set', async () => {
            await assert.rejects(async () => await ctx.update('t', {}, ''), {
                name: 'ImplementationError',
                message: 'data is not set'
            });
            assert.equal(ctx.exec.mock.callCount(), 0);
        });

        it('should reject with an error if where expression is not set', async () => {
            await assert.rejects(async () => await ctx.update('t', { col1: 'val1' }, ''), {
                name: 'ImplementationError',
                message: 'where expression is not set'
            });
            assert.equal(ctx.exec.mock.callCount(), 0);
        });

        it('should handle array values in where clause', async () => {
            await ctx.update('t', { col1: 'val1' }, { col2: [1, 'abc', null] });

            const [call] = ctx.exec.mock.calls;
            assert.deepEqual(call.arguments, [`UPDATE \`t\` SET \`col1\` = 'val1' WHERE \`col2\` IN (1, 'abc', NULL)`]);
        });

        it('should reject empty arrays in where clause', async () => {
            await assert.rejects(async () => ctx.update('t', { col1: 'val1' }, { col1: [] }), {
                name: 'ImplementationError',
                message: 'Empty arrays in WHERE clause are not supported'
            });
        });
    });

    describe('#delete', () => {
        beforeEach(() => mock.method(ctx, 'exec', async () => Promise.resolve({ affectedRows: 1 })));
        afterEach(() => ctx.exec.mock.restore());

        it('should accept where as a string', async () => {
            await ctx.delete('t', '1 = 1');

            const [call] = ctx.exec.mock.calls;
            assert.deepEqual(call.arguments, [`DELETE FROM \`t\` WHERE 1 = 1`]);
        });

        it('should accept where as an object', async () => {
            await ctx.delete('t', { col1: 'val1', col2: 1, col3: ctx.raw('CURDATE()') });

            const [call] = ctx.exec.mock.calls;
            assert.deepEqual(call.arguments, [
                `DELETE FROM \`t\` WHERE \`col1\` = 'val1' AND \`col2\` = 1 AND \`col3\` = CURDATE()`
            ]);
        });

        it('should reject with an error if where expression is not set', async () => {
            await assert.rejects(async () => await ctx.delete('t', ''), {
                name: 'ImplementationError',
                message: 'where expression is not set'
            });
            assert.equal(ctx.exec.mock.callCount(), 0);
        });

        it('should handle array values in where clause', async () => {
            await ctx.delete('t', { col1: [1, 'abc', null] });

            const [call] = ctx.exec.mock.calls;
            assert.deepEqual(call.arguments, [`DELETE FROM \`t\` WHERE \`col1\` IN (1, 'abc', NULL)`]);
        });

        it('should reject empty arrays in where clause', async () => {
            await assert.rejects(async () => ctx.delete('t', { col1: [] }), {
                name: 'ImplementationError',
                message: 'Empty arrays in WHERE clause are not supported'
            });
        });
    });

    describe('#upsert', () => {
        beforeEach(() => mock.method(ctx, 'exec', async () => Promise.resolve({ affectedRows: 1 })));
        afterEach(() => ctx.exec.mock.restore());

        it('should throw an Implementation error if update parameter is missing', async () => {
            await assert.rejects(() => ctx.upsert('t', { col1: 'val1', col2: 1, col3: ctx.raw('NOW()') }), {
                name: 'ImplementationError',
                message: 'Update parameter must be either an object or an array of strings'
            });
        });

        it('should accept assignment list as an array of column names', async () => {
            await ctx.upsert('t', { col1: 'val1', col2: 1, col3: ctx.raw('NOW()') }, ['col1', 'col2']);

            const [call] = ctx.exec.mock.calls;
            assert.deepEqual(call.arguments, [
                `INSERT INTO \`t\` (\`col1\`, \`col2\`, \`col3\`) VALUES ('val1', 1, NOW()) AS \`new_values\` ON DUPLICATE KEY UPDATE \`col1\` = \`new_values\`.\`col1\`, \`col2\` = \`new_values\`.\`col2\``
            ]);
        });

        it('should accept assignment list as an object', async () => {
            await ctx.upsert('t', { col1: 'val1', col2: 1, col3: ctx.raw('NOW()') }, { col1: 'foo' });

            const [call] = ctx.exec.mock.calls;
            assert.deepEqual(call.arguments, [
                `INSERT INTO \`t\` (\`col1\`, \`col2\`, \`col3\`) VALUES ('val1', 1, NOW()) AS \`new_values\` ON DUPLICATE KEY UPDATE \`col1\` = 'foo'`
            ]);
        });

        it('must not use alias for raw values', async () => {
            await ctx.upsert('t', { id: 1, ts: '2000-01-01 00:00:00' }, { ts: ctx.raw('NOW()') });

            const [call] = ctx.exec.mock.calls;
            assert.deepEqual(call.arguments, [
                `INSERT INTO \`t\` (\`id\`, \`ts\`) VALUES (1, '2000-01-01 00:00:00') AS \`new_values\` ON DUPLICATE KEY UPDATE \`ts\` = NOW()`
            ]);
        });

        it('should support custom alias', async () => {
            await ctx.upsert('t', { id: 1, col1: 'val1' }, ['col1'], 'funkyAlias');

            const [call] = ctx.exec.mock.calls;
            assert.deepEqual(call.arguments, [
                `INSERT INTO \`t\` (\`id\`, \`col1\`) VALUES (1, 'val1') AS \`funkyAlias\` ON DUPLICATE KEY UPDATE \`col1\` = \`funkyAlias\`.\`col1\``
            ]);
        });
    });
});
