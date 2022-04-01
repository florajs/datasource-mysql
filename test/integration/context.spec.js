'use strict';

const { expect } = require('chai');

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
            expect(result).to.be.an('array').and.to.have.length(0);
        });

        it('should accept query params as an array', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const result = await ctx.query('SELECT "id", "col1" FROM "t" WHERE "id" = ?', [1]);

            expect(result).to.eql([{ id: 1, col1: 'foo' }]);
        });

        it('should accept query params as an object', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const result = await ctx.query('SELECT "id", "col1" FROM "t" WHERE "id" = :id', { id: 1 });

            expect(result).to.eql([{ id: 1, col1: 'foo' }]);
        });

        it('should return typecasted result', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const [item] = await ctx.query('SELECT "id" FROM "t" WHERE "id" = 1');

            expect(item).to.have.property('id', 1);
        });
    });

    describe('#queryRow', () => {
        it('should return null for empty results', async () => {
            const result = await ctx.queryRow('SELECT "col1" FROM "t" WHERE "id" = 1337');
            expect(result).to.be.null;
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
                const result = await ctx.queryRow(sql);

                expect(result).to.eql({ id: 1, col1: 'foo' });
            });
        });

        it('should accept query params as an array', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const result = await ctx.queryRow('SELECT "id", "col1" FROM "t" WHERE "id" = ?', [1]);

            expect(result).to.eql({ id: 1, col1: 'foo' });
        });

        it('should accept query params as an object', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const result = await ctx.queryRow('SELECT "id", "col1" FROM "t" WHERE "id" = :id', { id: 1 });

            expect(result).to.eql({ id: 1, col1: 'foo' });
        });

        it('should return typecasted result', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const row = await ctx.queryRow('SELECT "id", "col1" FROM "t" WHERE "id" = 1');

            expect(row).to.have.property('id', 1);
        });
    });

    describe('#queryOne', () => {
        it('should return null for empty results', async () => {
            const result = await ctx.queryOne('SELECT "col1" FROM "t" WHERE "id" = 1337');
            expect(result).to.be.null;
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

                expect(result).to.equal('foo');
            });
        });

        it('should accept query params as an array', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const result = await ctx.queryOne('SELECT "col1" FROM "t" WHERE "id" = ?', [1]);

            expect(result).to.equal('foo');
        });

        it('should accept query params as an object', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const result = await ctx.queryOne('SELECT "col1" FROM "t" WHERE "id" = :id', { id: 1 });

            expect(result).to.equal('foo');
        });

        it('should return typecasted result', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const id = await ctx.queryOne('SELECT "id" FROM "t" WHERE "id" = 1');

            expect(id).to.equal(1);
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

                expect(result).to.eql(['foo', 'bar']);
            });
        });

        it('should accept query params as an array', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const result = await ctx.queryCol('SELECT "col1" FROM "t" WHERE "id" = ?', [1]);

            expect(result).to.eql(['foo']);
        });

        it('should accept query params as an object', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const result = await ctx.queryCol('SELECT "col1" FROM "t" WHERE "id" = :id', { id: 1 });

            expect(result).to.eql(['foo']);
        });

        it('should return typecasted result', async () => {
            await ctx.insert('t', [
                { id: 1, col1: 'foo' },
                { id: 2, col1: 'bar' }
            ]);
            const [id] = await ctx.queryCol('SELECT "id" FROM "t" WHERE "id" = 1');

            expect(id).to.equal(1);
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

                expect(insertId).to.equal(1);
            });

            it('should return number of affected rows', async () => {
                const { affectedRows } = await ctx.insert('t', [
                    { id: 1, col1: 'foo' },
                    { id: 2, col1: 'bar' }
                ]);

                expect(affectedRows).to.equal(2);
            });
        });

        describe('#update', () => {
            it('should return number of changed rows', async () => {
                await ctx.insert('t', [
                    { id: 1, col1: 'foo' },
                    { id: 2, col1: 'bar' }
                ]);

                const { changedRows } = await ctx.update('t', { col1: 'test' }, { id: 1 });

                expect(changedRows).to.equal(1);
            });

            it('should return number of affected rows', async () => {
                await ctx.insert('t', [
                    { id: 1, col1: 'foo' },
                    { id: 2, col1: 'bar' }
                ]);

                const { affectedRows } = await ctx.update('t', { col1: 'test' }, '1 = 1');

                expect(affectedRows).to.equal(2);
            });
        });

        describe('#delete', () => {
            it('should return number of affected rows', async () => {
                await ctx.insert('t', [
                    { id: 1, col1: 'foo' },
                    { id: 2, col1: 'bar' }
                ]);

                const { affectedRows } = await ctx.delete('t', { id: 1 });

                expect(affectedRows).to.equal(1);
            });
        });

        describe('#upsert', () => {
            it('should return number of affected rows', async () => {
                const { affectedRows } = await tableWithAutoIncrement(ctx, 't1', async () =>
                    ctx.upsert('t1', { id: 1, col1: 'foobar' }, ['col1'])
                );

                expect(affectedRows).to.equal(1);
            });

            it('should return number of changed rows', async () => {
                const { changedRows } = await tableWithAutoIncrement(ctx, 't1', async () =>
                    ctx.upsert('t1', { id: 1, col1: 'foobar' }, ['col1'])
                );

                expect(changedRows).to.equal(0);
            });

            it('should accept data as an object', async () => {
                await ctx.insert('t', { id: 1, col1: 'foo' });
                await ctx.upsert('t', { id: 1, col1: 'bar' }, ['col1']);

                const result = await ctx.query('SELECT id, col1 FROM t');
                expect(result).to.be.eql([{ id: 1, col1: 'bar' }]);
            });

            it('should accept data as an array of objects', async () => {
                await ctx.insert('t', [
                    { id: 1, col1: 'foo' },
                    { id: 2, col1: 'bar' }
                ]);
                await ctx.upsert('t', [{ id: 1, col1: 'foobar' }], ['col1']);

                const result = await ctx.query('SELECT id, col1 FROM t');
                expect(result).to.be.eql([
                    { id: 1, col1: 'foobar' },
                    { id: 2, col1: 'bar' }
                ]);
            });

            it('should handle assignment list as an array of column names', async () => {
                await ctx.insert('t', [
                    { id: 1, col1: 'foo' },
                    { id: 2, col1: 'bar' }
                ]);
                await ctx.upsert('t', { id: 1, col1: 'foobar' }, ['col1']);

                const result = await ctx.query('SELECT id, col1 FROM t');
                expect(result).to.eql([
                    { id: 1, col1: 'foobar' },
                    { id: 2, col1: 'bar' }
                ]);
            });

            it('should handle assignment list as an object', async () => {
                await ctx.insert('t', [
                    { id: 1, col1: 'foo' },
                    { id: 2, col1: 'bar' }
                ]);
                await ctx.upsert(
                    't',
                    { id: 1, col1: 'foobar' },
                    { col1: ctx.raw(`CONCAT(${ctx.quoteIdentifier('col1')}, '|', 'bar')`) }
                );

                const result = await ctx.query('SELECT id, col1 FROM t');
                expect(result).to.eql([
                    { id: 1, col1: 'foo|bar' },
                    { id: 2, col1: 'bar' }
                ]);
            });
        });

        describe('#exec', () => {
            it(`should resolve/return with insertId property`, async () => {
                const { insertId } = await tableWithAutoIncrement(
                    ctx,
                    't1',
                    async () => await ctx.exec(`INSERT INTO t1 (col1) VALUES ('insertId')`)
                );

                expect(insertId).to.equal(1);
            });

            Object.entries({
                affectedRows: 1,
                changedRows: 1,
                insertId: 0
            }).forEach(([property, value]) => {
                it(`should resolve/return with ${property} property`, async () => {
                    await ctx.insert('t', [{ id: 1, col1: 'foo' }]);
                    const result = await ctx.exec(`UPDATE t SET col1 = 'changedRows' WHERE id = 1`);

                    expect(result).to.have.property(property, value);
                });
            });
        });
    });

    describe('#transaction', () => {
        it('should support callback parameter', async () => {
            await ctx.transaction(async (trx) => {
                await trx.insert('t', { id: 1, col1: 'foobar' });
            });
            const values = await ctx.queryRow('SELECT id, col1 FROM t');

            expect(values).to.eql({ id: 1, col1: 'foobar' });
        });

        it('should automatically rollback on errors', async () => {
            try {
                await ctx.transaction(async (trx) => {
                    await trx.insert('t', { id: 1, col1: 'foo' });
                    await trx.insert('nonexistent_table', { id: 1, col1: 'bar' });
                });
            } catch (e) {
                const values = await ctx.query('SELECT id, col1 FROM t');

                expect(e).to.have.property('code', 'ER_NO_SUCH_TABLE');
                expect(values).to.eql([]);

                return;
            }

            throw new Error('Expected an error to be thrown');
        });

        it('should rethrow errors', async () => {
            try {
                await ctx.transaction(async (trx) => {
                    await trx.insert('nonexistent_table', { col1: 'blablub' });
                });
            } catch (e) {
                expect(e)
                    .to.be.an.instanceof(Error)
                    .and.to.have.property('message')
                    .and.to.contain(`Table 'flora_mysql_testdb.nonexistent_table' doesn't exist`);
                return;
            }

            throw new Error('Expected an error to be thrown');
        });
    });
});
