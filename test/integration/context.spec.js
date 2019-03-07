'use strict';

const { expect } = require('chai');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');

describe('context', () => {
    const ds = FloraMysqlFactory.create();
    const db = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';
    const ctx = ds.getContext({ db });

    beforeEach(async () => await ctx.exec('START TRANSACTION'));
    afterEach(async () => await ctx.exec('ROLLBACK'));

    after(() => ds.close());

    describe('#query', () => {
        it('should return an array for empty results', async () => {
            const result = await ctx.query('SELECT "col1" FROM "t" WHERE "id" = 1337');
            expect(result).to.be.an('array').and.to.have.length(0);
        });

        it('should accept query params as an array', async () => {
            const result = await ctx.query('SELECT "id", "col1" FROM "t" WHERE "id" = ?', [1]);
            expect(result).to.eql([{ id: 1, col1: 'foo' }]);
        });

        it('should accept query params as an object', async () => {
            const result = await ctx.query('SELECT "id", "col1" FROM "t" WHERE "id" = :id', { id: 1 });
            expect(result).to.eql([{ id: 1, col1: 'foo' }]);
        });
    });

    describe('#queryRow', () => {
        it('should return null for empty results', async () => {
            const result = await ctx.queryRow('SELECT "col1" FROM "t" WHERE "id" = 1337');
            expect(result).to.be.null;
        });

        [
            ['should resolve to an object', 'SELECT "id", "col1" FROM "t" WHERE "id" = 1'],
            ['should handle multiple rows', 'SELECT "id", "col1" FROM "t" ORDER BY "id" ASC']
        ].forEach(([description, sql]) => {
            it(description, async () => {
                const result = await ctx.queryRow(sql);
                expect(result).to.eql({ id: 1, col1: 'foo' });
            });
        });

        it('should accept query params as an array', async () => {
            const result = await ctx.queryRow('SELECT "id", "col1" FROM "t" WHERE "id" = ?', [1]);
            expect(result).to.eql({ id: 1, col1: 'foo' });
        });

        it('should accept query params as an object', async () => {
            const result = await ctx.queryRow('SELECT "id", "col1" FROM "t" WHERE "id" = :id', { id: 1 });
            expect(result).to.eql({ id: 1, col1: 'foo' });
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
            ['should handle multiple rows', 'SELECT "col1" FROM "t" ORDER BY "id" ASC']
        ].forEach(([description, sql]) => {
            it(description, async () => {
                const result = await ctx.queryOne(sql);
                expect(result).to.equal('foo');
            });
        });

        it('should accept query params as an array', async () => {
            const result = await ctx.queryOne('SELECT "col1" FROM "t" WHERE "id" = ?', [1]);
            expect(result).to.equal('foo');
        });

        it('should accept query params as an object', async () => {
            const result = await ctx.queryOne('SELECT "col1" FROM "t" WHERE "id" = :id', { id: 1 });
            expect(result).to.equal('foo');
        });
    });

    describe('#queryCol', () => {
        [
            ['should resolve to an array of values', 'SELECT "col1" FROM "t"'],
            ['should handle aliases', 'SELECT "col1" AS "funkyAlias" FROM "t"'],
            ['should handle multiple columns', 'SELECT "col1", "id" FROM "t"']
        ].forEach(([description, sql]) => {
            it(description, async () => {
                const result = await ctx.queryCol(sql);
                expect(result).to.include('foo')
                    .and.to.include('bar')
                    .and.to.include('foobar');
            });
        });

        it('should accept query params as an array', async () => {
            const result = await ctx.queryCol('SELECT "col1" FROM "t" WHERE "id" = ?', [1]);
            expect(result).to.eql(['foo']);
        });

        it('should accept query params as an object', async () => {
            const result = await ctx.queryCol('SELECT "col1" FROM "t" WHERE "id" = :id', { id: 1 });
            expect(result).to.eql(['foo']);
        });
    });

    describe('#insert', () => {
        it('should return last inserted id', async () => {
            const { insertId } = await ctx.insert('t', { col1: 'test' });
            expect(insertId).to.be.at.least(1);
        });

        it('should return number of affected rows', async () => {
            const { affectedRows } = await ctx.insert('t', [{ col1: 'test' }, { col1: 'test1' }]);
            expect(affectedRows).to.equal(2);
        });
    });

    describe('#update', () => {
        it('should return number of changed rows', async () => {
            const { changedRows } = await ctx.update('t', { col1: 'test' }, { id: 1 });
            expect(changedRows).to.equal(1);
        });

        it('should return number of affected rows', async () => {
            const { affectedRows } = await ctx.update('t', { col1: 'test' }, `1 = 1`);
            expect(affectedRows).to.be.at.least(1);
        });
    });

    describe('#delete', () => {
        it('should return number of affected rows', async () => {
            const { affectedRows } = await ctx.delete('t', { id: 1 });
            expect(affectedRows).to.equal(1);
        });
    });

    describe('#exec', () => {
        it(`should resolve/return with insertId property`, async () => {
            const { insertId } = await ctx.exec(`INSERT INTO t (col1) VALUES ('insertId')`);
            expect(insertId).to.be.greaterThan(3);
        });

        Object.entries({
            affectedRows: 1,
            changedRows: 1,
            insertId: 0
        }).forEach(([property, value]) => {
            it(`should resolve/return with ${property} property`, async () => {
                const result = await ctx.exec(`UPDATE t SET col1 = 'changedRows' WHERE id = 1`);
                expect(result).to.have.property(property, value);
            });
        });
    });
});
