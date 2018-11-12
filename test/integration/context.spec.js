'use strict';

const { expect } = require('chai');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');

describe('context', () => {
    const ds = FloraMysqlFactory.create({
        servers: {
            default: {
                user: 'root',
                password: '',
                masters: [{ host: 'mysql' }],
                slaves: [{ host: 'mysql' }]
            }
        }
    });
    const ctx = ds.getContext({ db: 'flora_mysql_testdb' });

    beforeEach(async () => await ctx.exec('START TRANSACTION'));
    afterEach(async () => await ctx.exec('ROLLBACK'));

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
});
