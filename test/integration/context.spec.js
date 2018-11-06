'use strict';

const { expect } = require('chai');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');

describe('context', () => {
    const ds = FloraMysqlFactory.create();
    const ctx = ds.getContext({ db: 'flora_mysql_testdb' });

    beforeEach(async () => await ctx.exec('START TRANSACTION'));
    afterEach(async () => await ctx.exec('ROLLBACK'));

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
