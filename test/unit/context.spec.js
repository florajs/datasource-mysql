'use strict';

const util = require('util');

const { expect } = require('chai');
const sinon = require('sinon');
const sandbox = sinon.createSandbox();

const Connection = require('../../node_modules/mysql/lib/Connection');
const Expr = require('../../lib/expr');
const Transaction = require('../../lib/transaction');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');
const { ImplementationError } = require('flora-errors');

describe('context', () => {
    const ds = FloraMysqlFactory.create();
    const db = 'db';
    const ctx = ds.getContext({ db });

    afterEach(() => sandbox.restore());

    describe('interface', () => {
        it('should export insert function', () => {
            expect(ctx.insert).to.be.a('function');
        });

        it('should export update function', () => {
            expect(ctx.update).to.be.a('function');
        });

        it('should export delete function', () => {
            expect(ctx.delete).to.be.a('function');
        });

        it('should export upsert function', () => {
            expect(ctx.upsert).to.be.a('function');
        });

        it('should export query function', () => {
            expect(ctx.query).to.be.a('function');
        });

        it('should export exec function', () => {
            expect(ctx.exec).to.be.a('function');
        });

        it('should export transaction function', () => {
            expect(ctx.transaction).to.be.a('function');
        });
    });

    describe('#query', () => {
        let queryStub;

        beforeEach(() => {
            queryStub = sandbox.stub(ds, '_query').resolves({ results: [] });
        });

        it('should use a slave connection', async () => {
            const sql = 'SELECT 1 FROM dual';
            await ctx.query(sql);
            expect(queryStub).to.have.been.calledWith({ type: 'SLAVE', db, server: 'default' }, sql);
        });

        it('should handle placeholders', async () => {
            const sql = `SELECT id FROM "t" WHERE col1 = 'val1'`;
            await ctx.exec('SELECT id FROM "t" WHERE col1 = ?', ['val1']);
            expect(queryStub).to.have.been.calledWith({ type: 'MASTER', db, server: 'default' }, sql);
        });

        it('should handle named placeholders', async () => {
            const sql = `SELECT id FROM "t" WHERE col1 = 'val1'`;
            await ctx.query('SELECT id FROM "t" WHERE col1 = :col1', { col1: 'val1' });
            expect(queryStub).to.have.been.calledWith({ type: 'SLAVE', db, server: 'default' }, sql);
        });
    });

    describe('#exec', () => {
        let queryStub;

        beforeEach(() => {
            queryStub = sandbox.stub(ds, '_query').resolves({ results: [] });
        });

        it('should use a master connection', async () => {
            const sql = 'SELECT 1 FROM dual';
            await ctx.exec(sql);
            expect(queryStub).to.have.been.calledWith({ type: 'MASTER', db, server: 'default' }, sql);
        });

        it('should handle placeholders', async () => {
            const sql = `SELECT id FROM "t" WHERE col1 = 'val1'`;
            await ctx.exec('SELECT id FROM "t" WHERE col1 = ?', ['val1']);
            expect(queryStub).to.have.been.calledWith({ type: 'MASTER', db, server: 'default' }, sql);
        });

        it('should handle named placeholders', async () => {
            const sql = `SELECT id FROM "t" WHERE col1 = 'val1'`;
            await ctx.exec('SELECT id FROM "t" WHERE col1 = :col1', { col1: 'val1' });
            expect(queryStub).to.have.been.calledWith({ type: 'MASTER', db, server: 'default' }, sql);
        });
    });

    describe('parameter placeholders', () => {
        let queryStub;

        beforeEach(() => {
            queryStub = sandbox.stub(ds, '_query').resolves({ results: [] });
        });

        afterEach(() => sandbox.restore());

        [
            ['strings', 'val', 'SELECT "id" FROM "t" WHERE "col1" = ?',  `SELECT "id" FROM "t" WHERE "col1" = 'val'`],
            ['numbers', 1337, 'SELECT "id" FROM "t" WHERE "col1" = ?', 'SELECT "id" FROM "t" WHERE "col1" = 1337'],
            ['booleans', true, 'SELECT "id" FROM "t" WHERE "col1" = ?', 'SELECT "id" FROM "t" WHERE "col1" = true'],
            ['dates', new Date(2019, 0, 1, 0, 0, 0, 0), 'SELECT "id" FROM "t" WHERE "col1" = ?', `SELECT "id" FROM "t" WHERE "col1" = '2019-01-01 00:00:00.000'`],
            ['arrays', [1, 3, 3, 7], 'SELECT "id" FROM "t" WHERE "col1" IN (?)', `SELECT "id" FROM "t" WHERE "col1" IN (1, 3, 3, 7)`],
            ['sqlstringifiable objects', new Expr('CURDATE()'), 'SELECT "id" FROM "t" WHERE "col1" = ?', `SELECT "id" FROM "t" WHERE "col1" = CURDATE()`],
            ['null', null, 'SELECT "id" FROM "t" WHERE "col1" = ?', `SELECT "id" FROM "t" WHERE "col1" = NULL`],
        ].forEach(([type, value, query, sql]) => {
            it(`should support ${type}`, async () => {
                await ctx.exec(query, [value]);
                expect(queryStub).to.have.been.calledWith(sinon.match.object, sql);
            });
        });

        [
            ['objects', {}, '"values" must not be an empty object'],
            ['arrays', [], '"values" must not be an empty array']
        ].forEach(([type, params, msg]) => {
            it(`throw an error for empty ${type}`, async () => {
                try {
                    await ctx.exec('SELECT "id" FROM "t" WHERE "col1" IN (:col1)', params);
                } catch (e) {
                    expect(e).to.be.instanceOf(Error).with.property('message', msg);
                    expect(queryStub).to.not.have.been.called;
                    return;
                }

                throw new Error('Expected an error');
            });
        });

        it('throw an error for non-object or non-array values', async () => {
            try {
                await ctx.exec('SELECT "id" FROM "t" WHERE "col1" = ?', 'foo');
            } catch (e) {
                expect(e).to.be.instanceOf(Error).with.property('message', '"values" must be an object or an array');
                expect(queryStub).to.not.have.been.called;
                return;
            }

            throw new Error('Expected an error');
        });
    });

    describe('#insert', () => {
        let execStub;

        beforeEach(() => {
            execStub = sandbox.stub(ctx, 'exec').resolves({ insertId: 1, affectedRow: 1 });
        });

        it('should accept data as an object', async () => {
            await ctx.insert('t', { col1: 'val1', col2: 1, col3: new Expr('NOW()') });
            expect(execStub).to.have.been.calledWith(`INSERT INTO "t" ("col1", "col2", "col3") VALUES ('val1', 1, NOW())`);
        });

        it('should accept data as an array of objects', async () => {
            await ctx.insert('t', [{ col1: 'val1', col2: 1 }, { col1: 'val2', col2: 2 }]);
            expect(execStub).to.have.been.calledWith(`INSERT INTO "t" ("col1", "col2") VALUES ('val1', 1), ('val2', 2)`);
        });

        it('should reject with an error if data is not set', async () => {
            try {
                await ctx.insert('t')
            } catch (e) {
                expect(e).to.be.instanceOf(ImplementationError)
                    .with.property('message', 'data parameter is required');
                expect(execStub).to.not.have.been.called;
                return;
            }

            throw new Error('Expected promise to reject');
        });

        it('should reject with an error if data neither an object nor an array', async () => {
            try {
                await ctx.insert('t', 'foo')
            } catch (e) {
                expect(e).to.be.instanceOf(ImplementationError)
                    .with.property('message', 'data is neither an object nor an array of objects');
                expect(execStub).to.not.have.been.called;
                return;
            }

            throw new Error('Expected promise to reject');
        });
    });

    describe('#update', () => {
        let execStub;

        beforeEach(() => {
            execStub = sandbox.stub(ctx, 'exec').resolves({ changedRows: 1 });
        });

        it('should accept data as an object', async () => {
            await ctx.update('t', { col1: 'val1', col2: 1, col3: new Expr('NOW()') }, '1 = 1');
            expect(execStub).to.have.been.calledWith(`UPDATE "t" SET "col1" = 'val1', "col2" = 1, "col3" = NOW() WHERE 1 = 1`);
        });

        it('should accept where as an object', async () => {
            await ctx.update('t', { col1: 'val1' }, { col2: 1, col3: new Expr('CURDATE()') });
            expect(execStub).to.have.been.calledWith(`UPDATE "t" SET "col1" = 'val1' WHERE "col2" = 1 AND "col3" = CURDATE()`);
        });

        it('should reject with an error if data is not set', async () => {
            try {
                await ctx.update('t', {}, '');
            } catch (e) {
                expect(e).to.be.instanceOf(ImplementationError)
                    .with.property('message', 'data is not set');
                expect(execStub).to.not.have.been.called;
                return;
            }

            throw new Error('Expected promise to reject');
        });

        it('should reject with an error if where expression is not set', async () => {
            try {
                await ctx.update('t', { col1: 'val1' }, '');
            } catch (e) {
                expect(e).to.be.instanceOf(ImplementationError)
                    .with.property('message', 'where expression is not set');
                expect(execStub).to.not.have.been.called;
                return;
            }

            throw new Error('Expected promise to reject');
        });
    });

    describe('#delete', () => {
        let execStub;

        beforeEach(() => {
            execStub = sandbox.stub(ctx, 'exec').resolves({ affectedRows: 1 });
        });

        it('should accept where as a string', async () => {
            await ctx.delete('t', '1 = 1');
            expect(execStub).to.have.been.calledWith('DELETE FROM "t" WHERE 1 = 1');
        });

        it('should accept where as an object', async () => {
            await ctx.delete('t', { col1: 'val1', col2: 1, col3: new Expr('CURDATE()') });
            expect(execStub).to.have.been.calledWith(`DELETE FROM "t" WHERE "col1" = 'val1' AND "col2" = 1 AND "col3" = CURDATE()`);
        });

        it('should reject with an error if where expression is not set', async () => {
            try {
                await ctx.delete('t', '');
            } catch (e) {
                expect(e).to.be.instanceOf(ImplementationError)
                    .with.property('message', 'where expression is not set');
                expect(execStub).to.not.have.been.called;
                return;
            }

            throw new Error('Expected promise to reject');
        });
    });

    describe('#upsert', () => {
        let execStub;

        beforeEach(() => {
            execStub = sandbox.stub(ctx, 'exec').resolves({});
        });

        it('should accept assignment list as an array of column names', async () => {
            const sql = `INSERT INTO "t" ("col1", "col2", "col3") VALUES ('val1', 1, NOW()) ON DUPLICATE KEY UPDATE "col1" = VALUES("col1"), "col2" = VALUES("col2")`;
            await ctx.upsert('t', { col1: 'val1', col2: 1, col3: new Expr('NOW()') }, ['col1', 'col2']);
            expect(execStub).to.have.been.calledWith(sql);
        });

        it('should accept assignment list as an object', async () => {
            const sql = `INSERT INTO "t" ("col1", "col2", "col3") VALUES ('val1', 1, NOW()) ON DUPLICATE KEY UPDATE "col1" = 'foo', "col2" = col2 + 1`;
            await ctx.upsert('t', { col1: 'val1', col2: 1, col3: new Expr('NOW()') }, { col1: 'foo', col2: new Expr('col2 + 1') });
            expect(execStub).to.have.been.calledWith(sql);
        });
    });

    describe('#transaction', () => {
        it('should use master connection', async () => {
            const connectionSpy = sandbox.spy(ds, '_getConnection');
            const queryStub = sandbox.stub(Connection.prototype, 'query').yields(null, []);

            sandbox.stub(Connection.prototype, 'connect').yields(null);

            const trx = await ctx.transaction();

            expect(trx).to.be.instanceOf(Transaction);
            expect(connectionSpy).to.have.been.calledWith({ type: 'MASTER', db, server: 'default' });
            expect(queryStub).to.have.been.calledWith('START TRANSACTION');
        });
    });
});
