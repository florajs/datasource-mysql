'use strict';

const { expect } = require('chai');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');
const ciCfg = require('./ci-config');

describe('init queries', () => {
    const db = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';
    const ctxCfg = { db, useMaster: true };
    let ds;

    afterEach(() => ds.close());

    it('should set sql_mode to ANSI if no init queries are defined', async () => {
        ds = FloraMysqlFactory.create(ciCfg);
        const ctx = ds.getContext(ctxCfg);

        await ctx.query('SELECT 1 FROM dual');
        const sqlMode = await ctx.queryOne('SELECT @@sql_mode');

        expect(sqlMode).to.match(/\bANSI\b/);
    });

    it('should execute single init query', async () => {
        const config = { ...ciCfg, onConnect: 'SET SESSION max_execution_time = 1337' };

        ds = FloraMysqlFactory.create(config);
        const ctx = ds.getContext(ctxCfg);

        await ctx.query('SELECT 1 FROM dual');
        const maxExecutionTime = await ctx.queryOne('SELECT @@max_execution_time');

        expect(maxExecutionTime).to.equal(1337);
    });

    it('should execute multiple init queries', async () => {
        const config = {
            ...ciCfg,
            onConnect: [`SET SESSION sql_mode = 'ANSI_QUOTES'`, `SET SESSION max_execution_time = 1337`]
        };

        ds = FloraMysqlFactory.create(config);
        const ctx = ds.getContext(ctxCfg);
        await ctx.query('SELECT 1 FROM dual');

        const [sqlMode, maxExecutionTime] = await Promise.all([
            ctx.queryOne('SELECT @@sql_mode'),
            ctx.queryOne('SELECT @@max_execution_time')
        ]);

        expect(sqlMode).to.contain('ANSI_QUOTES');
        expect(maxExecutionTime).to.equal(1337);
    });

    it('should execute custom init function', async () => {
        const config = {
            ...ciCfg,
            onConnect: (connection) =>
                new Promise((resolve, reject) => {
                    connection.query('SET SESSION max_execution_time = 1337', (err) => {
                        if (err) return reject(err);
                        resolve();
                    });
                })
        };

        ds = FloraMysqlFactory.create(config);
        const ctx = ds.getContext(ctxCfg);
        await ctx.query('SELECT 1 FROM dual');
        const maxExecutionTime = await ctx.queryOne('SELECT @@max_execution_time');

        expect(maxExecutionTime).to.equal(1337);
    });

    it('should handle server specific init queries', async () => {
        const config = {
            ...ciCfg,
            onConnect: `SET SESSION sql_mode = 'ANSI_QUOTES'`,
            default: { onConnect: 'SET SESSION max_execution_time = 1337' }
        };

        ds = FloraMysqlFactory.create(config);
        const ctx = ds.getContext(ctxCfg);
        await ctx.query('SELECT 1 FROM dual');

        const [sqlMode, maxExecutionTime] = await Promise.all([
            ctx.queryOne('SELECT @@sql_mode'),
            ctx.queryOne('SELECT @@max_execution_time')
        ]);

        expect(sqlMode).to.contain('ANSI_QUOTES');
        expect(maxExecutionTime).to.equal(1337);
    });

    it('should handle errors', async () => {
        const config = { ...ciCfg, onConnect: 'SELECT nonExistentAttr FROM t' };

        ds = FloraMysqlFactory.create(config);
        const ctx = ds.getContext(ctxCfg);

        try {
            await ctx.query('SELECT 1 FROM dual');
        } catch (err) {
            expect(err).to.include({
                code: 'ER_BAD_FIELD_ERROR',
                message: `ER_BAD_FIELD_ERROR: Unknown column 'nonExistentAttr' in 'field list'`
            });
        }
    });
});
