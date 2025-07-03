'use strict';

const assert = require('node:assert/strict');
const { afterEach, describe, it } = require('node:test');

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

        assert.match(sqlMode, /\bANSI\b/);
    });

    it('should execute single init query', async () => {
        const config = { ...ciCfg, onConnect: 'SET SESSION max_execution_time = 1337' };

        ds = FloraMysqlFactory.create(config);
        const ctx = ds.getContext(ctxCfg);

        await ctx.query('SELECT 1 FROM dual');
        const maxExecutionTime = await ctx.queryOne('SELECT @@max_execution_time');

        assert.equal(maxExecutionTime, 1337);
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

        assert.match(sqlMode, /\bANSI_QUOTES\b/);
        assert.equal(maxExecutionTime, 1337);
    });

    it('should execute custom init function', async () => {
        const config = {
            ...ciCfg,
            onConnect: (connection) => connection.query('SET SESSION max_execution_time = 1337')
        };

        ds = FloraMysqlFactory.create(config);
        const ctx = ds.getContext(ctxCfg);
        await ctx.query('SELECT 1 FROM dual');
        const maxExecutionTime = await ctx.queryOne('SELECT @@max_execution_time');

        assert.equal(maxExecutionTime, 1337);
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

        assert.match(sqlMode, /\bANSI_QUOTES\b/);
        assert.equal(maxExecutionTime, 1337);
    });

    it('should handle errors', async () => {
        const config = { ...ciCfg, onConnect: 'SELECT nonExistentAttr FROM dual' };

        ds = FloraMysqlFactory.create(config);
        const ctx = ds.getContext(ctxCfg);

        await assert.rejects(async () => await ctx.query('SELECT 1 FROM dual'), {
            code: 'ER_BAD_FIELD_ERROR',
            message: `Unknown column 'nonExistentAttr' in 'field list'`
        });
    });
});
