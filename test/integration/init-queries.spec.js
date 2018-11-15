'use strict';

const chai = require('chai');
const { expect } = chai;
const sinon = require('sinon');

const PoolConnection = require('../../node_modules/mysql/lib/PoolConnection');
const { FloraMysqlFactory, defaultCfg } = require('../FloraMysqlFactory');

chai.use(require('sinon-chai'));

describe('init queries', () => {
    const db = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';
    const ctxCfg = { db, useMaster: true };
    let ds;
    let querySpy;

    beforeEach(() => querySpy = sinon.spy(PoolConnection.prototype, 'query'));

    afterEach(() => {
        querySpy.restore();
        return ds.close();
    });

    it('should set sql_mode to ANSI if no init queries are defined', async () => {
        ds = FloraMysqlFactory.create();
        const ctx = ds.getContext(ctxCfg);

        await ctx.query('SELECT 1 FROM dual');
        expect(querySpy).to.have.been.calledWith('SET SESSION sql_mode = \'ANSI\'');
    });

    it('should execute single init query', async () => {
        const initQuery = `SET SESSION sql_mode = 'ANSI_QUOTES'`;
        const config = Object.assign({}, defaultCfg, { onConnect: initQuery });

        ds = FloraMysqlFactory.create(config);
        const ctx = ds.getContext(ctxCfg);

        await ctx.query('SELECT 1 FROM dual');
        expect(querySpy).to.have.been.calledWith(initQuery);
    });

    it('should execute multiple init queries', async () => {
        const initQuery1 = `SET SESSION sql_mode = 'ANSI_QUOTES'`;
        const initQuery2 = `SET SESSION max_execution_time = 1`;
        const config = Object.assign({}, defaultCfg, { onConnect: [initQuery1, initQuery2] });

        ds = FloraMysqlFactory.create(config);
        const ctx = ds.getContext(ctxCfg);
        await ctx.query('SELECT 1 FROM dual');

        expect(querySpy)
            .to.have.been.calledWith(initQuery1)
            .and.to.have.been.calledWith(initQuery2);
    });

    it('should execute custom init function', async () => {
        const initQuery = `SET SESSION sql_mode = 'ANSI_QUOTES'`;
        const onConnect = sinon.spy((connection, done) => {
            connection.query(initQuery, err => done(err ? err : null));
        });
        const config = Object.assign({}, defaultCfg, { onConnect });

        ds = FloraMysqlFactory.create(config);
        const ctx = ds.getContext(ctxCfg);
        await ctx.query('SELECT 1 FROM dual');

        expect(querySpy).to.have.been.calledWith(initQuery);
        expect(onConnect).to.have.been.calledWith(sinon.match.instanceOf(PoolConnection), sinon.match.func);
    });

    it('should handle server specific init queries', async () => {
        const globalInitQuery = `SET SESSION sql_mode = 'ANSI_QUOTES'`;
        const serverInitQuery = 'SET SESSION max_execution_time = 1';
        const config = Object.assign({}, defaultCfg,
            { onConnect: globalInitQuery },
            { default: { onConnect: serverInitQuery }}
        );

        ds = FloraMysqlFactory.create(config);
        const ctx = ds.getContext(ctxCfg);
        await ctx.query('SELECT 1 FROM dual');

        expect(querySpy)
            .to.have.been.calledWith(globalInitQuery)
            .and.to.have.been.calledWith(serverInitQuery);
    });

    it('should handle errors', async () => {
        const config = Object.assign({}, defaultCfg, { onConnect: 'SELECT nonExistentAttr FROM t' });

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
