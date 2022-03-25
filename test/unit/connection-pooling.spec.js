'use strict';

const Connection = require('../../node_modules/mysql/lib/Connection');
const PoolCluster = require('../../node_modules/mysql/lib/PoolCluster');

const chai = require('chai');
const { expect } = chai;
const sinon = require('sinon');

const { FloraMysqlFactory, defaultCfg } = require('../FloraMysqlFactory');
const testCfg = {
    ...defaultCfg,
    servers: { default: { user: 'foo', password: 'bar', masters: [{ host: 'mysql.example.com' }] } }
};
const { cloneDeep } = require('../../lib/util');

const sandbox = sinon.createSandbox();

chai.use(require('sinon-chai'));

const PORT = process.env.MYSQL_PORT || 3306;

describe('connection pooling', () => {
    const ctxCfg = { db: 'test' };
    let poolSpy;

    beforeEach(() => {
        sandbox.stub(Connection.prototype, 'connect').yields(null);
        sandbox.stub(Connection.prototype, 'query').yields(null, []);
        poolSpy = sandbox.spy(PoolCluster.prototype, 'add');
    });

    afterEach(() => sandbox.restore());

    describe('single server', () => {
        describe('credentials', () => {
            it('server specifc', async () => {
                const ds = FloraMysqlFactory.create(testCfg);
                const ctx = ds.getContext(ctxCfg);

                await ctx.exec('SELECT 1 FROM dual');

                expect(poolSpy).to.have.been.calledWithMatch('mysql.example.com', { user: 'foo', password: 'bar' });
            });

            it('global', async () => {
                const cfg = { ...cloneDeep(testCfg), user: 'root', password: 'secret' };
                delete cfg.servers.default.user;
                delete cfg.servers.default.password;

                const ds = FloraMysqlFactory.create(cfg);
                const ctx = ds.getContext(ctxCfg);

                await ctx.exec('SELECT 1 FROM dual');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER_mysql.example.com', {
                    user: 'root',
                    password: 'secret'
                });
            });
        });

        describe('port', () => {
            it('default', async () => {
                const cfg = cloneDeep(testCfg);
                cfg.servers.default.port = PORT;
                const ds = FloraMysqlFactory.create(cfg);
                const ctx = ds.getContext(ctxCfg);

                await ctx.exec('SELECT 1 FROM dual');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER_mysql.example.com', { port: PORT });
            });

            it('custom', async () => {
                const cfg = { ...cloneDeep(testCfg) };
                cfg.servers.default.port = 1337;
                const ds = FloraMysqlFactory.create(cfg);
                const ctx = ds.getContext(ctxCfg);

                await ctx.exec('SELECT 1 FROM dual');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER_mysql.example.com', { port: 1337 });
            });
        });

        describe('connect timeout', () => {
            it('default', async () => {
                const ds = FloraMysqlFactory.create(testCfg);
                const ctx = ds.getContext(ctxCfg);
                await ctx.exec('SELECT 1 FROM dual');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER_mysql.example.com', { connectTimeout: 3000 });
            });

            it('server specific', async () => {
                const cfg = cloneDeep(testCfg);
                cfg.servers.default.connectTimeout = 1000;
                const ds = FloraMysqlFactory.create(cfg);
                const ctx = ds.getContext(ctxCfg);

                await ctx.exec('SELECT 1 FROM dual');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER_mysql.example.com', { connectTimeout: 1000 });
            });

            it('global', async () => {
                const cfg = { ...cloneDeep(testCfg), connectTimeout: 1500 };
                const ds = FloraMysqlFactory.create(cfg);
                const ctx = ds.getContext(ctxCfg);

                await ctx.exec('SELECT 1 FROM dual');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER_mysql.example.com', { connectTimeout: 1500 });
            });
        });

        describe('pool size', () => {
            it('default', async () => {
                const ds = FloraMysqlFactory.create(testCfg);
                const ctx = ds.getContext(ctxCfg);

                await ctx.exec('SELECT 1 FROM dual');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER_mysql.example.com', { connectionLimit: 10 });
            });

            it('server specific', async () => {
                const cfg = cloneDeep(testCfg);
                cfg.servers.default.poolSize = 100;
                const ds = FloraMysqlFactory.create(cfg);
                const ctx = ds.getContext(ctxCfg);

                await ctx.exec('SELECT 1 FROM dual');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER_mysql.example.com', { connectionLimit: 100 });
            });

            it('global', async () => {
                const cfg = { ...cloneDeep(testCfg), poolSize: 50 };
                const ds = FloraMysqlFactory.create(cfg);
                const ctx = ds.getContext(ctxCfg);

                await ctx.exec('SELECT 1 FROM dual');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER_mysql.example.com', { connectionLimit: 50 });
            });
        });

        it('should always use "master" server', async () => {
            const ds = FloraMysqlFactory.create(testCfg);
            const ctx = ds.getContext(ctxCfg);
            const connectionSpy = sandbox.spy(PoolCluster.prototype, 'getConnection');

            await ctx.exec('SELECT 1 FROM dual');

            expect(connectionSpy).to.have.been.always.calledWith('MASTER*', sinon.match.any);
        });
    });

    describe('master/slave', () => {
        xit('cluster config options');

        it('should create pools for masters and slaves', async () => {
            const user = 'root';
            const password = 'secret';
            const baseCfg = { user, password };
            const ds = FloraMysqlFactory.create({
                servers: {
                    default: {
                        user,
                        password,
                        masters: [{ host: 'mysql-master' }],
                        slaves: [
                            { host: 'mysql-slave1', port: 1337 },
                            { host: 'mysql-slave2', port: 4711 }
                        ]
                    }
                }
            });
            const ctx = ds.getContext(ctxCfg);
            const sql = 'SELECT 1 FROM dual';

            await Promise.all([ctx.exec(sql), ctx.query(sql)]);

            const masterCfg = { ...baseCfg, ...{ host: 'mysql-master', port: 3306 } };
            expect(poolSpy).to.have.been.calledWith('MASTER_mysql-master', sinon.match(masterCfg));

            const slaveCfg1 = { ...baseCfg, ...{ host: 'mysql-slave1', port: 1337 } };
            expect(poolSpy).to.have.been.calledWith('SLAVE_mysql-slave1', sinon.match(slaveCfg1));

            const slaveCfg2 = { ...baseCfg, ...{ host: 'mysql-slave2', port: 4711 } };
            expect(poolSpy).to.have.been.calledWith('SLAVE_mysql-slave2', sinon.match(slaveCfg2));
        });
    });
});
