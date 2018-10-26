'use strict';

const Connection = require('../../node_modules/mysql/lib/Connection');
const PoolCluster = require('../../node_modules/mysql/lib/PoolCluster');

const chai = require('chai');
const { expect } = chai;
const sinon = require('sinon');

const { FloraMysqlFactory, defaultCfg } = require('../FloraMysqlFactory');

const sandbox = sinon.createSandbox();

chai.use(require('sinon-chai'));

function clone(obj) {
    return JSON.parse(JSON.stringify(obj));
}

describe.only('connection pooling', () => {
    const database = 'test';
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
                const cfg = clone(defaultCfg);
                cfg.servers.default.user = 'foo';
                cfg.servers.default.password = 'bar';

                const ds = FloraMysqlFactory.create(cfg);
                await ds.query('default', database, 'SELECT 1');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER', { user: 'foo', password: 'bar' });
            });

            it('global', async () => {
                const cfg = clone(defaultCfg);
                cfg.user = 'root';
                cfg.password = 'secret';
                delete cfg.servers.default.user;
                delete cfg.servers.default.password;

                const ds = FloraMysqlFactory.create(cfg);
                await ds.query('default', database, 'SELECT 1');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER', { user: 'root', password: 'secret' });
            });
        });

        describe('port', () => {
            it('default', async () => {
                const ds = FloraMysqlFactory.create();
                await ds.query('default', database, 'SELECT 1');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER', { port: 3306 });
            });

            it('custom', async () => {
                const cfg = clone(defaultCfg);
                cfg.servers.default.port = 1337;

                const ds = FloraMysqlFactory.create(cfg);
                await ds.query('default', database, 'SELECT 1');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER', { port: 1337 });
            });
        });

        describe('connect timeout', () => {
            it('default', async () => {
                const ds = FloraMysqlFactory.create();
                await ds.query('default', database, 'SELECT 1');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER', { connectTimeout: 3000 });
            });

            it('server specific', async () => {
                const cfg = clone(defaultCfg);
                cfg.servers.default.connectTimeout = 1000;

                const ds = FloraMysqlFactory.create(cfg);
                await ds.query('default', database, 'SELECT 1');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER', { connectTimeout: 1000 });
            });

            it('global', async () => {
                const cfg = clone(defaultCfg);
                cfg.connectTimeout = 1500;

                const ds = FloraMysqlFactory.create(cfg);
                await ds.query('default', database, 'SELECT 1');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER', { connectTimeout: 1500 });
            });
        });

        describe('pool size', () => {
            it('default', async () => {
                const ds = FloraMysqlFactory.create();
                await ds.query('default', database, 'SELECT 1');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER', { connectionLimit: 10 });
            });

            it('server specific', async () => {
                const cfg = clone(defaultCfg);
                cfg.servers.default.poolSize = 100;

                const ds = FloraMysqlFactory.create(cfg);
                await ds.query('default', database, 'SELECT 1');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER', { connectionLimit: 100 });
            });

            it('global', async () => {
                const cfg = clone(defaultCfg);
                cfg.poolSize = 50;

                const ds = FloraMysqlFactory.create(cfg);
                await ds.query('default', database, 'SELECT 1');

                expect(poolSpy).to.have.been.calledWithMatch('MASTER', { connectionLimit: 50 });
            });
        });

        it('should always use "master" server', async () => {
            const ds = FloraMysqlFactory.create();
            const connectionSpy = sandbox.spy(PoolCluster.prototype, 'getConnection');

            await ds.query('default', database, 'SELECT 1');

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

            await ds.query('default', database, 'SELECT 1');

            const masterCfg = Object.assign({}, baseCfg, { host: 'mysql-master', port: 3306 });
            expect(poolSpy).to.have.been.calledWith('MASTER_mysql-master', sinon.match(masterCfg));

            const slaveCfg1 = Object.assign({}, baseCfg, { host: 'mysql-slave1', port: 1337 });
            expect(poolSpy).to.have.been.calledWith('SLAVE_mysql-slave1', sinon.match(slaveCfg1));

            const slaveCfg2 = Object.assign({}, baseCfg, { host: 'mysql-slave2', port: 4711 });
            expect(poolSpy).to.have.been.calledWith('SLAVE_mysql-slave2', sinon.match(slaveCfg2));
        });
    });
});
