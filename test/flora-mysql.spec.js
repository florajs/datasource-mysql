'use strict';

const _ = require('lodash');
const chai = require('chai');
const sinon = require('sinon');
const proxyquire = require('proxyquire');
const bunyan = require('bunyan');

const Connection = require('../lib/connection');
const FloraMysql = require('../index');

const expect = chai.expect;
chai.use(require('sinon-chai'));

const log = bunyan.createLogger({ name: 'null', streams: [] });

// mock Api instance
const api = {
    log: log
};

describe('flora-mysql DataSource', () => {
    let ds,
        serverCfg = {
            servers: {
                default: { host: 'db-server', user: 'joe', password: 'test' }
            }
        },
        astTpl = {
            _meta: { hasFilterPlaceholders: false },
            type: 'select',
            options: null,
            distinct: null,
            columns: [
                { expr: { type: 'column_ref', table: 't', column: 'col1' }, as: null },
                { expr: { type: 'column_ref', table: 't', column: 'col2' }, as: null }
            ],
            from: [{ db: null, table: 't', as: null }],
            where: null,
            groupby: null,
            having: null,
            orderby: null,
            limit: null
        };

    before(() => {
        sinon.stub(Connection.prototype, 'connect').yields(null, []); // don't connect to database
        sinon.stub(Connection.prototype, 'isConnected').returns(true);
    });

    after(() => {
        Connection.prototype.connect.restore();
        Connection.prototype.isConnected.restore();
    });

    describe('interface', () => {
        const ds = new FloraMysql(api, serverCfg);
        it('should export a query function', () => {
            expect(ds.process).to.be.a('function');
        });

        it('should export a prepare function', () => {
            expect(ds.prepare).to.be.a('function');
        });
    });

    describe('filter placeholder', () => {
        it('should add flag to indicate existence of placeholders in AST', () => {
            const dsConfig = { query: 'SELECT t.col1 FROM t WHERE 1 = 1 AND __floraFilterPlaceholder__' };

            (new FloraMysql(api, serverCfg)).prepare(dsConfig, []);

            expect(dsConfig.queryAST._meta).to.have.property('hasFilterPlaceholders', true);
        });

        it('should preserve hasFilterPlaceholders flag when AST is cloned', sinon.test((done) => {
            const dsConfig = { query: 'SELECT t.col1 FROM t WHERE 1 = 1 AND __floraFilterPlaceholder__' };
            const ds = new FloraMysql(api, serverCfg);
            const queryFn = sinon.stub(ds, 'query').yields(null, []);

            ds.prepare(dsConfig, ['col1']);

            ds.process({ attributes: ['col1'], queryAST: dsConfig.queryAST }, () => {
                expect(queryFn).to.have.been.called.once;
                expect(queryFn.firstCall.args[2]).to.not.contain('__floraFilterPlaceholder__');
                done();
            });
        }));
    });

    /*
    describe('connection params', () => {
        var ConnectionMock = sinon.spy(Connection),
            FloraMysql = proxyquire('../index', { './lib/connection': ConnectionMock });

        before(() => {
            sinon.stub(ConnectionMock.prototype, 'query').yields(null, []);
        });

        after(() => {
            ConnectionMock.prototype.query.restore();
        });

        it('should use host and default port', function (done) {
            var ds = new FloraMysql(api, serverCfg);

            ds.query('default', 'db', 'SELECT 1', () => {
                expect(ConnectionMock).to.have.been.calledWith({
                    host: 'db-server',
                    port: 3306,
                    db: 'db',
                    user: 'joe',
                    password: 'test'
                });
                done();
            });
        });

        it('should use custom port', function (done) {
            var ds = new FloraMysql(api, {
                servers: {
                    default: { host: 'db-server', port: 3307, user: 'joe', password: 'test' }
                }
            });

            ds.query('default', 'db', 'SELECT 1', () => {
                expect(ConnectionMock).to.have.been.calledWith({
                    host: 'db-server',
                    port: 3307,
                    db: 'db',
                    user: 'joe',
                    password: 'test'
                });
                done();
            });
        });

        it('should use socket', function (done) {
            var ds = new FloraMysql(api, {
                servers: {
                    default: { socket: '/path/to/socket.sock', user: 'joe', password: 'test' }
                }
            });

            ds.query('default', 'db', 'SELECT 1', () => {
                expect(ConnectionMock).to.have.been.calledWith({
                    unixSocket: '/path/to/socket.sock',
                    db: 'db',
                    user: 'joe',
                    password: 'test'
                });
                done();
            });
        });
    });
    */

    describe('generate AST DataSource config', () => {

        beforeEach(() => {
            ds = new FloraMysql(api, serverCfg);
        });

        it('should generate AST from SQL query', () => {
            const resourceConfig = { query: 'SELECT t.col1, t.col2 FROM t' };

            ds.prepare(resourceConfig, ['col1', 'col2']);

            expect(resourceConfig).to.have.property('queryAST');
            expect(resourceConfig.queryAST).to.eql(astTpl);
        });

        it('should prepare search attributes', () => {
            const resourceConfig = {
                searchable: 'col1,col2',
                query: 'SELECT t.col1, t.col2 FROM t'
            };

            ds.prepare(resourceConfig, ['col1', 'col2']);

            expect(resourceConfig.searchable)
                .to.be.instanceof(Array)
                .and.to.eql(['col1', 'col2']);
        });

        describe('error handling', () => {
            it('should append query on a parse error', () => {
                const sql = 'SELECT col1 FRO t';
                const resourceConfig = { query: sql };
                let exceptionThrown = false;

                try {
                    ds.prepare(resourceConfig, ['col1']);
                } catch (e) {
                    expect(e).to.have.property('query');
                    expect(e.query).to.equal(sql);
                    exceptionThrown = true;
                }

                expect(exceptionThrown).to.be.equal(true, 'Exception was not thrown');
            });

            it('should throw an error if an attribute is not available in SQL query', () => {
                const resourceConfig = { query: 'SELECT t.col1 FROM t' };

                expect(() => {
                    ds.prepare(resourceConfig, ['col1', 'col2']);
                }).to.throw(Error, 'Attribute "col2" is not provided by SQL query');
            });

            it('should throw an error if an attribute is not available as column alias', () => {
                const resourceConfig = { query: 'SELECT t.someWeirdColumnName AS col1 FROM t' };

                expect(() => {
                    ds.prepare(resourceConfig, ['col1', 'col2']);
                }).to.throw(Error, 'Attribute "col2" is not provided by SQL query');
            });

            it('should throw an error if columns are not fully qualified', () => {
                const resourceConfig = { query: 'SELECT t1.col1, attr AS col2 FROM t1 JOIN t2 ON t1.id = t2.id' };

                expect(() => {
                    ds.prepare(resourceConfig, ['col1', 'col2']);
                }).to.throw(Error, 'Column "attr" must be fully qualified');
            });

            it('should throw an error if columns are not unique', () => {
                const resourceConfig = { query: 'SELECT t.col1, someAttr AS col1 FROM t' };

                expect(() => {
                    ds.prepare(resourceConfig, ['col1', 'col2']);
                }).to.throw(Error);
            });
        });


        it('should generate AST from DataSource config if no SQL query is available', () => {
            const resourceConfig = { table: 't' };
            const attributes = ['col1', 'col2'];

            ds.prepare(resourceConfig, attributes);

            expect(resourceConfig).to.have.property('queryAST');
            expect(resourceConfig.queryAST).to.eql(astTpl);
        });
    });

    describe('flora request processing', () => {
        let ast;

        beforeEach(() => {
            ast = _.cloneDeep(astTpl);
            ds = new FloraMysql(api, serverCfg);
        });

        afterEach(() => {
            Connection.prototype.query.restore();
        });

        it('should generate SQL statement from flora request object', function (done) {
            const floraRequest = {
                database: 'db',
                attributes: ['col1'],
                queryAST: ast,
                filter: [
                    [{ attribute: 'col1', operator: 'equal', value: 'foo' }]
                ]
            };
            let sql = '';

            sinon.stub(Connection.prototype, 'query', function (query, callback) {
                sql = query;
                callback([]);
            });

            ds.process(floraRequest, () => {
                expect(sql).to.equal('SELECT "t"."col1" FROM "t" WHERE "t"."col1" = \'foo\'');
                done();
            });
        });

        it('should return query results in a callback', function (done) {
            const sampleRequest = { database: 'db', attributes: ['col1'], queryAST: ast };

            sinon.stub(Connection.prototype, 'query').yields(null, []);  // simulate empty result set
            ds.process(sampleRequest, function (err, result) {
                expect(err).to.eql(null);
                expect(result).to.eql({ totalCount: null, data: [] });
                done();
            });
        });

        describe('error handling', () => {
            it('should throw return an error if selected attribute has no corresponding column', function (done) {
                const floraRequest = {
                    attributes: ['id', 'nonexistentAttr'],
                    queryAST: _.assign({}, ast, {
                        columns: [
                            { expr: { type: 'column_ref', table: 't', column: 'id' }, as: '' }
                            // nonexistentAttribute is not defined as column
                        ]
                    })
                };

                sinon.stub(Connection.prototype, 'query').yields(null, []);

                ds.process(floraRequest, function (err) {
                    expect(err).to.be.instanceof(Error);
                    expect(err.message).to.equal('Attribute "nonexistentAttr" is not provided by SQL query');
                    done();
                });
            });

            it('should throw return an error if selected attribute has no corresponding alias', function (done) {
                const floraRequest = {
                    attributes: ['id', 'nonexistentAttr'],
                    queryAST: _.assign({}, ast, {
                        columns: [
                            { expr: { type: 'column_ref', table: 't', column: 'pk_id' }, as: 'id' }
                            // nonexistentAttribute is not defined as column
                        ]
                    })
                };

                sinon.stub(Connection.prototype, 'query').yields(null, []);

                ds.process(floraRequest, function (err) {
                    expect(err).to.be.instanceof(Error);
                    expect(err.message).to.equal('Attribute "nonexistentAttr" is not provided by SQL query');
                    done();
                });
            });
        });
    });

    describe('query timeout', () => {

/*        var queries = {
            insert: 'INSERT INTO foo (bar) SELECT \'bar\'',
            update: 'update foo set bar = \'bar\'',
            'delete': 'DELETE FROM foo WHERE bar = \'bar\''
        };*/

        beforeEach(() => {
            sinon.stub(Connection.prototype, 'query', function (sql, callback) {
                setTimeout(() => {
                    callback(null, []);
                }, 50);
            });
        });

        afterEach(() => {
            Connection.prototype.query.restore();
        });

        it('should abort long running SELECT queries', function (done) {
            const FloraMysql = proxyquire('../', { connection: Connection });
            const floraRequest = {
                database: 'db',
                attributes: ['col1'],
                queryAST: _.cloneDeep(astTpl)
            };
            let ds;
            let cfg = _.cloneDeep(serverCfg);

            cfg.servers.default.queryTimeout = 30;
            ds = new FloraMysql(api, cfg);

            ds.process(floraRequest, function (err) {
                expect(err).to.be.an.instanceof(Error);
                expect(err.message).to.contain('Query execution was interrupted');
                done();
            });
        });

/*        Object.keys(queries)
            .forEach(function (type) {
                it('should not abort long running ' + type.toUpperCase() + ' queries', function (done) {
                    var opts = _.merge({ server: { queryTimeout: 30 }}, serverCfg),
                        floraMysql = proxyquire('../', { connection: Connection }),
                        ds = floraMysql(opts);

                    ds.query('db', queries[type], function (err) {
                        expect(err).to.be.null();
                        done();
                    });
                });
            });*/
    });

    describe('transactions', () => {
        let queryFn;

        afterEach(() => {
            queryFn.restore();
        });

        it('should acquire a connection and start the transaction', function (done) {
            const ds = new FloraMysql(api, serverCfg);

            queryFn = sinon.stub(Connection.prototype, 'query');
            queryFn.onFirstCall().yields(null);

            ds.transaction('default', 'db', function (err, trx) {
                expect(err).to.be.null;
                expect(queryFn).to.be.calledOnce;
                expect(queryFn.firstCall.args[0]).to.equal('START TRANSACTION');
                done();
            });
        });

        it('should pass the query to the connection', function (done) {
            const ds = new FloraMysql(api, serverCfg);

            queryFn = sinon.stub(Connection.prototype, 'query');
            queryFn.onFirstCall().yields(null);
            queryFn.onSecondCall().yields(null);

            ds.transaction('default', 'db', function (err, trx) {
                expect(err).to.be.null;
                trx.query('SELECT id FROM user', function (commitErr) {
                    expect(queryFn).to.be.calledTwice;
                    expect(queryFn.secondCall.args[0]).to.equal('SELECT id FROM user');
                    done();
                });
            });
        });

        it('should send COMMIT on commit()', function (done) {
            const ds = new FloraMysql(api, serverCfg);

            queryFn = sinon.stub(Connection.prototype, 'query');
            queryFn.onFirstCall().yields(null);
            queryFn.onSecondCall().yields(null);

            ds.transaction('default', 'db', function (err, trx) {
                expect(err).to.be.null;
                trx.commit(function (commitErr) {
                    expect(queryFn).to.be.calledTwice;
                    expect(queryFn.secondCall.args[0]).to.equal('COMMIT');
                    done();
                });
            });
        });

        it('should send ROLLBACK on rollback()', function (done) {
            const ds = new FloraMysql(api, serverCfg);

            queryFn = sinon.stub(Connection.prototype, 'query');
            queryFn.onFirstCall().yields(null);
            queryFn.onSecondCall().yields(null);

            ds.transaction('default', 'db', function (err, trx) {
                expect(err).to.be.null;
                trx.rollback(function (rollbackErr) {
                    expect(queryFn).to.be.calledTwice;
                    expect(queryFn.secondCall.args[0]).to.equal('ROLLBACK');
                    done();
                });
            });
        });
    });

    describe('pagination', () => {
        const floraRequest = {
            database: 'db',
            attributes: ['col1', 'col2'],
            queryAST: _.cloneDeep(astTpl),
            limit: 15,
            page: 3
        };
        let queryFn;

        afterEach(() => {
            queryFn.restore();
        });

        it('should query available results if "page" attribute is set in request', function (done) {
            const ds = new FloraMysql(api, serverCfg);

            queryFn = sinon.stub(Connection.prototype, 'query');
            queryFn.onFirstCall().yields(null, [])
                .onSecondCall().yields(null, [{ totalCount: '5' }]);

            ds.process(floraRequest, function (err, result) {
                const firstQuery = queryFn.firstCall.args[0];
                const secondQuery = queryFn.secondCall.args[0];

                expect(queryFn).to.be.calledTwice;

                expect(firstQuery).to.match(/^SELECT\s+SQL_CALC_FOUND_ROWS\s/);
                expect(secondQuery).to.match(/^SELECT\s+FOUND_ROWS\(\)/);

                expect(result.totalCount).to.equal(5);
                done();
            });
        });
    });

    describe('connection pooling', () => {
        let request1;
        let request2;
        const emptyFn = () => {};
        const basicConfig = {
            servers: {
                default: { host: 'db-host1', user: 'joe', password: 'test' },
                server2: { host: 'db-host2', user: 'joe', password: 'test' }
            }
        };

        beforeEach(() => {
            sinon.stub(Connection.prototype, 'query').yields(null, []);
            request1 = { database: 'foo', attributes: ['col1'], queryAST: _.cloneDeep(astTpl) };
            request2 = { server: 'server2', database: 'bar', attributes: ['col2'], queryAST: _.cloneDeep(astTpl) };
        });

        afterEach(() => {
            Connection.prototype.query.restore();
        });

        it('should use "default" if no server is specified', function (done) {
            const ds = new FloraMysql(api, basicConfig);
            ds.process(request1, done);
        });

        it('should create separate pools per server and database', () => {
            const ds = new FloraMysql(api, basicConfig);

            ds.process(request1, emptyFn);
            ds.process(request2, emptyFn);

            expect(ds._pools).to.have.keys('default', 'server2');
            expect(ds._pools.default).to.have.keys('foo');
            expect(ds._pools.server2).to.have.keys('bar');
        });

        it('should set default pool size to 10', () => {
            const ds = new FloraMysql(api, basicConfig);
            ds.process(request1, emptyFn);
            expect(ds._pools.default.foo.getMaxPoolSize()).to.equal(10);
        });

        it('should make pool size configurable per server', () => {
            const cfg = _.cloneDeep(basicConfig),
                ds = new FloraMysql(api, cfg);

            cfg.servers.default.poolSize = 100;
            ds.process(request1, emptyFn);

            expect(ds._pools.default.foo.getMaxPoolSize()).to.equal(100);
        });

        it('should inherit pool size from datasource config', () => {
            let cfg = _.cloneDeep(basicConfig);

            cfg.poolSize = 15;
            cfg.servers.server2.poolSize = 100;

            let ds = new FloraMysql(api, cfg);
            ds.process(request1, emptyFn);
            ds.process(request2, emptyFn);

            expect(ds._pools.default.foo.getMaxPoolSize()).to.equal(15);
            expect(ds._pools.server2.bar.getMaxPoolSize()).to.equal(100);
        });
    });
});
