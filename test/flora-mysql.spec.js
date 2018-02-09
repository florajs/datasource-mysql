'use strict';

const chai = require('chai');
const bunyan = require('bunyan');
const { expect } = chai;
const PoolConnection = require('../node_modules/mysql/lib/PoolConnection');
const sinon = require('sinon');

const FloraMysql = require('../index');
const Transaction = require('../lib/transaction');

const TEST_DB = 'flora_mysql_testdb';
const log = bunyan.createLogger({ name: 'null', streams: [] });

// mock Api instance
const api = { log };

chai.use(require('sinon-chai'));

describe('flora-mysql DataSource', () => {
    const serverCfg = {
        servers: {
            default: { host: 'mysql', user: 'root', password: '' }
        }
    };
    const ds = new FloraMysql(api, serverCfg);
    const astTpl = {
        _meta:  { hasFilterPlaceholders: false },
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

    after(done => ds.close(done));

    describe('interface', () => {
        it('should export a query function', () => {
            expect(ds.process).to.be.a('function');
        });

        it('should export a prepare function', () => {
            expect(ds.prepare).to.be.a('function');
        });
    });

    describe('generate AST DataSource config', () => {
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

            it('should throw an error if neither query nor table is set', () => {
                const resourceConfig = {};

                expect(() => {
                    ds.prepare(resourceConfig, ['col1']);
                }).to.throw(Error, 'Option "query" or "table" must be specified');
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
        it('should return query results in a callback', (done) => {
            const floraRequest = {
                attributes: ['col1'],
                queryAST: astTpl,
                database: TEST_DB
            };

            ds.process(floraRequest, (err, result) => {
                expect(err).to.be.null;
                expect(result)
                    .to.have.property('totalCount')
                    .and.to.be.null;
                expect(result)
                    .to.have.property('data')
                    .and.to.be.an('array')
                    .and.not.to.be.empty;
                done();
            });
        });

        it('should query available results if "page" attribute is set in request', (done) => {
            const floraRequest = {
                database: TEST_DB,
                attributes: ['col1'],
                queryAST: astTpl,
                limit: 1,
                page: 2
            };

            ds.process(floraRequest, (err, result) => {
                expect(err).to.be.null;
                expect(result).to.have.property('totalCount').and.to.be.at.least(1);
                done();
            });
        });
    });

    describe('transaction', () => {
        let queryFnSpy;

        beforeEach(() => {
            queryFnSpy = sinon.spy(PoolConnection.prototype, 'query');
        });

        afterEach(() => {
            queryFnSpy.restore()
        });

        it('should acquire a connection and start the transaction', (done) => {
            ds.transaction('default', TEST_DB, (err, trx) => {
                expect(err).to.be.null;
                expect(trx).to.be.an.instanceOf(Transaction);
                expect(queryFnSpy).to.have.been.calledWith('START TRANSACTION');
                trx.rollback(done);
            });
        });

        it('should pass the query to the connection', (done) => {
            ds.transaction('default', TEST_DB, (err, trx) => {
                expect(err).to.be.null;
                trx.query('SELECT * FROM t LIMIT 1', (queryErr) => {
                    expect(queryErr).to.be.null;
                    trx.rollback(done);
                });
            });
        });

        it('should send COMMIT on commit()', (done) => {
            ds.transaction('default', TEST_DB, (err, trx) => {
                expect(err).to.be.null;
                trx.commit((commitErr) => {
                    expect(commitErr).to.be.null;
                    expect(queryFnSpy).to.have.been.calledWith('COMMIT');
                    done();
                });
            });
        });

        it('should send ROLLBACK on rollback()', (done) => {
            ds.transaction('default', TEST_DB, (err, trx) => {
                expect(err).to.be.null;
                trx.rollback((rollbackErr) => {
                    expect(rollbackErr).to.be.null;
                    expect(queryFnSpy).to.have.been.calledWith('ROLLBACK');
                    done();
                });
            });
        });
    });

    describe('error handling', () => {
        it('should return an error if selected attribute has no corresponding column', (done) => {
            const floraRequest = {
                attributes: ['col1', 'nonexistentAttr'], // nonexistentAttribute is not defined as column
                queryAST: astTpl,
                database: TEST_DB
            };

            ds.process(floraRequest, (err) => {
                expect(err).to.be.instanceof(Error);
                expect(err.message).to.equal('Attribute "nonexistentAttr" is not provided by SQL query');
                done();
            });
        });

        it('should return an error if selected attribute has no corresponding alias', (done) => {
            const floraRequest = {
                attributes: ['col1', 'nonexistentAttr'],
                queryAST: astTpl,
                database: TEST_DB
            };

            ds.process(floraRequest, (err) => {
                expect(err).to.be.instanceof(Error);
                expect(err.message).to.equal('Attribute "nonexistentAttr" is not provided by SQL query');
                done();
            });
        });
    });

    describe('init queries', () => {
        let ds;
        let querySpy;

        beforeEach(() => querySpy = sinon.spy(PoolConnection.prototype, 'query'));

        afterEach((done) => {
            querySpy.restore();
            ds.close(done)
        });

        it('should set sql_mode to ANSI if no init queries are defined', (done) => {
            ds =  new FloraMysql(api, serverCfg);
            ds.query('default', TEST_DB, 'SELECT 1', (err) => {
                expect(err).to.be.null;
                expect(querySpy).to.have.been.calledWith('SET SESSION sql_mode = \'ANSI\'');
                done();
            });
        });

        it('should execute single init query', (done) => {
            const initQuery = `SET SESSION sql_mode = 'ANSI_QUOTES'`;
            const config = Object.assign({}, serverCfg, { onConnect: initQuery });

            ds = new FloraMysql(api, config);
            ds.query('default', TEST_DB, 'SELECT 1', (err) => {
                expect(err).to.be.null;
                expect(querySpy).to.have.been.calledWith(initQuery);
                done();
            });
        });

        it('should execute multiple init queries', (done) => {
            const initQuery1 = `SET SESSION sql_mode = 'ANSI_QUOTES'`;
            const initQuery2 = `SET SESSION max_execution_time = 1`;
            const config = Object.assign({}, serverCfg, { onConnect: [initQuery1, initQuery2] });

            ds = new FloraMysql(api, config);
            ds.query('default', TEST_DB, 'SELECT 1', (err) => {
                expect(err).to.be.null;
                expect(querySpy)
                    .to.have.been.calledWith(initQuery1)
                    .and.to.have.been.calledWith(initQuery2);
                done();
            });
        });

        it('should execute custom init function', (done) => {
            const initQuery = `SET SESSION sql_mode = 'ANSI_QUOTES'`;
            const onConnect = sinon.spy((connection, done) => {
                connection.query(initQuery, err => done(err ? err : null));
            });
            const config = Object.assign({}, serverCfg, { onConnect });

            ds = new FloraMysql(api, config);
            ds.query('default', TEST_DB, 'SELECT 1', (err) => {
                expect(err).to.be.null;
                expect(querySpy).to.have.been.calledWith(initQuery);
                expect(onConnect).to.have.been.calledWith(sinon.match.instanceOf(PoolConnection), sinon.match.func);
                done();
            });
        });

        it('should handle server specific init queries', (done) => {
            const globalInitQuery = `SET SESSION sql_mode = 'ANSI_QUOTES'`;
            const serverInitQuery = 'SET SESSION max_execution_time = 1';
            const config = Object.assign({}, serverCfg,
                { onConnect: globalInitQuery },
                { default: { onConnect: serverInitQuery }}
            );

            ds = new FloraMysql(api, config);
            ds.query('default', TEST_DB, 'SELECT 1', (err) => {
                expect(err).to.be.null;
                expect(querySpy)
                    .to.have.been.calledWith(globalInitQuery)
                    .and.to.have.been.calledWith(serverInitQuery);
                done();
            });
        });

        it('should handle errors', (done) => {
            const config = Object.assign({}, serverCfg, { onConnect: 'SELECT nonExistentAttr FROM t' });

            ds = new FloraMysql(api, config);
            ds.query('default', TEST_DB, 'SELECT 1', (err) => {
                expect(err).to.be.an('Error');
                expect(err.code).to.equal('ER_BAD_FIELD_ERROR');
                done();
            });
        });
    });

    describe('query method', () => {
        it('should release pool connections manually', (done) => {
            const releaseSpy = sinon.spy(PoolConnection.prototype, 'release');

            ds.query('default', TEST_DB, 'SELECT 1', (err) => {
                expect(err).to.be.null;
                expect(releaseSpy).to.have.been.calledOnce;
                releaseSpy.restore();
                done();
            });
        });
    });
});
