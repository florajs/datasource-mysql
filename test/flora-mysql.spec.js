'use strict';

const chai = require('chai');
const bunyan = require('bunyan');
const { expect } = chai;
const PoolConnection = require('../node_modules/mysql/lib/PoolConnection');
const sinon = require('sinon');

const FloraMysql = require('../index');
const Transaction = require('../lib/transaction');
const { ImplementationError } = require('flora-errors');

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

            it('should throw an error if search attribute is not available in AST', () => {
                const resourceConfig = {
                    searchable: 'col1,nonExistentAttr',
                    query: 'SELECT t.col1 FROM t'
                };

                expect(() => {
                    ds.prepare(resourceConfig, ['col1']);
                }).to.throw(ImplementationError, `Attribute "nonExistentAttr" is not available in AST`);
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

        it('should return a transaction', async () => {
            const trx = await ds.transaction('default', TEST_DB);

            expect(trx).to.be.instanceOf(Transaction);
            await trx.rollback();
        });

        it('should acquire a connection and start the transaction', async () => {
            const trx = await ds.transaction('default', TEST_DB);

            expect(queryFnSpy).to.have.been.calledWith('START TRANSACTION');
            await trx.rollback();
        });

        it('should send COMMIT on commit()', async () => {
            const trx = await ds.transaction('default', TEST_DB);

            await trx.commit();
            expect(queryFnSpy).to.have.been.calledWith('COMMIT');
        });

        it('should send ROLLBACK on rollback()', async () => {
            const trx = await ds.transaction('default', TEST_DB);

            await trx.rollback();
            expect(queryFnSpy).to.have.been.calledWith('ROLLBACK');
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

        it('should set sql_mode to ANSI if no init queries are defined', async () => {
            ds =  new FloraMysql(api, serverCfg);

            await ds.query('default', TEST_DB, 'SELECT 1');
            expect(querySpy).to.have.been.calledWith('SET SESSION sql_mode = \'ANSI\'');
        });

        it('should execute single init query', async () => {
            const initQuery = `SET SESSION sql_mode = 'ANSI_QUOTES'`;
            const config = Object.assign({}, serverCfg, { onConnect: initQuery });

            ds = new FloraMysql(api, config);
            await ds.query('default', TEST_DB, 'SELECT 1');
            expect(querySpy).to.have.been.calledWith(initQuery);
        });

        it('should execute multiple init queries', async () => {
            const initQuery1 = `SET SESSION sql_mode = 'ANSI_QUOTES'`;
            const initQuery2 = `SET SESSION max_execution_time = 1`;
            const config = Object.assign({}, serverCfg, { onConnect: [initQuery1, initQuery2] });

            ds = new FloraMysql(api, config);
            await ds.query('default', TEST_DB, 'SELECT 1');

            expect(querySpy)
                .to.have.been.calledWith(initQuery1)
                .and.to.have.been.calledWith(initQuery2);
        });

        it('should execute custom init function', async () => {
            const initQuery = `SET SESSION sql_mode = 'ANSI_QUOTES'`;
            const onConnect = sinon.spy((connection, done) => {
                connection.query(initQuery, err => done(err ? err : null));
            });
            const config = Object.assign({}, serverCfg, { onConnect });

            ds = new FloraMysql(api, config);
            await ds.query('default', TEST_DB, 'SELECT 1');

            expect(querySpy).to.have.been.calledWith(initQuery);
            expect(onConnect).to.have.been.calledWith(sinon.match.instanceOf(PoolConnection), sinon.match.func);
        });

        it('should handle server specific init queries', async () => {
            const globalInitQuery = `SET SESSION sql_mode = 'ANSI_QUOTES'`;
            const serverInitQuery = 'SET SESSION max_execution_time = 1';
            const config = Object.assign({}, serverCfg,
                { onConnect: globalInitQuery },
                { default: { onConnect: serverInitQuery }}
            );

            ds = new FloraMysql(api, config);
            await ds.query('default', TEST_DB, 'SELECT 1');

            expect(querySpy)
                .to.have.been.calledWith(globalInitQuery)
                .and.to.have.been.calledWith(serverInitQuery);
        });

        it('should handle errors', async () => {
            const config = Object.assign({}, serverCfg, { onConnect: 'SELECT nonExistentAttr FROM t' });

            ds = new FloraMysql(api, config);

            try {
                await ds.query('default', TEST_DB, 'SELECT 1');
                throw new Error('Expected promise to reject');
            } catch (err) {
                expect(err.code).to.equal('ER_BAD_FIELD_ERROR');
            }
        });
    });

    describe('query method', () => {
        it('should release pool connections manually', async () => {
            const releaseSpy = sinon.spy(PoolConnection.prototype, 'release');

            await ds.query('default', TEST_DB, 'SELECT 1');
            expect(releaseSpy).to.have.been.calledOnce;
            releaseSpy.restore();
        });
    });
});
