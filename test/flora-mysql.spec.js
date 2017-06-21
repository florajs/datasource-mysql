'use strict';

const chai = require('chai');
const bunyan = require('bunyan');
const { expect } = chai;
const Connection = require('../node_modules/mysql/lib/Connection');
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
            queryFnSpy = sinon.spy(Connection.prototype, 'query');
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
});
