'use strict';

const chai = require('chai');
const expect = chai.expect;
const sinon = require('sinon');
const bunyan = require('bunyan');

const TEST_DB = 'flora_mysql_testdb';
const FloraMysql = require('../../index');
const Transaction = require('../../lib/transaction');
const { Connection } = require('mysql2');

const log = bunyan.createLogger({ name: 'null', streams: [] });

// mock Api instance
const api = {
    log: log
};

chai.use(require('sinon-chai'));

describe('flora-mysql', () => {
    const serverCfg = {
        servers: {
            default: { host: 'mysql', user: 'root' }
        }
    };
    const ds = new FloraMysql(api, serverCfg);
    const astTpl = {
        type: 'select',
        options: null,
        distinct: null,
        columns: [
            { expr: { type: 'column_ref', table: 't', column: 'id' }, as: null },
            { expr: { type: 'column_ref', table: 't', column: 'col1' }, as: null }
        ],
        from: [{ db: TEST_DB, table: 't', as: null }],
        where: null,
        groupby: null,
        having: null,
        orderby: null,
        limit: null
    };

    describe('flora request processing', () => {
        it('should return query results in a callback', (done) => {
            const floraRequest = {
                attributes: ['col1'],
                queryAST: astTpl
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

    describe('error handling', () => {
        it('should return an error if selected attribute has no corresponding column', (done) => {
            const floraRequest = {
                attributes: ['id', 'nonexistentAttr'], // nonexistentAttribute is not defined as column
                queryAST: astTpl
            };

            ds.process(floraRequest, (err) => {
                expect(err).to.be.instanceof(Error);
                expect(err.message).to.equal('Attribute "nonexistentAttr" is not provided by SQL query');
                done();
            });
        });

        it('should return an error if selected attribute has no corresponding alias', (done) => {
            const floraRequest = {
                attributes: ['id', 'nonexistentAttr'],
                queryAST: astTpl
            };

            ds.process(floraRequest, (err) => {
                expect(err).to.be.instanceof(Error);
                expect(err.message).to.equal('Attribute "nonexistentAttr" is not provided by SQL query');
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
});
