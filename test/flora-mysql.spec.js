'use strict';

var _           = require('lodash'),
    chai        = require('chai'),
    expect      = chai.expect,
    sinon       = require('sinon'),
    Connection  = require('../lib/connection'),
    FloraMysql  = require('../index'),
    proxyquire  = require('proxyquire'),
    bunyan      = require('bunyan');

chai.use(require('sinon-chai'));

var log = bunyan.createLogger({name: 'null', streams: []});

// mock Api instance
var api = {
    log: log
};

describe('flora-mysql DataSource', function () {
    var ds,
        serverCfg = {
            server: {
                host: 'db-server',
                user: 'joe',
                password: 'test'
            }
        },
        astTpl = {
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
            orderby: null,
            limit: null
        };

    before(function () {
        sinon.stub(Connection.prototype, 'connect').yields(); // don't connect to database
        sinon.stub(Connection.prototype, 'isConnected').returns(true);
    });

    after(function () {
        Connection.prototype.connect.restore();
        Connection.prototype.isConnected.restore();
    });

    describe('interface', function () {
        var ds = new FloraMysql(api, serverCfg);
        it('should export a query function', function () {
            expect(ds.process).to.be.a('function');
        });

        it('should export a prepare function', function () {
            expect(ds.prepare).to.be.a('function');
        });
    });

    describe('generate AST DataSource config', function () {

        beforeEach(function () {
            ds = new FloraMysql(api, serverCfg);
        });

        it('should generate AST from SQL query', function () {
            var resourceConfig = { query: 'SELECT t.col1, t.col2 FROM t' };

            ds.prepare(resourceConfig, ['col1', 'col2']);

            expect(resourceConfig).to.have.property('queryAST');
            expect(resourceConfig.queryAST).to.eql(_.cloneDeep(astTpl));
        });

        it('should prepare search attributes', function () {
            var resourceConfig = {
                searchable: 'col1,col2',
                query: 'SELECT t.col1, t.col2 FROM t'
            };

            ds.prepare(resourceConfig, ['col1', 'col2']);

            expect(resourceConfig.searchable)
                .to.be.instanceof(Array)
                .and.to.eql(['col1', 'col2']);
        });

        describe('error handling', function () {
            it('should append query on a parse error', function () {
                var sql = 'SELECT col1 FRO t',
                    resourceConfig = { query: sql },
                    exceptionThrown = false;

                try {
                    ds.prepare(resourceConfig, ['col1']);
                } catch (e) {
                    expect(e).to.have.property('query');
                    expect(e.query).to.equal(sql);
                    exceptionThrown = true;
                }

                expect(exceptionThrown).to.be.equal(true, 'Exception was not thrown');
            });

            it('should throw an error if an attribute is not available in SQL query', function () {
                var resourceConfig = { query: 'SELECT t.col1 FROM t' };

                expect(function () {
                    ds.prepare(resourceConfig, ['col1', 'col2']);
                }).to.throw(Error, 'Attribute "col2" is not provided by SQL query');
            });

            it('should throw an error if an attribute is not available as column alias', function () {
                var resourceConfig = { query: 'SELECT t.someWeirdColumnName AS col1 FROM t' };

                expect(function () {
                    ds.prepare(resourceConfig, ['col1', 'col2']);
                }).to.throw(Error, 'Attribute "col2" is not provided by SQL query');
            });

            it('should throw an error if columns are not fully qualified', function () {
                var resourceConfig = {
                    query: 'SELECT t1.col1, attr AS col2 FROM t1 JOIN t2 ON t1.id = t2.id'
                };

                expect(function () {
                    ds.prepare(resourceConfig, ['col1', 'col2']);
                }).to.throw(Error, 'Column "attr" must be fully qualified');
            });

            it('should throw an error if columns are not unique', function () {
                var resourceConfig = {
                    query: 'SELECT t.col1, someAttr AS col1 FROM t'
                };

                expect(function () {
                    ds.prepare(resourceConfig, ['col1', 'col2']);
                }).to.throw(Error);
            });
        });


        it('should generate AST from DataSource config if no SQL query is available', function () {
            var resourceConfig = { table: 't' },
                attributes = ['col1', 'col2'];

            ds.prepare(resourceConfig, attributes);

            expect(resourceConfig).to.have.property('queryAST');
            expect(resourceConfig.queryAST).to.eql(_.cloneDeep(astTpl));
        });
    });

    describe('flora request processing', function () {
        var ast;

        beforeEach(function () {
            ast = _.cloneDeep(astTpl);
            ds = new FloraMysql(api, serverCfg);
        });

        afterEach(function () {
            Connection.prototype.query.restore();
        });

        it('should generate SQL statement from flora request object', function (done) {
            var floraRequest = {
                    attributes: ['col1'],
                    queryAST: ast,
                    filter: [
                        [{ attribute: 'col1', operator: 'equal', value: 'foo' }]
                    ]
                },
                sql = '';

            sinon.stub(Connection.prototype, 'query', function (query, callback) {
                sql = query;
                callback([]);
            });

            ds.process(floraRequest, function () {
                expect(sql).to.equal('SELECT t.col1 FROM t WHERE t.col1 = \'foo\'');
                done();
            });
        });

        it('should return query results in a callback', function (done) {
            var sampleRequest = {
                    attributes: ['col1'],
                    queryAST: ast
                };

            sinon.stub(Connection.prototype, 'query').yields(null, []);  // simulate empty result set
            ds.process(sampleRequest, function (err, result) {
                expect(err).to.eql(null);
                expect(result).to.eql({ totalCount: null, data: [] });
                done();
            });
        });

        describe('error handling', function () {
            it('should throw return an error if selected attribute has no corresponding column', function (done) {
                var floraRequest = {
                        attributes: ['id', 'nonexistentAttr'],
                        queryAST: _.assign({}, astTpl, {
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
                var floraRequest = {
                        attributes: ['id', 'nonexistentAttr'],
                        queryAST: _.assign({}, astTpl, {
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

    describe('query timeout', function () {

/*        var queries = {
            insert: 'INSERT INTO foo (bar) SELECT \'bar\'',
            update: 'update foo set bar = \'bar\'',
            'delete': 'DELETE FROM foo WHERE bar = \'bar\''
        };*/

        beforeEach(function () {
            sinon.stub(Connection.prototype, 'query', function (sql, callback) {
                setTimeout(function () {
                    callback(null, []);
                }, 50);
            });
        });

        afterEach(function () {
            Connection.prototype.query.restore();
        });

        it('should abort long running SELECT queries', function (done) {
            var opts = _.merge({ server: { queryTimeout: 30 }}, serverCfg),
                FloraMysql = proxyquire('../', { connection: Connection }),
                ds = new FloraMysql(api, opts),
                floraRequest = {
                    attributes: ['col1'],
                    queryAST: _.cloneDeep(astTpl)
                };

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

    describe('pagination', function () {
        var floraRequest = {
                attributes: ['col1', 'col2'],
                queryAST: _.cloneDeep(astTpl),
                limit: 15,
                page: 3
            },
            queryFn;

        afterEach(function () {
            queryFn.restore();
        });

        it('should query available results if "page" attribute is set in request', function (done) {
            var ds = new FloraMysql(api, serverCfg);

            queryFn = sinon.stub(Connection.prototype, 'query');
            queryFn.onFirstCall().yields(null, [])
                .onSecondCall().yields(null, [{ totalCount: '5' }]);

            ds.process(floraRequest, function (err, result) {
                var firstQuery = queryFn.firstCall.args[0],
                    secondQuery = queryFn.secondCall.args[0];

                expect(queryFn).to.be.calledTwice;

                expect(firstQuery).to.match(/^SELECT\s+SQL_CALC_FOUND_ROWS\s/);
                expect(secondQuery).to.match(/^SELECT\s+FOUND_ROWS\(\)/);

                expect(result.totalCount).to.equal(5);
                done();
            });
        });
    });
});
