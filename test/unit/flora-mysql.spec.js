'use strict';

const { expect } = require('chai');
const { ImplementationError } = require('@florajs/errors');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');
const astTpl = require('../ast-tpl');

describe('mysql data source', () => {
    const ds = FloraMysqlFactory.create();

    describe('interface', () => {
        it('should export a query function', () => {
            expect(ds.process).to.be.a('function');
        });

        it('should export a prepare function', () => {
            expect(ds.prepare).to.be.a('function');
        });

        it('should export a getContext function', () => {
            expect(ds.getContext).to.be.a('function');
        });

        it('should export a foo function', () => {
            expect(ds.buildSqlAst).to.be.a('function');
        });
    });

    describe('generate AST data source config', () => {
        it('should generate AST from SQL query', () => {
            const resourceConfig = { database: 'test', query: 'SELECT t.id, t.col1, t.col2 FROM t' };

            ds.prepare(resourceConfig, ['id', 'col1', 'col2']);

            expect(resourceConfig).to.have.property('queryAstRaw').and.to.eql(astTpl);
        });

        it('should prepare search attributes', () => {
            const resourceConfig = {
                database: 'test',
                searchable: 'col1,col2',
                query: 'SELECT t.col1, t.col2 FROM t'
            };

            ds.prepare(resourceConfig, ['col1', 'col2']);

            expect(resourceConfig.searchable).to.be.instanceof(Array).and.to.eql(['col1', 'col2']);
        });

        describe('error handling', () => {
            it('should append query on a parse error', () => {
                const sql = 'SELECT col1 FRO t';
                const resourceConfig = { database: 'test', query: sql };
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

            it('should throw an error if database is not set', () => {
                const resourceConfig = { query: 'SELECT t.col1 FROM t' };

                expect(() => {
                    ds.prepare(resourceConfig, ['col1']);
                }).to.throw(ImplementationError, 'Database must be specified');
            });

            it('should throw an error if database is empty', () => {
                const resourceConfig = { database: '', query: 'SELECT t.col1 FROM t' };

                expect(() => {
                    ds.prepare(resourceConfig, ['col1']);
                }).to.throw(ImplementationError, 'Database must not be empty');
            });

            it('should throw an error if neither query nor table is set', () => {
                const resourceConfig = { database: 'test' };

                expect(() => {
                    ds.prepare(resourceConfig, ['col1']);
                }).to.throw(Error, 'Option "query" or "table" must be specified');
            });

            it('should throw an error if an attribute is not available in SQL query', () => {
                const resourceConfig = { database: 'test', query: 'SELECT t.col1 FROM t' };

                expect(() => {
                    ds.prepare(resourceConfig, ['col1', 'col2']);
                }).to.throw(Error, 'Attribute "col2" is not provided by SQL query');
            });

            it('should throw an error if an attribute is not available as column alias', () => {
                const resourceConfig = { database: 'test', query: 'SELECT t.someWeirdColumnName AS col1 FROM t' };

                expect(() => {
                    ds.prepare(resourceConfig, ['col1', 'col2']);
                }).to.throw(Error, 'Attribute "col2" is not provided by SQL query');
            });

            it('should throw an error if columns are not fully qualified', () => {
                const resourceConfig = {
                    database: 'test',
                    query: 'SELECT t1.col1, attr AS col2 FROM t1 JOIN t2 ON t1.id = t2.id'
                };

                expect(() => {
                    ds.prepare(resourceConfig, ['col1', 'col2']);
                }).to.throw(Error, 'Column "attr" must be fully qualified');
            });

            it('should throw an error if columns are not unique', () => {
                const resourceConfig = { database: 'test', query: 'SELECT t.col1, someAttr AS col1 FROM t' };

                expect(() => {
                    ds.prepare(resourceConfig, ['col1', 'col2']);
                }).to.throw(Error);
            });

            it('should throw an error if search attribute is not available in AST', () => {
                const resourceConfig = {
                    database: 'test',
                    searchable: 'col1,nonExistentAttr',
                    query: 'SELECT t.col1 FROM t'
                };

                expect(() => {
                    ds.prepare(resourceConfig, ['col1']);
                }).to.throw(ImplementationError, `Attribute "nonExistentAttr" is not available in AST`);
            });
        });

        it('should generate AST from data source config if no SQL query is available', () => {
            const resourceConfig = { database: 'test', table: 't' };
            const attributes = ['id', 'col1', 'col2'];

            ds.prepare(resourceConfig, attributes);

            expect(resourceConfig).to.have.property('queryAstRaw').and.to.eql(astTpl);
        });
    });
});
