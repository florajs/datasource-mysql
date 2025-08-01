'use strict';

const assert = require('node:assert/strict');
const { describe, it } = require('node:test');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');
const astTpl = require('../ast-tpl');

describe('mysql data source', () => {
    const ds = FloraMysqlFactory.create();

    describe('interface', () => {
        it('should export a query function', () => {
            assert.ok(typeof ds.process === 'function');
        });

        it('should export a prepare function', () => {
            assert.ok(typeof ds.prepare === 'function');
        });

        it('should export a getContext function', () => {
            assert.ok(typeof ds.getContext === 'function');
        });

        it('should export a buildSqlAst function', () => {
            assert.ok(typeof ds.buildSqlAst === 'function');
        });
    });

    describe('generate AST data source config', () => {
        it('should generate AST from SQL query', () => {
            const resourceConfig = {
                database: 'test',
                query: 'SELECT flora_request_processing.id, flora_request_processing.col1, flora_request_processing.dataCol FROM flora_request_processing'
            };

            ds.prepare(resourceConfig, ['id', 'col1', 'dataCol']);

            assert.ok(Object.hasOwn(resourceConfig, 'queryAstRaw'));
            assert.deepEqual(resourceConfig.queryAstRaw, astTpl);
        });

        it('should prepare search attributes', () => {
            const resourceConfig = {
                database: 'test',
                searchable: 'col1,dataCol',
                query: 'SELECT t.col1, t.dataCol FROM t'
            };

            ds.prepare(resourceConfig, ['col1', 'dataCol']);

            assert.ok(Array.isArray(resourceConfig.searchable));
            assert.deepEqual(resourceConfig.searchable, ['col1', 'dataCol']);
        });

        describe('error handling', () => {
            it('should append query on a parse error', () => {
                const sql = 'SELECT col1 FRO t';
                const resourceConfig = { database: 'test', query: sql };

                assert.throws(
                    () => ds.prepare(resourceConfig, ['col1']),
                    (err) => {
                        assert.ok(Object.hasOwn(err, 'query'));
                        assert.equal(err.query, sql);
                        return true;
                    }
                );
            });

            it('should throw an error if database is not set', () => {
                const resourceConfig = { query: 'SELECT t.col1 FROM t' };

                assert.throws(() => ds.prepare(resourceConfig, ['col1']), {
                    name: 'ImplementationError',
                    message: 'Database must be specified'
                });
            });

            it('should throw an error if database is empty', () => {
                const resourceConfig = { database: '', query: 'SELECT t.col1 FROM t' };

                assert.throws(() => ds.prepare(resourceConfig, ['col1']), {
                    name: 'ImplementationError',
                    message: 'Database must not be empty'
                });
            });

            it('should throw an error if neither query nor table is set', () => {
                const resourceConfig = { database: 'test' };

                assert.throws(() => ds.prepare(resourceConfig, ['col1']), {
                    name: 'ImplementationError',
                    message: 'Option "query" or "table" must be specified'
                });
            });

            it('should throw an error if an attribute is not available in SQL query', () => {
                const resourceConfig = { database: 'test', query: 'SELECT t.col1 FROM t' };

                assert.throws(() => ds.prepare(resourceConfig, ['col1', 'dataCol']), {
                    name: 'ImplementationError',
                    message: 'Attribute "dataCol" is not provided by SQL query'
                });
            });

            it('should throw an error if an attribute is not available as column alias', () => {
                const resourceConfig = { database: 'test', query: 'SELECT t.someWeirdColumnName AS col1 FROM t' };

                assert.throws(() => ds.prepare(resourceConfig, ['col1', 'dataCol']), {
                    name: 'ImplementationError',
                    message: 'Attribute "dataCol" is not provided by SQL query'
                });
            });

            it('should throw an error if columns are not fully qualified', () => {
                const resourceConfig = {
                    database: 'test',
                    query: 'SELECT t1.col1, attr AS dataCol FROM t1 JOIN t2 ON t1.id = t2.id'
                };

                assert.throws(() => ds.prepare(resourceConfig, ['col1', 'dataCol']), {
                    name: 'ImplementationError',
                    message: 'Column "attr" must be fully qualified'
                });
            });

            it('should throw an error if columns are not unique', () => {
                const resourceConfig = { database: 'test', query: 'SELECT t.col1, someAttr AS col1 FROM t' };

                assert.throws(() => ds.prepare(resourceConfig, ['col1', 'dataCol']), { name: 'ImplementationError' });
            });

            it('should throw an error if search attribute is not available in AST', () => {
                const resourceConfig = {
                    database: 'test',
                    searchable: 'col1,nonExistentAttr',
                    query: 'SELECT t.col1 FROM t'
                };

                assert.throws(() => ds.prepare(resourceConfig, ['col1']), {
                    name: 'ImplementationError',
                    message: `Attribute "nonExistentAttr" is not available in AST`
                });
            });
        });

        it('should generate AST from data source config if no SQL query is available', () => {
            const resourceConfig = { database: 'test', table: 'flora_request_processing' };
            const attributes = ['id', 'col1', 'dataCol'];

            ds.prepare(resourceConfig, attributes);

            assert.ok(Object.hasOwn(resourceConfig, 'queryAstRaw'));
            assert.deepEqual(resourceConfig.queryAstRaw, astTpl);
        });
    });
});
