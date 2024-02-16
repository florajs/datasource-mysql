'use strict';

const assert = require('node:assert/strict');

const astTpl = require('../ast-tpl');
const { FloraMysqlFactory } = require('../FloraMysqlFactory');
const ciCfg = require('./ci-config');

describe('error handling', () => {
    const ds = FloraMysqlFactory.create(ciCfg);
    const database = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';

    after(() => ds.close());

    it('should return an error if selected attribute has no corresponding column', async () => {
        const floraRequest = {
            attributes: ['col1', 'nonexistentAttr'], // nonexistentAttribute is not defined as column
            queryAstRaw: astTpl,
            database
        };

        assert.rejects(async () => await ds.process(floraRequest), {
            name: 'Error',
            message: 'Attribute "nonexistentAttr" is not provided by SQL query'
        });
    });

    it('should return an error if selected attribute has no corresponding alias', async () => {
        const floraRequest = {
            attributes: ['col1', 'nonexistentAttr'],
            queryAstRaw: astTpl,
            database
        };

        assert.rejects(async () => await ds.process(floraRequest), {
            name: 'Error',
            message: 'Attribute "nonexistentAttr" is not provided by SQL query'
        });
    });

    it('should log query in case of an error', async () => {
        const _explain = {};
        const floraRequest = {
            attributes: ['col1'],
            queryAstRaw: { ...astTpl, ...{ from: [{ db: null, table: 'nonexistent_table', as: null }] } },
            database,
            _explain
        };

        assert.rejects(
            async () => await ds.process(floraRequest),
            (err) => {
                assert.ok(err instanceof Error);

                assert.ok(Object.hasOwn(err, 'sql'));
                assert.equal(err.sql, 'SELECT "t"."col1" FROM "nonexistent_table"');

                assert.ok(Object.hasOwn(err, 'host'));

                return true;
            }
        );
    });

    it('should log connection errors', async () => {
        const _explain = {};
        const floraRequest = {
            attributes: ['col1'],
            queryAstRaw: { ...astTpl, ...{ from: [{ db: null, table: 'nonexistent_table', as: null }] } },
            database: 'nonexistent_database',
            _explain
        };

        assert.rejects(async () => await ds.process(floraRequest), {
            name: 'Error',
            message: /Unknown database 'nonexistent_database'/
        });
    });
});
