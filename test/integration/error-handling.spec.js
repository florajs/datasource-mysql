/* global after, describe, it */

'use strict';

const { expect } = require('chai');

const astTpl = require('../ast-tpl');
const { FloraMysqlFactory } = require('../FloraMysqlFactory');

describe('error handling', () => {
    const ds = FloraMysqlFactory.create();
    const database = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';

    after(() => ds.close());

    it('should return an error if selected attribute has no corresponding column', async () => {
        const floraRequest = {
            attributes: ['col1', 'nonexistentAttr'], // nonexistentAttribute is not defined as column
            queryAstRaw: astTpl,
            database
        };

        try {
            await ds.process(floraRequest);
        } catch (e) {
            expect(e).to.be.instanceof(Error);
            expect(e.message).to.equal('Attribute "nonexistentAttr" is not provided by SQL query');
            return;
        }

        throw new Error('Expected an error');
    });

    it('should return an error if selected attribute has no corresponding alias', async () => {
        const floraRequest = {
            attributes: ['col1', 'nonexistentAttr'],
            queryAstRaw: astTpl,
            database
        };

        try {
            await ds.process(floraRequest);
        } catch (e) {
            expect(e).to.be.instanceof(Error);
            expect(e.message).to.equal('Attribute "nonexistentAttr" is not provided by SQL query');
            return;
        }

        throw new Error('Expected an error');
    });

    it('should log query in case of an error', async () => {
        const _explain = {};
        const floraRequest = {
            attributes: ['col1'],
            queryAstRaw: { ...astTpl, ...{ from: [{ db: null, table: 'nonexistent_table', as: null }] } },
            database,
            _explain
        };

        try {
            await ds.process(floraRequest);
        } catch (e) {
            expect(e).to.be.an.instanceOf(Error);
            expect(_explain).to.have.property('sql', 'SELECT "t"."col1" FROM "nonexistent_table"');
            expect(_explain).to.have.property('host');
            return;
        }

        throw new Error('Expected an error');
    });

    it('should log connection errors', async () => {
        const _explain = {};
        const floraRequest = {
            attributes: ['col1'],
            queryAstRaw: { ...astTpl, ...{ from: [{ db: null, table: 'nonexistent_table', as: null }] } },
            database: 'nonexistent_database',
            _explain
        };

        try {
            await ds.process(floraRequest);
        } catch (e) {
            expect(e)
                .to.be.an.instanceOf(Error)
                .and.to.have.property('message')
                .to.include("Unknown database 'nonexistent_database'");
            return;
        }

        throw new Error('Expected an error');
    });
});
