'use strict';

const { expect } = require('chai');

const astTpl = require('../ast-tpl');
const { FloraMysqlFactory } = require('../FloraMysqlFactory');

describe('error handling', () => {
    const ds = FloraMysqlFactory.create();
    const database = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';

    after(() => ds.close());

    it('should return an error if selected attribute has no corresponding column', done => {
        const floraRequest = {
            attributes: ['col1', 'nonexistentAttr'], // nonexistentAttribute is not defined as column
            queryAST: astTpl,
            database
        };

        ds.process(floraRequest)
            .catch((err) => {
                expect(err).to.be.instanceof(Error);
                expect(err.message).to.equal('Attribute "nonexistentAttr" is not provided by SQL query');
                done();
            });
    });

    it('should return an error if selected attribute has no corresponding alias', done => {
        const floraRequest = {
            attributes: ['col1', 'nonexistentAttr'],
            queryAST: astTpl,
            database
        };

        ds.process(floraRequest)
            .catch((err) => {
                expect(err).to.be.instanceof(Error);
                expect(err.message).to.equal('Attribute "nonexistentAttr" is not provided by SQL query');
                done();
            });
    });

    it('should log query in case of an error', async () => {
        const _explain = {};
        const floraRequest = {
            attributes: ['col1'],
            queryAST: Object.assign({}, astTpl, { from: [{ db: null, table: 'nonexistent_table', as: null }] }),
            database,
            _explain
        };
        let err;

        try {
            await ds.process(floraRequest);
        } catch (e) {
            err = e;
        }

        expect(err).to.be.an.instanceOf(Error);
        expect(_explain).to.have.property('sql', 'SELECT "t"."col1" FROM "nonexistent_table"');
        expect(_explain).to.have.property('host');
    });
});
