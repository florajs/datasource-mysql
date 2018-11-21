'use strict';

const { expect } = require('chai');

const astTpl = require('../ast-tpl');
const { FloraMysqlFactory } = require('../FloraMysqlFactory');

describe('error handling', () => {
    const ds = FloraMysqlFactory.create();
    const database = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';

    after(done => ds.close(done));

    it('should return an error if selected attribute has no corresponding column', done => {
        const floraRequest = {
            attributes: ['col1', 'nonexistentAttr'], // nonexistentAttribute is not defined as column
            queryAST: astTpl,
            database
        };

        ds.process(floraRequest, (err) => {
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

        ds.process(floraRequest, (err) => {
            expect(err).to.be.instanceof(Error);
            expect(err.message).to.equal('Attribute "nonexistentAttr" is not provided by SQL query');
            done();
        });
    });
});
