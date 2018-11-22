'use strict';

const chai = require('chai');
const { expect } = chai;
const sinon = require('sinon');

const astTpl = require('../ast-tpl');
const { FloraMysqlFactory } = require('../FloraMysqlFactory');

chai.use(require('sinon-chai'));

describe('flora request processing', () => {
    const ds = FloraMysqlFactory.create();
    const database = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';

    after(() => ds.close());

    it('should return query results in a callback', () => {
        const floraRequest = {
            attributes: ['col1'],
            queryAST: astTpl,
            database
        };

        return ds.process(floraRequest)
            .then((result) => {
                expect(result)
                    .to.have.property('totalCount')
                    .and.to.be.null;
                expect(result)
                    .to.have.property('data')
                    .and.to.be.an('array')
                    .and.not.to.be.empty;
        });
    });

    it('should query available results if "page" attribute is set in request', () => {
        const floraRequest = {
            database,
            attributes: ['col1'],
            queryAST: astTpl,
            limit: 1,
            page: 2
        };

        return ds.process(floraRequest)
            .then((result) => {
                expect(result)
                    .to.have.property('totalCount')
                    .and.to.be.at.least(1);
            });
    });

    it('should respect useMaster', () => {
        const querySpy = sinon.spy(ds, '_query');
        const floraRequest = {
            database,
            useMaster: true,
            attributes: ['col1'],
            queryAST: astTpl,
            limit: 1,
            page: 2
        };

        return ds.process(floraRequest).then((result) => {
            expect(querySpy).to.have.been.calledWithMatch({ type: 'MASTER' });
            querySpy.restore();
        });
    });
});
