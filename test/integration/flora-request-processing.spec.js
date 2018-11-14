'use strict';

const { expect } = require('chai');

const astTpl = require('../ast-tpl');
const { FloraMysqlFactory } = require('../FloraMysqlFactory');

describe('flora request processing', () => {
    const ds = FloraMysqlFactory.create({
        servers: {
            default: {
                host: 'mysql',
                user: 'root',
                password: ''
            }
        }
    });
    const database = 'flora_mysql_testdb';

    after(done => ds.close(done));

    it('should return query results in a callback', done => {
        const floraRequest = {
            attributes: ['col1'],
            queryAST: astTpl,
            database
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

    it('should query available results if "page" attribute is set in request', done => {
        const floraRequest = {
            database,
            attributes: ['col1'],
            queryAST: astTpl,
            limit: 1,
            page: 2
        };

        ds.process(floraRequest, (err, result) => {
            expect(err).to.be.null;
            expect(result)
                .to.have.property('totalCount')
                .and.to.be.at.least(1);
            done();
        });
    });
});
