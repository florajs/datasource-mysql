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

    it('should handle flora requests', async () => {
        const result = await ds.process({
            attributes: ['id', 'col1'],
            queryAST: astTpl,
            database
        });

        expect(result)
            .to.have.property('totalCount')
            .and.to.be.null;

        expect(result)
            .to.have.property('data')
            .and.to.be.an('array')
            .and.not.to.be.empty;
    });

    it('should return result w/o type casting', async () => {
        const { data } = await ds.process({
            attributes: ['id'],
            queryAST: astTpl,
            database
        });
        const [item] = data;

        expect(item)
            .to.be.an('object')
            .and.to.have.property('id')
            .and.to.eql(Buffer.from('1'));
    });

    it('should query available results if "page" attribute is set in request', async () => {
        const result = await ds.process({
            database,
            attributes: ['col1'],
            queryAST: astTpl,
            limit: 1,
            page: 2
        });

        expect(result)
            .to.have.property('totalCount')
            .and.to.be.at.least(1);
    });

    it('should respect useMaster flag', async () => {
        const querySpy = sinon.spy(ds, '_query');
        const floraRequest = {
            database,
            useMaster: true,
            attributes: ['col1'],
            queryAST: astTpl,
            limit: 1,
            page: 2
        };

        await ds.process(floraRequest);

        expect(querySpy).to.have.been.calledWithMatch({ type: 'MASTER' });
        querySpy.restore();
    });
});
