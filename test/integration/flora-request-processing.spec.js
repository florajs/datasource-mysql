'use strict';

const chai = require('chai');
const { expect } = chai;
const sinon = require('sinon');

const astTpl = require('../ast-tpl');
const { FloraMysqlFactory } = require('../FloraMysqlFactory');
const ciCfg = require('./ci-config');

chai.use(require('sinon-chai'));

describe('flora request processing', () => {
    const ds = FloraMysqlFactory.create(ciCfg);
    const database = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';
    const ctx = ds.getContext({ db: database });

    before(async () => {
        await ctx.insert('t', [
            { id: 1, col1: 'foo' },
            { id: 2, col1: 'bar' }
        ]);
    });
    after(async () => {
        await ctx.exec('TRUNCATE TABLE t');
        await ds.close();
    });

    it('should handle flora requests', async () => {
        const result = await ds.process({
            attributes: ['id', 'col1'],
            queryAstRaw: astTpl,
            database
        });

        expect(result).to.have.property('totalCount').and.to.be.null;
        expect(result).to.have.property('data').and.to.be.an('array');

        const data = result.data.map(({ id, col1 }) => ({ id: parseInt(id, 10), col1 }));
        expect(data).to.eql([
            { id: 1, col1: 'foo' },
            { id: 2, col1: 'bar' }
        ]);
    });

    it('should return result w/o type casting', async () => {
        const { data } = await ds.process({
            attributes: ['id'],
            queryAstRaw: astTpl,
            database
        });
        const [item] = data;

        expect(item).to.be.an('object').and.to.have.property('id').and.to.eql(Buffer.from('1'));
    });

    it('should query available results if "page" attribute is set in request', async () => {
        const result = await ds.process({
            database,
            attributes: ['col1'],
            queryAstRaw: astTpl,
            limit: 1,
            page: 2
        });

        expect(result).to.have.property('totalCount').and.to.equal(2);
    });

    it('should respect useMaster flag', async () => {
        const querySpy = sinon.spy(ds, '_query');
        const floraRequest = {
            database,
            useMaster: true,
            attributes: ['col1'],
            queryAstRaw: astTpl,
            limit: 1,
            page: 2
        };

        await ds.process(floraRequest);

        expect(querySpy).to.have.been.calledWithMatch({ type: 'MASTER' });
        querySpy.restore();
    });

    it('should use modified query AST', async () => {
        const querySpy = sinon.spy(ds, '_query');
        const floraRequest = {
            database,
            attributes: ['col1'],
            queryAstRaw: astTpl
        };

        ds.buildSqlAst(floraRequest);

        // simulate modifying AST manually in pre-execute extension
        floraRequest.queryAst.where = {
            type: 'binary_expr',
            operator: '<',
            left: { type: 'column_ref', table: 't', column: 'id' },
            right: { type: 'number', value: 0 }
        };

        await ds.process(floraRequest);

        expect(querySpy).to.have.been.calledWith(
            sinon.match.object,
            sinon.match('WHERE "t"."id" < 0'),
            sinon.match.any
        );
        querySpy.restore();
    });
});
