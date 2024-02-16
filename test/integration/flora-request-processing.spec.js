'use strict';

const assert = require('node:assert/strict');
const { mock } = require('node:test');
const chai = require('chai');

const astTpl = require('../ast-tpl');
const { FloraMysqlFactory } = require('../FloraMysqlFactory');
const ciCfg = require('./ci-config');

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

        assert.ok(Object.hasOwn(result, 'totalCount'));
        assert.equal(result.totalCount, null);
        assert.ok(Object.hasOwn(result, 'data'));
        assert.ok(Array.isArray(result.data));

        const data = result.data.map(({ id, col1 }) => ({ id: parseInt(id, 10), col1 }));
        assert.deepEqual(data, [
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

        assert.ok(typeof item === 'object');
        assert.ok(Object.hasOwn(item, 'id'));
        assert.ok(item.id instanceof Buffer);
        assert.equal(item.id.toString(), '1');
    });

    it('should query available results if "page" attribute is set in request', async () => {
        const result = await ds.process({
            database,
            attributes: ['col1'],
            queryAstRaw: astTpl,
            limit: 1,
            page: 2
        });

        assert.ok(Object.hasOwn(result, 'totalCount'));
        assert.equal(result.totalCount, 2);
    });

    it('should respect useMaster flag', async () => {
        const floraRequest = {
            database,
            useMaster: true,
            attributes: ['col1'],
            queryAstRaw: astTpl,
            limit: 1,
            page: 2
        };

        mock.method(ds, '_query');
        await ds.process(floraRequest);

        const [call] = ds._query.mock.calls;
        const [ctx] = call.arguments;
        assert.ok(Object.hasOwn(ctx, 'type'));
        assert.equal(ctx.type, 'MASTER');

        ds._query.mock.restore();
    });

    it('should use modified query AST', async () => {
        mock.method(ds, '_query');
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

        const [call] = ds._query.mock.calls;
        const [, sql] = call.arguments;

        assert.equal(sql, 'SELECT "t"."col1" FROM "t" WHERE "t"."id" < 0');

        ds._query.mock.restore();
    });
});
