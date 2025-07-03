'use strict';

const assert = require('node:assert/strict');
const { after, describe, it, mock } = require('node:test');
const PoolConnection = require('mysql2/promise').PromisePoolConnection;

const { FloraMysqlFactory } = require('../FloraMysqlFactory');
const ciCfg = require('./ci-config');

describe('datasource-mysql', () => {
    const ds = FloraMysqlFactory.create(ciCfg);
    const db = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';
    const ctx = ds.getContext({ db, useMaster: true });

    after(() => ds.close());

    describe('query method', () => {
        it('should release pool connections manually', async () => {
            mock.method(PoolConnection.prototype, 'release');

            await ctx.query('SELECT 1 FROM dual');

            assert.equal(PoolConnection.prototype.release.mock.callCount(), 1);

            PoolConnection.prototype.release.mock.restore();
        });
    });
});
