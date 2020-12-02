'use strict';

const chai = require('chai');
const { expect } = chai;
const PoolConnection = require('../../node_modules/mysql/lib/PoolConnection');
const sinon = require('sinon');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');
const ciCfg = require('./ci-config');

describe('flora-mysql data source', () => {
    const ds = FloraMysqlFactory.create(ciCfg);
    const db = process.env.MYSQL_DATABASE || 'flora_mysql_testdb';
    const ctx = ds.getContext({ db, useMaster: true });

    after(() => ds.close());

    describe('query method', () => {
        it('should release pool connections manually', async () => {
            const releaseSpy = sinon.spy(PoolConnection.prototype, 'release');

            await ctx.query('SELECT 1 FROM dual');
            expect(releaseSpy).to.have.been.calledOnce;
            releaseSpy.restore();
        });
    });
});
