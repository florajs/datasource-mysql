'use strict';

const chai = require('chai');
const { expect } = chai;
const PoolConnection = require('../../node_modules/mysql/lib/PoolConnection');
const sinon = require('sinon');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');

describe('flora-mysql data source', () => {
    const ds = FloraMysqlFactory.create();
    const ctx = ds.getContext({ db: 'flora_mysql_testdb', useMaster: true });

    after(done => ds.close(done));

    describe('query method', () => {
        it('should release pool connections manually', async () => {
            const releaseSpy = sinon.spy(PoolConnection.prototype, 'release');

            await ctx.query('SELECT 1 FROM dual');
            expect(releaseSpy).to.have.been.calledOnce;
            releaseSpy.restore();
        });
    });
});
