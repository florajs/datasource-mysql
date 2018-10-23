'use strict';

const chai = require('chai');
const { expect } = chai;
const PoolConnection = require('../../node_modules/mysql/lib/PoolConnection');
const sinon = require('sinon');

const { FloraMysqlFactory } = require('../FloraMysqlFactory');

describe('flora-mysql data source', () => {
    const db = 'flora_mysql_testdb';
    const ds = FloraMysqlFactory.create();

    after(done => ds.close(done));

    describe('query method', () => {
        it('should release pool connections manually', async () => {
            const releaseSpy = sinon.spy(PoolConnection.prototype, 'release');

            await ds.query('default', db, 'SELECT 1');
            expect(releaseSpy).to.have.been.calledOnce;
            releaseSpy.restore();
        });
    });
});
