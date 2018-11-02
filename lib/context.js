'use strict';

const { insertStmt, updateStmt, deleteStmt } = require('./util');
const Transaction = require('./transaction');

class Context {
    /**
     * @param {Object}      ds          - Instance of Flora MySQL data source
     * @param {Object}      ctx
     * @param {string}      ctx.db
     * @param {string=}     ctx.server
     * @param {string=}     ctx.type
     */
    constructor(ds, ctx = {}) {
        ctx.server = ctx.server || 'default';
        this.ds = ds;
        this.ctx = ctx;
    }

    /**
     * @param {string} table
     * @param {Object|Array.<Object>} data
     * @return {Promise}
     */
    insert(table, data) {
        try {
            const sql = insertStmt(table, data);
            return this.exec(sql);
        } catch (e) {
            return Promise.reject(e);
        }
    }

    /**
     * @param {string}          table
     * @param {Object}          data
     * @param {Object|string}   where
     * @return {Promise}
     */
    update(table, data, where) {
        try {
            const sql = updateStmt(table, data, where);
            return this.exec(sql);
        } catch (e) {
            return Promise.reject(e);
        }
    }

    /**
     * @param {string} table
     * @param {string|Object} where
     * @return {Promise}
     */
    delete(table, where) {
        try {
            const sql = deleteStmt(table, where);
            return this.exec(sql);
        } catch (e) {
            return Promise.reject(e);
        }
    }

    /**
     * @param {string}  sql
     * @param {Object=} values
     * @return {Promise}
     */
    query(sql, values) {
        /*
        if (typeof values === 'object' && !Array.isArray(values) && isIterable(values)) {
            sql = compile(sql, values);
        }
        */

        return this.ds._query(Object.assign({}, { type: 'SLAVE' }, this.ctx), sql)
            .then(({ results }) => results);
    }

    /**
     * @param {string}  sql
     * @param {Object=} values
     * @return {Promise}
     */
    exec(sql, values) {
        /*
        if (typeof values === 'object' && !Array.isArray(values) && isIterable(values)) {
            sql = compile(sql, values);
        }
        */

        return this.ds._query(Object.assign({}, { type: 'MASTER' }, this.ctx), sql)
            .then(({ results }) => results);
    }

    /**
     * @return {Promise.<Transaction>}
     */
    transaction() {
        let trx;

        return this.ds._getConnection(Object.assign({}, this.ctx, { type: 'MASTER' }))
            .then((connection) => {
                trx = new Transaction(connection);
                return trx.begin();
            })
            .then(() => trx);
    }
}

module.exports = Context;
