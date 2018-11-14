'use strict';

const Expr = require('./expr');
const Transaction = require('./transaction');
const {
    insertStmt,
    updateStmt,
    deleteStmt,
    upsertStmt,
    bindParams,
    getRow,
    getColumn,
    getField
} = require('./util');

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
        const sql = insertStmt(table, data);
        return this.exec(sql).then(({ insertId, affectedRows }) => ({ insertId, affectedRows }));
    }

    /**
     * @param {string}          table
     * @param {Object}          data
     * @param {Object|string}   where
     * @return {Promise}
     */
    update(table, data, where) {
        const sql = updateStmt(table, data, where);
        return this.exec(sql)
            .then(({ changedRows, affectedRows }) => ({ changedRows, affectedRows }));
    }

    /**
     * @param {string} table
     * @param {string|Object} where
     * @return {Promise}
     */
    delete(table, where) {
        const sql = deleteStmt(table, where);
        return this.exec(sql).then(({ affectedRows }) => ({ affectedRows }));
    }

    /**
     * @param {string}                  table
     * @param {Object|Array.<Object>}   data
     * @param {Array.<string>|Object}   update
     * @return {Promise}
     */
    upsert(table, data, update) {
        const sql = upsertStmt(table, data, update);
        return this.exec(sql);
    }

    expr(val) {
        return new Expr(val);
    }

    /**
     * @param {string}          sql
     * @param {(Object|Array)=} values
     * @return {Promise}
     */
    query(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);

        return this.ds._query(Object.assign({}, { type: 'SLAVE' }, this.ctx), sql)
            .then(({ results }) => results);
    }

    /**
     * @param {string}                  sql
     * @param {(Object|Array)=}         values
     * @return {Promise.<Object|null>}
     */
    queryRow(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);

        return this
            .ds
            ._query(Object.assign({}, { type: 'SLAVE' }, this.ctx), sql)
            .then(getRow);
    }

    /**
     * @param {string}                  sql
     * @param {(Object|Array)=}         values
     * @return {Promise.<date|number|string|null>}
     */
    queryOne(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);

        return this.ds._query(Object.assign({}, { type: 'SLAVE' }, this.ctx), sql)
            .then(getField);
    }

    /**
     * @param {string}          sql
     * @param {(Object|Array)=} values
     * @return {Promise.<Array>}
     */
    queryCol(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);

        return this.ds._query(Object.assign({}, { type: 'SLAVE' }, this.ctx), sql)
            .then(getColumn);
    }

    /**
     * @param {string}          sql
     * @param {(Object|Array)=} values
     * @return {Promise}
     */
    exec(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);

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