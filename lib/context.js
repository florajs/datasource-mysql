'use strict';

const { escape, escapeId, raw } = require('mysql');
const Transaction = require('./transaction');
const { insertStmt, updateStmt, deleteStmt, upsertStmt, bindParams, getRow, getColumn, getField } = require('./util');
const { ImplementationError } = require('@florajs/errors');

class Context {
    /**
     * @param {Object}      ds          - Instance of Flora MySQL data source
     * @param {Object}      ctx
     * @param {string}      ctx.db
     * @param {string=}     ctx.server
     * @param {string=}     ctx.type
     */
    constructor(ds, ctx = {}) {
        if (!Object.hasOwn(ctx, 'db')) {
            throw new ImplementationError('Context requires a db (database) property');
        }

        if (typeof ctx.db !== 'string' || ctx.db.trim().length === 0) {
            throw new ImplementationError('Invalid value for db (database) property');
        }

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
        return this.exec(sql).then(({ changedRows, affectedRows }) => ({ changedRows, affectedRows }));
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
     * @param {?string}                 [alias=new]
     * @return {Promise}
     */
    upsert(table, data, update, alias = 'new_values') {
        const sql = upsertStmt(table, data, update, alias);
        return this.exec(sql);
    }

    raw(val) {
        return raw(val);
    }

    quote(val) {
        return escape(val);
    }

    quoteIdentifier(identifier) {
        return escapeId(identifier);
    }

    /**
     * @param {string}          sql
     * @param {(Object|Array)=} values
     * @return {Promise}
     */
    query(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        return this.ds._query({ ...{ type: 'SLAVE' }, ...this.ctx }, sql).then(({ results }) => results);
    }

    /**
     * @param {string}                  sql
     * @param {(Object|Array)=}         values
     * @return {Promise.<Object|null>}
     */
    queryRow(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        return this.ds._query({ ...{ type: 'SLAVE' }, ...this.ctx }, sql).then(getRow);
    }

    /**
     * @param {string}                  sql
     * @param {(Object|Array)=}         values
     * @return {Promise.<date|number|string|null>}
     */
    queryOne(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        return this.ds._query({ ...{ type: 'SLAVE' }, ...this.ctx }, sql).then(getField);
    }

    /**
     * @param {string}          sql
     * @param {(Object|Array)=} values
     * @return {Promise.<Array>}
     */
    queryCol(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        return this.ds._query({ type: 'SLAVE', ...this.ctx }, sql).then(getColumn);
    }

    /**
     * @param {string}          sql
     * @param {(Object|Array)=} values
     * @return {Promise}
     */
    exec(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        return this.ds
            ._query({ type: 'MASTER', ...this.ctx }, sql)
            .then(({ results: { insertId, affectedRows, changedRows } }) => ({ insertId, affectedRows, changedRows }));
    }

    /**
     * @callback transactionCallback
     * @param {Transaction} transaction
     */
    /**
     * @param {transactionCallback=} callback
     * @return {Promise.<Transaction|void>}
     */
    async transaction(callback) {
        const connection = await this.ds._getConnection({ ...this.ctx, type: 'MASTER' });
        const transaction = new Transaction(connection);

        await transaction.begin();

        if (callback) {
            try {
                await callback(transaction);
            } catch (e) {
                try {
                    await transaction.rollback();
                } catch (rollbackError) {
                    // ignore rollback error
                }
                throw e;
            }

            await transaction.commit();

            return;
        }

        return transaction;
    }
}

module.exports = Context;
