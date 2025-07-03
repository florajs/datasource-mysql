'use strict';

const { escape, escapeId, raw } = require('mysql2/promise');
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
        /** @type {import('../index.js')} */
        this.ds = ds;
        this.ctx = ctx;
    }

    /**
     * @param {string} table
     * @param {Object|Array.<Object>} data
     * @return {Promise}
     */
    async insert(table, data) {
        const sql = insertStmt(table, data);
        const { insertId, affectedRows } = await this.exec(sql);
        return { insertId, affectedRows };
    }

    /**
     * @param {string}          table
     * @param {Object}          data
     * @param {Object|string}   where
     * @return {Promise}
     */
    async update(table, data, where) {
        const sql = updateStmt(table, data, where);
        const { changedRows, affectedRows } = await this.exec(sql);
        return { changedRows, affectedRows };
    }

    /**
     * @param {string} table
     * @param {string|Object} where
     * @return {Promise}
     */
    async delete(table, where) {
        const sql = deleteStmt(table, where);
        const { affectedRows } = await this.exec(sql);
        return { affectedRows };
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
    async query(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        const { results } = await this.ds._query({ ...{ type: 'SLAVE' }, ...this.ctx }, sql);
        return results;
    }

    /**
     * @param {string}                  sql
     * @param {(Object|Array)=}         values
     * @return {Promise.<Object|null>}
     */
    async queryRow(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        const result_1 = await this.ds._query({ ...{ type: 'SLAVE' }, ...this.ctx }, sql);
        return getRow(result_1);
    }

    /**
     * @param {string}                  sql
     * @param {(Object|Array)=}         values
     * @return {Promise.<date|number|string|null>}
     */
    async queryOne(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        const result = await this.ds._query({ ...{ type: 'SLAVE' }, ...this.ctx }, sql);
        return getField(result);
    }

    /**
     * @param {string}          sql
     * @param {(Object|Array)=} values
     * @return {Promise.<Array>}
     */
    async queryCol(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        const result_2 = await this.ds._query({ type: 'SLAVE', ...this.ctx }, sql);
        return getColumn(result_2);
    }

    /**
     * @param {string}          sql
     * @param {(Object|Array)=} values
     * @return {Promise}
     */
    async exec(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        const {
            results: { insertId, affectedRows, changedRows }
        } = await this.ds._query({ type: 'MASTER', ...this.ctx }, sql);
        return { insertId, affectedRows, changedRows };
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
