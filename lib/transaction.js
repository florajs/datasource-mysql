'use strict';

const { escape, escapeId, raw } = require('mysql2/promise');
const { insertStmt, updateStmt, deleteStmt, bindParams, upsertStmt, getRow, getColumn, getField } = require('./util');

class Transaction {
    /**
     * @param {import('mysql2/promise').PoolConnection} connection
     */
    constructor(connection) {
        /** @type import('mysql2/promise').PoolConnection */
        this.connection = connection;
    }

    /**
     * @returns {Promise}
     */
    begin() {
        return this.exec('START TRANSACTION');
    }

    /**
     * @returns {Promise}
     */
    commit() {
        return this.exec('COMMIT').finally(() => this.connection.release());
    }

    /**
     * @returns {Promise}
     */
    rollback() {
        return this.exec('ROLLBACK').finally(() => this.connection.release());
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
    async upsert(table, data, update, alias = 'new_values') {
        const sql = upsertStmt(table, data, update, alias);
        const { affectedRows, changedRows } = await this.exec(sql);
        return { affectedRows, changedRows };
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
        const { results } = await this._query(sql);
        return results;
    }

    /**
     * @param {string}                  sql
     * @param {(Object|Array)=}         values
     * @return {Promise.<Object|null>}
     */
    async queryRow(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);

        const res = await this._query(sql);
        return getRow(res);
    }

    /**
     * @param {string}          sql
     * @param {(Object|Array)=} values
     * @return {Promise.<date|number|string|null>}
     */
    async queryOne(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        const result = await this._query(sql);
        return getField(result);
    }

    /**
     * @param {string}          sql
     * @param {(Object|Array)=} values
     * @return {Promise}
     */
    async queryCol(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        const result = await this._query(sql);
        return getColumn(result);
    }

    /**
     * @param {string}          sql
     * @param {(Object|Array)=} values
     * @return {Promise<{insertId: number, affectedRows: number, changedRows: number}>}
     */
    async exec(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        const {
            results: { insertId, affectedRows, changedRows }
        } = await this._query(sql);
        return {
            insertId,
            affectedRows,
            changedRows
        };
    }

    /**
     * @param {string} sql
     * @private
     */
    async _query(sql) {
        const [results, fields] = await this.connection.query(sql);
        return { results, fields };
    }
}

module.exports = Transaction;
