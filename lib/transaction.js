'use strict';

const { escape, escapeId, raw } = require('mysql');
const { insertStmt, updateStmt, deleteStmt, bindParams, upsertStmt, getRow, getColumn, getField } = require('./util');

class Transaction {
    /**
     * @param {Object} connection
     */
    constructor(connection) {
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
    upsert(table, data, update, alias = 'new') {
        const sql = upsertStmt(table, data, update, alias);
        return this.exec(sql).then(({ affectedRows, changedRows }) => ({ affectedRows, changedRows }));
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
        return this._query(sql).then(({ results }) => results);
    }

    /**
     * @param {string}                  sql
     * @param {(Object|Array)=}         values
     * @return {Promise.<Object|null>}
     */
    queryRow(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);

        return this._query(sql).then(getRow);
    }

    /**
     * @param {string}          sql
     * @param {(Object|Array)=} values
     * @return {Promise.<date|number|string|null>}
     */
    queryOne(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        return this._query(sql).then(getField);
    }

    /**
     * @param {string}          sql
     * @param {(Object|Array)=} values
     * @return {Promise}
     */
    queryCol(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        return this._query(sql).then(getColumn);
    }

    /**
     * @param {string}          sql
     * @param {(Object|Array)=} values
     * @return {Promise}
     */
    exec(sql, values) {
        if (typeof values !== 'undefined') sql = bindParams(sql, values);
        return this._query(sql).then(({ results: { insertId, affectedRows, changedRows } }) => ({
            insertId,
            affectedRows,
            changedRows
        }));
    }

    /**
     * @param {string} sql
     * @return {Promise}
     * @private
     */
    _query(sql) {
        return new Promise((resolve, reject) => {
            this.connection.query(sql, (err, results, fields) => {
                if (err) return reject(err);
                return resolve({ results, fields });
            });
        });
    }
}

module.exports = Transaction;
