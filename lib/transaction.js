'use strict';

const { insertStmt, updateStmt, deleteStmt } = require('./util');

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
        return this.exec('COMMIT')
            .finally(() => this.connection.release());
    }

    /**
     * @returns {Promise}
     */
    rollback() {
        return this.exec('ROLLBACK')
            .finally(() => this.connection.release());
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
     * @param {string} sql
     * @returns {Promise}
     */
    query(sql) {
        return this.exec(sql);
    }

    /**
     * @param {string} sql
     * @return {Promise}
     */
    exec(sql) {
        return new Promise((resolve, reject) => {
            this.connection.query(sql, (err, results) => {
                if (err) return reject(err);
                return resolve(results);
            });
        });
    }
}

module.exports = Transaction;
