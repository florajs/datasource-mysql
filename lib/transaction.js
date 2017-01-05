'use strict';

class Transaction {
    /**
     * @param {Object} connection
     * @param {Object} pool
     */
    constructor(connection, pool) {
        this.connection = connection;
        this.pool = pool;
    }

    /**
     * @param {Function} callback
     */
    begin(callback) {
        this.connection.query('START TRANSACTION', callback);
    }

    /**
     * @param {Function} callback
     */
    commit(callback) {
        this.connection.query('COMMIT', (err) => {
            if (err) return callback(err);
            this.pool.release(this.connection);
            return callback();
        });
    }

    /**
     * @param {Function} callback
     */
    rollback(callback) {
        this.connection.query('ROLLBACK', (err) => {
            if (err) return callback(err);
            this.pool.release(this.connection);
            return callback();
        });
    }

    /**
     * @param {string} sql
     * @param {Function} callback
     */
    query(sql, callback) {
        this.connection.query(sql, callback);
    }
}

module.exports = Transaction;
