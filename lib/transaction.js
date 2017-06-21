'use strict';

class Transaction {
    /**
     * @param {Object} connection
     */
    constructor(connection) {
        this.connection = connection;
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
            this.connection.release();
            return callback(null);
        });
    }

    /**
     * @param {Function} callback
     */
    rollback(callback) {
        this.connection.query('ROLLBACK', (err) => {
            if (err) return callback(err);
            this.connection.release();
            return callback(null);
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
