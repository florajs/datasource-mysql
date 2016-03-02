'use strict';

/**
 * @param {Object} connection
 * @param {Object} pool
 */
function Transaction(connection, pool) {
    this.connection = connection;
    this.pool = pool;
}

/**
 * @param {Function} callback
 */
Transaction.prototype.begin = function (callback) {
    this.connection.query('START TRANSACTION', callback);
};

/**
 * @param {Function} callback
 */
Transaction.prototype.commit = function (callback) {
    var self = this;

    this.connection.query('COMMIT', function (err) {
        if (err) return callback(err);
        self.pool.release(self.connection);
        callback();
    });
};

/**
 * @param {Function} callback
 */
Transaction.prototype.rollback = function (callback) {
    var self = this;

    this.connection.query('ROLLBACK', function (err) {
        if (err) return callback(err);
        self.pool.release(self.connection);
        callback();
    });
};

/**
 * @param {string} sql
 * @param {Function} callback
 */
Transaction.prototype.query = function (sql, callback) {
    this.connection.query(sql, callback);
};

module.exports = Transaction;
