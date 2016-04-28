'use strict';

var Client = require('mariasql');

/**
 * @param {Object} config
 * @param {string} config.host      - IP/name of database host
 * @param {string} config.user      - Name of the database user
 * @param {string} config.password  - User's password
 * @param {string} config.db        - Database to connect to
 * @constructor
 */
function Connection(config) {
    this.connection = new Client();
    this._cfg = config;
}

/**
 * @param {Function} callback
 */
Connection.prototype.connect = function (callback) {
    var self = this;

    function onReady() {
        self.query('SET SESSION sql_mode = "ANSI";', callback);
        self.connection.removeListener('error', onError);
    }

    function onError(err) {
        self.connection.removeListener('ready', onReady);
        callback(err);
    }

    this.connection.connect(this._cfg);
    this.connection
        .once('ready', onReady)
        .once('error', onError);
};

Connection.prototype.close = function () {
    try { // connection might be already closed
        this.connection.end();
    } catch (e) {}
};

/**
 * @return {boolean}
 */
Connection.prototype.isConnected = function () {
    return this.connection.connected;
};

/**
 * @param {string} sql
 * @param {Function} callback
 */
Connection.prototype.query = function (sql, callback) {
    try { // query method throws errors if not connected
        this.connection.query(sql, null, {metadata: true}, function (err, result) {
            if (err) return callback(err);
            if (result.info) {
                result.affectedRows = result.info.affectedRows;
                result.insertId = result.info.insertId;
            }
            return callback(null, result);
        });
    } catch (e) {
        callback(e);
    }
};

module.exports = Connection;
