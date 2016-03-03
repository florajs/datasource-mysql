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
 * @param {Function} [callback]
 */
Connection.prototype.connect = function (callback) {
    var self = this;
    this.connection.connect(this._cfg);

    if (typeof callback === 'function') {
        this.connection
            .on('ready', function () {
                self.query('SET SESSION sql_mode = "ANSI";', callback);
            })
            .on('error', callback);
    }
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
    var callbackCalled = false;
    try { // query method throws errors if not connected
        this.connection
            .query(sql)
            .on('result', function processDbResult(result) {
                var dbResult = [];
                result
                    .on('data', function (row) {
                        dbResult.push(row);
                    })
                    .on('error', function (err) {
                        err.info = { query: sql, code: err.code };
                        if (!callbackCalled) callback(err);
                        callbackCalled = true;
                    })
                    .on('end', function () {
                        if (!callbackCalled) callback(null, dbResult);
                        callbackCalled = true;
                    });
            });
    } catch (e) {
        if (!callbackCalled) callback(e);
        callbackCalled = true;
    }
};

module.exports = Connection;
