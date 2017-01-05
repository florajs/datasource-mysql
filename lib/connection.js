'use strict';

const Client = require('mariasql');

class Connection {
    /**
     * @param {Object} config
     * @param {string} config.host      - IP/name of database host
     * @param {string} config.user      - Name of the database user
     * @param {string} config.password  - User's password
     * @param {string} config.db        - Database to connect to
     * @constructor
     */
    constructor(config) {
        this.connection = new Client();
        this._cfg = config;
    }

    /**
     * @param {Function} callback
     */
    connect(callback) {
        let onError;

        const onReady = () => {
            this.query('SET SESSION sql_mode = "ANSI";', callback);
            this.connection.removeListener('error', onError);
        };

        onError = (err) => {
            this.connection.removeListener('ready', onReady);
            callback(err);
        };

        this.connection.connect(this._cfg);
        this.connection.once('ready', onReady);
        this.connection.once('error', onError);
    }

    close() {
        try { // connection might be already closed
            this.connection.end();
        } catch (e) {
            // ignore
        }
    }

    /**
     * @return {boolean}
     */
    isConnected() {
        return this.connection.connected;
    }

    /**
     * @param {string} sql
     * @param {Function} callback
     */
    query(sql, callback) {
        try { // query method throws errors if not connected
            this.connection.query(sql, null, { metadata: true }, (err, result) => {
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
    }
}

module.exports = Connection;
