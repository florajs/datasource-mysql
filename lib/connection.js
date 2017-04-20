'use strict';

const mysql = require('mysql2');

class Connection {
    /**
     * @param {Object} config
     * @param {string} config.host      - IP/name of database host
     * @param {string} config.user      - Name of the database user
     * @param {string} config.password  - User's password
     * @param {string} config.database  - Database to connect to
     * @constructor
     */
    constructor(config) {
        this.connection = mysql.createConnection(config);
    }

    /**
     * @param {Function} callback
     */
    connect(callback) {
        this.connection.connect((err) => {
            if (err) callback(err);
            else this.connection.query('SET SESSION sql_mode = "ANSI";', callback);
        });
    }

    close() {
        try { // connection might be already closed
            this.connection.destroy();
        } catch (e) {
            // ignore
        }
    }

    isConnected(callback) {
        this.connection.ping(err => callback(!err));
    }

    /**
     * @param {string} sql
     * @param {Function} callback
     */
    query(sql, callback) {
        this.connection.query(sql, (err, result) => {
            if (err) return callback(err);

            if (!Array.isArray(result)) { // INSERT, UPDATE, DELETE
                result = { affectedRows: result.affectedRows, insertId: result.insertId };
            }

            return callback(null, result);
        });
    }
}

module.exports = Connection;
