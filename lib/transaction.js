'use strict';

function query(connection, sql) {
    return new Promise((resolve, reject) => {
        connection.query(sql, (err, results) => {
            if (err) return reject(err);
            return resolve(results);
        });
    });
}

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
        return query(this.connection, 'START TRANSACTION');
    }

    /**
     * @returns {Promise}
     */
    commit() {
        return query(this.connection, 'COMMIT')
            .finally(() => this.connection.release());
    }

    /**
     * @returns {Promise}
     */
    rollback() {
        return query(this.connection, 'ROLLBACK')
            .finally(() => this.connection.release());
    }

    /**
     * @param {string} sql
     * @returns {Promise}
     */
    query(sql) {
        return query(this.connection, sql);
    }
}

module.exports = Transaction;
