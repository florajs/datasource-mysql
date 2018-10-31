'use strict';

const util = require('util');

const { escape } = require('mysql');
const { ImplementationError } = require('flora-errors');
const Transaction = require('./transaction');

const isIterable = obj => obj[Symbol.iterator] !== 'undefined';
const escapeIdentifier = identifier => `"${identifier}"`;

class Context {
    /**
     * @param {Object}      ds          - Instance of Flora MySQL data source
     * @param {Object}      ctx
     * @param {string}      ctx.db
     * @param {string=}     ctx.server
     * @param {string=}     ctx.type
     */
    constructor(ds, ctx = {}) {
        ctx.server = ctx.server || 'default';
        this.ds = ds;
        this.ctx = ctx;
    }

    /**
     * @param {string} table
     * @param {Object|Array.<Object>} data
     * @return {Promise}
     */
    insert(table, data) {
        const insertTpl = `INSERT INTO "${table}" (%s) VALUES %s;`;

        if (data === null || typeof data === 'undefined') {
            return Promise.reject(new ImplementationError('data parameter is required'));
        }

        if (typeof data === 'object' && !Array.isArray(data) && isIterable(data)) {
            const keys = Object.keys(data);
            const columns = keys.map(escapeIdentifier).join(', ');
            const values = '(' + keys.map(key => data[key]).map(escape).join(', ') + ')';
            const sql = util.format(insertTpl, columns, values);
            return this.exec(sql);
        }

        if (Array.isArray(data) && data.every(isIterable)) {
            const keys = Object.keys(data[0]);
            const columns = keys.map(escapeIdentifier).join(', ');
            const values = data.map(row => '(' + keys.map(key => escape(row[key])).join(', ') + ')');
            const sql = util.format(insertTpl, columns, values.join(', '));
            return this.exec(sql);
        }

        return Promise.reject(new ImplementationError('data is neither an object nor an array of objects'));
    }

    /**
     * @param {string}          table
     * @param {Object}          data
     * @param {Object|string}   where
     * @return {Promise}
     */
    update(table, data, where) {
        const updateTpl = `UPDATE "${table}" SET %s WHERE %s;`;
        let assignmentList = '';

        if (typeof data === 'object' && !Array.isArray(data) && isIterable(data)) {
            assignmentList = Object.keys(data)
                .map(key => `"${key}" = ${escape(data[key])}`)
                .join(', ');
        }

        if (!assignmentList) {
            return Promise.reject(new ImplementationError('data is not set'));
        }

        let whereExpr = typeof where === 'string' ? where.trim() : '';
        if (typeof where === 'object' && !Array.isArray(where) && isIterable(where)) {
            whereExpr = Object.keys(where)
                .map(key => `"${key}" = ${escape(where[key])}`)
                .join(' AND ');
        }

        if (!whereExpr) {
            return Promise.reject(new ImplementationError('where expression is not set'));
        }

        const sql = util.format(updateTpl, assignmentList, whereExpr);
        return this.exec(sql);
    }

    /**
     * @param {string}          table
     * @param {Object|string}   where
     * @return {Promise}
     */
    delete(table, where) {
        const deleteTpl = `DELETE FROM "${table}" WHERE %s;`;
        let whereExpr = typeof where === 'string' ? where : '';

        if (typeof where === 'object' && !Array.isArray(where) && isIterable(where)) {
            whereExpr = Object.keys(where)
                .map(key => `"${key}" = ${escape(where[key])}`)
                .join(' AND ');
        }

        if (!whereExpr) {
            return Promise.reject(new ImplementationError('where expression is not set'));
        }

        const sql = util.format(deleteTpl, whereExpr);
        return this.exec(sql);
    }

    /**
     * @param {string}  sql
     * @param {Object=} values
     * @return {Promise}
     */
    query(sql, values) {
        /*
        if (typeof values === 'object' && !Array.isArray(values) && isIterable(values)) {
            sql = compile(sql, values);
        }
        */

        return this.ds._query(Object.assign({}, { type: 'SLAVE' }, this.ctx), sql)
            .then(({ results }) => results);
    }

    /**
     * @param {string}  sql
     * @param {Object=} values
     * @return {Promise}
     */
    exec(sql, values) {
        /*
        if (typeof values === 'object' && !Array.isArray(values) && isIterable(values)) {
            sql = compile(sql, values);
        }
        */

        return this.ds._query(Object.assign({}, { type: 'MASTER' }, this.ctx), sql)
            .then(({ results }) => results);
    }

    /**
     * @return {Promise.<Transaction>}
     */
    transaction() {
        let trx;

        return this.ds._getConnection(Object.assign({}, this.ctx, { type: 'MASTER' }))
            .then((connection) => {
                trx = new Transaction(connection);
                return trx.begin();
            })
            .then(() => trx);
    }
}

module.exports = Context;
