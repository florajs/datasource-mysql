'use strict';

const util = require('util');
const { escape } = require('mysql');
const unname = require('named-placeholders')();
const Sqlstring = require('sqlstring');

const { ImplementationError } = require('flora-errors');

const isIterable = obj => obj[Symbol.iterator] !== 'undefined';
const escapeIdentifier = identifier => `"${identifier}"`;

function isEscapable(val) {
    const type = typeof val;

    return ['boolean', 'number', 'string'].includes(type) ||
        (val instanceof Date) ||
        Array.isArray(val) ||
        val === null ||
        (type === 'object' && typeof val.toSqlString === 'function');
}

/**
 * Filter tree by function
 *
 * @param {Function} checkFn
 * @return {function(Object)}
 */
function filterTree(checkFn) {
    return function walk(node) {
        const items = [];

        if (checkFn(node)) {
            items.push(node);
        } else {
            Object.keys(node)
                .filter((attr) => { // remove simple types
                    const value = node[attr];
                    return value !== null && typeof value === 'object';
                })
                .forEach((attr) => {
                    items.push(...walk(node[attr]));
                });
        }

        return items;
    };
}

/**
 * @param {string} table
 * @param {Object|Array.<Object>} data
 * @return {string}
 */
function insertStmt(table, data) {
    const insertTpl = `INSERT INTO "${table}" (%s) VALUES %s`;

    if (data === null || typeof data === 'undefined') {
        throw new ImplementationError('data parameter is required');
    }

    if (typeof data === 'object' && !Array.isArray(data) && isIterable(data)) {
        const keys = Object.keys(data);
        const columns = keys.map(escapeIdentifier).join(', ');
        const values = '(' + keys.map(key => data[key]).map(escape).join(', ') + ')';

        return util.format(insertTpl, columns, values);
    }

    if (Array.isArray(data) && data.every(isIterable)) {
        const keys = Object.keys(data[0]);
        const columns = keys.map(escapeIdentifier).join(', ');
        const values = data.map(row => '(' + keys.map(key => escape(row[key])).join(', ') + ')');
        return util.format(insertTpl, columns, values.join(', '));
    }

    throw new ImplementationError('data is neither an object nor an array of objects');
}

/**
 * @param {string}          table
 * @param {Object}          data
 * @param {Object|string}   where
 * @return {string}
 */
function updateStmt(table, data, where) {
    const updateTpl = `UPDATE "${table}" SET %s WHERE %s`;
    let assignmentList = '';

    if (typeof data === 'object' && !Array.isArray(data) && isIterable(data)) {
        assignmentList = Object.keys(data)
            .map(key => `"${key}" = ${escape(data[key])}`)
            .join(', ');
    }

    if (!assignmentList) {
        throw new ImplementationError('data is not set');
    }

    let whereExpr = typeof where === 'string' ? where.trim() : '';
    if (typeof where === 'object' && !Array.isArray(where) && isIterable(where)) {
        whereExpr = Object.keys(where)
            .map(key => `"${key}" = ${escape(where[key])}`)
            .join(' AND ');
    }

    if (!whereExpr) {
        throw new ImplementationError('where expression is not set');
    }

    return util.format(updateTpl, assignmentList, whereExpr);
}

/**
 * @param {string}          table
 * @param {Object|string}   where
 * @return {string}
 */
function deleteStmt(table, where) {
    const deleteTpl = `DELETE FROM "${table}" WHERE %s`;
    let whereExpr = typeof where === 'string' ? where : '';

    if (typeof where === 'object' && !Array.isArray(where) && isIterable(where)) {
        whereExpr = Object.keys(where)
            .map(key => `"${key}" = ${escape(where[key])}`)
            .join(' AND ');
    }

    if (!whereExpr) {
        throw new ImplementationError('where expression is not set');
    }

    return util.format(deleteTpl, whereExpr);
}

/**
 * @param {string}                  table
 * @param {Object|Array.<Object>}   data
 * @param {Array.<string>|Object}   update
 * @return {string}
 */
function upsertStmt(table, data, update) {
    if (!update || typeof update !== 'object') {
        throw new Error('Update parameter must be either an object or an array of strings');
    }

    const sql = insertStmt(table, data) + ' ON DUPLICATE KEY UPDATE ';

    if (!Array.isArray(update) && isIterable(update)) {
        return sql + Object.keys(update)
            .map(column => `"${column}" = ${escape(update[column])}`)
            .join(', ');
    }

    if (Array.isArray(update) && update.every(item => typeof item === 'string')) {
        return sql + update.map(column => `"${column}" = VALUES("${column}")`)
            .join(', ');
    }

    return sql;
}

/**
 * @param {string}          sql
 * @param {Object|Array}    values
 * @return {string}
 */
function bindParams(sql, values) {
    if (typeof values === 'object') {
        if (!Array.isArray(values) && isIterable(values)) {
            if (!Object.keys(values).length) throw new Error('"values" must not be an empty object');
            const [tpl, params] = unname(sql, values);
            return Sqlstring.format(tpl, params);
        }

        if (Array.isArray(values) && values.every(isEscapable)) {
            if (!values.length) throw new Error('"values" must not be an empty array');
            return Sqlstring.format(sql, values);
        }
    }

    throw new Error('"values" must be an object or an array');
}

function getColumn({ results, fields }) {
    const fieldName = fields[0].name;
    return results.map(item => item[fieldName]);
}

function getField({ results, fields }) {
    if (!results.length) return null;
    return results[0][fields[0].name];
}

module.exports = {
    filterTree,
    insertStmt,
    updateStmt,
    deleteStmt,
    upsertStmt,
    bindParams,
    getColumn,
    getField
};
