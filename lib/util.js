'use strict';

const util = require('util');
const { escape, escapeId } = require('mysql');
const unname = require('named-placeholders')();
const Sqlstring = require('sqlstring');

const { ImplementationError } = require('@florajs/errors');

const isIterable = (obj) => obj[Symbol.iterator] !== 'undefined';

function isEscapable(val) {
    const type = typeof val;

    return (
        ['boolean', 'number', 'string'].includes(type) ||
        val instanceof Date ||
        Array.isArray(val) ||
        val === null ||
        (type === 'object' && typeof val.toSqlString === 'function')
    );
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
            Object.entries(node)
                .filter(([, value]) => value !== null && typeof value === 'object') // remove primitive values
                .forEach(([attr]) => items.push(...walk(node[attr])));
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
    const insertTpl = `INSERT INTO ${escapeId(table)} (%s) VALUES %s`;

    if (data === null || typeof data === 'undefined') {
        throw new ImplementationError('data parameter is required');
    }

    if (typeof data === 'object' && !Array.isArray(data) && isIterable(data)) {
        const keys = Object.keys(data);
        const columns = keys.map((identifier) => escapeId(identifier)).join(', ');
        const values = '(' + keys.map((key) => escape(data[key])).join(', ') + ')';

        return util.format(insertTpl, columns, values);
    }

    if (Array.isArray(data) && data.every(isIterable)) {
        const keys = Object.keys(data[0]);
        const columns = keys.map((identifier) => escapeId(identifier)).join(', ');
        const values = data.map((row) => '(' + keys.map((key) => escape(row[key])).join(', ') + ')');
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
    const updateTpl = `UPDATE ${escapeId(table)} SET %s WHERE %s`;
    let assignmentList = '';

    if (typeof data === 'object' && !Array.isArray(data) && isIterable(data)) {
        assignmentList = Object.entries(data)
            .map(([attr, value]) => escapeId(attr) + ' = ' + escape(value))
            .join(', ');
    }

    if (!assignmentList) {
        throw new ImplementationError('data is not set');
    }

    let whereExpr = typeof where === 'string' ? where.trim() : '';
    if (typeof where === 'object' && !Array.isArray(where) && isIterable(where)) {
        whereExpr = Object.entries(where)
            .map(([attr, value]) => escapeId(attr) + ' = ' + escape(value))
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
    const deleteTpl = `DELETE FROM ${escapeId(table)} WHERE %s`;
    let whereExpr = typeof where === 'string' ? where : '';

    if (typeof where === 'object' && !Array.isArray(where) && isIterable(where)) {
        whereExpr = Object.entries(where)
            .map(([attr, value]) => escapeId(attr) + ' = ' + escape(value))
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
 * @param {string}                  alias
 * @return {string}
 */
function upsertStmt(table, data, update, alias) {
    if (!update || typeof update !== 'object') {
        throw new ImplementationError('Update parameter must be either an object or an array of strings');
    }

    const sql = `${insertStmt(table, data)} AS ${escapeId(alias)} ON DUPLICATE KEY UPDATE `;

    if (!Array.isArray(update) && isIterable(update)) {
        return (
            sql +
            Object.entries(update)
                .map(([attr, value]) => escapeId(attr) + ' = ' + escape(value))
                .join(', ')
        );
    }

    if (Array.isArray(update) && update.every((item) => typeof item === 'string')) {
        return sql + update.map((column) => `${escapeId(column)} = ${escapeId(alias)}.${escapeId(column)}`).join(', ');
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
            if (!Object.keys(values).length) throw new ImplementationError('"values" must not be an empty object');
            const [tpl, params] = unname(sql, values);
            return Sqlstring.format(tpl, params);
        }

        if (Array.isArray(values) && values.every(isEscapable)) {
            if (!values.length) throw new ImplementationError('"values" must not be an empty array');
            return Sqlstring.format(sql, values);
        }
    }

    throw new ImplementationError('"values" must be an object or an array');
}

function getRow({ results }) {
    if (!results.length) return null;
    return results[0];
}

function getColumn({ results, fields }) {
    const fieldName = fields[0].name;
    return results.map((item) => item[fieldName]);
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
    getRow,
    getColumn,
    getField
};
