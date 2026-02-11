'use strict';

const util = require('util');
const { escape, escapeId } = require('mysql2/promise');
const unname = require('named-placeholders')();
const SqlEscaper = require('sql-escaper');

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
 * @param {string|Object.<string, null|boolean|number|string|array>} where
 * @return {string}
 */
function whereExpr(where) {
    if (typeof where === 'string' && where.trim().length > 0) {
        return where;
    }

    if (typeof where === 'object' && !Array.isArray(where) && isIterable(where)) {
        return Object.entries(where)
            .map(([attr, value]) => {
                if (value === null) {
                    return `${escapeId(attr)} IS NULL`;
                }

                if (!Array.isArray(value)) {
                    return `${escapeId(attr)} = ${escape(value)}`;
                }

                if (!value.length) {
                    throw new ImplementationError('Empty arrays in WHERE clause are not supported');
                }

                return `${escapeId(attr)} IN (${escape(value)})`;
            })
            .join(' AND ');
    }

    throw new ImplementationError('where expression is not set');
}

/**
 * @param {string}          table
 * @param {Object}          data
 * @param {Object|string}   where
 * @return {string}
 */
function updateStmt(table, data, where) {
    let assignmentList = '';

    if (typeof data === 'object' && !Array.isArray(data) && isIterable(data)) {
        assignmentList = Object.entries(data)
            .map(([attr, value]) => escapeId(attr) + ' = ' + escape(value))
            .join(', ');
    }

    if (!assignmentList) {
        throw new ImplementationError('data is not set');
    }

    return `UPDATE ${escapeId(table)} SET ${assignmentList} WHERE ${whereExpr(where)}`;
}

/**
 * @param {string}          table
 * @param {Object|string}   where
 * @return {string}
 */
function deleteStmt(table, where) {
    return `DELETE FROM ${escapeId(table)} WHERE ${whereExpr(where)}`;
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
            return SqlEscaper.format(tpl, params);
        }

        if (Array.isArray(values) && values.every(isEscapable)) {
            if (!values.length) throw new ImplementationError('"values" must not be an empty array');
            return SqlEscaper.format(sql, values);
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
