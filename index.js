'use strict';

const mysql = require('mysql2');
const { Parser } = require('flora-sql-parser');
const astUtil = require('flora-sql-parser').util;

const generateAST = require('./lib/sql-query-builder');
const checkAST = require('./lib/sql-query-checker');
const optimizeAST = require('./lib/sql-query-optimizer');
const Transaction = require('./lib/transaction');

/**
 * Deep-clone an object and try to be efficient
 *
 * @param {object} obj
 * @return {object}
 */
function cloneDeep(obj) {
    return JSON.parse(JSON.stringify(obj));
}

/**
 * Check if flora request attribute has column or alias equivalent in SQL query.
 *
 * @param {string} attribute
 * @param {(Array.<string>|Array.<Object>)} columns
 * @return boolean
 * @private
 */
function hasSqlEquivalent(attribute, columns) {
    return columns.some(column => (column.expr.column === attribute || column.as === attribute));
}

/**
 * Check if each attribute has a SQL counterpart.
 *
 * @param {Array.<string>} attributes
 * @param {Array.<string>} columns
 * @private
 */
function checkSqlEquivalents(attributes, columns) {
    attributes.forEach((attribute) => {
        if (!hasSqlEquivalent(attribute, columns)) {
            throw new Error('Attribute "' + attribute + '" is not provided by SQL query');
        }
    });
}

/**
 * @param {Object} request
 * @return {string}
 */
function buildSql(request) {
    request.queryAST = cloneDeep(request.queryAST);

    /** @type {Object} */
    const ast = generateAST(request);

    checkSqlEquivalents(request.attributes, ast.columns);

    if (request.page) {
        if (!Array.isArray(ast.options)) ast.options = [];
        ast.options.push('SQL_CALC_FOUND_ROWS');
    }

    optimizeAST(ast, request.attributes);

    return astUtil.astToSQL(ast);
}

class DataSource {
    /**
     * @constructor
     * @param {Api} api
     * @param {Object} config
     */
    constructor(api, config) {
        this._log = api.log.child({ component: 'flora-mysql' });
        this._parser = new Parser();
        this._config = config;
        this._pools = {};
        this._status = config._status;

        /* // FIXME!
        if (this._status) {
            this._status.onStatus(() => {
                const stats = {};

                Object.keys(this._pools).forEach((server) => {
                    if (!stats[server]) stats[server] = {};
                    Object.keys(this._pools[server]).forEach((db) => {
                        const pool = this._getConnectionPool(server, db);

                        stats[server][db] = {
                            open: pool.getPoolSize(),
                            sleeping: pool.availableObjectsCount(),
                            waiting: pool.waitingClientsCount()
                        };
                    });
                });

                this._status.set('pools', stats);
            });
        }*/
    }

    /**
     * Add property queryAST to DataSource config.
     *
     * @param {Object} dsConfig DataSource config object
     * @param {Array.<string>=} attributes List of resource config attributes mapped to DataSource.
     */
    prepare(dsConfig, attributes) {
        let ast;

        if (dsConfig.searchable) dsConfig.searchable = dsConfig.searchable.split(',');

        if (dsConfig.query && dsConfig.query.trim() !== '') {
            try { // add query to exception
                ast = this._parser.parse(dsConfig.query);

                ast._meta = ast._meta || {};
                ast._meta.hasFilterPlaceholders = dsConfig.query.includes('__floraFilterPlaceholder__');
            } catch (e) {
                if (e.location) {
                    e.message += ' Error at SQL-line ' + e.location.start.line + ' (col ' + e.location.start.column + ')';
                }
                e.query = dsConfig.query;
                throw e;
            }

            checkAST(ast); // check if columns are unique and fully qualified
            checkSqlEquivalents(attributes, ast.columns);
        } else {
            ast = {
                _meta: { hasFilterPlaceholders: false },
                type: 'select',
                options: null,
                distinct: null,
                columns: Array.isArray(attributes)
                    ? attributes.map(attribute => ({
                        expr: { type: 'column_ref', table: dsConfig.table, column: attribute },
                        as: null
                    }))
                    : '',
                from: [{ db: null, table: dsConfig.table, as: null }],
                where: null,
                groupby: null,
                having: null,
                orderby: null,
                limit: null
            };
        }

        dsConfig.queryAST = ast;
    }

    /**
     * Create SQL statement from flora request and run query against database.
     *
     * @param {Object} request
     * @param {Function} callback
     */
    process(request, callback) {
        const server = request.server || 'default';
        const db = request.database;
        let sql;

        try {
            sql = buildSql(request);
        } catch (e) {
            return callback(e);
        }

        if (request._status) {
            request._status.set('server', server);
            request._status.set('database', db);
            request._status.set('sql', sql);
        }

        if (request.page) sql += '; SELECT FOUND_ROWS() AS totalCount';
        if (request._explain) request._explain.executedQuery = sql;

        return this.query(server, db, sql, (err, results) => {
            if (err) return callback(err);

            return callback(null, {
                data: !request.page ? results : results[0],
                totalCount: !request.page ? null : parseInt(results[1][0].totalCount, 10)
            });
        });
    }

    /**
     * @param {Function} callback
     */
    close(callback) {
        const connectionPools = [];

        function drain(pool) { // TODO: test
            return new Promise(resolve => pool.end(resolve));
        }

        Object.keys(this._pools).forEach((server) => {
            Object.keys(this._pools[server]).forEach((database) => {
                this._log.debug('closing MySQL pool "%s" at "%s"', database, server);
                connectionPools.push(drain(this._pools[server][database]));
            });
        });

        Promise.all(connectionPools)
            .then(() => callback())
            .catch(callback);
    }

    /**
     * @param {String} server
     * @param {String} db
     * @param {Function} callback
     */
    transaction(server, db, callback) {
        const pool = this._getConnectionPool(server, db);

        pool.getConnection((poolErr, connection) => {
            if (poolErr) return callback(poolErr);

            const trx = new Transaction(connection);
            return trx.begin((trxErr) => {
                if (trxErr) return callback(trxErr);
                return callback(null, trx);
            });
        });
    }

    /**
     * @param {string} server
     * @param {string} database
     * @return {Object}
     * @private
     */
    _getConnectionPool(server, database) {
        if (this._pools[server] && this._pools[server][database]) {
            return this._pools[server][database];
        }

        this._log.trace('creating MySQL pool "%s"', database);

        const serverCfg = this._config.servers[server];
        const pool = mysql.createPool({
            host: serverCfg.host,
            port: serverCfg.port || 3306,
            user: serverCfg.user,
            password: serverCfg.password,
            database,
            connectTimeout: serverCfg.connectTimeout || this._config.connectTimeout || 3000,
            queueLimit: serverCfg.poolSize || this._config.poolSize || 10,
            multipleStatements: true // pagination queries
        });

        pool.on('connection', connection => connection.query('SET SESSION sql_mode = "ANSI";'));

        if (typeof this._pools[server] !== 'object') this._pools[server] = {};
        this._pools[server][database] = pool;
        return pool;
    }

    /**
     * Low-level query function. Subsequent calls may
     * use different connections from connection pool.
     *
     * @param {string} server
     * @param {string} db
     * @param {string} sql
     * @param {Function} callback
     */
    query(server, db, sql, callback) {
        if (this._status) this._status.increment('dataSourceQueries');
        this._log.trace({ sql }, 'executing query');

        const pool = this._getConnectionPool(server, db);
        pool.query(sql, (err, result) => {
            if (err) return callback(err);
            return callback(null, result);
        });
    }
}

module.exports = DataSource;
