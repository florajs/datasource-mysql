'use strict';

const has = require('has');
const mysql = require('mysql');
const { Parser } = require('flora-sql-parser');
const astUtil = require('flora-sql-parser').util;
const { ImplementationError } = require('flora-errors');

const generateAST = require('./lib/sql-query-builder');
const checkAST = require('./lib/sql-query-checker');
const optimizeAST = require('./lib/sql-query-optimizer');

const Context = require('./lib/context');

/**
 * Deep-clone an object and try to be efficient
 *
 * @param {object} obj
 * @returns {object}
 */
function cloneDeep(obj) {
    return JSON.parse(JSON.stringify(obj));
}

/**
 * Check if flora request attribute has column or alias equivalent in SQL query.
 *
 * @param {string} attribute
 * @param {(Array.<string>|Array.<Object>)} columns
 * @returns boolean
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
 * @returns {string}
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

/**
 * Initialize pool connection (proper error handling
 * is quiet impossible in during pool connection event)
 *
 * @param {Connection} connection
 * @param {Array.<string|Array.<string>|Function>} initConfigs
 * @returns {Promise}
 */
function initConnection(connection, initConfigs) {
    function query(sql) {
        return new Promise((resolve, reject) => {
            connection.query(sql, (err) => {
                if (err) return reject(err);
                return resolve();
            });
        });
    }

    const initQueries = initConfigs.map((initCfg) => {
        if (typeof initCfg === 'string') {
            return query(initCfg);
        } else if (Array.isArray(initCfg)) {
            return initCfg.every(item => typeof item === 'string')
                ? Promise.all(initCfg.map(query))
                : Promise.reject(new Error('All items must be of type string'));
        } else if (typeof initCfg === 'function') {
            return initCfg(connection);
        }

        return Promise.reject(new Error('onConnect can either be a string, an array of strings or a function'));
    });

    return Promise.all(initQueries);
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

        if (this._status) this._status.onStatus(() => this._status.set('pools', this._collectPoolStatus()));
    }

    _collectPoolStatus() {
        const stats = {};
        const statProps = {
            open: '_allConnections',
            sleeping: '_freeConnections',
            waiting: '_acquiringConnections'
        };

        Object.keys(this._pools).forEach((server) => {
            if (!stats[server]) stats[server] = {};
            Object.keys(this._pools[server]).forEach((db) => {
                const pool = this._getConnectionPool(server, db);
                const poolStats = {};

                Object.keys(statProps).forEach((prop) => {
                    const statProp = statProps[prop];
                    if (!pool[statProp] || !Array.isArray(pool[statProp])) return;
                    poolStats[prop] = pool[statProp].length;
                });

                poolStats.open -= poolStats.sleeping;

                stats[server][db] = poolStats;
            });
        });

        return stats;
    }

    /**
     * Add property queryAST to DataSource config.
     *
     * @param {Object} dsConfig DataSource config object
     * @param {Array.<string>=} attributes List of resource config attributes mapped to DataSource.
     */
    prepare(dsConfig, attributes) {
        let ast;

        if (dsConfig.query && dsConfig.query.trim().length > 0) {
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
        } else if (dsConfig.table && dsConfig.table.trim().length > 0) {
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
                limit: null,
                with: null
            };
        } else {
            throw new ImplementationError('Option "query" or "table" must be specified');
        }

        if (dsConfig.searchable) {
            dsConfig.searchable = dsConfig.searchable.split(',');
            dsConfig.searchable.forEach((attr) => {
                if (ast.columns.find(col => col.expr.column === attr || col.as === attr)) return;
                throw new ImplementationError(`Attribute "${attr}" is not available in AST`);
            });
        }

        dsConfig.queryAST = ast;
        dsConfig.useMaster = dsConfig.useMaster === 'true';
    }

    /**
     * Create SQL statement from flora request and run query against database.
     *
     * @param {Object} request
     * @returns {Promise}
     */
    async process(request) {
        const server = request.server || 'default';
        const db = request.database;
        const connectionType = request.useMaster ? 'MASTER' : 'SLAVE';
        let sql;

        sql = buildSql(request);

        if (request._status) {
            request._status.set('server', server);
            request._status.set('database', db);
            request._status.set('sql', sql);
        }

        if (request.page) sql += '; SELECT FOUND_ROWS() AS totalCount';

        return this._query({ type: connectionType, server, db }, sql)
            .then(({ results, host }) => {
                if (has(request, '_explain')) Object.assign(request._explain, { host, sql });

                return {
                    data: !request.page ? results : results[0],
                    totalCount: !request.page ? null : parseInt(results[1][0].totalCount, 10)
                };
            })
            .catch((err) => {
                this._log.info(err);
                throw err;
            });
    }

    /**
     * @returns {Promise}
     */
    close() {
        const connectionPools = [];

        function drain(pool) {
            return new Promise(resolve => pool.end(resolve));
        }

        Object.keys(this._pools).forEach((server) => {
            Object.keys(this._pools[server]).forEach((database) => {
                this._log.debug('closing MySQL pool "%s" at "%s"', database, server);
                connectionPools.push(drain(this._pools[server][database]));
            });
        });

        return Promise.all(connectionPools);
    }

    getContext(ctx) {
        if (has(ctx, 'useMaster') && !!ctx.useMaster) {
            ctx.type = 'MASTER';
            delete ctx.useMaster;
        }
        return new Context(this, ctx);
    }

    /**
     * @param {Object} serverCfg
     * @param {string} database
     * @return {Object}
     * @private
     */
    _prepareServerCfg(serverCfg, database) {
        serverCfg.masters = serverCfg.masters || [{}];
        serverCfg.slaves = serverCfg.slaves || [];

        const baseCfg = {
            host: serverCfg.host,
            port: serverCfg.port || 3306,
            user: serverCfg.user || this._config.user,
            password: serverCfg.password || this._config.password,
            database,
            connectTimeout: serverCfg.connectTimeout || this._config.connectTimeout || 3000,
            connectionLimit: serverCfg.poolSize || this._config.poolSize || 10,
            dateStrings: true, // force date types to be returned as strings
            multipleStatements: true // pagination queries
        };

        const clusterCfg = {};
        ['masters', 'slaves'].forEach((type) => {
            const pattern = type.slice(0, -1).toUpperCase();
            serverCfg[type].forEach((hostCfg) => {
                clusterCfg[`${pattern}_${hostCfg.host}`] = Object.assign({}, baseCfg, hostCfg);
            });
        });

        return clusterCfg;
    }

    /**
     * @param {string} server
     * @param {string} database
     * @returns {Object}
     * @private
     */
    _getConnectionPool(server, database) {
        if (this._pools[server] && this._pools[server][database]) {
            return this._pools[server][database];
        }

        this._log.trace('creating MySQL pool "%s"', database);

        const pool = mysql.createPoolCluster();
        const serverCfg = this._config.servers[server];
        const clusterCfg = this._prepareServerCfg(serverCfg, database);

        Object.keys(clusterCfg)
            .filter(serverId => has(clusterCfg, serverId))
            .forEach(serverId => pool.add(serverId, clusterCfg[serverId]));

        if (typeof this._pools[server] !== 'object') this._pools[server] = {};
        this._pools[server][database] = pool;
        return pool;
    }

    /**
     * Low-level query function. Subsequent calls may
     * use different connections from connection pool.
     *
     * @param {Object}      ctx
     * @param {string}      ctx.type
     * @param {string=}     ctx.server
     * @param {string}      ctx.db
     * @param {string}      sql
     * @returns {Promise}
     * @private
     */
    _query(ctx, sql) {
        return this._getConnection(ctx)
            .then((connection) => {
                const { host } = connection.config;

                if (this._status) this._status.increment('dataSourceQueries');
                this._log.trace({ host, sql }, 'executing query');

                return new Promise((resolve, reject) => {
                    connection.query(sql, (err, results, fields) => {
                        connection.release();
                        if (err) return reject(err);
                        return resolve({ results, fields, host });
                    });
                });
            });
    }

    /**
     * @param {Object}  ctx
     * @param {string}  ctx.type
     * @param {string}  ctx.server
     * @param {string}  ctx.db
     * @returns {Promise}
     * @private
     */
    _getConnection({ type, server, db }) {
        return new Promise((resolve, reject) => {
            this._getConnectionPool(server, db)
                .getConnection(`${type}*`, (err, connection) => {
                    if (err) {
                        if (type === 'SLAVE' && err.code === 'POOL_NOEXIST') {
                            return resolve(this._getConnection({ type: 'MASTER', server, db }));
                        }
                        return reject(err);
                    }
                    return resolve(connection);
                });
        }).then((connection) => {
            if (has(connection, '_floraInitialized')) return connection;

            const config = this._config;
            const init = [has(config, 'onConnect') ? config.onConnect : 'SET SESSION sql_mode = \'ANSI\''];
            if (has(config, server) && has(config[server], 'onConnect')) init.push(config[server].onConnect);

            this._log.trace('initialize connection');
            return initConnection(connection, init)
                .then(() => Object.defineProperty(connection, '_floraInitialized', { value: true }))
                .then(() => {
                    if (this._status) this._status.increment('dataSourceConnects');
                })
                .then(() => connection);
        });
    }
}

module.exports = DataSource;
