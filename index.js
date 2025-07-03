'use strict';

const mysql = require('mysql2/promise');
const { Parser } = require('@florajs/sql-parser');
const astUtil = require('@florajs/sql-parser').util;
const { ImplementationError } = require('@florajs/errors');

const generateAST = require('./lib/sql-query-builder');
const checkAST = require('./lib/sql-query-checker');
const optimizeAST = require('./lib/sql-query-optimizer');
const status = require('./lib/connection-status');

const Context = require('./lib/context');

/**
 * Check if flora request attribute has column or alias equivalent in SQL query.
 *
 * @param {string} attribute
 * @param {(Array.<string>|Array.<Object>)} columns
 * @returns boolean
 * @private
 */
function hasSqlEquivalent(attribute, columns) {
    return columns.some((column) => column.expr.column === attribute || column.as === attribute);
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
            throw new ImplementationError('Attribute "' + attribute + '" is not provided by SQL query');
        }
    });
}

/**
 * Initialize pool connection (proper error handling
 * is quite impossible in during pool connection event)
 *
 * @param {import('mysql2/promise').PoolConnection} connection
 * @param {Array.<string|Array.<string>|Function>} initConfigs
 * @returns {Promise}
 */
function initConnection(connection, initConfigs) {
    const initQueries = initConfigs.map((initCfg) => {
        if (typeof initCfg === 'string') {
            return connection.query(initCfg);
        }

        if (Array.isArray(initCfg)) {
            return initCfg.every((item) => typeof item === 'string')
                ? Promise.all(initCfg.map((sql) => connection.query(sql)))
                : Promise.reject(new Error('All items must be of type string'));
        }

        if (typeof initCfg === 'function') {
            return initCfg(connection);
        }

        return Promise.reject(new Error('onConnect can either be a string, an array of strings or a function'));
    });

    return Promise.all(initQueries);
}

function astify(dsConfig, attributes) {
    if (typeof dsConfig.database !== 'string') {
        throw new ImplementationError('Database must be specified');
    }

    if (!dsConfig.database.trim().length) {
        throw new ImplementationError('Database must not be empty');
    }

    if (dsConfig.query?.trim().length > 0) {
        let ast;

        try {
            // add query to exception
            ast = new Parser().parse(dsConfig.query);
        } catch (e) {
            if (e.location) {
                e.message += ' Error at SQL-line ' + e.location.start.line + ' (col ' + e.location.start.column + ')';
            }
            e.query = dsConfig.query;
            throw e;
        }

        checkAST(ast); // check if columns are unique and fully qualified
        checkSqlEquivalents(attributes, ast.columns);

        return ast;
    }

    if (dsConfig.table?.trim().length > 0) {
        return {
            type: 'select',
            options: null,
            distinct: null,
            columns: Array.isArray(attributes)
                ? attributes.map((attribute) => ({
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
    }

    throw new ImplementationError('Option "query" or "table" must be specified');
}

class DataSource {
    /**
     * @constructor
     * @param {Api} api
     * @param {Object} config
     */
    constructor(api, config) {
        this._log = api.log.child({ component: 'datasource-mysql' });
        this._config = config;
        /** @type {{ [server: string]: { [database: string]: import('mysql2/promise').PoolCluster } }} */
        this._pools = {};
        this._status = config._status;

        if (this._status) this._status.onStatus(() => this._status.set('pools', status(this._pools)));
    }

    /**
     * Add property queryAst to DataSource config.
     *
     * @param {Object} dsConfig DataSource config object
     * @param {Array.<string>=} attributes List of resource config attributes mapped to DataSource.
     */
    prepare(dsConfig, attributes) {
        const ast = astify(dsConfig, attributes);

        if (dsConfig.searchable) {
            dsConfig.searchable = dsConfig.searchable.split(',');
            dsConfig.searchable.forEach((attr) => {
                if (ast.columns.find((col) => col.expr.column === attr || col.as === attr)) return;
                throw new ImplementationError(`Attribute "${attr}" is not available in AST`);
            });
        }

        dsConfig.queryAstRaw = ast;
        dsConfig.useMaster = dsConfig.useMaster === 'true';
    }

    /**
     * Create SQL statement from flora request and run query against database.
     *
     * @param {Object}      request
     * @param {string}      request.server
     * @param {string}      request.database
     * @param {number}      request.page
     * @param {boolean=}    request.useMaster
     * @param {Object=}     request.queryAst
     * @param {Array}       request.attributes
     * @param {Object=}     request._explain
     * @param {Object=}     request._status
     * @returns {Promise}
     */
    async process(request) {
        const { server = 'default', database, useMaster = false, _explain = {} } = request;
        let sql;

        if (!Object.hasOwn(request, 'queryAst')) this.buildSqlAst(request);

        const isLimitPer = Object.hasOwn(request, 'limitPer') && request.limitPer !== null;
        request.queryAst = optimizeAST(request.queryAst, request.attributes, isLimitPer);
        sql = astUtil.astToSQL(request.queryAst);
        _explain.sql = sql;

        if (request.page) sql += '; SELECT FOUND_ROWS() AS totalCount';
        if (request._status) request._status.set({ server, database, sql });

        return this._query({ type: useMaster ? 'MASTER' : 'SLAVE', server, db: database }, sql, _explain)
            .then(({ results }) => {
                // console.log('>>>>>', { results });
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

        for (const [server, dbs] of Object.entries(this._pools)) {
            for (const [database, pool] of Object.entries(dbs)) {
                this._log.debug('closing MySQL pool "%s" at "%s"', database, server);
                connectionPools.push(pool.end());
            }
        }

        return Promise.all(connectionPools);
    }

    getContext(ctx) {
        if (Object.hasOwn(ctx, 'useMaster') && !!ctx.useMaster) {
            ctx.type = 'MASTER';
            delete ctx.useMaster;
        }
        return new Context(this, ctx);
    }

    /**
     * @param {Object} request
     */
    buildSqlAst(request) {
        request.queryAst = structuredClone(request.queryAstRaw);
        request.queryAst = generateAST(request);

        checkSqlEquivalents(request.attributes, request.queryAst.columns);

        if (request.page) {
            if (!Array.isArray(request.queryAst.options)) request.queryAst.options = [];
            request.queryAst.options.push('SQL_CALC_FOUND_ROWS');
        }
    }

    /**
     * @param {Object} serverCfg
     * @param {string} database
     * @return {Object}
     * @private
     */
    _prepareServerCfg(serverCfg, database) {
        const baseCfg = {
            host: serverCfg.host,
            port: serverCfg.port || 3306,
            user: serverCfg.user || this._config.user,
            password: serverCfg.password || this._config.password,
            charset: serverCfg.charset || this._config.charset,
            database,
            connectTimeout: serverCfg.connectTimeout || this._config.connectTimeout || 3000,
            connectionLimit: serverCfg.poolSize || this._config.poolSize || 10,
            dateStrings: true, // force date types to be returned as strings
            multipleStatements: true, // pagination queries,
            enableKeepAlive: true
        };

        return ['masters', 'slaves']
            .filter((type) => Object.hasOwn(serverCfg, type) && Array.isArray(serverCfg[type]))
            .flatMap((type) => serverCfg[type].map((hostCfg) => ({ type, hostCfg })))
            .reduce((cfg, { type, hostCfg }) => {
                const serverType = type.slice(0, -1).toUpperCase();
                cfg[`${serverType.toUpperCase()}_${hostCfg.host}`] = { ...baseCfg, ...hostCfg };
                return cfg;
            }, {});
    }

    /**
     * @param {string} server
     * @param {string} database
     * @returns {import('mysql2/promise').PoolCluster}
     * @private
     */
    _getConnectionPool(server, database) {
        if (this._pools[server]?.[database]) {
            return this._pools[server][database];
        }

        this._log.trace('creating MySQL pool "%s"', database);

        const pool = mysql.createPoolCluster({ restoreNodeTimeout: 5000 });
        const serverCfg = this._config.servers[server];
        const clusterCfg = this._prepareServerCfg(serverCfg, database);

        Object.keys(clusterCfg)
            .filter((serverId) => Object.hasOwn(clusterCfg, serverId))
            .forEach((serverId) => pool.add(serverId, clusterCfg[serverId]));

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
     * @param {Object=}     explain
     * @returns {Promise}
     * @private
     */
    async _query(ctx, sql, explain = {}) {
        /** @type {import('mysql2/promise').PoolConnection} */
        const connection = await this._getConnection(ctx);
        const { host } = connection.config;
        explain.host = host;
        this._status?.increment('dataSourceQueries');
        this._log.trace({ host, sql }, 'executing query');
        try {
            const [results, fields] = await connection.query({ sql });
            return { results, fields };
        } finally {
            connection.release();
        }
    }

    /**
     * @param {Object}  ctx
     * @param {string}  ctx.type
     * @param {string}  ctx.server
     * @param {string}  ctx.db
     * @returns {Promise}
     * @private
     */
    async _getConnection({ type, server, db }) {
        /** @type {import('mysql2/promise').PoolConnection} */
        let connection;
        try {
            connection = await this._getConnectionPool(server, db).getConnection(`${type}*`);
        } catch (err) {
            if (type === 'SLAVE' && err.code === 'POOL_NOEXIST') {
                return await this._getConnection({ type: 'MASTER', server, db });
            }
            throw err;
        }

        if (Object.hasOwn(connection, '_floraInitialized')) return connection;

        const config = this._config;
        const init = [Object.hasOwn(config, 'onConnect') ? config.onConnect : "SET SESSION sql_mode = 'ANSI'"];
        if (Object.hasOwn(config, server) && Object.hasOwn(config[server], 'onConnect')) {
            init.push(config[server].onConnect);
        }

        this._log.trace('initialize connection');
        await initConnection(connection, init);

        Object.defineProperty(connection, '_floraInitialized', { value: true });
        this._status?.increment('dataSourceConnects');

        return connection;
    }
}

module.exports = DataSource;
