'use strict';

const has = require('has');
const poolModule = require('generic-pool');
const { Parser } = require('flora-sql-parser');
const astUtil = require('flora-sql-parser').util;

const generateAST = require('./lib/sql-query-builder');
const checkAST = require('./lib/sql-query-checker');
const optimizeAST = require('./lib/sql-query-optimizer');
const Connection = require('./lib/connection');
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

/**
 * @param conn
 * @param sql
 * @param callback
 * @private
 */
function runQuery(conn, sql, callback) {
    conn.query(sql, callback);
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
        // cache query functions for pagination queries (see _paginatedQuery function)
        this._queryFnPool = {};
        this._status = config._status;

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
        }
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

        if (request._explain) request._explain.executedQuery = sql;

        if (!request.page) {
            return this.query(server, db, sql, (err, result) => {
                if (err) return callback(err);
                return callback(null, { totalCount: null, data: result });
            });
        }

        return this._paginatedQuery(server, db, sql, callback);
    }

    /**
     * @param {Function} callback
     */
    close(callback) {
        const connectionPools = [];

        function drain(pool) {
            return new Promise((resolve) => {
                pool.drain(() => pool.destroyAllNow(resolve));
            });
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

        pool.acquire((poolErr, connection) => {
            if (poolErr) return callback(poolErr);

            const trx = new Transaction(connection, pool);
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

        const pool = poolModule.Pool({
            name: database,
            max: this._config.servers[server].poolSize || this._config.poolSize || 10,
            idleTimeoutMillis: 30000,
            create: (callback) => {
                const serverCfg = this._config.servers[server];
                const cfg = {
                    user: serverCfg.user,
                    password: serverCfg.password,
                    db: database
                };

                if (!has(serverCfg, 'socket')) {
                    cfg.host = serverCfg.host;
                    cfg.port = serverCfg.port || 3306;
                } else {
                    cfg.unixSocket = serverCfg.socket;
                }

                const db = new Connection(cfg);

                if (this._status) this._status.increment('dataSourceConnects');
                this._log.trace('connecting to "' + (serverCfg.host ? serverCfg.host : 'socket') + '/' + database + '"');

                db.connection.on('error', (err) => {
                    this._log.warn(err, 'Connection error, destroying connection to "' + (serverCfg.host ? serverCfg.host : 'socket') + '/' + database + '"');
                    pool.destroy(db);
                });

                db.connect((err) => {
                    if (err) return callback(err);
                    return callback(null, db);
                });
            },
            destroy: connection => connection.close(),
            validate: connection => connection.isConnected()
        });

        if (this._config.servers[server].queryTimeout) {
            pool.flora = { queryTimeout: this._config.servers[server].queryTimeout };
        }

        if (typeof this._pools[server] !== 'object') this._pools[server] = {};
        this._pools[server][database] = pool;
        return pool;
    }

    /**
     * Low-level query function. Subsequent calls may
     * may use different connections from connection pool.
     *
     * @param {string} db
     * @param {string} sql
     * @param {Function} callback
     */
    query(server, db, sql, callback) {
        const pool = this._getConnectionPool(server, db);

        pool.acquire((connectionErr, connection) => {
            let queryTimeout;

            if (connectionErr) return callback(connectionErr);

            if (pool.flora && pool.flora.queryTimeout && sql.toLowerCase().indexOf('select') === 0) {
                queryTimeout = setTimeout(() => {
                    queryTimeout = null;
                    pool.release(connection);
                    callback(new Error('Query execution was interrupted (query timeout exceeded).'));
                }, pool.flora.queryTimeout);
            }

            if (this._status) this._status.increment('dataSourceQueries');
            this._log.trace({ sql }, 'executing query');

            return connection.query(sql, (queryError, result) => {
                if (queryError) {
                    // remove connection from pool on connection problems
                    // http://dev.mysql.com/doc/refman/5.6/en/error-messages-client.html
                    if (queryError && queryError.code >= 2000 && queryError.code < 2100) {
                        pool.destroy(connection);
                    } else pool.release(connection);
                    callback(queryError);
                    return;
                }

                if (queryTimeout !== null) {
                    clearTimeout(queryTimeout);
                    pool.release(connection);
                    callback(null, result);
                }
            });
        });
    }

    /**
     * We cannot safely rely on getting same connection from
     * pool to run 'SELECT FOUND_ROWS()' after original query.
     *
     * @param {string} server
     * @param {string} db
     * @param {string} sql
     * @param {Function} callback
     * @return {*}
     * @private
     */
    _paginatedQuery(server, db, sql, callback) {
        if (this._queryFnPool[server] && this._queryFnPool[server][db]) {
            return this._queryFnPool[server][db](sql, callback);
        }

        const queryFn = this._getConnectionPool(server, db).pooled((connection, sqlQuery, cb) => {
            if (this._status) this._status.increment('dataSourceQueries');
            this._log.trace({ sql }, 'executing paginated query');

            connection.query(sqlQuery, (queryError, rows) => {
                if (queryError) return cb(queryError);

                if (this._status) this._status.increment('dataSourceQueries');
                const result = { data: rows };
                return runQuery(connection, 'SELECT FOUND_ROWS() AS totalCount', (err, paginationInfo) => {
                    if (err) return cb(err);
                    result.totalCount = parseInt(paginationInfo[0].totalCount, 10);
                    return cb(null, result);
                });
            });
        });

        if (typeof this._queryFnPool[server] !== 'object') this._queryFnPool[server] = {};
        this._queryFnPool[server][db] = queryFn;
        return queryFn(sql, callback);
    }
}

module.exports = DataSource;
