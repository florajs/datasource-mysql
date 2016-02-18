'use strict';

var Promise = require('when').Promise,
    poolModule = require('generic-pool'),
    Parser = require('flora-sql-parser').Parser,
    astUtil = require('flora-sql-parser').util,
    generateAST = require('./lib/sql-query-builder'),
    checkAST = require('./lib/sql-query-checker'),
    optimizeAST = require('./lib/sql-query-optimizer'),
    Connection = require('./lib/connection');

/**
 * @constructor
 * @param {Api} api
 * @param {Object} config
 */
var DataSource = module.exports = function (api, config) {
    this._log = api.log.child({component: 'flora-mysql'});
    this._parser = new Parser();
    this._config = config;
    this._pools = {};
    this._queryFnPool = {}; // cache query functions for pagination queries (see _paginatedQuery function)
    this._status = config._status;

    if (this._status) {
        var self = this;
        this._status.onStatus(function () {
            var stats = {};

            Object.keys(self._pools).forEach(function (server) {
                if (!stats[server]) stats[server] = {};
                Object.keys(self._pools[server]).forEach(function (db) {
                    var pool = self._getConnectionPool(server, db);

                    stats[server][db] = {
                        open: pool.getPoolSize(),
                        sleeping: pool.availableObjectsCount(),
                        waiting: pool.waitingClientsCount()
                    };
                });
            });

            this.set('pools', stats);
        });
    }
};

/**
 * Add property queryAST to DataSource config.
 *
 * @param {Object} dsConfig DataSource config object
 * @param {Array.<string>=} attributes List of resource config attributes mapped to DataSource.
 */
DataSource.prototype.prepare = function (dsConfig, attributes) {
    var ast;

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
            columns: Array.isArray(attributes) ? attributes.map(function (attribute) {
                return {
                    expr: { type: 'column_ref', table: dsConfig.table, column: attribute },
                    as: null
                };
            }) : '',
            from: [{ db: null, table: dsConfig.table, as: null }],
            where: null,
            groupby: null,
            having: null,
            orderby: null,
            limit: null
        };
    }

    dsConfig.queryAST = ast;
};

/**
 * Create SQL statement from flora request and run query against database.
 *
 * @param {Object} request
 * @param {Function} callback
 */
DataSource.prototype.process = function (request, callback) {
    var server = request.server || 'default',
        db = request.database,
        sql;

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
        this.query(server, db, sql, function (err, result) {
            if (err) return callback(err);
            callback(null, { totalCount: null, data: result });
        });
    } else {
        this._paginatedQuery(server, db, sql, callback);
    }
};

/**
 * @param {Function} callback
 */
DataSource.prototype.close = function (callback) {
    var connectionPools = [];

    function drain(pool) {
        return new Promise(function (resolve) {
            pool.drain(function () {
                pool.destroyAllNow(resolve);
            });
        });
    }

    for (var server in this._pools) {
        for (var database in this._pools[server]) {
            this._log.debug('closing MySQL pool "%s" at "%s"', database, server);
            connectionPools.push(drain(this._pools[server][database]));
        }
    }

    Promise.all(connectionPools).then(callback);
};

/**
 * @param {string} server
 * @param {string} database
 * @return {Object}
 * @private
 */
DataSource.prototype._getConnectionPool = function (server, database) {
    var pool;
    var self = this;

    if (this._pools[server] && this._pools[server][database]) return this._pools[server][database];

    this._log.trace('creating MySQL pool "%s"', database);

    pool = poolModule.Pool({
        name: database,
        max: this._config.servers[server].poolSize || this._config.poolSize || 10,
        idleTimeoutMillis: 30000,
        create: function (callback) {
            var serverCfg = self._config.servers[server],
                cfg = {
                    user: serverCfg.user,
                    password: serverCfg.password,
                    db: database
                },
                db;

            if (!serverCfg.hasOwnProperty('socket')) {
                cfg.host = serverCfg.host;
                cfg.port = serverCfg.port || 3306;
            } else {
                cfg.unixSocket = serverCfg.socket;
            }

            db = new Connection(cfg);

            if (self._status) self._status.increment('dataSourceConnects');
            self._log.trace('connecting to "' + (serverCfg.host ? serverCfg.host : 'socket') + '/' + database + '"');

            db.connect(function (err) {
                if (err) return callback(err);
                callback(null, db);
            });
        },
        destroy: function (connection) {
            connection.close();
        },
        validate: function (connection) {
            return connection.isConnected();
        }
    });

    if (this._config.servers[server].queryTimeout) {
        pool.flora = { queryTimeout: this._config.servers[server].queryTimeout };
    }

    if (typeof this._pools[server] !== 'object') this._pools[server] = {};
    this._pools[server][database] = pool;
    return pool;
};

/**
 * Low-level query function. Subsequent calls may
 * may use different connections from connection pool.
 *
 * @param {string} db
 * @param {string} sql
 * @param {Function} callback
 */
DataSource.prototype.query = function (server, db, sql, callback) {
    var pool = this._getConnectionPool(server, db);
    var self = this;

    pool.acquire(function (connectionErr, connection) {
        var queryTimeout;

        if (connectionErr) return callback(connectionErr);

        if (pool.flora && pool.flora.queryTimeout && sql.toLowerCase().indexOf('select') === 0) {
            queryTimeout = setTimeout(function handleQueryTimeout() {
                queryTimeout = null;
                pool.release(connection);
                callback(new Error('Query execution was interrupted (query timeout exceeded).'));
            }, pool.flora.queryTimeout);
        }

        if (self._status) self._status.increment('dataSourceQueries');
        self._log.trace({sql: sql}, 'executing query');

        connection.query(sql, function (queryError, result) {
            if (queryError) {
                // remove connection from pool on connection problems
                // http://dev.mysql.com/doc/refman/5.6/en/error-messages-client.html
                if (queryError && queryError.code >= 2000 && queryError.code < 2100) pool.destroy(connection);
                else pool.release(connection);
                return callback(queryError);
            }

            if (queryTimeout !== null) {
                clearTimeout(queryTimeout);
                pool.release(connection);
                callback(null, result);
            }
        });
    });
};

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
DataSource.prototype._paginatedQuery = function (server, db, sql, callback) {
    var queryFn;
    var self = this;

    if (this._queryFnPool[server] && this._queryFnPool[server][db]) return this._queryFnPool[server][db](sql, callback);

    queryFn = this._getConnectionPool(server, db).pooled(function (connection, sqlQuery, cb) {
        if (self._status) self._status.increment('dataSourceQueries');
        self._log.trace({sql: sql}, 'executing paginated query');

        connection.query(sqlQuery, function (queryError, rows) {
            var result;

            if (queryError) return cb(queryError);

            if (self._status) self._status.increment('dataSourceQueries');
            result = { data: rows };
            runQuery(connection, 'SELECT FOUND_ROWS() AS totalCount', function (err, paginationInfo) {
                if (err) return cb(err);
                result.totalCount = parseInt(paginationInfo[0].totalCount, 10);
                cb(null, result);
            });
        });
    });

    if (typeof this._queryFnPool[server] !== 'object') this._queryFnPool[server] = {};
    this._queryFnPool[server][db] = queryFn;
    queryFn(sql, callback);
};

/**
 * @param {Object} request
 * @return {string}
 */
function buildSql(request) {
    /** @type {Object} */
    var ast = generateAST(request);

    checkSqlEquivalents(request.attributes, ast.columns);

    if (request.page) {
        if (! Array.isArray(ast.options)) ast.options = [];
        ast.options.push('SQL_CALC_FOUND_ROWS');
    }

    optimizeAST(ast, request.attributes);

    return astUtil.astToSQL(ast);
}

/**
 * Check if each attribute has a SQL counterpart.
 *
 * @param {Array.<string>} attributes
 * @param {Array.<string>} columns
 * @private
 */
function checkSqlEquivalents(attributes, columns) {
    attributes.forEach(function (attribute) {
        if (! hasSqlEquivalent(attribute, columns)) {
            throw new Error('Attribute "' + attribute + '" is not provided by SQL query');
        }
    });
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
    return columns.some(function checkAttributeSqlEquivalent(column) {
        return column.expr.column === attribute || column.as === attribute;
    });
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
