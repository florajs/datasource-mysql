'use strict';

var poolModule = require('generic-pool'),
    Parser = require('flora-sql-parser').Parser,
    astUtil = require('flora-sql-parser').util,
    async = require('async'),
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
            // Fix pegjs throwing SyntaxErrors without stack
            if (!e.stack) Error.captureStackTrace(e, e.constructor);

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
    var db = request.database,
        sql;

    try {
        sql = buildSql(request);
    } catch (e) {
        return callback(e);
    }

    this._log.trace({sql: sql}, 'processing request');

    if (! request.page) {
        this.query(db, sql, function (err, result) {
            if (err) return callback(err);
            callback(null, { totalCount: null, data: result });
        });
    } else {
        this._paginatedQuery(db, sql, callback);
    }
};

/**
 * @param {Function} callback
 */
DataSource.prototype.close = function (callback) {
    var self = this;

    async.parallel(Object.keys(this._pools).map(function (database) {
        return function (next) {
            self._log.trace('closing MySQL pool "%s"', database);
            self._pools[database].drain(function () {
                self._pools[database].destroyAllNow();
                next();
            });
        };
    }), callback);
};

/**
 * @param {string} database
 * @return {Object}
 * @private
 */
DataSource.prototype._getConnectionPool = function (database) {
    var pool;
    var self = this;

    if (this._pools[database]) return this._pools[database];

    this._log.trace('creating MySQL pool "%s"', database);

    pool = poolModule.Pool({
        name: database,
        max: this._config.server.poolSize || 10,
        idleTimeoutMillis: 30000,
        create: function (callback) {
            var db = new Connection({
                host: self._config.server.host,
                user: self._config.server.user,
                password: self._config.server.password,
                db: database
            });
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

    pool.flora = { queryTimeout: this._config.server.queryTimeout || 60000 };
    this._pools[database] = pool;
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
DataSource.prototype.query = function (db, sql, callback) {
    var pool = this._getConnectionPool(db);

    pool.acquire(function (connectionErr, connection) {
        var queryTimeout;

        if (connectionErr) return callback(connectionErr);

        if (sql.toLowerCase().indexOf('select') === 0) {
            queryTimeout = setTimeout(function handleQueryTimeout() {
                queryTimeout = null;
                pool.release(connection);
                callback(new Error('Query execution was interrupted (query timeout exceeded).'));
            }, pool.flora.queryTimeout);
        }

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
 * @param {string} db
 * @param {string} sql
 * @param {Function} callback
 * @return {*}
 * @private
 */
DataSource.prototype._paginatedQuery = function (db, sql, callback) {
    var queryFn;

    if (this._queryFnPool[db]) return this._queryFnPool[db](sql, callback);

    queryFn = this._getConnectionPool(db).pooled(function (connection, sqlQuery, cb) {
        connection.query(sqlQuery, function (queryError, rows) {
            var result;

            if (queryError) return cb(queryError);

            result = { data: rows };
            runQuery(connection, 'SELECT FOUND_ROWS() AS totalCount', function (err, paginationInfo) {
                if (err) return cb(err);
                result.totalCount = parseInt(paginationInfo[0].totalCount, 10);
                cb(null, result);
            });
        });
    });

    this._queryFnPool[db] = queryFn;
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

    ast = optimizeAST(ast, request.attributes);

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
