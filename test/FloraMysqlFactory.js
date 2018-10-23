'use strict';

const bunyan = require('bunyan');

const FloraMysql = require('../index');

// mock Api instance
const log = bunyan.createLogger({ name: 'null', streams: [] });
const api = { log };

const defaultCfg = {
    servers: {
        default: { host: 'mysql', user: 'root', password: '' }
    }
};

class FloraMysqlFactory {
    static create(cfg) {
        return new FloraMysql(api, cfg || defaultCfg)
    }
}

module.exports = {
    FloraMysqlFactory,
    defaultCfg
};
