'use strict';

const { defaultCfg } = require('../FloraMysqlFactory');
const ciCfg = JSON.parse(JSON.stringify(defaultCfg));

ciCfg.servers.default.masters = [{ host: process.env.MYSQL_HOST || 'localhost' }];

module.exports = ciCfg;
