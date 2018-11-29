'use strict';

const dns = require('dns').promises;
const { EventEmitter } = require('events');
const has = require('has');

function indexServers(servers, server) {
    servers[server.name] = server;
    return servers;
}

/**
 * Extract MySQL master/slave servers from DNS SRV entries
 */
class DnsHealthCheck extends EventEmitter {
    /**
     * @param {Object}  opts
     * @param {string}  opts.masters
     * @param {string=} opts.slaves
     * @param {number}  [opts.timeout=10000]
     */
    constructor(opts) {
        super();

        if (typeof opts !== 'object' || !has(opts, 'masters')) {
            throw new Error('\'masters\' option must be set!');
        }

        this._services = { master: opts.masters };
        if (has(opts, 'slaves')) this._services.slave = opts.slaves;

        this._timeout = opts.timeout || 10000;
        this._intervals = [];
    }

    start() {
        this._intervals = Object.keys(this._services)
            .map((type) => {
                const service = this._services[type];
                return setInterval(() => this._check({ type, service }), this._timeout);
            });

        const promises = Object.keys(this._services)
            .map(type => this._check({ type, service: this._services[type], initial: true }));

        return Promise.all(promises)
            .then(([masters, slaves]) => {
                this._servers = { masters: masters.reduce(indexServers, {}) };
                if (slaves) this._servers.slaves = slaves.reduce(indexServers, {});

                // TODO: replace by deepClone method?
                return JSON.parse(JSON.stringify(this._servers)); // TODO: expose all keys?!?
            });
    }

    stop() {
        if (!this._intervals.length) return;

        this._intervals.forEach(clearInterval);
        this._intervals = [];
        this._servers = {};
    }

    async _check({ type, service, initial = false }) {
        try {
            const response = await dns.resolveSrv(service);
            if (initial) return response;

            const servers = this._servers[type + 's'];
            const availableServers = response.reduce(indexServers, {});

            Object.keys(availableServers)
                .filter(server => !has(servers, server))
                .forEach((server) => {
                    this.emit('add', type, server);
                    servers[server] = availableServers[server];
                });

            Object.keys(servers)
                .filter(server => !has(availableServers, server))
                .forEach((server) => {
                    this.emit('remove', type, server);
                    delete servers[server];
                });

            return response;
        } catch (e) {
            throw e;
        }
    }
}

module.exports = DnsHealthCheck;
