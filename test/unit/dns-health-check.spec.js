'use strict';

const dns = require('dns').promises;

const chai = require('chai');
const { expect } = chai;
const sinon = require('sinon');

const DnsHealthCheck = require('../../lib/dns-health-check');

const masterService = 'master-endpoint.example.com';
const slaveService = 'slave-endpoint.example.com';

const master1 = { name: 'master01.example.com', port: 3306, priority: 1, weight: 0 };
const master2 = { name: 'master02.example.com', port: 3306, priority: 1, weight: 5 };
const slave1 = { name: 'slave01.example.com', port: 3306, priority: 1, weight: 0 };
const slave2 = { name: 'slave02.example.com', port: 3306, priority: 1, weight: 0 };

chai.use(require('sinon-chai'));

describe('dns-health-check', () => {
    let sandbox;
    let checker;

    beforeEach(() => {
        sandbox = sinon.createSandbox({ useFakeTimers: true });
    });

    afterEach(() => {
        if (checker) checker.stop();
        checker = null;
        sandbox.restore();
    });

    it('should exist', () => {
        expect(DnsHealthCheck).to.be.a('function');
    });

    ['on', 'off', 'start', 'stop'].forEach(method => {
        it(`should be expose ${method} method`, () => {
            const checker = new DnsHealthCheck({ masters: masterService });
            expect(checker[method]).to.be.a('function');
        });
    });

    describe('options', () => {
        it('should throw an error if \'masters\' option is not set', () => {
            expect(() => {
                // noinspection JSCheckFunctionSignatures
                new DnsHealthCheck({});
            }).to.throw(Error, '\'masters\' option must be set');
        });

        [
            ['should set default update interval to 10000 milliseconds', {}, 10500],
            ['should set make update interval configurable', { timeout: 3000 }, 3500]
        ].forEach(([description, cfg, milliseconds]) => {
            it(description, async () => {
                const stub = sandbox.stub(dns, 'resolveSrv').resolves([master1, master2]);

                checker = new DnsHealthCheck(Object.assign({}, { masters: masterService }, cfg));
                await checker.start();
                sandbox.clock.tick(milliseconds);

                expect(stub).to.have.been.calledTwice
                    .and.to.have.been.always.calledWith(masterService);
            });
        });
    });

    it('should trigger resolveSrv request to configured master-service', async () => {
        const stub = sandbox.stub(dns, 'resolveSrv').resolves([master1, master2]);
        checker = new DnsHealthCheck({ masters: masterService });

        await checker.start();

        expect(stub).to.have.been.calledWith(masterService);
    });

    it('should trigger resolveSrv request to configured slave-service', async () => {
        const stub = sandbox.stub(dns, 'resolveSrv').resolves([master1, master2]);
        checker = new DnsHealthCheck({ masters: masterService, slaves: slaveService });

        await checker.start();

        expect(stub).to.have.been.calledWith(slaveService);
    });

    it('should resolve servers immediately', async () => {
        const stub = sandbox.stub(dns, 'resolveSrv');
        checker = new DnsHealthCheck({ masters: masterService, slaves: slaveService });

        stub.withArgs(masterService)
            .resolves([master1, master2]);

        stub.withArgs(slaveService)
            .resolves([slave1, slave2]);

        const servers = await checker.start();

        expect(stub).to.have.callCount(2);
        expect(servers).to.have.property('masters')
            .and.to.eql({ 'master01.example.com': master1, 'master02.example.com': master2 });
        expect(servers).to.have.property('slaves')
            .and.to.eql({ 'slave01.example.com': slave1, 'slave02.example.com': slave2 });
    });

    describe('events', () => {
        [
            { eventType: 'add', firstResponse: [master1], secondResponse: [master1, master2] },
            { eventType: 'remove', firstResponse: [master1, master2], secondResponse: [master1] }
        ].forEach(({ eventType, firstResponse, secondResponse }) => {
            it(`should trigger ${eventType} event`, async () => {
                const stub = sandbox.stub(dns, 'resolveSrv');

                stub.onFirstCall()
                    .resolves(firstResponse);

                stub.onSecondCall()
                    .resolves(secondResponse);

                checker = new DnsHealthCheck({ masters: masterService });
                const eventPromise = new Promise((resolve, reject) => {
                    checker.once(eventType, (type, server) => {
                        if (type !== 'master') return reject(new Error('Expected type equal master'));
                        if (server !== 'master02.example.com') return reject(new Error('Expected server to equal master02.example.com'));
                        return resolve();
                    });
                });

                await checker.start();
                sandbox.clock.tick(10500);
                await eventPromise;
            });
        })
    });
});
