'use strict';

// see https://raw.githubusercontent.com/mysqljs/mysql/master/tool/free-port.js

const Net = require('net');

const PORT_END    = 60000;
const PORT_START  = 1000;
const TCP_TIMEOUT = 1000;

process.nextTick(run);

function check(port, callback) {
    const socket = Net.createConnection(port, 'localhost');
    const timer  = setTimeout(() => {
        socket.destroy();
        callback(undefined);
    }, TCP_TIMEOUT);

    socket.on('connect', () => {
        clearTimeout(timer);
        socket.destroy();
        callback(true);
    });

    socket.on('error', (err) => {
        clearTimeout(timer);
        if (err.syscall === 'connect' && err.code === 'ECONNREFUSED') {
            callback(false);
        } else {
            callback(undefined);
        }
    });
}

function run() {
    function next() {
        const port = PORT_START + Math.floor(Math.random() * (PORT_END - PORT_START + 1));

        check(port, function (used) {
            if (used === false) {
                console.log('%d', port);
                process.exit(0);
            } else {
                setTimeout(next, 0);
            }
        });
    }

    next();
}
