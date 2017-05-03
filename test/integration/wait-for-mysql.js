'use strict';

const mysql = require('mysql2');
const connection = mysql.createConnection({ // TODO: use environment variables
    user: 'root',
    database: 'flora_mysql_testdb',
    host: 'mysql'
});

connection.once('error', () => {
    process.exit(1);
});

connection.query('SELECT * FROM t LIMIT 1', (err) => {
    if (err) process.exit(1);
    process.exit(0);
});

/*
const RETRY_TIMEOUT = 1000;

function retry() {
    setTimeout(waitForMySQL, RETRY_TIMEOUT);
}

function waitForMySQL() {
    const connection = mysql.createConnection({ // TODO: use environment variables
        user: 'root',
        database: 'flora_mysql_testdb',
        host: 'mysql'
    });

    connection.once('error', retry);
    connection.query('SELECT * FROM t LIMIT 1', (err) => {
        if (err) return retry();
        console.log('Successfully connected to MySQL');
        process.exit(0);
    });
}

setTimeout(waitForMySQL, 3000);
*/
