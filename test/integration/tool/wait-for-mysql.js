'use strict';

const mysql = require('mysql');
const [,, host, port, database] = process.argv;

const connection = mysql.createConnection({ user: 'root', database, host, port });

connection.once('error', () => process.exit(1));
connection.query('SELECT * FROM t LIMIT 1', err => process.exit(err ? 1 : 0));
