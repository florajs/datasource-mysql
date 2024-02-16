'use strict';

const mysql = require('mysql');
const [, , host, port, database] = process.argv;

const connection = mysql.createConnection({ user: 'root', database, host, port });

connection.once('error', () => process.exit(1));
connection.query('SELECT 1 FROM dual', (err) => process.exit(err ? 1 : 0));
