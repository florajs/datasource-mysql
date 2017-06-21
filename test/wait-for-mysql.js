'use strict';

const mysql = require('mysql');
const connection = mysql.createConnection({
    user: 'root',
    database: 'flora_mysql_testdb',
    host: 'mysql'
});

connection.once('error', () => process.exit(1));

connection.query('SELECT * FROM t LIMIT 1', (err) => {
    if (err) process.exit(1);
    process.exit(0);
});
