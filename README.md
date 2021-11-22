# flora-mysql

![](https://github.com/godmodelabs/flora-mysql/workflows/ci/badge.svg)
[![NPM version](https://img.shields.io/npm/v/flora-mysql.svg?style=flat)](https://www.npmjs.com/package/flora-mysql)
[![NPM downloads](https://img.shields.io/npm/dm/flora-mysql.svg?style=flat)](https://www.npmjs.com/package/flora-mysql)


MySQL data source for [Flora](https://github.com/godmodelabs/flora), based on the [mysql](https://www.npmjs.com/package/mysql) module.

## Usage

### Add flora-mysql to Flora config

```js
module.exports = {
    …
    dataSources: {
        mysql: {
            constructor: require('flora-mysql'),
            options: {
                onConnect: ['SET SESSION max_execution_time = 30000'],
                servers: {
                    default: {
                        user: 'dbuser',
                        password: '…',
                        masters: [{ host: 'db01' }],
                        slaves: [{ host: 'db02' }, { host: 'db03' }],
                    }
                    specialServer: {
                        user: 'dbuser2',
                        password: '…',
                        masters: [{ host: 'specialdb01' }],
                    },
                },
            },
        },
        …
};
```

### Use data source in resources

`SELECT` queries may be executed on one of the `slaves` servers (if present), unless `useMaster` is set to `true`.

```js
// Get a Context instance
const db = api.dataSources.mysql.getContext({
    db: 'users', // database name
    // server: 'default', // default: 'default'
    // useMaster: false, // default: false
});

// Query
const allRows = await db.query(
    'SELECT firstname, lastname FROM profiles'
);

// Query with parameters
const someRows = await db.query(
    'SELECT firstname, lastname FROM profiles WHERE id = ?',
    [ 1000 ]
);

// Query with named parameters
const someRows = await db.query(
    'SELECT firstname, lastname FROM profiles WHERE id = :userId',
    { userId: 1000 }
);

// Single column
const ids = await db.queryCol(
    'SELECT id FROM profiles WHERE lastname = "Smith"'
);

// Single row (first result)
const { firstname, lastname } = await db.queryRow(
    'SELECT firstname, lastname FROM profiles WHERE id = 1000'
);

// Single value (first result)
const firstname = await db.queryOne(
    'SELECT firstname FROM profiles WHERE id = 1000'
);
```

### Insert, update, delete

Write queries are executed on `master` servers automatically, even when `useMaster` was set to `false` in `getContext`.

```js
// Insert a row
db.insert('profiles', {
    firstname: 'Max',
    lastname: 'Mustermann',
    updatedAt: db.raw('NOW()'), // pass through unescaped
});

// Upsert (insert or update)
db.upsert(
    'profiles', 
    { id: 1000, firstname: 'Max' },
    [ 'firstname' ] // Update firstname to "Max" if profile with id 1000 already exists
);

// Update
db.update(
    'profiles',
    { updatedAt: db.raw('NOW()') }, // SET updatedAt=NOW()
    { id: 1000 } // WHERE id=1000
);

// Delete
db.delete('profiles', { id: 1000 });
```

### Transactions

```js
const trx = await db.transaction();
try {
    await trx.update('profiles', …);
    await trx.insert('log', …);
    await trx.commit();
} catch (err) {
    try {
        // Ignore rollback errors
        await trx.rollback();
    } catch (ignoreErr) { }
    throw err;
}

// Same as above, but shorter:
await db.transaction(async (trx) => {
    await trx.update('profiles', …);
    await trx.insert('log', …);
});
```

## Features

- When being used as data source for resource-processor, SQL queries are optimized and only the columns needed are selected.

## License

[MIT](LICENSE)
