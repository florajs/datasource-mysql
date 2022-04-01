'use strict';

module.exports = async function tableWithAutoIncrement(ctx, tableName, callback) {
    await ctx.exec(`DROP TABLE IF EXISTS ${tableName}`);
    await ctx.exec(`
        CREATE TABLE ${tableName} (
          id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
          col1 VARCHAR(50) DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    `);

    return callback();
};
