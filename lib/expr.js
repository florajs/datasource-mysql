'use strict';

class Expr {
    /**
     * SQL expressions are not escaped
     *
     * @param {string} expr
     */
    constructor(expr) {
        this.expr = expr;
    }

    // noinspection JSUnusedGlobalSymbols
    toSqlString() {
        return this.expr;
    }
}

module.exports = Expr;
