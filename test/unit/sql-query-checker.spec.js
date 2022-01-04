'use strict';

const { expect } = require('chai');
const { Parser } = require('@florajs/sql-parser');
const check = require('../../lib/sql-query-checker');

describe('SQL query checker', () => {
    const parser = new Parser();
    let ast;

    it('should not throw an error if all columns are fully qualified with table(s)', () => {
        ast = parser.parse('select t.col1, t.col2 from t');
        expect(() => check(ast)).to.not.throw(Error);
    });

    it('should not throw an error if columns are not fully qualified and query contains only one table', () => {
        ast = parser.parse('select col1, col2 from t');
        expect(() => check(ast)).to.not.throw(Error);
    });

    it('should throw an error if SELECT expression contains item(s) without fully qualified table(s)', () => {
        ast = parser.parse('select col1, t1.col2 from t join t2 on t1.id = t2.id');
        expect(() => check(ast)).to.throw(Error, 'Column "col1" must be fully qualified');
    });

    it('should throw an error if JOINs contain columns without table', () => {
        const sql = 'select t1.col1, t2.col1 from t1 join t2 on t1.id = t1_pk_id';

        ast = parser.parse(sql);
        expect(() => check(ast)).to.throw(Error, 'Column "t1_pk_id" must be fully qualified');
    });

    it('should resolve nested expressions', () => {
        const sql = `select case func(t1.col1, t2.col2) when 1 then 'one' else other_func(attr) end
                    from t1
                    join t2 on t1.id = t2.id`;

        ast = parser.parse(sql);
        expect(() => check(ast)).to.throw(Error, 'Column "attr" must be fully qualified');
    });

    it('should check where clause', () => {
        const sql = `
            SELECT
                t1.id,
                l10n.value
            FROM t1
            LEFT JOIN t1_l10n l10n ON t1.interest = l10n.id
            WHERE locale = 'de'`;

        ast = parser.parse(sql);
        expect(() => check(ast)).to.throw(Error, 'Column "locale" must be fully qualified');
    });
});
