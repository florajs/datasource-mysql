'use strict';

var expect = require('chai').expect,
    Parser = require('flora-sql-parser').Parser,
    check  = require('../lib/sql-query-checker');

describe('SQL query checker', function () {
    var ast, parser = new Parser();

    it('should not throw an error if all columns are fully qualified with table(s)', function () {
        ast = parser.parse('select t.col1, t.col2 from t');
        expect(function () {
            check(ast);
        }).to.not.throw(Error);
    });

    it('should not throw an error if columns are not fully qualified and query contains only one table', function () {
        ast = parser.parse('select col1, col2 from t');
        expect(function () {
            check(ast);
        }).to.not.throw(Error);
    });

    it('should throw an error if SELECT expression contains item(s) without fully qualified table(s)', function () {
        ast = parser.parse('select col1, t1.col2 from t join t2 on t1.id = t2.id');
        expect(function () {
            check(ast);
        }).to.throw(Error, 'Column "col1" must be fully qualified');
    });

    it('should throw an error if JOINs contain columns without table', function () {
        var sql = 'select t1.col1, t2.col1 from t1 join t2 on t1.id = t1_pk_id';

        ast = parser.parse(sql);
        expect(function () {
            check(ast);
        }).to.throw(Error, 'Column "t1_pk_id" must be fully qualified');
    });

    it('should resolve nested expressions', function () {
        var sql = ['select case func(t1.col1, t2.col2) when 1 then \'one\' else other_func(attr) end',
                    'from t1 ',
                    'join t2 on t1.id = t2.id'];

        ast = parser.parse(sql.join(' '));
        expect(function () {
            check(ast);
        }).to.throw(Error, 'Column "attr" must be fully qualified');
    });

});
