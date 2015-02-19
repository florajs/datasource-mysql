'use strict';

var expect = require('chai').expect,
    optimize = require('../lib/sql-query-optimizer');

describe('SQL query optimizer', function () {
    var ast, optimized;

    it('should clone AST', function () {
        ast = {
            type: 'select',
            columns: [{ expr: { type: 'column_ref', table: 't', column: 'col1' }, as: '' }],
            from: [{ db: '', table: 't1', as: '' }],
            where: '',
            groupby: '',
            orderby: '',
            limit: ''
        };

        optimized = optimize(ast, ['col1']);
        expect(optimized).to.not.equal(ast);
    });

    it('should only modify SELECT statements', function () {
        ast = { type: 'UPDATE' };
        optimized = optimize(ast, ['col1']);
        expect(optimized).to.eql(ast);
    });

    it('should remove unused columns/attributes from AST', function () {
        ast = {
            type: 'select',
            columns: [
                { expr: { type: 'column_ref', table: 't', column: 'col1' }, as: '' },
                { expr: { type: 'column_ref', table: 't', column: 'col2' }, as: '' },
                { expr: { type: 'column_ref', table: 't', column: 'col3' }, as: 'alias' },
                { expr: { type: 'column_ref', table: 't', column: 'col4' }, as: '' }
            ],
            from: [{ db: '', table: 't1', as: '' }],
            where: '',
            groupby: '',
            orderby: ''
        };

        optimized = optimize(ast, ['col1', 'alias']);
        expect(optimized.columns).to.eql([
            { expr: { type: 'column_ref', table: 't', column: 'col1' }, as: '' },
            { expr: { type: 'column_ref', table: 't', column: 'col3' }, as: 'alias' }
        ]);
    });

    it('should not remove INNER JOINs from AST', function () {
        ast = {
            type: 'select',
            columns: [
                { expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: '' },
                { expr: { type: 'column_ref', table: 't2', column: 'col2' }, as: '' }
            ],
            from: [
                { db: '', table: 't1', as: '' },
                { db: '', table: 't2', as: '', join: 'INNER JOIN', on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'id' },
                    right: { type: 'column_ref', table: 't2', column: 'id' }
                }}
            ],
            where: '',
            groupby: '',
            orderby: ''
        };

        optimized = optimize(ast, ['col1']);
        expect(optimized.from).to.eql(ast.from);
    });

    it('should remove unreferenced LEFT JOINs from AST', function () {
        // should remove LEFT JOIN on t2 because col2 attribute is not requested
        ast = {
            type: 'select',
            columns: [
                { expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: '' },
                { expr: { type: 'column_ref', table: 't2', column: 'col2' }, as: '' }
            ],
            from: [
                { db: '', table: 't', as: '' },
                { db: '', table: 't2', as: '', join: 'LEFT JOIN', on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'id' },
                    right: { type: 'column_ref', table: 't2', column: 'id' }
                }}
            ],
            where: '',
            groupby: '',
            orderby: ''
        };

        optimized = optimize(ast, ['col1']);
        expect(optimized.from).to.eql([{ db: '', table: 't', as: '' }]);
    });

    it('should pay attention to table aliases', function () {
        // t3 must not be removed because it's used by it's alias
        ast = {
            type: 'select',
            columns: [
                { expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: '' },
                { expr: { type: 'column_ref', table: 't2', column: 'col2' }, as: '' },
                { expr: { type: 'column_ref', table: 'alias', column: 'col3' }, as: '' }
            ],
            from: [
                { db: '', table: 't', as: '' },
                { db: '', table: 't2', as: '', join: 'LEFT JOIN', on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'id' },
                    right: { type: 'column_ref', table: '', column: 'id' }
                }},
                { db: '', table: 't3', as: 'alias', join: 'LEFT JOIN', on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'id' },
                    right: { type: 'column_ref', table: 'alias', column: 'id' }
                }}
            ],
            where: '',
            groupby: '',
            orderby: ''
        };

        optimized = optimize(ast, ['col1', 'col3']);
        expect(optimized.from).to.eql([
            { db: '', table: 't', as: '' },
            { db: '', table: 't3', as: 'alias', join: 'LEFT JOIN', on: {
                type: 'binary_expr',
                operator: '=',
                left: { type: 'column_ref', table: 't1', column: 'id' },
                right: { type: 'column_ref', table: 'alias', column: 'id' }
            }}
        ]);
    });

    it('should not remove LEFT JOIN if joined table is referenced in WHERE clause', function () {
        ast = {
            type: 'select',
            columns: [{ expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: '' }],
            from: [
                { db: '', table: 't', as: '' },
                { db: '', table: 't2', as: '', join: 'LEFT JOIN', on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'id' },
                    right: { type: 'column_ref', table: 't2', column: 'id' }
                }}
            ],
            where: {
                type: 'binary_expr',
                operator: 'AND',
                left: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'col1' },
                    right: { type: 'string', value: 'foo' }
                },
                right: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't2', column: 'col' },
                    right: { type: 'string', value: 'foobar' }
                }
            },
            groupby: '',
            orderby: ''
        };

        optimized = optimize(ast, ['col1']);
        expect(optimized.from).to.eql(ast.from);
    });

    it('should not remove LEFT JOIN if table is referenced in GROUP BY clause', function () {
        ast = {
            type: 'select',
            columns: [{ expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: '' }],
            from: [
                { db: '', table: 't', as: '' },
                { db: '', table: 't2', as: '', join: 'LEFT JOIN', on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'id' },
                    right: { type: 'column_ref', table: 't2', column: 'id' }
                }}
            ],
            where: '',
            groupby: [
                { type: 'column_ref', table: 't1', column: 'col1' },
                { type: 'column_ref', table: 't2', column: 'col2' }
            ],
            orderby: ''
        };

        optimized = optimize(ast, ['col1']);
        expect(optimized.from).to.eql(ast.from);
    });

    it('should not remove LEFT JOIN if table is referenced in ORDER BY clause', function () {
        ast = {
            type: 'select',
            columns: [{ expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: '' }],
            from: [
                { db: '', table: 't', as: '' },
                { db: '', table: 't2', as: '', join: 'LEFT JOIN', on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'id' },
                    right: { type: 'column_ref', table: 't2', column: 'id' }
                }}
            ],
            where: '',
            groupby: '',
            orderby: [
                { expr: { type: 'column_ref', table: 't1', column: 'col1' }, type: 'ASC' },
                { expr: { type: 'column_ref', table: 't2', column: 'col2' }, type: 'DESC' }
            ]
        };

        optimized = optimize(ast, ['col1']);
        expect(optimized.from).to.eql(ast.from);
    });

    it('should not remove "parent" table/LEFT JOIN if "child" table/LEFT JOIN is needed', function () {
        ast = {
            type: 'select',
            distinct: '',
            columns: [
                { expr: { type: 'column_ref', table: 'instrument', column: 'id' }, as: '' },
                { expr: { type: 'column_ref', table: 'ctde', column: 'name' }, as: 'nameDE' },
                { expr: { type: 'column_ref', table: 'cten', column: 'name' }, as: 'nameEN' }
            ],
            from: [
                { db: '', table: 'instrument', as: '' },
                { db: '', table: 'country', as: 'c', join: 'LEFT JOIN', on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 'instrument', column: 'countryId' },
                    right: { type: 'column_ref', table: 'c', column: 'id' }
                }},
                { db: '', table: 'country_translation', as: 'ctde', join: 'LEFT JOIN', on: {
                    type: 'binary_expr',
                    operator: 'AND',
                    left: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 'c', column: 'id' },
                        right: { type: 'column_ref', table: 'ctde', column: 'countryId' }
                    },
                    right: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 'ctde', column: 'lang' },
                        right: { type: 'string', value: 'de' }
                    }
                }},
                { db: '', table: 'country_translation', as: 'cten', join: 'LEFT JOIN', on: {
                    type: 'binary_expr',
                    operator: 'AND',
                    left: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 'c', column: 'id' },
                        right: { type: 'column_ref', table: 'cten', column: 'countryId' }
                    },
                    right: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 'cten', column: 'lang' },
                        right: { type: 'string', value: 'en' }
                    }
                }}
            ],
            where: '',
            groupby: '',
            orderby: '',
            limit: ''
        };

        optimized = optimize(ast, ['id', 'nameDE']);
        expect(optimized.from).to.eql([
            { db: '', table: 'instrument', as: '' },
            { db: '', table: 'country', as: 'c', join: 'LEFT JOIN', on: {
                type: 'binary_expr',
                operator: '=',
                left: { type: 'column_ref', table: 'instrument', column: 'countryId' },
                right: { type: 'column_ref', table: 'c', column: 'id' }
            }},
            { db: '', table: 'country_translation', as: 'ctde', join: 'LEFT JOIN', on: {
                type: 'binary_expr',
                operator: 'AND',
                left: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 'c', column: 'id' },
                    right: { type: 'column_ref', table: 'ctde', column: 'countryId' }
                },
                right: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 'ctde', column: 'lang' },
                    right: { type: 'string', value: 'de' }
                }
            }}
        ]);
    });
});
