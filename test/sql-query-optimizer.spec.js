'use strict';

var expect = require('chai').expect,
    optimize = require('../lib/sql-query-optimizer'),
    _ = require('lodash');

describe('SQL query optimizer', function () {
    var ast;

    it('should only modify SELECT statements', function () {
        var nonOptimized;

        ast = { type: 'UPDATE' };
        nonOptimized = _.clone(ast);
        optimize(ast, ['col1']);

        expect(nonOptimized).to.eql(ast);
    });

    it('should remove unused columns/attributes from AST', function () {
        ast = {
            type: 'select',
            columns: [
                { expr: { type: 'column_ref', table: 't', column: 'col1' }, as: null },
                { expr: { type: 'column_ref', table: 't', column: 'col2' }, as: null },
                { expr: { type: 'column_ref', table: 't', column: 'col3' }, as: 'alias' },
                { expr: { type: 'column_ref', table: 't', column: 'col4' }, as: null }
            ],
            from: [{ db: null, table: 't1', as: null }],
            where: null,
            groupby: null,
            orderby: null
        };

        optimize(ast, ['col1', 'alias']);
        expect(ast.columns).to.eql([
            { expr: { type: 'column_ref', table: 't', column: 'col1' }, as: null },
            { expr: { type: 'column_ref', table: 't', column: 'col3' }, as: 'alias' }
        ]);
    });

    it('should not remove INNER JOINs from AST', function () {
        ast = {
            type: 'select',
            columns: [
                { expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: null },
                { expr: { type: 'column_ref', table: 't2', column: 'col2' }, as: null }
            ],
            from: [
                { db: null, table: 't1', as: null },
                { db: null, table: 't2', as: null, join: 'INNER JOIN', on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'id' },
                    right: { type: 'column_ref', table: 't2', column: 'id' }
                }}
            ],
            where: null,
            groupby: null,
            orderby: null
        };

        optimize(ast, ['col1']);
        expect(ast.from).to.eql(ast.from);
    });

    it('should remove unreferenced LEFT JOINs from AST', function () {
        // should remove LEFT JOIN on t2 because col2 attribute is not requested
        ast = {
            type: 'select',
            columns: [
                { expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: null },
                { expr: { type: 'column_ref', table: 't2', column: 'col2' }, as: null }
            ],
            from: [
                { db: null, table: 't', as: null },
                { db: null, table: 't2', as: null, join: 'LEFT JOIN', on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'id' },
                    right: { type: 'column_ref', table: 't2', column: 'id' }
                }}
            ],
            where: null,
            groupby: null,
            orderby: null
        };

        optimize(ast, ['col1']);
        expect(ast.from).to.eql([{ db: null, table: 't', as: null }]);
    });

    it('should pay attention to table aliases', function () {
        // t3 must not be removed because it's used by it's alias
        ast = {
            type: 'select',
            columns: [
                { expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: null },
                { expr: { type: 'column_ref', table: 't2', column: 'col2' }, as: null },
                { expr: { type: 'column_ref', table: 'alias', column: 'col3' }, as: null }
            ],
            from: [
                { db: null, table: 't', as: null },
                { db: null, table: 't2', as: null, join: 'LEFT JOIN', on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'id' },
                    right: { type: 'column_ref', table: null, column: 'id' }
                }},
                { db: null, table: 't3', as: 'alias', join: 'LEFT JOIN', on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'id' },
                    right: { type: 'column_ref', table: 'alias', column: 'id' }
                }}
            ],
            where: null,
            groupby: null,
            orderby: null
        };

        optimize(ast, ['col1', 'col3']);
        expect(ast.from).to.eql([
            { db: null, table: 't', as: null },
            { db: null, table: 't3', as: 'alias', join: 'LEFT JOIN', on: {
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
            columns: [{ expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: null }],
            from: [
                { db: null, table: 't', as: null },
                { db: null, table: 't2', as: null, join: 'LEFT JOIN', on: {
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
            groupby: null,
            orderby: null
        };

        optimize(ast, ['col1']);
        expect(ast.from).to.eql(ast.from);
    });

    it('should not remove LEFT JOIN if table is referenced in GROUP BY clause', function () {
        var nonOptimized;

        ast = {
            type: 'select',
            columns: [{ expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: null }],
            from: [
                { db: null, table: 't', as: null },
                { db: null, table: 't2', as: null, join: 'LEFT JOIN', on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'id' },
                    right: { type: 'column_ref', table: 't2', column: 'id' }
                }}
            ],
            where: null,
            groupby: [
                { type: 'column_ref', table: 't1', column: 'col1' },
                { type: 'column_ref', table: 't2', column: 'col2' }
            ],
            orderby: null
        };

        nonOptimized = _.cloneDeep(ast);

        optimize(ast, ['col1']);
        expect(ast.from).to.eql(nonOptimized.from);
    });

    it('should not remove LEFT JOIN if table is referenced in ORDER BY clause', function () {
        var nonOptimized;

        ast = {
            type: 'select',
            columns: [{ expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: null }],
            from: [
                { db: null, table: 't', as: null },
                { db: null, table: 't2', as: null, join: 'LEFT JOIN', on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'id' },
                    right: { type: 'column_ref', table: 't2', column: 'id' }
                }}
            ],
            where: null,
            groupby: null,
            orderby: [
                { expr: { type: 'column_ref', table: 't1', column: 'col1' }, type: 'ASC' },
                { expr: { type: 'column_ref', table: 't2', column: 'col2' }, type: 'DESC' }
            ]
        };
        nonOptimized = _.cloneDeep(ast);

        optimize(ast, ['col1']);
        expect(ast.from).to.eql(nonOptimized.from);
    });

    it('should not remove "parent" table/LEFT JOIN if "child" table/LEFT JOIN is needed', function () {
        ast = {
            type: 'select',
            distinct: null,
            columns: [
                { expr: { type: 'column_ref', table: 'instrument', column: 'id' }, as: null },
                { expr: { type: 'column_ref', table: 'ctde', column: 'name' }, as: 'nameDE' },
                { expr: { type: 'column_ref', table: 'cten', column: 'name' }, as: 'nameEN' }
            ],
            from: [
                { db: null, table: 'instrument', as: null },
                { db: null, table: 'country', as: 'c', join: 'LEFT JOIN', on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 'instrument', column: 'countryId' },
                    right: { type: 'column_ref', table: 'c', column: 'id' }
                }},
                { db: null, table: 'country_translation', as: 'ctde', join: 'LEFT JOIN', on: {
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
                { db: null, table: 'country_translation', as: 'cten', join: 'LEFT JOIN', on: {
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
            where: null,
            groupby: null,
            orderby: null,
            limit: null
        };

        optimize(ast, ['id', 'nameDE']);
        expect(ast.from).to.eql([
            { db: null, table: 'instrument', as: null },
            { db: null, table: 'country', as: 'c', join: 'LEFT JOIN', on: {
                type: 'binary_expr',
                operator: '=',
                left: { type: 'column_ref', table: 'instrument', column: 'countryId' },
                right: { type: 'column_ref', table: 'c', column: 'id' }
            }},
            { db: null, table: 'country_translation', as: 'ctde', join: 'LEFT JOIN', on: {
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
