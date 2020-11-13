'use strict';

const { expect } = require('chai');
const optimize = require('../../lib/sql-query-optimizer');

function cloneDeep(ast) {
    return JSON.parse(JSON.stringify(ast));
}

describe('SQL query optimizer', () => {
    let ast;

    it('should only modify SELECT statements', function () {
        ast = { type: 'UPDATE' };
        const initialAST = cloneDeep(ast);
        optimize(ast, ['col1']);

        expect(initialAST).to.eql(ast);
    });

    it('should remove unused columns/attributes from AST', () => {
        // SELECT t.col1, t.col2, t.col3 AS alias, t.col4 FROM t1
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

        const optimizedAst = optimize(ast, ['col1', 'alias']);
        expect(optimizedAst.columns).to.eql([
            // SELECT t.col1, t.col3 AS alias FROM t1
            { expr: { type: 'column_ref', table: 't', column: 'col1' }, as: null },
            { expr: { type: 'column_ref', table: 't', column: 'col3' }, as: 'alias' }
        ]);
    });

    it('should not remove INNER JOINs from AST', () => {
        // SELECT t1.col1, t2.col2 FROM t1 INNER JOIN t2 ON t1.id = t2.id
        ast = {
            type: 'select',
            columns: [
                { expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: null },
                { expr: { type: 'column_ref', table: 't2', column: 'col2' }, as: null }
            ],
            from: [
                { db: null, table: 't1', as: null },
                {
                    db: null,
                    table: 't2',
                    as: null,
                    join: 'INNER JOIN',
                    on: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 't1', column: 'id' },
                        right: { type: 'column_ref', table: 't2', column: 'id' }
                    }
                }
            ],
            where: null,
            groupby: null,
            orderby: null
        };

        const initialAST = cloneDeep(ast);

        const optimizedAst = optimize(ast, ['col1']);
        expect(optimizedAst.from).to.eql(initialAST.from);
    });

    it('should remove unreferenced LEFT JOINs from AST', () => {
        // should remove LEFT JOIN on t2 because col2 attribute is not requested
        // SELECT t1.col1, t2.col2 FROM t LEFT JOIN t2 ON t1.id = t2.id
        ast = {
            type: 'select',
            columns: [
                { expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: null },
                { expr: { type: 'column_ref', table: 't2', column: 'col2' }, as: null }
            ],
            from: [
                { db: null, table: 't', as: null },
                {
                    db: null,
                    table: 't2',
                    as: null,
                    join: 'LEFT JOIN',
                    on: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 't1', column: 'id' },
                        right: { type: 'column_ref', table: 't2', column: 'id' }
                    }
                }
            ],
            where: null,
            groupby: null,
            orderby: null
        };

        const optimizedAst = optimize(ast, ['col1']);
        expect(optimizedAst.from).to.eql([{ db: null, table: 't', as: null }]); // SELECT t1.col1 FROM t
    });

    it('should pay attention to table aliases', () => {
        /**
         * t3 must not be removed because it's used by it's alias
         *
         * SELECT t1.col1, t2.col2, alias.col3
         * FROM t
         *   LEFT JOIN t2 ON t1.id = id
         *   LEFT JOIN t3 AS alias ON t1.id = alias.id
         */
        ast = {
            type: 'select',
            columns: [
                { expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: null },
                { expr: { type: 'column_ref', table: 't2', column: 'col2' }, as: null },
                { expr: { type: 'column_ref', table: 'alias', column: 'col3' }, as: null }
            ],
            from: [
                { db: null, table: 't', as: null },
                {
                    db: null,
                    table: 't2',
                    as: null,
                    join: 'LEFT JOIN',
                    on: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 't1', column: 'id' },
                        right: { type: 'column_ref', table: null, column: 'id' }
                    }
                },
                {
                    db: null,
                    table: 't3',
                    as: 'alias',
                    join: 'LEFT JOIN',
                    on: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 't1', column: 'id' },
                        right: { type: 'column_ref', table: 'alias', column: 'id' }
                    }
                }
            ],
            where: null,
            groupby: null,
            orderby: null
        };

        const optimizedAst = optimize(ast, ['col1', 'col3']);
        expect(optimizedAst.from).to.eql([
            // SELECT t1.col1, alias.col3 FROM t LEFT JOIN t3 AS alias ON t1.id = alias.id
            { db: null, table: 't', as: null },
            {
                db: null,
                table: 't3',
                as: 'alias',
                join: 'LEFT JOIN',
                on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'id' },
                    right: { type: 'column_ref', table: 'alias', column: 'id' }
                }
            }
        ]);
    });

    it('should not remove LEFT JOIN if joined table is referenced in WHERE clause', () => {
        // SELECT t1.col1 FROM t LEFT JOIN t2 ON t1.id = t2.id WHERE t1.col1 = 'foo' AND t2.col = 'foobar'
        ast = {
            type: 'select',
            columns: [{ expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: null }],
            from: [
                { db: null, table: 't', as: null },
                {
                    db: null,
                    table: 't2',
                    as: null,
                    join: 'LEFT JOIN',
                    on: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 't1', column: 'id' },
                        right: { type: 'column_ref', table: 't2', column: 'id' }
                    }
                }
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

        const optimizedAst = optimize(ast, ['col1']);
        expect(optimizedAst.from).to.eql(ast.from);
    });

    it('should not remove LEFT JOIN if table is referenced in GROUP BY clause', () => {
        // SELECT t1.col1 FROM t LEFT JOIN t2 ON t1.id = t2.id GROUP BY t1.col1, t2.col2
        ast = {
            type: 'select',
            columns: [{ expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: null }],
            from: [
                { db: null, table: 't', as: null },
                {
                    db: null,
                    table: 't2',
                    as: null,
                    join: 'LEFT JOIN',
                    on: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 't1', column: 'id' },
                        right: { type: 'column_ref', table: 't2', column: 'id' }
                    }
                }
            ],
            where: null,
            groupby: [
                { type: 'column_ref', table: 't1', column: 'col1' },
                { type: 'column_ref', table: 't2', column: 'col2' }
            ],
            orderby: null
        };

        const initialAST = cloneDeep(ast);

        const optimizedAst = optimize(ast, ['col1']);
        expect(optimizedAst.from).to.eql(initialAST.from);
    });

    it('should not remove LEFT JOIN if table is referenced in ORDER BY clause', () => {
        ast = {
            type: 'select',
            columns: [{ expr: { type: 'column_ref', table: 't1', column: 'col1' }, as: null }],
            from: [
                { db: null, table: 't', as: null },
                {
                    db: null,
                    table: 't2',
                    as: null,
                    join: 'LEFT JOIN',
                    on: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 't1', column: 'id' },
                        right: { type: 'column_ref', table: 't2', column: 'id' }
                    }
                }
            ],
            where: null,
            groupby: null,
            orderby: [
                { expr: { type: 'column_ref', table: 't1', column: 'col1' }, type: 'ASC' },
                { expr: { type: 'column_ref', table: 't2', column: 'col2' }, type: 'DESC' }
            ]
        };
        const initialAST = cloneDeep(ast);

        const optimizedAst = optimize(ast, ['col1']);
        expect(optimizedAst.from).to.eql(initialAST.from);
    });

    it('should not remove "parent" table/LEFT JOIN if "child" table/LEFT JOIN is needed', () => {
        /*
            SELECT
              instrument.id, ctde.name AS nameDE, cten.name AS nameEN
            FROM instrument
              LEFT JOIN country AS c ON instrument.countryId = c.id
              LEFT JOIN country_translation AS ctde ON c.id = ctde.countryId AND ctde.lang = 'de'
              LEFT JOIN country_translation AS cten ON c.id = cten.countryId AND cten.lang = 'en'
         */
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
                {
                    db: null,
                    table: 'country',
                    as: 'c',
                    join: 'LEFT JOIN',
                    on: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 'instrument', column: 'countryId' },
                        right: { type: 'column_ref', table: 'c', column: 'id' }
                    }
                },
                {
                    db: null,
                    table: 'country_translation',
                    as: 'ctde',
                    join: 'LEFT JOIN',
                    on: {
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
                    }
                },
                {
                    db: null,
                    table: 'country_translation',
                    as: 'cten',
                    join: 'LEFT JOIN',
                    on: {
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
                    }
                }
            ],
            where: null,
            groupby: null,
            orderby: null,
            limit: null
        };

        const optimizedAst = optimize(ast, ['id', 'nameDE']);
        /*
         SELECT instrument.id, ctde.name AS nameDE
         FROM instrument
           LEFT JOIN country AS c ON instrument.countryId = c.id
           LEFT JOIN country_translation AS ctde ON c.id = ctde.countryId AND ctde.lang = 'de'
         */
        expect(optimizedAst.from).to.eql([
            { db: null, table: 'instrument', as: null },
            {
                db: null,
                table: 'country',
                as: 'c',
                join: 'LEFT JOIN',
                on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 'instrument', column: 'countryId' },
                    right: { type: 'column_ref', table: 'c', column: 'id' }
                }
            },
            {
                db: null,
                table: 'country_translation',
                as: 'ctde',
                join: 'LEFT JOIN',
                on: {
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
                }
            }
        ]);
    });

    it("should not remove alias if it's used in GROUP BY clause", () => {
        // SELECT t.id, IFNULL(col1, "foo") AS alias FROM t ORDER BY alias DESC
        ast = {
            type: 'select',
            options: null,
            distinct: null,
            columns: [
                { expr: { type: 'column_ref', table: 't', column: 'id' }, as: null },
                {
                    expr: {
                        type: 'function',
                        name: 'IFNULL',
                        args: {
                            type: 'expr_list',
                            value: [
                                { type: 'column_ref', table: null, column: 'col1' },
                                { type: 'string', value: 'foo' }
                            ]
                        }
                    },
                    as: 'alias'
                }
            ],
            from: [{ db: null, table: 't', as: null }],
            where: null,
            groupby: [{ expr: { type: 'column_ref', table: null, column: 'alias' }, type: 'DESC' }],
            orderby: null,
            limit: null
        };

        const optimizedAst = optimize(ast, ['id']);
        expect(optimizedAst.columns).to.eql([
            { expr: { type: 'column_ref', table: 't', column: 'id' }, as: null },
            {
                expr: {
                    type: 'function',
                    name: 'IFNULL',
                    args: {
                        type: 'expr_list',
                        value: [
                            { type: 'column_ref', table: null, column: 'col1' },
                            { type: 'string', value: 'foo' }
                        ]
                    }
                },
                as: 'alias'
            }
        ]);
    });

    it("should not remove alias if it's used in ORDER BY clause", () => {
        // SELECT id, IFNULL(col1, "foo") AS alias FROM t ORDER BY alias DESC
        ast = {
            type: 'select',
            options: null,
            distinct: null,
            columns: [
                { expr: { type: 'column_ref', table: null, column: 'id' }, as: null },
                {
                    expr: {
                        type: 'function',
                        name: 'IFNULL',
                        args: {
                            type: 'expr_list',
                            value: [
                                { type: 'column_ref', table: null, column: 'col1' },
                                { type: 'string', value: 'foo' }
                            ]
                        }
                    },
                    as: 'alias'
                }
            ],
            from: [{ db: null, table: 't', as: null }],
            where: null,
            groupby: null,
            orderby: [{ expr: { type: 'column_ref', table: null, column: 'alias' }, type: 'DESC' }],
            limit: null
        };

        const optimizedAst = optimize(ast, ['id']);
        expect(optimizedAst.columns).to.eql([
            { expr: { type: 'column_ref', table: null, column: 'id' }, as: null },
            {
                expr: {
                    type: 'function',
                    name: 'IFNULL',
                    args: {
                        type: 'expr_list',
                        value: [
                            { type: 'column_ref', table: null, column: 'col1' },
                            { type: 'string', value: 'foo' }
                        ]
                    }
                },
                as: 'alias'
            }
        ]);
    });

    describe('limit-per', () => {
        beforeEach(() => {
            /**
             * SELECT ...
             * FROM (VALUES ROW(1), ROW(3)) limitper_ids (id)
             * JOIN LATERAL (
             *      SELECT t.col1, t.col2
             *      FROM t
             *      WHERE limitper_ids.id = t.id
             * ) limitper ON TRUE
             */
            ast = {
                type: 'select',
                options: null,
                distinct: null,
                columns: [{ expr: { type: 'column_ref', table: 'limitper', column: 'col1' }, as: null }],
                from: [
                    {
                        expr: {
                            type: 'values',
                            value: [
                                { type: 'row_value', keyword: true, value: [{ type: 'number', value: 1 }] },
                                { type: 'row_value', keyword: true, value: [{ type: 'number', value: 3 }] }
                            ]
                        },
                        as: 'limitper_ids',
                        columns: ['id']
                    },
                    {
                        expr: {
                            with: null,
                            type: 'select',
                            options: null,
                            distinct: null,
                            columns: [
                                { expr: { type: 'column_ref', table: 't', column: 'col1' }, as: null },
                                { expr: { type: 'column_ref', table: 't', column: 'col2' }, as: null }
                            ],
                            from: [{ db: null, table: 't', as: null }],
                            where: {
                                type: 'binary_expr',
                                operator: '=',
                                left: { type: 'column_ref', table: 'limitper_ids', column: 'id' },
                                right: { type: 'column_ref', table: 't', column: 'id' }
                            },
                            groupby: null,
                            having: null,
                            orderby: null,
                            limit: null,
                            parentheses: true
                        },
                        as: 'limitper',
                        lateral: true,
                        columns: null,
                        join: 'INNER JOIN',
                        on: { type: 'bool', value: true }
                    }
                ],
                where: null,
                groupby: null,
                orderby: null,
                limit: null
            };
        });

        it('should optimize column clause of lateral join', () => {
            const optimizedAst = optimize(ast, ['col1'], true);
            const [, { expr: subSelect }] = optimizedAst.from;

            expect(subSelect.columns).to.eql([{ expr: { type: 'column_ref', table: 't', column: 'col1' }, as: null }]);
        });

        it('should optimize from clause', () => {
            // add LEFT join to sub-query
            ast.from[1].expr.from.push({
                db: null,
                table: 't_l10n',
                as: null,
                join: `LEFT JOIN`,
                on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'id' },
                    right: { type: 'column_ref', table: 't_l10n', column: 'id' }
                }
            });

            const optimizedAst = optimize(ast, ['col1'], true);
            const [, { expr: subSelect }] = optimizedAst.from;

            expect(subSelect.from).to.eql([{ db: null, table: 't', as: null }]);
        });
    });
});
