'use strict';

const assert = require('node:assert/strict');
const { beforeEach, describe, it } = require('node:test');
const optimize = require('../../lib/sql-query-optimizer');

describe('SQL query optimizer', () => {
    let ast;

    it('should only modify SELECT statements', function () {
        ast = { type: 'UPDATE' };
        const initialAST = structuredClone(ast);
        optimize(ast, ['col1']);

        assert.deepEqual(ast, initialAST);
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

        assert.deepEqual(optimizedAst.columns, [
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

        const initialAST = structuredClone(ast);

        const optimizedAst = optimize(ast, ['col1']);

        assert.deepEqual(optimizedAst.from, initialAST.from);
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

        assert.deepEqual(optimizedAst.from, [{ db: null, table: 't', as: null }]);
    });

    it('should not remove JOIN dependencies from AST', () => {
        /* should not remove t1 LEFT JOIN because it's required for JOINs on t2/t3
         * SELECT t.id
         * FROM t
         * LEFT JOIN t1 ON t.id = t1.id
         * LEFT JOIN t2 ON t1.id = t2.id
         * LEFT JOIN t3 ON t2.id = t3.id
         * WHERE t3.id = 1
         */
        const from = [
            { db: null, table: 't', as: null },
            {
                db: null,
                table: 't1',
                as: null,
                join: 'LEFT JOIN',
                on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't', column: 'id' },
                    right: { type: 'column_ref', table: 't1', column: 'id' }
                }
            },
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
            },
            {
                db: null,
                table: 't3',
                as: null,
                join: 'LEFT JOIN',
                on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't2', column: 'id' },
                    right: { type: 'column_ref', table: 't3', column: 'id' }
                }
            }
        ];
        const optimizedAst = optimize(
            {
                with: null,
                type: 'select',
                options: null,
                distinct: null,
                columns: [{ expr: { type: 'column_ref', table: 't', column: 'id' }, as: null }],
                from,
                where: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't3', column: 'id' },
                    right: { type: 'number', value: 1 }
                },
                groupby: null,
                having: null,
                orderby: null,
                limit: null
            },
            ['id']
        );

        assert.deepEqual(optimizedAst.from, from);
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

        assert.deepEqual(optimizedAst.from, [
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

        assert.deepEqual(optimizedAst.from, ast.from);
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

        const initialAST = structuredClone(ast);

        const optimizedAst = optimize(ast, ['col1']);

        assert.deepEqual(optimizedAst.from, initialAST.from);
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
        const initialAST = structuredClone(ast);

        const optimizedAst = optimize(ast, ['col1']);

        assert.deepEqual(optimizedAst.from, initialAST.from);
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
        assert.deepEqual(optimizedAst.from, [
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

    it('should not remove tables used in LEFT JOIN conditions', () => {
        /*
            SELECT
              t1.id,
              t2.col1,
              t4.name
            FROM t1
              LEFT JOIN t2 ON t1.id = t2.t1id
              LEFT JOIN t3 ON t1.id = t3.t1id
              LEFT JOIN t4 ON t3.id = t4.t3id
         */
        ast = {
            with: null,
            type: 'select',
            options: null,
            distinct: null,
            columns: [
                {
                    expr: { type: 'column_ref', table: 't1', column: 'id' },
                    as: null
                },
                {
                    expr: { type: 'column_ref', table: 't1', column: 'col1' },
                    as: null
                },
                {
                    expr: { type: 'column_ref', table: 't4', column: 'name' },
                    as: null
                }
            ],
            from: [
                { db: null, table: 't1', as: null },
                {
                    db: null,
                    table: 't2',
                    as: null,
                    join: 'LEFT JOIN',
                    on: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 't1', column: 'id' },
                        right: { type: 'column_ref', table: 't2', column: 't1id' }
                    }
                },
                {
                    db: null,
                    table: 't3',
                    as: null,
                    join: 'LEFT JOIN',
                    on: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 't1', column: 'id' },
                        right: { type: 'column_ref', table: 't3', column: 't1id' }
                    }
                },
                {
                    db: null,
                    table: 't4',
                    as: null,
                    join: 'LEFT JOIN',
                    on: {
                        type: 'binary_expr',
                        operator: '=',
                        left: { type: 'column_ref', table: 't3', column: 'id' },
                        right: { type: 'column_ref', table: 't4', column: 't3id' }
                    }
                }
            ],
            where: null,
            groupby: null,
            having: null,
            orderby: null,
            limit: null
        };
        const optimizedAst = optimize(ast, ['id', 'name']);

        assert.deepEqual(optimizedAst.from, [
            { db: null, table: 't1', as: null },
            {
                db: null,
                table: 't3',
                as: null,
                join: 'LEFT JOIN',
                on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't1', column: 'id' },
                    right: { type: 'column_ref', table: 't3', column: 't1id' }
                }
            },
            {
                db: null,
                table: 't4',
                as: null,
                join: 'LEFT JOIN',
                on: {
                    type: 'binary_expr',
                    operator: '=',
                    left: { type: 'column_ref', table: 't3', column: 'id' },
                    right: { type: 'column_ref', table: 't4', column: 't3id' }
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

        assert.deepEqual(optimizedAst.columns, [
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

        assert.deepEqual(optimizedAst.columns, [
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

            assert.deepEqual(subSelect.columns, [
                { expr: { type: 'column_ref', table: 't', column: 'col1' }, as: null }
            ]);
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

            assert.deepEqual(subSelect.from, [{ db: null, table: 't', as: null }]);
        });
    });

    it('should not touch union queries', () => {
        // SELECT "t1"."id", "t1"."col" FROM "t1" UNION SELECT "t2"."id", "t2"."col" FROM "t2"
        ast = {
            with: null,
            type: 'select',
            options: null,
            distinct: null,
            columns: [
                {
                    expr: { type: 'column_ref', table: 't1', column: 'id' },
                    as: null
                },
                {
                    expr: { type: 'column_ref', table: 't1', column: 'col' },
                    as: null
                }
            ],
            from: [{ db: null, table: 't1', as: null }],
            where: null,
            groupby: null,
            having: null,
            orderby: null,
            limit: null,
            _next: {
                with: null,
                type: 'select',
                options: null,
                distinct: null,
                columns: [
                    {
                        expr: { type: 'column_ref', table: 't2', column: 'id' },
                        as: null
                    },
                    {
                        expr: { type: 'column_ref', table: 't2', column: 'col' },
                        as: null
                    }
                ],
                from: [{ db: null, table: 't2', as: null }],
                where: null,
                groupby: null,
                having: null,
                orderby: null,
                limit: null
            }
        };
        const originalAst = structuredClone(ast);

        const optimizedAst = optimize(ast, ['id']);

        assert.deepEqual(optimizedAst, originalAst);
    });
});
