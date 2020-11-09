module.exports = {
    with: null,
    type: 'select',
    distinct: null,
    columns: [
        { expr: { type: 'column_ref', table: 't', column: 'col1' }, as: null },
        { expr: { type: 'column_ref', table: 't', column: 'col2' }, as: null },
        { expr: { type: 'column_ref', table: 't', column: 'col3' }, as: 'columnAlias' }
    ],
    from: [{ db: null, table: 't', as: null }],
    where: null,
    groupby: null,
    orderby: null,
    limit: null
};
