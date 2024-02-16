'use strict';

module.exports = {
    type: 'select',
    options: null,
    distinct: null,
    columns: [
        { expr: { type: 'column_ref', table: 'flora_request_processing', column: 'id' }, as: null },
        { expr: { type: 'column_ref', table: 'flora_request_processing', column: 'col1' }, as: null },
        { expr: { type: 'column_ref', table: 'flora_request_processing', column: 'col2' }, as: null }
    ],
    from: [{ db: null, table: 'flora_request_processing', as: null }],
    where: null,
    groupby: null,
    having: null,
    orderby: null,
    limit: null,
    with: null
};
