module.exports = {

    comment: 'View 2',

    columns: {
        id       : 'UInt8    // PK',
    },
    
    pk: 'id',

    wrap: true,

    sql: `SELECT 'two' AS label, 2 AS id`,

}