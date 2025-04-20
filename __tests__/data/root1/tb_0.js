module.exports = {

    comment: 'Table 0',

    columns: {
        id       : 'UInt8  {codec: "ZSTD(22)"} // PK',
        drop_me  : null,
        drop_me_too : null,
    },

    pk: 'id',

    data: [
        {id: 0},
    ],
    
}