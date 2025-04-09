module.exports = {

    comment: 'Table 1',

    columns: {
        id       : 'UInt8    // PK',
        label    : `String?='on'   // Human readable`,
        amount   : 'Decimal(10,2)=0 // Some money',
        cnt      : 'UInt16=0 // Mutations counter',
    },

    pk: 'id',

    partitionBy: 'cnt',

    data: [
        {id: 0, label: 'zero'},
    ],

}