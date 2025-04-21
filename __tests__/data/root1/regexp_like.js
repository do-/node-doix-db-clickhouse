module.exports = {

    comment: 'Oracle, PostgreSQL etc. like REGEXP_LIKE',

    parameters: [
    	'x String',
    	'y String',
    ],

    body: 'match (x, y)',

}