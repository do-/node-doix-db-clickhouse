const {DbColumn} = require ('doix-db')
const {DbLangCh} = require ('..'), lang = new DbLangCh ()

test ('parseType', () => {

	expect (lang.parseType ('Int32')).toStrictEqual (new DbColumn({type: 'Int32'}))
	expect (lang.parseType ('FixedString(1)')).toStrictEqual (new DbColumn({type: 'FixedString', size: 1}))
	expect (lang.parseType ('Nullable(Int32)')).toStrictEqual (new DbColumn({type: 'Int32', nullable: true}))
	expect (lang.parseType ('Nullable( Decimal(10, 2))')).toStrictEqual (new DbColumn({type: 'Decimal', nullable: true, size: 10, scale: 2}))

	expect (lang.getTypeDefinition ('text').name).toBe ('String')
	expect (lang.getTypeDefinition ('Geo').name).toBe ('Geo')

})