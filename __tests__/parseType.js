const {DbLangCh} = require ('..'), lang = new DbLangCh ()
const {DbTypeArithmeticInt, DbTypeCharacter} = require ('doix-db')

test ('parseType', () => {

	expect (lang.parseType ('Int32').typeDef).toBeInstanceOf (DbTypeArithmeticInt)
	expect (lang.parseType ('FixedString(1)').size).toBe (1)
	expect (lang.parseType ('Nullable(Int32)').nullable).toBe (true)
	expect (lang.parseType ('Nullable( Decimal(10, 2))').scale).toBe (2)

})

test ('getTypeDefinition', () => {

	expect (lang.getTypeDefinition ('text')).toBeInstanceOf (DbTypeCharacter)
	expect (lang.getTypeDefinition ('Geo').name).toBe ('Geo')

})