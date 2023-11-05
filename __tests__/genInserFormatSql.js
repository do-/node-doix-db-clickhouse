const {DbLangCh} = require ('..'), lang = new DbLangCh ()

test ('genInserFormatSql', () => {

	expect (lang.genInserFormatSql ('_', ['id', {name: 'label'}])).toBe ('INSERT INTO "_" ("id","label") FORMAT CSV')
	expect (lang.genInserFormatSql ('users', ['id', 'label'], {FORMAT: 'JSON'})).toBe ('INSERT INTO "users" ("id","label") FORMAT JSON')

})