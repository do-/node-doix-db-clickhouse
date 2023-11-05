const {DbLangCh} = require ('..'), lang = new DbLangCh ()

test ('genInsertFormatSql', () => {

	expect (lang.genInsertFormatSql ('_', ['id', {name: 'label'}])).toBe ('INSERT INTO "_" ("id","label") FORMAT CSV')
	expect (lang.genInsertFormatSql ('users', ['id', 'label'], {FORMAT: 'JSON'})).toBe ('INSERT INTO "users" ("id","label") FORMAT JSON')

})