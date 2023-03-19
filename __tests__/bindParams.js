const {DbLangCh} = require ('..'), lang = new DbLangCh ()

test ('bindParams', () => {

	expect (lang.bindParams ('SELECT 1-1/2--', [])).toBe ('SELECT 1-1/2')

	expect (lang.bindParams ('SELECT * FROM t WHERE id = ?', [1])).toBe ('SELECT * FROM t WHERE id = 1')
	
	expect (lang.bindParams ('SELECT * FROM t WHERE id = ? AND label LIKE ?', [1, "Don't you know?"])).toBe ("SELECT * FROM t WHERE id = 1 AND label LIKE 'Don''t you know?'")

	expect (lang.bindParams ("SELECT * FROM t WHERE id = ? AND label='Don''t you know?'", [1])).toBe ("SELECT * FROM t WHERE id = 1 AND label='Don''t you know?'")

	expect (lang.bindParams ('SELECT * FROM t /*What /*the he// is*/t?*/ WHERE id = ?', [1])).toBe ('SELECT * FROM t  WHERE id = 1')

	expect (lang.bindParams (`
		SELECT
			id
--			, label ???
		FROM
			t
		WHERE
			id = ?
	`, [1]).trim ().replace (/\s+/g, ' ')).toBe (`SELECT id FROM t WHERE id = 1`)

})