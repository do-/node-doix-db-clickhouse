const {DbLangCh} = require ('..'), lang = new DbLangCh ()

test ('bindParams', () => {

	const bindParams = ((sql, params) => {
		const call = {sql, params, options: {}}
		lang.normalizeSQL (call)
		return call.sql
	})

	expect (bindParams ('SELECT 1-1/2--', [])).toBe ('SELECT 1-1/2')

	expect (bindParams ('SELECT * FROM t WHERE id = ?', [1])).toBe ('SELECT * FROM t WHERE id = 1')
	
	expect (bindParams ('SELECT * FROM t WHERE id = ? AND label LIKE ?', [1, "Don't you know?"])).toBe ("SELECT * FROM t WHERE id = 1 AND label LIKE 'Don''t you know?'")

	expect (bindParams ("SELECT * FROM t WHERE id = ? AND label='Don''t you know?'", [1])).toBe ("SELECT * FROM t WHERE id = 1 AND label='Don''t you know?'")

	expect (bindParams ('SELECT * FROM t /*What /*the he// is*/t?*/ WHERE id = ?', [1])).toBe ('SELECT * FROM t  WHERE id = 1')

	expect (bindParams (`
		SELECT
			id
--			, label ???
		FROM
			t
		WHERE
			id = ?
	`, [1]).trim ().replace (/\s+/g, ' ')).toBe (`SELECT id FROM t WHERE id = 1`)

})