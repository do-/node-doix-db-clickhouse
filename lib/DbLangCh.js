const {DbLang, DbColumn, DbType, DbTypeArithmeticInt, DbTypeCharacter, DbTypeDate, DbTypeTimestamp, DbTypeArithmeticFixed} = require ('doix-db')

const CH_QUEST = '?'.charAt (0)
const CH_QUOTE = "'".charAt (0)
const CH_SLASH = '/'.charAt (0)
const CH_ASTER = '*'.charAt (0)
const CH_MINUS = '-'.charAt (0)

const TYPE_MAP = new Map ([ 
	['String', DbTypeCharacter],
	['Date', DbTypeDate],
	['Date32', DbTypeDate],
	['DateTime', DbTypeTimestamp],
	['DateTime64', DbTypeTimestamp],
	['Decimal', DbTypeArithmeticFixed],
].map (([name, clazz]) => [name, new clazz ({name})]))

{

	for (let bytes = 1; bytes <= 32; bytes <<= 1) {

		let name = 'Int' + (bytes << 3)

		const signed = new DbTypeArithmeticInt  ({name, bytes})

		TYPE_MAP.set (name, signed)

		if (bytes <= 4) TYPE_MAP.set ('INT' + bytes, signed)

		name = 'U' + name

		TYPE_MAP.set (name, new DbTypeArithmeticInt  ({name, bytes, isSigned: false}))

	}

	for (const [name, aliases] of [
		['Int8',   ['TINYINT', 'BOOL', 'BOOLEAN', 'INT1']],
		['Int16',  ['SMALLINT', 'INT2']],
		['Int32',  ['INT', 'INT4', 'INTEGER']],
		['Int64',  ['BIGINT']],
		['String', ['LONGTEXT', 'MEDIUMTEXT', 'TINYTEXT', 'TEXT', 'LONGBLOB', 'MEDIUMBLOB', 'TINYBLOB', 'BLOB', 'VARCHAR', 'CHAR']],
	]) for (const alias of aliases) TYPE_MAP.set (alias, TYPE_MAP.get (name))

}

class DbLangCh extends DbLang {

	constructor () {
	
		super ()
		
	}

	getDbColumnTypeDim (col) {

		if (col.typeDef === TYPE_MAP.get ('DateTime64')) col.scale = 8

		return super.getDbColumnTypeDim (col)

	}

	genColumnDefinition (column) {

		const {qName, typeDim, nullable} = column

		let s = typeDim
		
		if (nullable) s = 'Nullable(' + s + ')'

		s = qName + ' ' + s

		if (column.default != null) s += ' DEFAULT ' + this.genColumnDefault (column)

		return s

	}

	getTypeDefinition (name) {

		if (TYPE_MAP.has (name)) return TYPE_MAP.get (name)

		{

			const uc = name.toUpperCase ()

			if (TYPE_MAP.has (uc)) return TYPE_MAP.get (uc)

		}

		return new DbType ({name})

	}

	parseType (src) {
		
		const o = {}

		if (src.slice (0, 9) === 'Nullable(') {
		
			o.nullable = true
			
			src = src.slice (9, -1)

		}

		const pos = src.indexOf ('(')
		
		if (pos < 0) {
		
			o.type = src.trim ()
		
		}
		else {

			o.type = src.slice (0, pos).trim ()
			
			const dim = src.substring (pos + 1, src.indexOf (')'))
			
			const comma = dim.indexOf (',')
			
			if (comma < 0) {
			
				o.size = parseInt (dim.trim ())
			
			}
			else {

				o.size = parseInt (dim.slice (0, comma).trim ())

				o.scale = parseInt (dim.slice (comma + 1).trim ())

			}

		}

		return new DbColumn (o)

	}

	genInsertFormatSql (name, columns, options = {}) {

		if (!options.FORMAT) options.FORMAT = 'CSV'
	
		let sql = ''; for (let column of columns) {

			if (sql.length !== 0) sql += ','

			sql += this.quoteName (typeof column === 'string' ? column : column.name)

		}
		
		return `INSERT INTO ${this.quoteName (name)} (${sql}) FORMAT ${options.FORMAT}`
	
	}

	normalizeSQL (call) {
		
		const {params} = call

		call.sql = (src => {

		const ST_SQL     = 0
		const ST_LITERAL = 1
		const ST_COMMENT = 2

		const {length} = src

		let n = 0, dst = '', last = 0, next = -1, depth = 0, state = ST_SQL

		while (next < length) {

			next ++; const c = src.charAt (next)

			switch (state) {

				case ST_LITERAL:
					if (c === CH_QUOTE && src.charAt (next + 1) !== CH_QUOTE) state = ST_LITERAL
					break

				case ST_SQL:

					switch (c) {

						case CH_QUEST:
							const s = src.slice (last, next)
							dst += s
							dst += this.quoteLiteral (params [n ++])
							last = next + 1
							break

						case CH_QUOTE:
							state = ST_LITERAL
							break

						case CH_SLASH:
							if (src.charAt (next + 1) !== CH_ASTER) break
							dst += src.slice (last, next)
							state = ST_COMMENT
							depth = 1
							next ++
							break

						case CH_MINUS:
							if (src.charAt (next + 1) !== CH_MINUS) break
							dst += src.slice (last, next)
							last = src.indexOf ('\n', next)
							if (last < 0) return dst
							next = last
							break

					}
					break

				case ST_COMMENT:

					if (c !== CH_SLASH) break				

					if (src.charAt (next - 1) === CH_ASTER) {
						depth --
						if (depth > 0) break
						state = ST_SQL
						last = next + 1
					}
					else if (src.charAt (next + 1) === CH_ASTER) {
						depth ++
					}
					break

			}

		}

		return dst + src.slice (last)

		}) (call.sql.trim ())

		const {maxRows} = call.options

		if (maxRows > 0) call.sql += ' FORMAT JSONCompactEachRowWithNamesAndTypes'

		call.params = []

	}

	genSelectColumnsSql () {

		return `
			SELECT
				c.table
				, c.position
				, c.name
				, c.comment
				, c.type
				, c.default_expression
				, t.engine 
				, t.sorting_key
				, t.partition_key
				, t.comment	table_comment	
			FROM 
				system.columns c
				JOIN system.tables t ON 
					c.database = t.database AND 
					c.table    = t.name
			WHERE 
				c.database = currentDatabase () 
				AND t.engine <> 'View'
			ORDER BY 
				c.table
				, c.position DESC
		`

	}

}

module.exports = DbLangCh