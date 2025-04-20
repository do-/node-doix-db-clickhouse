const crypto = require ('crypto')
const {DbLang, DbColumn, DbType, DbTypeArithmeticInt, DbTypeCharacter, DbTypeDate, DbTypeTimestamp, DbTypeArithmeticFixed, DbTable, DbView} = require ('doix-db')

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

	compareColumns (asIs, toBe) {
	
		const diff = super.compareColumns (asIs, toBe)

		for (const k of ['codec']) if (asIs [k] !== toBe [k]) diff.push (k)

		return diff

	}

	* genColumnConstraints ({codec}) {

		if (codec) yield `CODEC(${codec})`

	}

	genColumnDataType (column) {

		const {typeDim, nullable} = column

		return nullable ? `Nullable(${typeDim})` : typeDim

	}	

	getTypeDefinition (name) {

		if (TYPE_MAP.has (name)) return TYPE_MAP.get (name)

		{

			const uc = name.toUpperCase ()

			if (TYPE_MAP.has (uc)) return TYPE_MAP.get (uc)

		}

		return new DbType ({name})

	}

	getDbObjectName (o) {
		
		if (o instanceof DbTable && !('engine' in o)) o.engine = 'MergeTree'

		return super.getDbObjectName (o)

	}

	parseType (src) {
		
		const o = {}

		if (src.slice (0, 9) === 'Nullable(') {
		
			o.nullable = true
			
			src = src.slice (9, -1)

		}
		else {

			o.nullable = false

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

		o.typeDef = this.getTypeDefinition (o.type)

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

		call.params = []

	}

	genSelectColumnsSql () {

		return /*sql*/`
			SELECT
				c.table
				, c.position
				, c.name
				, c.comment
				, c.type
				, CASE
					WHEN c.default_expression IS NULL THEN NULL
					WHEN c.default_expression IN ('', 'NULL') THEN NULL
					WHEN c.default_expression LIKE '''%''' THEN replaceAll (substring (c.default_expression, 2, -1), '''''', '''')
					ELSE c.default_expression
				END default_expression
				, SUBSTRING (NULLIF (compression_codec, ''), 7, -1) codec
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

	genCreateTable ({qName, pk, columns, engine, partitionBy}) {

		let sql = `CREATE TABLE ${qName} (${Object.values (columns).map (c => this.genColumnDefinition (c))}) ENGINE=${engine}`
	
		if (engine.slice (-9) === 'MergeTree') {

			const orderBy = pk.map (k => columns [k].qName)

			sql += ` ORDER BY ${orderBy}`

			if (partitionBy != null) sql += ` PARTITION BY ${partitionBy}`

			sql += ` PRIMARY KEY ${orderBy}`

		}

		return sql

	}

	genCreate (o) {
	
		if (o instanceof DbTable) return this.genCreateTable (o)
		
		throw Error (`Don't know how to create ` + o.constructor.name)

	}

	getRequiredMutation (asIs, toBe) {

		const {toDo} = asIs

		if (toDo.has ('migrate-column')) return 'migrate'

		if (toDo.has ('add-column') || toDo.has ('alter-column')) return 'alter'

		return null
	
	}

	* genAlter (asIs, toBe) {

		if (toBe instanceof DbTable) {
		
			for (const qp of this.genAlterTable (asIs, toBe)) yield qp
			
		}
		else {

			throw Error (`Don't know how to alter ` + toBe.constructor.name)

		}

	}

	getRequiredColumnMutation (asIs, toBe) {

		if (asIs.nullable && !toBe.nullable) return 'migrate-column'

		if (asIs.diff.length === 0) return null

		return 'alter-column'
	
	}

	* genDropTableColumns () {

		const {asIs} = this.migrationPlan; if (!asIs) return

		for (const {qName, toDo} of asIs.values ()) {
			
			const columnsToDrop = toDo?.get ('drop-column'); if (columnsToDrop && columnsToDrop.length !== 0)

				yield `ALTER TABLE ${qName} ` + columnsToDrop.map (name => `DROP COLUMN ${this.quoteName (name)}`)

		}

	}

	* genAlterTable (asIs, {qName}) {
	
		const {toDo} = asIs, actions = []

		for (const [name, verb] of [
			['alter-column', 'MODIFY'],
			['add-column',   'ADD'],
		])
		
			if (toDo.has (name)) 
				
				for (const column of toDo.get (name)) 
					
					actions.push (verb + ' COLUMN ' + this.genColumnDefinition (column))

		yield [`ALTER TABLE ${qName} ${actions}`]

	}

	* genMigrate (asIs, toBe) {

		const tmpName = '_' + crypto.randomUUID ().replaceAll ('-', '_')

		yield [this.genCreateTable ({...toBe, qName: tmpName})]

		const {qName, columns, pk} = toBe, cols = Object.values (columns)

		yield [`INSERT INTO ${tmpName}(${cols.map (i => i.qName)}) SELECT ${
			
			cols.map (i => i.nullable && i.default ? `COALESCE(${i.qName},${i.default}) AS ${i.qName}` : i.qName)
		
		} FROM ${qName} WHERE ${

			pk.map (i => columns [i].qName + ' IS NOT NULL').join (' AND ')

		}`]

		yield [`DROP TABLE ${qName}`]

		yield [`RENAME TABLE ${tmpName} TO ${qName}`]

	}

	genReCreate (o) {
	
		if (o instanceof DbView) return this.genReCreateView (o)
		
		throw Error (`Don't know how to recreate ` + o.constructor.name)

	}

	genReCreateView ({qName, options, specification, sql}) {

		return [`CREATE OR REPLACE ${options} VIEW ${qName} ${specification} AS ${sql}`]

	}	

	* genDDL () {

		const {migrationPlan} = this; if (!migrationPlan) throw Error ('genDDL called without a migration plan')

		const {asIs, toDo} = migrationPlan

		for (const s of this.genDropTableColumns ()) yield [s]		

		if (toDo.has   ('create')) for (const o of toDo.get  ('create')) yield [this.genCreate (o)]

		if (toDo.has    ('alter')) for (const o of toDo.get   ('alter'))  for (const qp of this.genAlter (asIs.get (o.name), o)) yield qp

		if (toDo.has  ('migrate')) for (const o of toDo.get ('migrate'))  for (const qp of this.genMigrate (asIs.get (o.name), o)) yield qp

		if (toDo.has ('recreate')) for (const o of toDo.get ('recreate')) yield this.genReCreate (o)

		// for (const s of this.genUpsertData ()) yield s

		// if (toDo.has ('comment')) for (const o of toDo.get ('comment')) yield [this.genComment (o)]

	}	

}

module.exports = DbLangCh