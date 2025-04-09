const http = require ('http')
require ('url')
const {randomUUID}  = require ('crypto')
const DbRecordReaderCh = require ('./DbRecordReaderCh.js')
const DbColumnXformerCh = require ('./DbColumnXformerCh.js')
const DbTableCollectorCh = require ('./DbTableCollectorCh.js')
const {DbClient} = require ('doix-db')
const { type } = require('os')

const CALL = Symbol.for ('doixDbCall')

const readBody = (res, ok, fail) => {

	res.on ('error', fail)

	let message = ''

	res.on ('end', () => {

		res.off ('error', fail)

		ok (message)

	})

	res.on ('data', s => message += s)

}

const isResponseOK = (res, ok, fail) => {

	res.setEncoding ('utf-8')

	const {statusCode} = res; if (statusCode == 200) return ok (res)

	readBody (res,

		s => fail (Error (`ClickHouse server error (HTTP ${statusCode}) ${s}`.trim ())),

		fail

	)

	return false

}

class DbClientCh extends DbClient {

	constructor (pool) {
	
		super ()

		this.pool = pool
		
		this.uuid = randomUUID ()
	
	}
	
	release () {
		
	}

	async setSession (id, timeout) {
		
		this.sessionId = arguments.length === 0 ? this.uuid : id
		
		if (timeout > 0) this.sessionTimeout = timeout

	}

	getURL (wantResult) {

		const url = new URL (this.pool.url), {searchParams} = url

		{

			const {sessionId} = this; if (sessionId != null) {

				searchParams.set ('session_id', sessionId)

				const {sessionTimeout} = this; if (sessionTimeout > 0) searchParams.set ('session_timeout', sessionTimeout)

			}
		
		}

		{

			const {database} = this; if (database != null) searchParams.set ('database', database)
		
		}

		if (wantResult) {
		
			searchParams.set ('output_format_json_quote_decimals', '1')

			searchParams.set ('output_format_decimal_trailing_zeros', '1')

		}
		else {

			searchParams.set ('max_partitions_per_insert_block', '0')

		}

		return url.toString ()

	}

	async exec (call) {
/*
		{

			const {stack} = new Error ()

			call.prependListener ('error', err => {

				const {message} = err

				err.stack = `Error: ${message}\n` + stack.slice (1 + stack.indexOf ('\n'))

			})

		}
*/
		const wantResult = call.options.maxRows !== 0

		let {sql} = call; if (wantResult) sql += ' FORMAT JSONCompactEachRowWithNamesAndTypes'

		const req = call.raw = http.request (this.getURL (wantResult), {...this.pool.options, method: 'POST'})

		req.write (sql + '\n'); if (call.options.isPut) return

		let res; try {

			res = await new Promise ((ok, fail) => {
				req.once ('error', fail)
				req.once ('response', res => isResponseOK (res, ok, fail))
				req.end ()
			})	

		}
		catch (cause) {

			throw Error (cause.message, {cause})

		}

		if (!wantResult) return res.destroy ()

		const wrp = new DbRecordReaderCh (call)

		const destroyIt = e => {

//			if (e) e.cause = cause

			res.destroy (e)

		}

		wrp.on ('end', destroyIt)		
		wrp.on ('error', destroyIt)

		call.rows = res.pipe (wrp)

	}

	async putObjectStream (name, columns, options) {

		const table = this.model.find (name); if (!table) throw Error (`${name} not found`)

		const csv = this.toCsv ({table, columns})

		const os = await this.putBinaryStream (name, csv.columns.map (i => i.name), {...options, FORMAT: 'CSV'})

		csv.on ('error',  () => os.destroy ())
		os.on ('complete', () => csv.emit ('complete'))

		csv.pipe (os)

		return csv

	}

	async putBinaryStream (name, columns, options) {

		const sql = this.lang.genInsertFormatSql (name, columns, options)

		const call = this.call (sql, [], {...options, maxRows: 0, isPut: true})

		await call.exec ()

		const req = call.raw; req [CALL] = call

		call.on ('finish', () => 'error' in call ? null : req.emit ('complete'))

		req.on ('error', err => call.emit ('error', err))

		const fail = e => req.destroy (e)

		req.once ('response', res => isResponseOK (res,
			res => readBody (res, s => call.finish (call.rows = s), fail),
			fail
		))

		return req

	}

	async getStreamOfExistingTables () {

		const {lang} = this, rs = await this.getStream (this.lang.genSelectColumnsSql ())
		
		return rs	
			.pipe (new DbColumnXformerCh ({lang}))
			.pipe (new DbTableCollectorCh ({lang}))

	}

}

module.exports = DbClientCh