const http = require ('http')
require ('url')
const {randomUUID}  = require ('crypto')
const DbRecordReaderCh = require ('./DbRecordReaderCh.js')
const DbColumnXformerCh = require ('./DbColumnXformerCh.js')
const DbTableCollectorCh = require ('./DbTableCollectorCh.js')
const {DbClient} = require ('doix-db')

const assertSuccess = async (res) => {

	const {statusCode} = res; if (statusCode == 200) return

	let message = ''; for await (const s of res) message += s

	throw Error (`ClickHouse server error (${statusCode}): ${message}`)

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

	get url () {

		const {pool} = this; if (!pool) return ''

		const url = new URL (pool.url), {searchParams} = url

		{

			const {sessionId} = this; if (sessionId != null) {

				searchParams.set ('session_id', sessionId)

				const {sessionTimeout} = this; if (sessionTimeout > 0) searchParams.set ('session_timeout', sessionTimeout)

			}
		
		}

		{

			const {database} = this; if (database != null) searchParams.set ('database', database)
		
		}

		return url.toString ()

	}

	async exec (call) {

		const res = await new Promise ((ok, fail) => {

			const req = call.raw = http.request (this.url, {...this.pool.options, method: 'POST'}, ok)

			req.once ('error', fail)

			req.write (call.sql)

			req.end ()

		})

		res.setEncoding ('utf8')

		call.raw.removeAllListeners ('error')

		await assertSuccess (res)

		if (call.options.maxRows === 0) return res.destroy ()

		const wrp = new DbRecordReaderCh (call)

		const destroyIt = e => res.destroy (e)
		wrp.on ('end', destroyIt)		
		wrp.on ('error', destroyIt)

		call.rows = res.pipe (wrp)

	}

	async getStreamOfExistingTables () {

		const {lang} = this, rs = await this.getStream (this.lang.genSelectColumnsSql ())
		
		return rs	
			.pipe (new DbColumnXformerCh ({lang}))
			.pipe (new DbTableCollectorCh ())

	}

}

module.exports = DbClientCh