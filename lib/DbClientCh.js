const http = require ('http')
require ('url')
const EventEmitter  = require ('events')
const {randomUUID}  = require ('crypto')
const DbRecordReaderCh = require ('./DbRecordReaderCh.js')
const DbColumnXformerCh = require ('./DbColumnXformerCh.js')
const DbTableCollectorCh = require ('./DbTableCollectorCh.js')
const {DbClient} = require ('doix-db')

class DbClientCh extends DbClient {

	constructor (pool) {
	
		super ()

		this.pool = pool
		
		this.uuid = randomUUID ()
	
	}
	
	release () {
		
	}

	async getStream (sql, params = [], options = {}) {

		const {rowMode} = options, {lang} = this

		const res = await this.do (sql + ' FORMAT JSONCompactEachRowWithNamesAndTypes', params, {keep: true})

		const wrp = new DbRecordReaderCh ({rowMode, lang})
		
		const destroyIt = e => res.destroy (e)
		
		wrp.on ('error', destroyIt)
		wrp.on ('close', destroyIt)

		return res.pipe (wrp)

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

	async do (sql, params = [], options = {}) {

		const {pool, url} = this, {agent} = pool
		
		try {
	
			this.emit ('start', this, {sql, params})

			const result = await new Promise ((ok, fail) => {

				const req = http.request (url, {...pool.options, method: 'POST'}, res => {

					res.once ('close', () => this.emit ('finish'))

					const {statusCode} = res; if (statusCode == 200) return ok (res)

					const err = new Error (res.statusMessage)
					
					err.code = statusCode
					err.response = res

					fail (err)

				})
				
				req.write (this.lang.bindParams (sql, params))

				req.end ()

			})
			
			if (options.keep) return result
			
			result.destroy ()
			
			result.emit ('close') // https://github.com/nodejs/node/issues/40528
			
		}
		catch (cause) {

			const {response} = cause; if (!response) throw cause
						
			response.setEncoding ('utf8')
			
			let {message} = response; for await (const s of response) message += s

			const e = new Error (message)

			this.emit ('error', this, e)

			throw Error ('ClickHouse server returned an error: ' + message, {cause})

		}

	}

	async getStreamOfExistingTables () {

		const {lang} = this, rs = await this.getStream (this.lang.genSelectColumnsSql ())
		
		return rs	
			.pipe (new DbColumnXformerCh ({lang}))
			.pipe (new DbTableCollectorCh ())

	}

}

module.exports = DbClientCh