const http = require ('http')
const EventEmitter  = require ('events')
const {randomUUID}  = require ('crypto')

class DbClientPg extends EventEmitter {

	constructor (pool) {
	
		super ()

		this.pool = pool
		
		this.uuid = randomUUID ()
	
	}
	
	release () {
		
	}
	
	async do (sql, params = [], options = {}) {
	
		const {pool} = this, {url, agent} = pool
	
//		sql = this.lang.normalizeSQL (sql)
	
		try {

			this.emit ('start', this, {sql, params})

			const result = await new Promise ((ok, fail) => {

				const req = http.request (url, {...pool.options, method: 'POST'}, res => {

					res.on ('end', () => this.emit ('finish'))

					const {statusCode} = res; if (statusCode == 200) return ok (res)

					const err = new Error (res.statusMessage)
					
					err.code = statusCode
					err.response = res

					fail (err)

				})
				
				req.write (sql)
				req.end ()

			})
			
			if (options.keep) return result
			
			result.destroy ()

		}
		catch (cause) {
		
			const {response} = cause
			
			response.setEncoding ('utf8')
			
			let {message} = response; for await (const s of response) message += s

			const e = new Error (message)

			this.emit ('error', this, e)

			throw Error ('ClickHouse server returned an error: ' + message, {cause})

		}

	}

}

module.exports = DbClientPg