const {Agent} = require ('http')

const {DbEventLogger, DbPool, DbLang} = require ('doix-db')

const DbClientCh = require ('./DbClientCh.js')

class DbPoolCh extends DbPool {

	constructor (o) {

		super (o)
		
		this.url = o.url

		if (!o.options) o.options = {}

		if (!o.options.agent) o.options.agent = new Agent (o.agent || {})

		this.options = o.options
		
		this.wrapper = DbClientCh
		
		this.lang = o.lang || new DbLang ()

	}

	async acquire () {

		return this

	}

}

module.exports = DbPoolCh