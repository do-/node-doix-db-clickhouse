const {Transform}     = require ('stream')
const {StringDecoder} = require ('string_decoder')
const {DbTypeArithmeticInt} = require ('doix-db')

module.exports = class extends Transform {

	constructor (call) {

		super ({readableObjectMode: true})
		
		this.entries = []

		this.s = ''
		
		this.decoder = new StringDecoder ('utf8')
		
		this.call = call

		this.lang = call.db.lang

		this.objectMode = call.objectMode
		
		this.intCols = []

	}
			
	_transform (chunk, encoding, callback) {

		this.split (this.decoder.write (chunk))
	
		callback ()

	}

	_flush (callback) {
			
		this.split (this.decoder.end ())
		
		if (this.s.length !== 0) this.process (this.s)

		callback ()

	}
	
	split (d) {
	
		const s = this.s + d
		
		let prev = 0
		
		while (true) {
		
			let next = s.indexOf ('\n', prev)
			
			if (next < 0) {
			
				this.s = s.slice (prev)
				
				break
			
			}
			
			this.process (s.slice (prev, next))
			
			prev = next + 1

		}

	}

	process (s) {

		let a = JSON.parse (s), {entries} = this, {length} = entries

		if (length === 0) {

			this.entries = a.map (i => [i, null])

		}
		else if (!this.call.columns) {
				
			this.call.columns = []
		
			for (let i = 0; i < length; i ++) {
			
				const col = this.lang.parseType (a [i]); col.name = entries [i] [0]

				this.call.columns.push (col)

				if (col.typeDef instanceof DbTypeArithmeticInt) this.intCols.push (i)
		
			}

		}
		else {

			for (const i of this.intCols) if (a [i] !== null) a [i] = parseInt (a [i])

			if (this.objectMode) {
				
				for (let i = 0; i < length; i ++) entries [i] [1] = a [i]
								
				a = Object.fromEntries (entries)

			}

			this.push (a)

		}
	
	}

}