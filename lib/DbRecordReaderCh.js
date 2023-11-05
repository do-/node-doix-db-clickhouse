const {Transform}     = require ('stream')
const {StringDecoder} = require ('string_decoder')

module.exports = class extends Transform {

	constructor (call) {

		super ({readableObjectMode: true})
		
		this.names = []

		this.s = ''
		
		this.decoder = new StringDecoder ('utf8')
		
		this.call = call

		this.lang = call.db.lang

		if (!call.objectMode) this.rowMode = 'array'
		
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

		const a = JSON.parse (s), {names} = this, {length} = names

		if (length === 0) {

			this.names = a

		}
		else if (!this.call.columns) {
				
			this.call.columns = []
		
			for (let i = 0; i < length; i ++) {
			
				const col = this.lang.parseType (a [i]), {type} = col
				
				if (type.slice (0, 3) === 'Int' || type.slice (0, 4) === 'UInt') this.intCols.push (i)
				
				col.name = names [i]
			
				this.call.columns.push (col)
		
			}

		}
		else {

			for (const i of this.intCols)

				if (a [i] !== null) 

					a [i] = parseInt (a [i])

			switch (this.rowMode) {

				case 'array':
					this.push (a)
					break

				default: 
					const r = {}; for (let i = 0; i < length; i ++) r [names [i]] = a [i]
					this.push (r)

			}

		}
	
	}

}