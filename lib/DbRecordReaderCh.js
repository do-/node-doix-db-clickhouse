const {Transform}     = require ('stream')
const {StringDecoder} = require ('string_decoder')

const COLS = Symbol.for ('columns')

module.exports = class extends Transform {

	constructor (options) {

		super ({readableObjectMode: true})
		
		this.names = []
		this [COLS] = null

		this.s = ''
		
		this.decoder = new StringDecoder ('utf8')
		
		this.isArrayMode = options.rowMode === 'array'

		this.lang = options.lang

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
		else if (this [COLS] === null) {
				
			this [COLS] = []
		
			for (let i = 0; i < length; i ++) {
			
				const col = this.lang.parseType (a [i])
				
				col.name = names [i]
			
				this [COLS].push (col)
		
			}
		
		}
		else if (this.isArrayMode) {

			this.push (a)

		}
		else {

			const r = {}; for (let i = 0; i < length; i ++) r [names [i]] = a [i]

			this.push (r)

		}
	
	}

}