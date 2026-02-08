const {Transform}     = require ('stream')
const {StringDecoder} = require ('string_decoder')
const {DbTypeArithmeticInt, DbTypeArithmeticFloat} = require ('doix-db')
const {CSVEventEmitter} = require ('csv-events')

module.exports = class extends Transform {

	constructor (call) {

		super ({readableObjectMode: true})
				
		this.decoder = new StringDecoder ('utf8')
		
		this.call = call
		this.lang = call.db.lang
		this.objectMode = call.objectMode
		this.createRecord ()
		
		this.colNames = []
		this.call.columns = []

		this.ee = new CSVEventEmitter ({})

		this.ee.on ('c', c  => this.onCell (c))
		this.ee.on ('r', () => this.onEndOfLine ())

		this.onCell      = this.onNameCell
		this.onEndOfLine = this.onEndOf1stLine

	}

	createRecord () {

		this.record = this.objectMode ? {} : []

	}

	onEndOf1stLine () {

		this.onEndOfLine = this.onEndOf2ndLine

		this.onCell      = this.onTypeCell
		this.onEndOfLine = this.onEndOf2ndLine

	}

	onEndOf2ndLine () {

		this.onCell      = this.objectMode ? this.onObjectDataCell : this.onArrayDataCell
		this.onEndOfLine = this.onEndOfDataLine

	}

	onEndOfDataLine () {

		this.push (this.record)
		this.createRecord ()

	}

	onNameCell (col) {

		this.colNames [col] = this.ee.value

	}

	onTypeCell (col) {

		const def = this.lang.parseType (this.ee.value); def.name = this.colNames [col]

		this.call.columns [col] = def

	}

	getValue (col) {

		const {ee: {value}, call: {columns}} = this, {typeDef} = columns [col]

		if (value === '\\N') return null

		if (typeDef instanceof DbTypeArithmeticInt)   return parseInt (value)

		if (typeDef instanceof DbTypeArithmeticFloat) {

			switch (value) {

				case 'inf'  : return Number.POSITIVE_INFINITY
				case '-inf' : return Number.NEGATIVE_INFINITY
				case 'nan'  : return Number.NaN
				default     : return parseFloat (value)

			}

		}

		return value

	}

	onObjectDataCell (col) {

		this.record [this.colNames [col]] = this.getValue (col)

	}

	onArrayDataCell (col) {

		this.record [col] = this.getValue (col)

	}
			
	_transform (chunk, _, callback) {

		this.ee.write (this.decoder.write (chunk))
	
		callback ()

	}

	_flush (callback) {

		this.ee.once ('end', callback)

		this.ee.end (this.decoder.end ())

	}
	
}