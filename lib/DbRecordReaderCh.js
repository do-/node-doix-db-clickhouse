const {DbTypeArithmeticInt, DbTypeArithmeticFloat} = require ('doix-db')
const {CSVColumn, CSVReader} = require ('csv-events')

class ResultColumn extends CSVColumn {

	constructor (reader, options) {

		super (reader, options)

		const def = reader.call.db.lang.parseType (options [1])

		this.typeDef = def.typeDef

		def.name = this.name

		reader.call.columns.push (def)

	}

	get value () {

		const v = super.value; if (v === '\\N') return null

		const {typeDef} = this

		if (typeDef instanceof DbTypeArithmeticInt) return parseInt (v)

 		if (typeDef instanceof DbTypeArithmeticFloat) {

			switch (v) {

				case 'inf'  : return Number.POSITIVE_INFINITY
				case '-inf' : return Number.NEGATIVE_INFINITY
				case 'nan'  : return Number.NaN
				default     : return parseFloat (v)

			}

		}

		return v

	}

}

module.exports = class extends CSVReader {

	constructor (call) {

		super ({
			empty: '',
			header: 2,
			recordClass: call.objectMode ? Object : Array,
			columnClass: ResultColumn
		})
		
		this.call = call
		call.columns = []
		this.lang = call.db.lang

	}
	
}