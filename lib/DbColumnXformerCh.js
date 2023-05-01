const {Transform} = require ('stream')

module.exports = class extends Transform {

	constructor ({lang}) {

		super ({readableObjectMode: true, writableObjectMode: true})
		
		this.lang = lang
		
	}
			
	_transform (_src, encoding, callback) {

		const {name, type, comment, default_expression} = _src
		
		const column = this.lang.parseType (type)
		
		column.name = name
		column._src = _src
		
		column.comment = comment

		if (default_expression) column.default = default_expression

		this.push (column)

		callback ()

	}
	
}