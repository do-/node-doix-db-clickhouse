const {Transform} = require ('stream')
const {DbColumn}  = require ('doix-db')

module.exports = class extends Transform {

	constructor (options) {

		super ({readableObjectMode: true, writableObjectMode: true})
		
	}
			
	_transform (chunk, encoding, callback) {

		const {name, type, comment, default_expression, numeric_precision, numeric_scale} = chunk

		const o = {name, type, comment, _src: chunk}
		
		if (default_expression) o.default = default_expression

		if (numeric_precision > 0) o.size = numeric_precision

		if (numeric_scale > 0) o.scale = numeric_scale
		
		if (type.slice (0, 9) === 'Nullable(') {

			o.type = type.slice (9, type.length - 1)

			o.nullable = true

		}
		else {

			o.nullable = false

		}
		
		{
		
			const pos = o.type.indexOf ('(')
			
			if (pos !== -1) o.type = o.type.slice (0, pos)

		}
		
		this.push (new DbColumn (o))
	
		callback ()

	}
	
}