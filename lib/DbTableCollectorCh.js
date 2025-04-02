const {Transform} = require ('stream')
const {DbTable}  = require ('doix-db')

module.exports = class extends Transform {

	constructor ({lang}) {

		super ({readableObjectMode: true, writableObjectMode: true})

		this.columns = {}

		this.lang = lang

	}
			
	_transform (column, encoding, callback) {

		const {columns} = this, {_src} = column; delete column._src
		
		columns [column.name] = column
		
		if (_src.position === 1) {

			const o = {

				qName: this.lang.quoteName (_src.table),
			
				name: _src.table,

				comment: _src.table_comment,
			
				columns,
				
			}

			const {sorting_key} = _src; if (sorting_key) o.pk = sorting_key.split (',')

			this.push (new DbTable (o))

			this.columns = {}

		}

		callback ()

	}

}