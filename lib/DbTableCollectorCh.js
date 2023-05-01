const {Transform} = require ('stream')
const {DbTable}  = require ('doix-db')

module.exports = class extends Transform {

	constructor (options) {

		super ({readableObjectMode: true, writableObjectMode: true})
		
		this.columns = {}
		
	}
			
	_transform (column, encoding, callback) {
	
		const {columns} = this, {_src} = column; delete column._src
		
		columns [column.name] = column
		
		if (_src.position === 1) {

			this.push (new DbTable ({
			
				name: _src.table,

				comment: _src.table_comment,

				pk: _src.sorting_key.split (','),
			
				columns,
				
			}))

		}

		callback ()

	}

}