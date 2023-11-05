const {DbColumn} = require ('doix-db')
const {DbLangCh} = require ('..')
const DbRecordReaderCh = require ('../lib/DbRecordReaderCh.js')

test ('basic', async () => {

	const call = {db: {lang: new DbLangCh ()}, options: {}, objectMode: true}

	const s = new DbRecordReaderCh (call)

	const a = []
	
	s.on ('data', r => a.push (r))

	s.write ('["id"]\n')
	s.write ('["Int32"]\n')
	s.write ('["1"]')
	s.write ('\n["2"]')
	s.end ()

	expect (a).toStrictEqual ([{id: 1},{id: 2}])
	
	expect (call.columns).toStrictEqual ([new DbColumn({name: 'id', type: 'Int32'})])

})
