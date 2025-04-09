const {DbColumn} = require ('doix-db')
const {DbLangCh} = require ('..')
const DbRecordReaderCh = require ('../lib/DbRecordReaderCh.js')

test ('basic', async () => {

	const lang = new DbLangCh ()

	const call = {db: {lang}, options: {}, objectMode: true}

	const s = new DbRecordReaderCh (call)

	const a = []
	
	s.on ('data', r => a.push (r))

	s.write ('["id"]\n')
	s.write ('["Int32"]\n')
	s.write ('["1"]')
	s.write ('\n["2"]')
	s.end ()

	expect (a).toStrictEqual ([{id: 1},{id: 2}])

	const col = new DbColumn({name: 'id', type: 'Int32', nullable: false})
	col.setLang (lang)
	delete col.typeDim
	delete col.qName
	
	expect (call.columns).toStrictEqual ([col])

})
