const {DbColumn} = require ('doix-db')
const {DbLangCh} = require ('..')
const DbRecordReaderCh = require ('../lib/DbRecordReaderCh.js')

test ('basic', async () => {

	const lang = new DbLangCh ()

	const call = {db: {lang}, options: {}, objectMode: true}

	const s = new DbRecordReaderCh (call)

	const a = []
	
	s.on ('data', r => a.push (r))

	s.write ('"id",t\n')
	s.write ('"Int32",Float32\n')
	s.write ('"1",-inf')
	s.write ('\n2,nan')
	s.end ()

	expect (a).toStrictEqual ([{id: 1, t: -Infinity},{id: 2, t: Number.NaN}])

	const col = new DbColumn({name: 'id', type: 'Int32', nullable: false})
	col.setLang (lang)
	delete col.typeDim
	delete col.qName
	
	const col2 = new DbColumn({name: 't', type: 'Float32', nullable: false})
	col2.setLang (lang)
	delete col2.typeDim
	delete col2.qName

	expect (call.columns).toStrictEqual ([col, col2])

})
