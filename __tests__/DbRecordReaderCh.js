const DbRecordReaderCh = require ('../lib/DbRecordReaderCh.js')

test ('basic', async () => {

	const s = new DbRecordReaderCh ({})
	
	const a = []
	
	s.on ('data', r => a.push (r))

	s.write ('["id"]\n')
	s.write ('["Int"]\n')
	s.write ('["1"]')
	s.write ('\n["2"]')
	s.end ()
		
	expect (a).toStrictEqual ([{id: '1'},{id: '2'}])
	
})
