const {DbPoolCh} = require ('..')

test ('basic', async () => {

	const url = process.env.CONNECTION_STRING

	expect (new DbPoolCh ({url: process.env.CONNECTION_STRING})).toBeInstanceOf (DbPoolCh)
	expect (new DbPoolCh ({url: process.env.CONNECTION_STRING, options: {}})).toBeInstanceOf (DbPoolCh)
	expect (new DbPoolCh ({url: process.env.CONNECTION_STRING, options: {agent: {}}})).toBeInstanceOf (DbPoolCh)
	
})
