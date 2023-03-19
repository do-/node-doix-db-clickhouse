const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbClientCh, DbPoolCh} = require ('..')

const pool = new DbPoolCh ({
	url: process.env.CONNECTION_STRING,
})

test ('e7707', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		await expect (			
			(async () => {
				const res = await db.do ('...', [], {keep: true})
			})()			
		).rejects.toThrow ()

	}
	finally {

		await db.release ()

	}
	
})

test ('basic', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')
		
		await db.do ('DROP TABLE IF EXISTS _t')
		await db.do ('CREATE TABLE _t ENGINE Memory AS SELECT "number" id FROM system.numbers LIMIT ?', [2])

		const res = await db.do ('select * from _t', [], {keep: true})

		res.setEncoding ('utf8')
			
		let t = ''; for await (const s of res) t += s
		
		expect (t).toBe ('0\n1\n')

	}
	finally {

		await db.release ()

	}
	
})
