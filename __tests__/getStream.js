const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolCh} = require ('..')

const pool = new DbPoolCh ({
	url: process.env.CONNECTION_STRING,
})

pool.logger = job.logger

test ('basic', async () => {
	
	try {
	
		var db = await pool.setResource (job, 'db')
		
		const res = await db.getStream ('select "number" id from system.numbers LIMIT 2')
			
		let a = []; for await (const r of res) a.push (r)

		expect (a).toStrictEqual ([{id: 0}, {id: 1}])

	}
	finally {

		await db.release ()

	}
	
})

test ('array', async () => {
	
	try {
	
		pool.setProxy (job, 'db')
		
		const res = await job.db.getStream ('select "number" id from system.numbers LIMIT ?', [2], {rowMode: 'array'})
			
		let a = []; for await (const r of res) a.push (r)

		expect (a).toStrictEqual ([[0],[1]])

	}
	finally {

		await job.db.release ()

	}
	
})