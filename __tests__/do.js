const MockJob = require ('./lib/MockJob.js'), job = new MockJob (), jobSuper = new MockJob ()
const {DbPoolCh} = require ('..')
const dbName = 'doix_test_db_1'

const poolSuper = new DbPoolCh ({
	url: process.env.CONNECTION_STRING,
})

poolSuper.logger = jobSuper.logger

const pool = new DbPoolCh ({
	url: process.env.CONNECTION_STRING,
	database: dbName,
})

pool.logger = job.logger

test ('e7707', async () => {
	
	try {
	
		var db = await pool.setResource (job, 'db')

		await db.do ('...')

		throw '?'

	}
	catch (err) {

		expect (err.stack).toMatch ('do.js')

	}
	finally {

		await db.release ()

	}
	
})

test ('basic', async () => {
	
	try {

		var db = await poolSuper.setResource (jobSuper, 'db')
		
		await db.do (`DROP DATABASE IF EXISTS ${dbName}`)
		await db.do (`CREATE DATABASE ${dbName}`)

	}
	finally {

		await db.release ()

	}
	
	try {
	
		var db = await pool.setResource (job, 'db')

		await db.do (`DROP TABLE IF EXISTS ${dbName}._t`)
		await db.do (`CREATE TABLE ${dbName}._t ENGINE MergeTree ORDER BY (id) AS SELECT "number" id FROM system.numbers LIMIT ?`, [2])
		const data = await db.getArray (`SELECT * FROM _t`)
		expect (data).toEqual ([{id: 0}, {id: 1}])
		await db.do (`DROP DATABASE ${dbName}`)

	}
	finally {

		await db.release ()

	}

})
