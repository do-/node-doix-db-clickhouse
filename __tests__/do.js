const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolCh} = require ('..')

const pool = new DbPoolCh ({
	url: process.env.CONNECTION_STRING,
})

pool.logger = job.logger

test ('e7707', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		await expect (db.do ('...')).rejects.toThrow ()

	}
	finally {

		await db.release ()

	}
	
})

test ('basic', async () => {

	const dbName = 'doix_test_db_1'
	
	try {
	
		var db = await pool.toSet (job, 'db')
		
		await db.setSession ()

		await db.do (`DROP DATABASE IF EXISTS ${dbName}`)
		await db.do (`CREATE DATABASE ${dbName}`)
		db.database = dbName
		await db.do (`DROP TABLE IF EXISTS _t`)
		await db.do ('CREATE TABLE _t ENGINE MergeTree ORDER BY (id) AS SELECT "number" id FROM system.numbers LIMIT ?', [2])
		await db.do (`DROP DATABASE ${dbName}`)

	}
	finally {

		await db.release ()

	}
	
})
