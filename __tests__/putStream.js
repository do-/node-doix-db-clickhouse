const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolCh} = require ('..')

const pool = new DbPoolCh ({
	url: process.env.CONNECTION_STRING,
})

pool.logger = job.logger


test ('bad', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')
		
		const os = await db.putStream ('', ['id', 'name'])

		const call = os [Symbol.for ('doixDbCall')]

		await new Promise ((ok, fail) => {

			os.on ('error', fail)
			call.on ('finish', () => call.error ? null : ok ())
			
			os.write ('1,"Name One"\n')
			os.end ()
	
		})

	}
	catch (err) {

		expect (err).toBeInstanceOf (Error)

	}
	finally {

		await db.release ()

	}
	
})

test ('basic', async () => {

	const dbName = 'doix_test_db_3'
	
	try {
	
		var db = await pool.toSet (job, 'db')
		
		await db.setSession ()

		await db.do (`DROP DATABASE IF EXISTS ${dbName}`)
		await db.do (`CREATE DATABASE ${dbName}`)
		db.database = dbName

		await db.do ('CREATE TEMPORARY TABLE _ (id UInt8, name String)')

		const os = await db.putStream ('_', ['id', 'name'])
		const call = os [Symbol.for ('doixDbCall')]

		await new Promise ((ok, fail) => {

			os.on ('error', fail)
			call.on ('finish', () => call.error ? null : ok ())
			
			os.write ('1,"Name One"\n')
			os.end ()
	
		})

		const a = await db.getArray ('SELECT *  FROM _')

		expect (a).toStrictEqual ([{id: 1, name: 'Name One'}])		

		await db.do (`DROP DATABASE ${dbName}`)

	}
	finally {

		await db.release ()

	}
	
})
