const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolCh} = require ('..')

const pool = new DbPoolCh ({
	url: process.env.CONNECTION_STRING,
})

pool.logger = job.logger


test ('bad', async () => {
	
	try {
	
		var db = await pool.setResource (job, 'db')
		
		const os = await db.putBinaryStream ('', ['id', 'name'])

		await new Promise ((ok, fail) => {

			os.on ('error', fail)
			os.on ('complete', ok)
			
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
	
		var db = await pool.setResource (job, 'db')
		
		await db.setSession ()

		await db.do (`DROP DATABASE IF EXISTS ${dbName}`)
		await db.do (`CREATE DATABASE ${dbName}`)
		db.database = dbName

		await db.do ('CREATE TEMPORARY TABLE _ (id UInt8, name Nullable(String), dt Nullable(Date), amount Nullable(Decimal(10,2)))')

		const os = await db.putBinaryStream ('_', ['id', 'name', 'dt', 'amount'], {})

		await new Promise ((ok, fail) => {

			os.on ('error', fail)
			os.on ('complete', ok)
			
			os.write ('1,"Name ""One""",1970-01-01,3.72\n')
			os.write ('2,,,\n')
			os.end ()
	
		})

		const a = await db.getArray ('SELECT *  FROM _')

		expect (a).toStrictEqual ([
			{id: 1, name: 'Name "One"', dt: '1970-01-01', amount: "3.72"},
			{id: 2, name: null, dt: null, amount: null},
		])		

		await db.do (`DROP DATABASE ${dbName}`)

	}
	finally {

		await db.release ()

	}
	
})
