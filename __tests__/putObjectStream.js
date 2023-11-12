const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbModel} = require ('doix-db')
const {DbPoolCh} = require ('..')

const pool = new DbPoolCh ({
	url: process.env.CONNECTION_STRING,
})

pool.logger = job.logger

test ('bad option', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')
		
		const os = await db.putStream ('', ['id', 'name'], {objectMode: 1})

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


test ('bad table', async () => {

	const dbName = 'doix_test_db_4'

	const model = new DbModel ({db: pool})
	model.defaultSchema.add ('_', {columns: {
		id: 'UInt8!',
		name: 'String',
		dt: 'Date',
		ts: 'DateTime64',
		amount: 'Decimal(10,2)',
	}})
		
	try {
	
		var db = await pool.toSet (job, 'db')
		
		const os = await db.putStream ('', ['id', 'name'], {objectMode: true})

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

test ('rte', async () => {

	const dbName = 'doix_test_db_4'

	const model = new DbModel ({db: pool})
	model.defaultSchema.add ('_', {columns: {
		id: 'UInt8!',
		name: 'String',
		dt: 'Date',
		ts: 'DateTime64',
		amount: 'Decimal(10,2)',
	}})
		
	try {
	
		var db = await pool.toSet (job, 'db')

		await db.setSession ()

		await db.do (`DROP DATABASE IF EXISTS ${dbName}`)
		await db.do (`CREATE DATABASE ${dbName}`)
		db.database = dbName

		await db.do ('CREATE TEMPORARY TABLE _ (id UInt8, name Nullable(String), dt Nullable(Date), ts Nullable(DateTime64), amount Nullable(Decimal(10,2)))')

		const os = await db.putStream ('_', ['id', 'name', 'dt', 'amount', 'ts'], {objectMode: true})

		const src = [
			{id: 1, name: 'Name "One"', dt: 'xxx', amount: 3.72, ts: '2000-01-01 01:23:45.678'},
			{id: 2, name: null, dt: null, amount: null, ts: null},
		]

		let err

		try {

			await new Promise ((ok, fail) => {

				os.on ('error', fail)
				os.on ('complete', ok)
	
				for (const r of src) os.write (r)
				os.end ()
	
			})
	
		}
		catch (e) {
			err = e
		}

		expect (err.message).toMatch ('xxx')

		await db.do (`DROP DATABASE ${dbName}`)

	}
	finally {

		await db.release ()

	}
	
})

test ('basic', async () => {

	const dbName = 'doix_test_db_4'

	const model = new DbModel ({db: pool})
	model.defaultSchema.add ('_', {columns: {
		id: 'UInt8!',
		name: 'String',
		dt: 'Date',
		ts: 'DateTime64',
		amount: 'Decimal(10,2)',
	}})
		
	try {
	
		var db = await pool.toSet (job, 'db')

		await db.setSession ()

		await db.do (`DROP DATABASE IF EXISTS ${dbName}`)
		await db.do (`CREATE DATABASE ${dbName}`)
		db.database = dbName

		await db.do ('CREATE TEMPORARY TABLE _ (id UInt8, name Nullable(String), dt Nullable(Date), ts Nullable(DateTime64), amount Nullable(Decimal(10,2)))')

		const os = await db.putStream ('_', ['id', 'name', 'dt', 'amount', 'ts'], {objectMode: true})

		const src = [
			{id: 1, name: 'Name "One"', dt: '1970-01-01', amount: 3.72, ts: '2000-01-01 01:23:45.678'},
			{id: 2, name: null, dt: null, amount: null, ts: null},
		]

		await new Promise ((ok, fail) => {

			os.on ('error', fail)
			os.on ('complete', ok)

			for (const r of src) os.write (r)
			os.end ()

		})

		{
			const rs = await db.getArray ('SELECT *  FROM _')
			expect (rs).toStrictEqual (src)
		}

		await db.do (`DROP DATABASE ${dbName}`)

	}
	finally {

		await db.release ()

	}
	
})
