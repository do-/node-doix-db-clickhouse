const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbClientCh, DbPoolCh} = require ('..')

const pool = new DbPoolCh ({
	url: process.env.CONNECTION_STRING,
})

test ('basic', async () => {

	const dbName = 'doix_test_db_2'
	
	try {
	
		var db = await pool.toSet (job, 'db')
		
		await db.setSession ()

		await db.do (`DROP DATABASE IF EXISTS ${dbName}`)
		await db.do (`CREATE DATABASE ${dbName}`)

		db.database = dbName

		await db.do ('CREATE TABLE users (id UInt32, label Nullable(String), salary Nullable(Decimal(10, 2)) DEFAULT 0) ENGINE MergeTree ORDER BY (id)')

		const a = [], ts = await db.getStreamOfExistingTables ()

		for await (const t of ts) a.push (t)
					
		expect (a).toHaveLength (1)
		
		const [t] = a
		
		expect (t.pk).toStrictEqual (['id'])
		expect (Object.keys (t.columns).sort ()).toStrictEqual (['id', 'label', 'salary'])
		expect (t.columns.label.type).toBe ('String')
		expect (t.columns.salary.size).toBe (10)
		expect (t.columns.salary.scale).toBe (2)

		await db.do (`DROP DATABASE ${dbName}`)

	}
	finally {

		await db.release ()

	}
	
})
