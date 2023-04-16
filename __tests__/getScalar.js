const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbClientCh, DbPoolCh} = require ('..')

const pool = new DbPoolCh ({
	url: process.env.CONNECTION_STRING,
})

test ('c0NNe+|0N e7707', async () => {


	try {
	
		var db = await (new DbPoolCh ({})).toSet (job, 'db')

		const s = await db.getScalar ('...')	

	}
	catch (x) {

		expect (x).toBeDefined ()

	}
	finally {

		await db.release ()

	}

})
	
test ('e7707', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		const s = await db.getScalar ('...')	

	}
	catch (x) {

		expect (x).toBeInstanceOf (Error)

	}
	finally {

		await db.release ()

	}
	
})

test ('sequence', async () => {
	
	try {
	
		pool.setProxy (job, 'db')

		const o = await job.db.getScalar ('SELECT number from system.numbers where number between ? AND ?', [1, 10])

		expect (o).toBe (1)

	}
	finally {

		await job.db.release ()

	}
	
})

test ('1-to-1', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		const o = await db.getScalar ('SELECT 1::UInt32 id', [])

		expect (o).toBe (1)

	}
	finally {

		await db.release ()

	}
	
})

test ('default', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		const o = await db.getScalar ('SELECT ? id WHERE 0=1', ['z'])

		expect (o).toBeUndefined ()

	}
	finally {

		await db.release ()

	}
	
})

test ('custom default', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		const DEF = -1

		const o = await db.getScalar ('SELECT 1 id WHERE 0=1', [], {notFound: DEF})

		expect (o).toBe (DEF)

	}
	finally {

		await db.release ()

	}
	
})

test ('custom error', async () => {
	
	const DEF = new Error ('Not Found')

	try {
	
		var db = await pool.toSet (job, 'db')
		
		const o = await db.getScalar ('SELECT 1 id WHERE 0=1', [], {notFound: DEF})

	}
	catch (x) {

		expect (x).toBe (DEF)

	}
	finally {

		await db.release ()

	}
	
})
