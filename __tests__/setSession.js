const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolCh} = require ('..')

const pool = new DbPoolCh ({
	url: process.env.CONNECTION_STRING,
})

pool.logger = job.logger

test ('basic', async () => {
	
	try {
	
		await pool.setProxy (job, 'db')
				
		await job.db.setSession ()

		expect (job.db.getURL (true)).toMatch ('session_id=' + job.db.uuid)
		
		await job.db.setSession (null)

		expect (job.db.getURL (false)).not.toMatch ('session_id')

		await job.db.setSession ('abc', 123)

		expect (job.db.getURL (true)).toMatch ('session_id=abc')
		expect (job.db.getURL (false)).toMatch ('session_timeout=123')

	}
	finally {

		await job.db.release ()

	}
	
})
