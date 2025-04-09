const Path = require ('path')
const {DbModel} = require ('doix-db')
const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolCh} = require ('..')

const pool = new DbPoolCh ({
	url: process.env.CONNECTION_STRING,
})

pool.logger = job.logger

test ('model', async () => {

	try {

		const dbName = 'doix_test_db_5'

		const model = new DbModel ({
			src: {
				root: Path.join (__dirname, 'data', 'root1')
			},
			db: pool
		})

		var db = await pool.setResource (job, 'db')

		expect (() => [...db.lang.genAlter ({}, {})]).toThrow ("Don't")
		expect (() => db.lang.genReCreate ({}, {})).toThrow ("Don't")

		expect (() => [...db.lang.genDDL ()]).toThrow ("without")

		await db.do (`DROP DATABASE IF EXISTS ${dbName}`)
		await db.do (`CREATE DATABASE ${dbName}`)
		db.database = dbName

		{

			const plan = db.createMigrationPlan ()

			for (const i of db.lang.genDDL ()); // coverage: DDL for empty plan

			await plan.loadStructure ()
			plan.inspectStructure ()

			expect ([...plan.genDDL ()]).toHaveLength (0)

		}

		model.loadModules ()

		const plan = db.createMigrationPlan ()
		await plan.loadStructure ()
		plan.inspectStructure ()

		expect (() => db.lang.genCreate (db.model.find ('vw_1'))).toThrow ("'t know")

		await db.doAll (plan.genDDL ())
		
		{

/*			
			{

				const a = await db.getArray ('SELECT * FROM tb_1 ORDER BY id')

				expect (a).toStrictEqual ([
					{id: 0, label: 'zero', amount: '0.00', cnt: 1},
					{id: 1, label: 'on', amount: '0.00', cnt: 1},
				])
			
			}
*/				

			await db.do ('INSERT INTO tb_1 (id, label) VALUES (?, ?)', [1, 'one'])

			{

				const a = await db.getArray ('SELECT * FROM tb_1 ORDER BY id')

				expect (a).toStrictEqual ([
					{id: 1, label: 'one', amount: '0.00', cnt: 0},
				])
			
			}	

		}			
		

		{

			await db.do ('INSERT INTO tb_0 (id) VALUES (?)', [1])

			expect (await db.getArray ('SELECT * FROM tb_0 ORDER BY id')).toStrictEqual ([
				{id: 1},
			])

			await db.do (`ALTER TABLE tb_0 ADD COLUMN drop_me String DEFAULT ''`)
			await db.do (`ALTER TABLE tb_0 ADD COLUMN drop_me_too String DEFAULT ''`)

			expect (await db.getArray ('SELECT * FROM tb_0 ORDER BY id')).toStrictEqual ([
				{id: 1, drop_me: '', drop_me_too: ''},
			])

			const plan = db.createMigrationPlan (); await plan.loadStructure (); plan.inspectStructure ()

			await db.doAll (plan.genDDL ())

			expect (await db.getArray ('SELECT * FROM tb_0 ORDER BY id')).toStrictEqual ([
				{id: 1},
			])

		}

		{
		
			await db.do (`ALTER TABLE tb_1 DROP COLUMN amount`)

			const plan = db.createMigrationPlan (); await plan.loadStructure (); plan.inspectStructure ()

			{

				const a = await db.getArray ('SELECT * FROM tb_1 ORDER BY id')

				expect (a).toStrictEqual ([
					{id: 1, label: 'one', cnt: 0},
				])
			
			}

			for (const [sql, params] of plan.genDDL ()) await db.do (sql, params)


			{

				const a = await db.getArray ('SELECT * FROM tb_1 ORDER BY id')

				expect (a).toStrictEqual ([
					{id: 1, label: 'one', amount: '0.00', cnt: 0},
				])
			
			}

		}
		
		{
		
			await db.do (`TRUNCATE TABLE tb_1`)

			await db.do (`ALTER TABLE tb_1 MODIFY COLUMN amount Decimal(5,1) DEFAULT 1,  MODIFY COLUMN cnt DEFAULT 1`)
			
			await db.do ('INSERT INTO tb_1 (id, label) VALUES (?, ?)', [2, 'two'])

			{

				const a = await db.getArray ('SELECT * FROM tb_1 ORDER BY id')

				expect (a).toStrictEqual ([
					{id: 2, label: 'two', amount: '1.0', cnt: 1}
				])
			
			}

			const plan = db.createMigrationPlan (); await plan.loadStructure (); plan.inspectStructure ()

			await db.doAll (plan.genDDL ())

			{

				const a = await db.getArray ('SELECT * FROM tb_1 ORDER BY id')

				expect (a).toStrictEqual ([
					{id: 2, label: 'two', amount: '1.00', cnt: 1}
				])
			
			}

		}

		{
		
			await db.do (`TRUNCATE TABLE tb_1`)

			await db.do (`ALTER TABLE tb_1 MODIFY COLUMN amount Nullable(Decimal(5,1)) DEFAULT NULL,  MODIFY COLUMN cnt DEFAULT 2`)
			
			await db.do ('INSERT INTO tb_1 (id, label, amount) VALUES (?, ?, ?)', [2, 'two', 1])
			await db.do ('INSERT INTO tb_1 (id, label) VALUES (?, ?)', [3, 'three'])

			{

				const a = await db.getArray ('SELECT * FROM tb_1 ORDER BY id')

				expect (a).toStrictEqual ([
					{id: 2, label: 'two',   amount: '1.0', cnt: 2},
					{id: 3, label: 'three', amount: null,  cnt: 2}
				])
			
			}

			const plan = db.createMigrationPlan (); await plan.loadStructure (); plan.inspectStructure ()

			await db.doAll (plan.genDDL ())

			{

				const a = await db.getArray ('SELECT * FROM tb_1 ORDER BY id')

				expect (a).toStrictEqual ([
					{id: 2, label: 'two',   amount: '1.00', cnt: 2},
					{id: 3, label: 'three', amount: '0.00', cnt: 2}
				])
			
			}

		}

		{

			const a = await db.getArray ('SELECT * FROM vw_1 ORDER BY id')

			expect (a).toStrictEqual ([{id: 1}])

		}

		await db.do (`DROP DATABASE IF EXISTS ${dbName}`)

	}
	finally {

		await db.release ()

	}

})