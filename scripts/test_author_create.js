const { connectMySQL, getMySQL } = require('../config/database');
const AuthorModel = require('../models/mysql/authorModel');

async function test() {
    try {
        console.log('Testing AuthorModel.create...');
        const connected = await connectMySQL();
        if (!connected) {
            console.error('Failed to connect to MySQL');
            return;
        }

        // 1. Test creation without paper_id
        console.log('\nCase 1: Creating author without paper_id');
        const authorName1 = 'Test Author ' + Date.now();
        const result1 = await AuthorModel.create({ author_name: authorName1 });
        console.log('✓ Created author 1:', result1.insertId);

        // 2. Test creation with paper_id
        // First, let's find a valid paper_id if possible, or just use a dummy one if we are sure it exists
        // For safety, let's query for one
        const pool = getMySQL();
        const [papers] = await pool.execute('SELECT paper_id FROM papers LIMIT 1');

        if (papers.length > 0) {
            const paper_id = papers[0].paper_id;
            console.log(`\nCase 2: Creating author with paper_id: ${paper_id}`);
            const authorName2 = 'Test Author Linked ' + Date.now();
            const result2 = await AuthorModel.create({ author_name: authorName2, paper_id });
            console.log('✓ Created author 2 and linked to paper:', result2.insertId);
        } else {
            console.log('\nCase 2: Skipped (no papers found in database)');
        }

        console.log('\nTest completed successfully!');
    } catch (error) {
        console.error('\nTest failed:', error);
    } finally {
        process.exit(0);
    }
}

test();
