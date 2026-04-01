// test_author_create exercises backend author creation logic from a script.
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

        
        console.log('\nCase 1: Creating author without paper_id');
        const authorName1 = 'Test Author ' + Date.now();
        const result1 = await AuthorModel.create({ author_name: authorName1 });
        console.log('✓ Created author 1:', result1.insertId);

        
        
        
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
