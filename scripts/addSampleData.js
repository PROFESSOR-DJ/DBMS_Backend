const { connectMySQL, connectMongoDB, getMySQL, getMongoDB } = require('../config/database');
const bcrypt = require('bcryptjs');
require('dotenv').config();

console.log('\n✅ SAFE MODE: This script will NOT delete existing data\n');

const samplePapers = [
  {
    paper_id: 'sample_test_001',
    title: 'Sample Test Paper for Development',
    year: 2024,
    journal: 'Test Journal',
    abstract: 'This is a sample paper added for testing without deleting existing data.',
    authors: ['Test Author'],
    doi: '10.1145/test001',
    has_full_text: true,
    is_covid19: false,
    sha: 'test123',
    source: 'Test Data',
    citation_count: 0,
    keywords: ['test', 'sample']
  }
];

const addToMySQL = async () => {
  const connection = getMySQL();
  
  try {
    console.log('📝 Adding sample data to MySQL...');
    
    let added = 0, skipped = 0;
    
    for (const paper of samplePapers) {
      const [existing] = await connection.execute(
        'SELECT paper_id FROM paper WHERE paper_id = ?',
        [paper.paper_id]
      );
      
      if (existing.length > 0) {
        skipped++;
        continue;
      }
      
      await connection.execute(
        'INSERT INTO paper (paper_id, title, year, journal) VALUES (?, ?, ?, ?)',
        [paper.paper_id, paper.title, paper.year, paper.journal]
      );
      added++;
      
      for (const authorName of paper.authors) {
        const [existingAuthor] = await connection.execute(
          'SELECT author_id FROM author WHERE name = ?',
          [authorName]
        );
        
        let authorId;
        if (existingAuthor.length > 0) {
          authorId = existingAuthor[0].author_id;
        } else {
          const [authorResult] = await connection.execute(
            'INSERT INTO author (name) VALUES (?)',
            [authorName]
          );
          authorId = authorResult.insertId;
        }
        
        try {
          await connection.execute(
            'INSERT INTO paper_author (paper_id, author_id) VALUES (?, ?)',
            [paper.paper_id, authorId]
          );
        } catch (err) {
          if (err.code !== 'ER_DUP_ENTRY') throw err;
        }
      }
    }
    
    console.log(`   ✓ Added: ${added}, Skipped: ${skipped}`);
    
    // Add test user if not exists
    const [existingUser] = await connection.execute(
      'SELECT user_id FROM users WHERE email = ?',
      ['test@example.com']
    );
    
    if (existingUser.length === 0) {
      const password_hash = await bcrypt.hash('password123', 10);
      await connection.execute(
        'INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)',
        ['testuser', 'test@example.com', password_hash]
      );
      console.log('   ✓ Added test user');
    }
    
    console.log('✅ MySQL update complete\n');
    
  } catch (error) {
    console.error('❌ Error updating MySQL:', error.message);
    throw error;
  }
};

const addToMongoDB = async () => {
  const db = getMongoDB();
  
  try {
    console.log('📝 Adding sample data to MongoDB...');
    
    let added = 0, skipped = 0;
    
    for (const paper of samplePapers) {
      const existing = await db.collection('papers').findOne({ paper_id: paper.paper_id });
      
      if (existing) {
        skipped++;
        continue;
      }
      
      await db.collection('papers').insertOne({
        paper_id: paper.paper_id,
        title: paper.title,
        abstract: paper.abstract,
        authors: paper.authors,
        doi: paper.doi,
        has_full_text: paper.has_full_text,
        is_covid19: paper.is_covid19,
        journal: paper.journal,
        sha: paper.sha,
        source: paper.source,
        year: paper.year,
        citation_count: paper.citation_count || 0,
        keywords: paper.keywords || []
      });
      added++;
    }
    
    const total = await db.collection('papers').countDocuments();
    console.log(`   ✓ Added: ${added}, Skipped: ${skipped}`);
    console.log(`   📊 Total documents: ${total.toLocaleString()}`);
    console.log('✅ MongoDB update complete\n');
    
  } catch (error) {
    console.error('❌ Error updating MongoDB:', error.message);
    throw error;
  }
};

const main = async () => {
  try {
    await connectMySQL();
    await connectMongoDB();
    
    await addToMySQL();
    await addToMongoDB();
    
    console.log('🎉 Sample data added successfully!\n');
    process.exit(0);
    
  } catch (error) {
    console.error('\n❌ Failed:', error.message);
    process.exit(1);
  }
};

main();