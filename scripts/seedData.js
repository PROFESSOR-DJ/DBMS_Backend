const { connectMySQL, connectMongoDB, getMySQL, getMongoDB } = require('../config/database');
const bcrypt = require('bcryptjs');
require('dotenv').config();

// WARNING BANNER
console.log('\n⚠️  ═══════════════════════════════════════════════════════════ ⚠️');
console.log('⚠️  WARNING: This script will DELETE ALL existing data!        ⚠️');
console.log('⚠️  This should ONLY be used for development/testing.          ⚠️');
console.log('⚠️  ═══════════════════════════════════════════════════════════ ⚠️\n');

const readline = require('readline');
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

const askConfirmation = () => {
  return new Promise((resolve) => {
    rl.question('Type "DELETE ALL DATA" to confirm you want to proceed: ', (answer) => {
      rl.close();
      resolve(answer === 'DELETE ALL DATA');
    });
  });
};

const samplePapers = [
  {
    paper_id: 'sample_001',
    title: 'Sample Paper - A Comprehensive Survey of Database Management Systems',
    year: 2023,
    journal: 'ACM Computing Surveys',
    abstract: 'This is a sample paper for testing purposes.',
    authors: ['John Smith', 'Jane Doe'],
    doi: '10.1145/sample001',
    has_full_text: true,
    is_covid19: false,
    sha: 'sample123',
    source: 'Sample Data',
    citation_count: 10,
    keywords: ['database', 'sample']
  }
];

const seedMySQL = async () => {
  const connection = getMySQL();
  
  try {
    console.log('🌱 Seeding MySQL database...');
    
    // Clear existing data
    await connection.execute('SET FOREIGN_KEY_CHECKS = 0');
    await connection.execute('TRUNCATE TABLE paper_author');
    await connection.execute('TRUNCATE TABLE author');
    await connection.execute('TRUNCATE TABLE paper');
    await connection.execute('TRUNCATE TABLE users');
    await connection.execute('SET FOREIGN_KEY_CHECKS = 1');
    
    console.log('   ✓ Cleared existing data');
    
    // Add sample papers
    for (const paper of samplePapers) {
      await connection.execute(
        'INSERT INTO paper (paper_id, title, year, journal) VALUES (?, ?, ?, ?)',
        [paper.paper_id, paper.title, paper.year, paper.journal]
      );
      
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
        
        await connection.execute(
          'INSERT INTO paper_author (paper_id, author_id) VALUES (?, ?)',
          [paper.paper_id, authorId]
        );
      }
    }
    
    console.log(`   ✓ Added ${samplePapers.length} sample papers`);
    
    // Add test user
    const password_hash = await bcrypt.hash('password123', 10);
    await connection.execute(
      'INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)',
      ['testuser', 'test@example.com', password_hash]
    );
    
    console.log('   ✓ Added test user');
    console.log('✅ MySQL seeded successfully\n');
    
  } catch (error) {
    console.error('❌ Error seeding MySQL:', error.message);
    throw error;
  }
};

const seedMongoDB = async () => {
  const db = getMongoDB();
  
  try {
    console.log('🌱 Seeding MongoDB...');
    
    const deleteResult = await db.collection('papers').deleteMany({});
    console.log(`   ✓ Cleared ${deleteResult.deletedCount} existing documents`);
    
    const papersToInsert = samplePapers.map(paper => ({
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
    }));
    
    const result = await db.collection('papers').insertMany(papersToInsert);
    console.log(`   ✓ Added ${result.insertedCount} sample documents`);
    console.log('✅ MongoDB seeded successfully\n');
    
  } catch (error) {
    console.error('❌ Error seeding MongoDB:', error.message);
    throw error;
  }
};

const main = async () => {
  console.log('\n🚀 Database Seed Script\n');
  
  // Ask for confirmation
  const confirmed = await askConfirmation();
  
  if (!confirmed) {
    console.log('\n❌ Seeding cancelled. Your data is safe.\n');
    process.exit(0);
  }
  
  try {
    console.log('\n📡 Initializing database connections...\n');
    await connectMySQL();
    await connectMongoDB();
    
    await seedMySQL();
    await seedMongoDB();
    
    console.log('🎉 Seeding completed!\n');
    console.log('Test user: test@example.com / password123\n');
    
    process.exit(0);
    
  } catch (error) {
    console.error('\n❌ Seeding failed:', error.message);
    process.exit(1);
  }
};

main();