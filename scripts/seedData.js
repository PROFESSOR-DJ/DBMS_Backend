const { getMySQL } = require('../config/database');
const { getMongoDB } = require('../config/database');
require('dotenv').config();

const samplePapers = [
  {
    paper_id: 'paper_001',
    title: 'A Comprehensive Survey of Database Management Systems',
    year: 2023,
    journal: 'ACM Computing Surveys',
    abstract: 'This paper provides a comprehensive survey of modern database management systems...',
    authors: ['John Smith', 'Jane Doe', 'Robert Johnson'],
    doi: '10.1145/1234567',
    has_full_text: true,
    is_covid19: false,
    sha: 'abc123def456',
    source: 'ACM Digital Library',
    citation_count: 45,
    keywords: ['database', 'survey', 'management', 'systems']
  },
  {
    paper_id: 'paper_002',
    title: 'Machine Learning Approaches to Data Mining',
    year: 2022,
    journal: 'IEEE Transactions on Knowledge and Data Engineering',
    abstract: 'This paper explores various machine learning techniques applied to data mining problems...',
    authors: ['Robert Johnson', 'Maria Garcia', 'David Lee'],
    doi: '10.1109/TKDE.2021.1234567',
    has_full_text: true,
    is_covid19: false,
    sha: 'def456ghi789',
    source: 'IEEE Xplore',
    citation_count: 89,
    keywords: ['machine learning', 'data mining', 'algorithms', 'classification']
  },
  {
    paper_id: 'paper_003',
    title: 'Blockchain-Based Secure Data Sharing for Healthcare Research',
    year: 2023,
    journal: 'Nature Communications',
    abstract: 'We propose a blockchain-based framework for secure data sharing in healthcare systems...',
    authors: ['Wei Zhang', 'Yuki Tanaka'],
    doi: '10.1038/s41467-023-12345-6',
    has_full_text: true,
    is_covid19: true,
    sha: 'ghi789jkl012',
    source: 'Nature Publishing Group',
    citation_count: 127,
    keywords: ['blockchain', 'security', 'data sharing', 'healthcare', 'covid19']
  },
  {
    paper_id: 'paper_004',
    title: 'Big Data Analytics in Healthcare: COVID-19 Case Study',
    year: 2022,
    journal: 'Journal of Medical Systems',
    abstract: 'This study examines the application of big data analytics in healthcare systems with COVID-19 focus...',
    authors: ['Sarah Miller', 'James Wilson', 'Emma Brown'],
    doi: '10.1007/s10916-022-01845-9',
    has_full_text: true,
    is_covid19: true,
    sha: 'jkl012mno345',
    source: 'Springer',
    citation_count: 78,
    keywords: ['big data', 'healthcare', 'analytics', 'covid19', 'pandemic']
  },
  {
    paper_id: 'paper_005',
    title: 'Cloud Database Performance Optimization for Research Data',
    year: 2023,
    journal: 'Proceedings of the VLDB Endowment',
    abstract: 'We present techniques for optimizing cloud database performance through intelligent caching...',
    authors: ['Michael Chen', 'Lisa Wang'],
    doi: '10.14778/1234567.1234568',
    has_full_text: false,
    is_covid19: false,
    sha: 'mno345pqr678',
    source: 'VLDB',
    citation_count: 56,
    keywords: ['cloud computing', 'database', 'performance', 'optimization']
  }
];

const seedMySQL = async () => {
  const connection = await getMySQL();
  
  try {
    console.log('🌱 Seeding MySQL database...');
    
    // Clear existing data
    await connection.execute('DELETE FROM paper_author');
    await connection.execute('DELETE FROM author');
    await connection.execute('DELETE FROM paper');
    await connection.execute('DELETE FROM users');
    
    // Add sample papers
    for (const paper of samplePapers) {
      await connection.execute(
        'INSERT INTO paper (paper_id, title, year, journal) VALUES (?, ?, ?, ?)',
        [paper.paper_id, paper.title, paper.year, paper.journal]
      );
      
      // Add authors and relationships
      for (const authorName of paper.authors) {
        // Check if author exists
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
        
        // Create relationship
        await connection.execute(
          'INSERT INTO paper_author (paper_id, author_id) VALUES (?, ?)',
          [paper.paper_id, authorId]
        );
      }
    }
    
    // Add a test user
    const bcrypt = require('bcryptjs');
    const password_hash = await bcrypt.hash('password123', 10);
    await connection.execute(
      'INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)',
      ['testuser', 'test@example.com', password_hash]
    );
    
    console.log(`✅ MySQL seeded with ${samplePapers.length} papers`);
    
  } catch (error) {
    console.error('❌ Error seeding MySQL:', error.message);
    throw error;
  }
};

const seedMongoDB = async () => {
  const db = getMongoDB();
  
  try {
    console.log('🌱 Seeding MongoDB...');
    
    // Clear existing data
    await db.collection('papers').deleteMany({});
    
    // Add sample papers with proper schema
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
      citation_count: paper.citation_count,
      keywords: paper.keywords,
      created_at: new Date(),
      updated_at: new Date()
    }));
    
    await db.collection('papers').insertMany(papersToInsert);
    
    console.log(`✅ MongoDB seeded with ${samplePapers.length} documents`);
    console.log('   Schema includes: paper_id, title, abstract, authors, doi, has_full_text, is_covid19, journal, sha, source, year');
    
    // Show some statistics
    const stats = await db.collection('papers').aggregate([
      {
        $group: {
          _id: null,
          total: { $sum: 1 },
          covid19: { $sum: { $cond: [{ $eq: ["$is_covid19", true] }, 1, 0] } },
          withFullText: { $sum: { $cond: [{ $eq: ["$has_full_text", true] }, 1, 0] } }
        }
      }
    ]).toArray();
    
    if (stats[0]) {
      console.log('📊 Sample statistics:');
      console.log(`   Total papers: ${stats[0].total}`);
      console.log(`   COVID-19 papers: ${stats[0].covid19}`);
      console.log(`   Papers with full text: ${stats[0].withFullText}`);
    }
    
  } catch (error) {
    console.error('❌ Error seeding MongoDB:', error.message);
    throw error;
  }
};

const main = async () => {
  console.log('🚀 Starting database seeding...\n');
  
  try {
    await seedMySQL();
    console.log('');
    await seedMongoDB();
    
    console.log('\n🎉 Seeding completed successfully!');
    console.log('\nDatabase Schema Summary:');
    console.log('MySQL (Normalized Schema):');
    console.log('  - paper(id, title, year, journal)');
    console.log('  - author(id, name)');
    console.log('  - paper_author(paper_id, author_id)');
    console.log('  - users(id, username, email, password_hash)');
    
    console.log('\nMongoDB (Document Schema):');
    console.log('  - papers collection with fields matching research schema');
    console.log('  - Includes: paper_id, title, abstract, authors[], doi, is_covid19, etc.');
    
    console.log('\nTest user created:');
    console.log('- Email: test@example.com');
    console.log('- Password: password123');
    
  } catch (error) {
    console.error('\n❌ Seeding failed:', error.message);
    process.exit(1);
  }
};

main();