// Rebuilds MongoDB's single text index so Papers page search includes authors.
//
// Run once:
//   cd DBMS_Backend
//   node scripts/rebuild_mongo_text_index.js

require('dotenv').config();
const { MongoClient } = require('mongodb');

async function main() {
  const uri = process.env.MONGODB_URI;
  if (!uri) throw new Error('MONGODB_URI is not set in .env');

  const dbName = process.env.MONGODB_DB || 'research_db';
  const client = new MongoClient(uri);
  await client.connect();

  try {
    const collection = client.db(dbName).collection('papers');
    const indexes = await collection.indexes();
    const textIndexes = indexes.filter(index => index.key?._fts === 'text');

    for (const index of textIndexes) {
      console.log(`Dropping text index: ${index.name}`);
      await collection.dropIndex(index.name);
    }

    console.log('Creating text_search index on title, abstract, authors, journal, keywords, doi...');
    await collection.createIndex(
      {
        title: 'text',
        abstract: 'text',
        authors: 'text',
        journal: 'text',
        keywords: 'text',
        doi: 'text',
      },
      {
        name: 'text_search',
        weights: {
          title: 10,
          authors: 8,
          journal: 4,
          keywords: 3,
          abstract: 2,
          doi: 1,
        },
      }
    );

    console.log('MongoDB text index rebuilt.');
  } finally {
    await client.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
