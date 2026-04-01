// initDatabases creates and prepares the backend database structures.
const initMongoDB = async () => {
  let client;
  try {
    console.log('🔧 Initializing MongoDB...');
    
    client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db(process.env.MONGODB_DB);

    
    const collections = await db.listCollections({ name: 'papers' }).toArray();
    if (collections.length > 0) {
      console.log('⚠️ Dropping existing papers collection to avoid index conflicts...');
      await db.collection('papers').drop();
    }

    
    await db.createCollection('papers');
    
    
    const papersCollection = db.collection('papers');
    
    
    console.log('Creating text index for search...');
    try {
      
      const existingIndexes = await papersCollection.indexes();
      for (const index of existingIndexes) {
        if (index.name === 'text_search' || index.key && index.key._fts === 'text') {
          await papersCollection.dropIndex(index.name);
          console.log(`Dropped existing index: ${index.name}`);
        }
      }
      
      
      await papersCollection.createIndex(
        { title: 'text', abstract: 'text' },
        { name: 'text_search' }
      );
      console.log('✅ Created text index');
    } catch (indexError) {
      console.log('⚠️ Could not create text index:', indexError.message);
    }

    
    console.log('Creating field indexes...');
    await papersCollection.createIndex({ journal: 1 }, { name: 'idx_journal' });
    await papersCollection.createIndex({ year: 1 }, { name: 'idx_year' });
    await papersCollection.createIndex({ authors: 1 }, { name: 'idx_authors' });
    await papersCollection.createIndex({ paper_id: 1 }, { name: 'idx_paper_id', unique: true });
    await papersCollection.createIndex({ is_covid19: 1 }, { name: 'idx_covid19' });
    await papersCollection.createIndex({ source: 1 }, { name: 'idx_source' });

    console.log('✅ MongoDB initialized successfully');
    console.log('   - Created: papers collection');
    console.log('   - Added: 7 indexes for optimized queries');
    
  } catch (error) {
    console.error('❌ Error initializing MongoDB:', error.message);
    
  } finally {
    if (client) await client.close();
  }
};
