const initMongoDB = async () => {
  let client;
  try {
    console.log('🔧 Initializing MongoDB...');
    
    client = new MongoClient(process.env.MONGODB_URI);
    await client.connect();
    const db = client.db(process.env.MONGODB_DB);

    // Check if collection exists, drop if it does to avoid index conflicts
    const collections = await db.listCollections({ name: 'papers' }).toArray();
    if (collections.length > 0) {
      console.log('⚠️ Dropping existing papers collection to avoid index conflicts...');
      await db.collection('papers').drop();
    }

    // Create collection
    await db.createCollection('papers');
    
    // Get collection reference
    const papersCollection = db.collection('papers');
    
    // Create indexes according to your schema
    console.log('Creating text index for search...');
    try {
      // Drop existing text index if it exists with different name
      const existingIndexes = await papersCollection.indexes();
      for (const index of existingIndexes) {
        if (index.name === 'text_search' || index.key && index.key._fts === 'text') {
          await papersCollection.dropIndex(index.name);
          console.log(`Dropped existing index: ${index.name}`);
        }
      }
      
      // Create text index with specific name
      await papersCollection.createIndex(
        { title: 'text', abstract: 'text' },
        { name: 'text_search' }
      );
      console.log('✅ Created text index');
    } catch (indexError) {
      console.log('⚠️ Could not create text index:', indexError.message);
    }

    // Create other indexes based on your schema
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
    // Don't throw error - continue with MySQL
  } finally {
    if (client) await client.close();
  }
};