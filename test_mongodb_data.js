require('dotenv').config();
const { MongoClient } = require('mongodb');

(async () => {
  const uri = process.env.MONGODB_URI || 'mongodb://localhost:27017';
  const dbName = process.env.MONGODB_DB || 'research_db';
  
  console.log('🔍 Testing MongoDB Connection and Data...');
  console.log(`📊 URI: ${uri}`);
  console.log(`📁 Database: ${dbName}\n`);

  const client = new MongoClient(uri);
  
  try {
    await client.connect();
    console.log('✅ Connected to MongoDB\n');
    
    const db = client.db(dbName);
    
    // Check collections
    const collections = await db.listCollections().toArray();
    console.log('📋 Collections in database:');
    collections.forEach(col => console.log(`   - ${col.name}`));
    console.log();
    
    // Check papers collection
    const papersCollection = db.collection('papers');
    const paperCount = await papersCollection.countDocuments();
    console.log(`📄 Papers collection: ${paperCount} documents`);
    
    if (paperCount > 0) {
      const sample = await papersCollection.findOne();
      console.log('\n📋 Sample paper document:');
      console.log('   ID:', sample._id);
      console.log('   Paper ID:', sample.paper_id);
      console.log('   Title:', sample.title?.substring(0, 60) + (sample.title?.length > 60 ? '...' : ''));
      console.log('   Year:', sample.year);
      console.log('   Journal:', sample.journal);
      console.log('   Authors:', (sample.authors || []).length, 'authors');
    } else {
      console.log('   ⚠️  No papers found in MongoDB!');
      console.log('   Run: npm run seed:mongo to load sample data');
    }
    
    // Test advanced search
    console.log('\n🔍 Testing advanced search...');
    const searchResult = await papersCollection.find({ 
      title: { $regex: '', $options: 'i' }
    }).limit(1).toArray();
    console.log(`   Search found ${searchResult.length} result(s)`);
    
    console.log('\n✅ MongoDB test complete!');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await client.close();
  }
})();
