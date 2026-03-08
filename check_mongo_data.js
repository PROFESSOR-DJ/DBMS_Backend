require('dotenv').config();
const { MongoClient } = require('mongodb');

(async () => {
  const uri = process.env.MONGODB_URI || 'mongodb://localhost:27017';
  const dbName = process.env.MONGODB_DB || 'research_db';
  
  const client = new MongoClient(uri);
  
  try {
    await client.connect();
    
    const db = client.db(dbName);
    const papersCollection = db.collection('papers');
    const paperCount = await papersCollection.countDocuments();
    
    console.log('✅ MongoDB Connected');
    console.log('📄 Papers count:', paperCount);
    
    if (paperCount > 0) {
      console.log('✅ Data exists in MongoDB!');
      const sample = await papersCollection.findOne();
      console.log('Sample:', JSON.stringify(sample, null, 2).substring(0, 300));
    } else {
      console.log('❌ No papers in MongoDB - You need to load sample data first');
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await client.close();
  }
})();
