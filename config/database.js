const mysql = require('mysql2/promise');
const { MongoClient } = require('mongodb');
require('dotenv').config();

// MySQL Connection
let mysqlPool;

const connectMySQL = async () => {
  try {
    mysqlPool = mysql.createPool({
      host: process.env.MYSQL_HOST || 'localhost',
      port: process.env.MYSQL_PORT || 3306,
      user: process.env.MYSQL_USER || 'root',
      password: process.env.MYSQL_PASSWORD || 'root',
      database: process.env.MYSQL_DATABASE || 'research_sql',
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0
    });

    // Test connection
    const connection = await mysqlPool.getConnection();
    console.log('✅ MySQL database connected successfully');
    console.log(`   Database: ${process.env.MYSQL_DATABASE || 'research_sql'}`);
    connection.release();
    return true;
  } catch (error) {
    console.error('❌ Error connecting to MySQL database:', error.message);
    return false;
  }
};

// MongoDB Connection
let mongoClient;
let mongoDB;

const connectMongoDB = async () => {
  try {
    const uri = process.env.MONGODB_URI;

    if (!uri) {
      console.log('   ⚠️  MONGODB_URI not found in .env, skipping MongoDB connection');
      mongoDB = null;
      return false;
    }

    const dbName = process.env.MONGODB_DB || 'research_db';

    console.log('🔗 Connecting to MongoDB...');
    console.log(`   URI: ${uri}`);
    console.log(`   Database: ${dbName}`);

    mongoClient = new MongoClient(uri);
    await mongoClient.connect();
    mongoDB = mongoClient.db(dbName);

    // Test connection
    await mongoDB.command({ ping: 1 });
    console.log('✅ MongoDB database connected successfully');

    // Create indexes for better performance
    try {
      const papersCollection = mongoDB.collection('papers');

      // Check existing indexes
      const existingIndexes = await papersCollection.indexes();
      const hasTextIndex = existingIndexes.some(idx => idx.name === 'text_search' || (idx.key && idx.key._fts === 'text'));

      if (!hasTextIndex) {
        await papersCollection.createIndex(
          { title: 'text', abstract: 'text' },
          { name: 'text_search' }
        );
        console.log('   ✓ Created text search index');
      }

      // Create other indexes if they don't exist
      await papersCollection.createIndex({ paper_id: 1 }, { unique: true, name: 'idx_paper_id' });
      await papersCollection.createIndex({ journal: 1 }, { name: 'idx_journal' });
      await papersCollection.createIndex({ year: 1 }, { name: 'idx_year' });
      await papersCollection.createIndex({ authors: 1 }, { name: 'idx_authors' });
      await papersCollection.createIndex({ is_covid19: 1 }, { name: 'idx_covid19' });

      console.log('   ✓ MongoDB indexes verified/created');
    } catch (indexError) {
      console.log('   ⚠️  Index creation warning:', indexError.message);
    }

    return true;
  } catch (error) {
    console.error('❌ Error connecting to MongoDB:', error.message);
    console.log('   ⚠️  Continuing without MongoDB');
    mongoDB = null;
    return false;
  }
};

// Get database instances
const getMySQL = () => {
  if (!mysqlPool) {
    throw new Error('MySQL pool not initialized. Call connectMySQL() first.');
  }
  return mysqlPool;
};

const getMongoDB = () => {
  if (!mongoDB) {
    throw new Error('MongoDB not initialized. Call connectMongoDB() first.');
  }
  return mongoDB;
};

const isMongoDBConnected = () => {
  return !!mongoDB;
};

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down gracefully...');
  if (mysqlPool) {
    await mysqlPool.end();
    console.log('   ✓ MySQL connections closed');
  }
  if (mongoClient) {
    await mongoClient.close();
    console.log('   ✓ MongoDB connections closed');
  }
  process.exit(0);
});

module.exports = {
  connectMySQL,
  connectMongoDB,
  getMySQL,
  getMongoDB,
  isMongoDBConnected
};