const mysql = require('mysql2/promise');
const { MongoClient } = require('mongodb');
require('dotenv').config();

// MySQL Connection
let mysqlPool;

const connectMySQL = async () => {
  try {
    mysqlPool = mysql.createPool({
      host: process.env.MYSQL_HOST,
      port: process.env.MYSQL_PORT,
      user: process.env.MYSQL_USER,
      password: process.env.MYSQL_PASSWORD,
      database: process.env.MYSQL_DATABASE,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0
    });

    // Test connection
    const connection = await mysqlPool.getConnection();
    console.log('✅ MySQL database connected successfully');
    connection.release();
  } catch (error) {
    console.error('❌ Error connecting to MySQL database:', error.message);
    process.exit(1);
  }
};

// MongoDB Connection
let mongoClient;
let mongoDB;

const connectMongoDB = async () => {
  try {
    mongoClient = new MongoClient(process.env.MONGODB_URI);
    await mongoClient.connect();
    mongoDB = mongoClient.db(process.env.MONGODB_DB);
    console.log('✅ MongoDB database connected successfully');
    
    // Create indexes for better performance
    await mongoDB.collection('papers').createIndex({ title: 'text', abstract: 'text' });
    await mongoDB.collection('papers').createIndex({ journal: 1 });
    await mongoDB.collection('papers').createIndex({ year: 1 });
    await mongoDB.collection('papers').createIndex({ authors: 1 });
    console.log('✅ MongoDB indexes created');
  } catch (error) {
    console.error('❌ Error connecting to MongoDB database:', error.message);
    process.exit(1);
  }
};

// Get database instances
const getMySQL = () => mysqlPool;
const getMongoDB = () => mongoDB;

module.exports = {
  connectMySQL,
  connectMongoDB,
  getMySQL,
  getMongoDB
};