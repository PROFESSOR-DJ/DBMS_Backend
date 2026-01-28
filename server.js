const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const dotenv = require('dotenv');

// Load environment variables
dotenv.config();

// Import database connections
const { connectMySQL } = require('./config/database');
const { connectMongoDB } = require('./config/database');

// Import routes
const authRoutes = require('./routes/authRoutes');
const paperRoutes = require('./routes/paperRoutes');
const statsRoutes = require('./routes/statsRoutes');
const hybridRoutes = require('./routes/hybridRoutes');

// Initialize Express app
const app = express();

// Middleware
app.use(helmet());
app.use(cors({
  origin: 'http://localhost:3000',
  credentials: true
}));
app.use(morgan('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Database connections
connectMySQL();
connectMongoDB();

// Routes
app.use('/api/auth', authRoutes);
app.use('/api/papers', paperRoutes);
app.use('/api/stats', statsRoutes);
app.use('/api/hybrid', hybridRoutes);

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.status(200).json({
    status: 'OK',
    message: 'Hybrid DBMS Backend is running',
    timestamp: new Date().toISOString(),
    databases: {
      mysql: 'connected',
      mongodb: 'connected',
      postgresql: 'to_be_implemented',
      neo4j: 'to_be_implemented'
    }
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: 'Route not found' });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: 'Something went wrong!' });
});

const connectMongoDB = async () => {
  try {
    console.log('🔗 Connecting to MongoDB...');
    
    mongoClient = new MongoClient(process.env.MONGODB_URI);
    await mongoClient.connect();
    mongoDB = mongoClient.db(process.env.MONGODB_DB);
    
    // Test connection
    await mongoDB.command({ ping: 1 });
    console.log('✅ MongoDB database connected successfully');
    
  } catch (error) {
    console.error('❌ Error connecting to MongoDB:', error.message);
    console.log('⚠️  MongoDB connection failed, but continuing with MySQL only');
    mongoDB = null; // Set to null so we can check later
  }
};

// Start server
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log('Connected to:');
  console.log('- MySQL for normalized relational data');
  console.log('- MongoDB for document-based data');
  console.log('- PostgreSQL for analytics (to be implemented)');
  console.log('- Neo4j for graph relationships (to be implemented)');
});