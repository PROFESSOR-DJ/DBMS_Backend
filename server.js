const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const dotenv = require('dotenv');

// Load environment variables
dotenv.config();

// Import database connections
const { connectMySQL, connectMongoDB } = require('./config/database');

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
  origin: process.env.FRONTEND_URL || 'http://localhost:3000',
  credentials: true
}));
app.use(morgan('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Initialize databases
const initDatabases = async () => {
  try {
    await connectMySQL();
    await connectMongoDB();
    console.log('✅ All databases connected successfully');
  } catch (error) {
    console.error('❌ Database connection failed:', error.message);
    // Don't exit - let the app run with whatever connected
  }
};

initDatabases();

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
  res.status(err.status || 500).json({ 
    error: err.message || 'Something went wrong!',
    ...(process.env.NODE_ENV === 'development' && { stack: err.stack })
  });
});

// Start server
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`
╔══════════════════════════════════════════════════════════════╗
║  🚀 Server running on port ${PORT}                              ║
║  📊 Research DBMS Backend API                                ║
╚══════════════════════════════════════════════════════════════╝

Connected to:
  • MySQL    → research_sql (localhost:3306)
  • MongoDB  → research_db (localhost:27017)
  
Ready to accept requests at http://localhost:${PORT}
  `);
});