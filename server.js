const express  = require('express');
const cors     = require('cors');
const helmet   = require('helmet');
const morgan   = require('morgan');
const dotenv   = require('dotenv');

dotenv.config();

const { connectMySQL, connectMongoDB }           = require('./config/database');
const { errorMiddleware, notFoundMiddleware }     = require('./middleware/errorMiddleware');
const { connectNeo4j }   = require('./config/neo4jDatabase');
const graphRoutes        = require('./routes/graphRoutes');
const authRoutes   = require('./routes/authRoutes');
const paperRoutes  = require('./routes/paperRoutes');
const statsRoutes  = require('./routes/statsRoutes');
const hybridRoutes = require('./routes/hybridRoutes');
const authorRoutes = require('./routes/authorRoutes');

const app = express();

// ── Security & logging middleware ──
app.use(helmet());
app.use(cors({
  origin:      process.env.FRONTEND_URL || 'http://localhost:3000',
  credentials: true,
}));
app.use(morgan('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ── Database initialisation ──
const initDatabases = async () => {
  try {
    await connectMySQL();
    await connectMongoDB();
    await connectNeo4j();           // ← ADD THIS LINE
    console.log('✅ All databases connected successfully');
  } catch (error) {
    console.error('❌ Database connection failed:', error.message);
  }
};

initDatabases();

// ── API routes ──
app.use('/api/auth',    authRoutes);
app.use('/api/papers',  paperRoutes);
app.use('/api/stats',   statsRoutes);
app.use('/api/hybrid',  hybridRoutes);
app.use('/api/authors', authorRoutes);
app.use('/api/graph',   graphRoutes);
// ── Health check ──
app.get('/api/health', (req, res) => {
  res.status(200).json({
    status:    'OK',
    message:   'Hybrid DBMS Backend is running',
    timestamp: new Date().toISOString(),
    databases: {
      mysql:      'connected',
      mongodb:    'connected',
      postgresql: 'to_be_implemented',
      neo4j:      'connected',
    },
  });
});

// ── 404 handler (must come after all routes) ──
app.use(notFoundMiddleware);

// ── Global error handler (must be last) ──
app.use(errorMiddleware);

// ── Start server ──
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`
╔══════════════════════════════════════════════════════════════╗
║  🚀 Server running on port ${PORT}                              ║
║  📊 Research DBMS Backend API                                ║
╚══════════════════════════════════════════════════════════════╝

Connected to:
  • MySQL    → research_mysql2 (localhost:3306)
  • MongoDB  → research_db     (localhost:27017)

Ready at http://localhost:${PORT}
  `);
});