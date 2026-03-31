/**
 * config/neo4jDatabase.js
 * Neo4j driver — mirrors the style of config/database.js
 */
const neo4j = require('neo4j-driver');

let driver = null;

const connectNeo4j = async () => {
  try {
    const uri      = process.env.NEO4J_URI      || 'bolt://localhost:7687';
    const user     = process.env.NEO4J_USER     || 'neo4j';
    const password = process.env.NEO4J_PASSWORD || 'your_password';

    driver = neo4j.driver(uri, neo4j.auth.basic(user, password));

    // Verify connectivity
    await driver.verifyConnectivity();
    console.log('✅ Neo4j database connected successfully');
    console.log(`   URI: ${uri}`);
    return true;
  } catch (error) {
    console.error('❌ Neo4j connection failed:', error.message);
    console.log('   ⚠️  Continuing without Neo4j');
    driver = null;
    return false;
  }
};

/**
 * Returns a new session.
 * Always close sessions after use: session.close()
 */
const getNeo4jSession = () => {
  if (!driver) throw new Error('Neo4j driver not initialised. Call connectNeo4j() first.');
  return driver.session({ database: process.env.NEO4J_DATABASE || 'neo4j' });
};

const isNeo4jConnected = () => !!driver;

// Helper — run a query and auto-close the session
const runQuery = async (cypher, params = {}) => {
  const session = getNeo4jSession();
  try {
    const result = await session.run(cypher, params);
    return result.records;
  } finally {
    await session.close();
  }
};

// Graceful shutdown
process.on('SIGINT', async () => {
  if (driver) {
    await driver.close();
    console.log('   ✓ Neo4j driver closed');
  }
});

module.exports = { connectNeo4j, getNeo4jSession, isNeo4jConnected, runQuery };