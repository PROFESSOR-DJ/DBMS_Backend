// neo4jDatabase creates and manages the backend Neo4j connection.
const neo4j = require('neo4j-driver');

let driver = null;

const connectNeo4j = async () => {
  try {
    const uri      = process.env.NEO4J_URI      || 'bolt://localhost:7687';
    const user     = process.env.NEO4J_USER     || 'neo4j';
    const password = process.env.NEO4J_PASSWORD || 'Gitcode123$';

    driver = neo4j.driver(uri, neo4j.auth.basic(user, password));

    
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





const getNeo4jSession = () => {
  if (!driver) throw new Error('Neo4j driver not initialised. Call connectNeo4j() first.');
  return driver.session({ database: process.env.NEO4J_DATABASE || 'research-graph' });
};

const isNeo4jConnected = () => !!driver;


const runQuery = async (cypher, params = {}) => {
  const session = getNeo4jSession();
  try {
    const result = await session.run(cypher, params);
    return result.records;
  } finally {
    await session.close();
  }
};


process.on('SIGINT', async () => {
  if (driver) {
    await driver.close();
    console.log('   ✓ Neo4j driver closed');
  }
});

module.exports = { connectNeo4j, getNeo4jSession, isNeo4jConnected, runQuery };
