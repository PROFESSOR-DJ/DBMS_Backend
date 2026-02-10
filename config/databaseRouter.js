/**
 * Database Router - Determines optimal database for each operation
 * Based on polyglot persistence principles
 */

const DatabaseRouter = {
  // Use SQL for normalized, relational queries with integrity constraints
  useSQLFor: [
    'user-authentication',
    'user-management',
    'paper-author-relationships',
    'referential-integrity',
    'transactional-operations',
    'entity-management'
  ],

  // Use MongoDB for document-based, text search, and analytics
  useMongoDBFor: [
    'full-text-search',
    'paper-metadata-browsing',
    'keyword-search',
    'abstract-search',
    'flexible-queries',
    'large-document-storage',
    'aggregation-analytics'
  ],

  // Use PostgreSQL for analytics and reporting (future)
  usePostgreSQLFor: [
    'complex-analytics',
    'trend-analysis',
    'statistical-queries',
    'reporting-dashboards'
  ],

  // Use Neo4j for graph relationships (future)
  useNeo4jFor: [
    'collaboration-networks',
    'citation-graphs',
    'author-relationships',
    'research-communities'
  ],

  /**
   * Determine which database to use for a given operation
   */
  getDatabaseForOperation(operation) {
    if (this.useSQLFor.includes(operation)) return 'mysql';
    if (this.useMongoDBFor.includes(operation)) return 'mongodb';
    if (this.usePostgreSQLFor.includes(operation)) return 'postgresql';
    if (this.useNeo4jFor.includes(operation)) return 'neo4j';
    
    // Default to MongoDB for flexibility
    return 'mongodb';
  }
};

module.exports = DatabaseRouter;