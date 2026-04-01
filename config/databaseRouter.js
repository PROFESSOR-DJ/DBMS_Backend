// databaseRouter selects backend database operations based on routing rules.
const DatabaseRouter = {
  
  useSQLFor: [
    'user-authentication',
    'user-management',
    'paper-author-relationships',
    'referential-integrity',
    'transactional-operations',
    'entity-management'
  ],

  
  useMongoDBFor: [
    'full-text-search',
    'paper-metadata-browsing',
    'keyword-search',
    'abstract-search',
    'flexible-queries',
    'large-document-storage',
    'aggregation-analytics'
  ],

  
  usePostgreSQLFor: [
    'complex-analytics',
    'trend-analysis',
    'statistical-queries',
    'reporting-dashboards'
  ],

  
  useNeo4jFor: [
    'collaboration-networks',
    'citation-graphs',
    'author-relationships',
    'research-communities'
  ],

  


  getDatabaseForOperation(operation) {
    if (this.useSQLFor.includes(operation)) return 'mysql';
    if (this.useMongoDBFor.includes(operation)) return 'mongodb';
    if (this.usePostgreSQLFor.includes(operation)) return 'postgresql';
    if (this.useNeo4jFor.includes(operation)) return 'neo4j';
    
    
    return 'mongodb';
  }
};

module.exports = DatabaseRouter;
