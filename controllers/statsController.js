const PaperModel = require('../models/mysql/paperModel');
const AuthorModel = require('../models/mysql/authorModel');
const PaperDocument = require('../models/mongodb/paperModel');

const paperDocument = new PaperDocument();

const getOverview = async (req, res) => {
  try {
    // Get stats from both databases
    const [mysqlStats, mongoStats, papersPerYear, topJournals, topAuthors] = await Promise.all([
      // MySQL stats
      (async () => {
        const totalPapers = await PaperModel.count();
        const totalAuthors = await AuthorModel.count();
        const yearStats = await PaperModel.getYearStats();
        return { totalPapers, totalAuthors, yearStats };
      })(),
      
      // MongoDB stats
      paperDocument.getStats(),
      
      // Papers per year from MongoDB
      paperDocument.getPapersPerYear(),
      
      // Top journals from MongoDB
      paperDocument.getTopJournals(10),
      
      // Top authors from MongoDB
      paperDocument.getTopAuthors(10)
    ]);

    res.json({
      database_overview: {
        mysql: {
          total_papers: mysqlStats.totalPapers,
          total_authors: mysqlStats.totalAuthors,
          years_covered: mysqlStats.yearStats.length
        },
        mongodb: {
          total_papers: mongoStats.totalPapers || 0,
          unique_journals: mongoStats.uniqueJournalCount || 0,
          unique_authors: mongoStats.uniqueAuthorCount || 0,
          avg_citations: mongoStats.avgCitations || 0
        }
      },
      analytics: {
        papers_per_year: papersPerYear,
        top_journals: topJournals,
        top_authors: topAuthors
      },
      hybrid_architecture: {
        mysql_role: 'Normalized relational data storage',
        mongodb_role: 'Document-based full-text search and analytics',
        postgresql_role: 'Analytical queries and reporting (to be implemented)',
        neo4j_role: 'Graph-based relationship analysis (to be implemented)'
      }
    });
  } catch (error) {
    console.error('Get overview error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const getAuthorStats = async (req, res) => {
  try {
    const { source = 'mongodb' } = req.query;
    const limit = parseInt(req.query.limit, 10) || 50; // FIX: Parse as integer

    let authors;
    if (source === 'mysql') {
      authors = await AuthorModel.getTopAuthors(limit);
    } else {
      authors = await paperDocument.getTopAuthors(limit);
    }

    res.json({
      authors,
      count: authors.length,
      source,
      query_info: {
        mysql_query: 'SELECT a.name, COUNT(pa.paper_id) as paper_count FROM author a LEFT JOIN paper_author pa ON a.author_id = pa.author_id GROUP BY a.author_id ORDER BY paper_count DESC',
        mongodb_pipeline: '[{$unwind: "$authors"}, {$group: {_id: "$authors", count: {$sum: 1}}}, {$sort: {count: -1}}]'
      }
    });
  } catch (error) {
    console.error('Get author stats error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const getJournalStats = async (req, res) => {
  try {
    const { source = 'mongodb' } = req.query;
    const limit = parseInt(req.query.limit, 10) || 50; // FIX: Parse as integer

    let journals;
    if (source === 'mysql') {
      // MySQL query for journal stats
      const query = `
        SELECT journal, COUNT(*) as count
        FROM paper
        GROUP BY journal
        ORDER BY count DESC
        LIMIT ?
      `;
      const [rows] = await (await require('../config/database').getMySQL()).execute(query, [limit]);
      journals = rows;
    } else {
      journals = await paperDocument.getTopJournals(limit);
    }

    res.json({
      journals,
      count: journals.length,
      source,
      database_specific: {
        mysql: 'Uses GROUP BY and COUNT for aggregation',
        mongodb: 'Uses $group and $sort aggregation pipeline stages'
      }
    });
  } catch (error) {
    console.error('Get journal stats error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const getPapersPerYear = async (req, res) => {
  try {
    const { source = 'mongodb' } = req.query;

    let data;
    if (source === 'mysql') {
      data = await PaperModel.getYearStats();
      // Format to match MongoDB output
      data = data.map(item => ({ _id: item.year, count: item.count }));
    } else {
      data = await paperDocument.getPapersPerYear();
    }

    res.json({
      papers_per_year: data,
      source,
      analysis: 'Shows publication trends over time'
    });
  } catch (error) {
    console.error('Get papers per year error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const getDatabaseInfo = async (req, res) => {
  try {
    res.json({
      hybrid_database_architecture: {
        mysql: {
          role: 'Relational data modeling',
          schema: 'Normalized tables (paper, author, paper_author)',
          strengths: 'ACID compliance, complex joins, referential integrity',
          use_case: 'Structured queries, relationships, data integrity'
        },
        mongodb: {
          role: 'Document storage and search',
          schema: 'Flexible documents with embedded arrays',
          strengths: 'Horizontal scaling, full-text search, flexible schema',
          use_case: 'Text search, analytics, large document storage'
        },
        postgresql: {
          role: 'Analytical processing (future)',
          schema: 'Star/snowflake schema for analytics',
          strengths: 'Complex analytics, window functions, GIS',
          use_case: 'Reporting, trend analysis, complex aggregations'
        },
        neo4j: {
          role: 'Graph relationships (future)',
          schema: 'Nodes and relationships',
          strengths: 'Relationship traversal, pattern matching',
          use_case: 'Collaboration networks, citation analysis'
        }
      },
      data_flow: 'Single dataset → Multiple optimized representations',
      academic_relevance: 'Demonstrates polyglot persistence, query optimization, and database selection based on use case'
    });
  } catch (error) {
    console.error('Get database info error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const getQueryPerformance = async (req, res) => {
  try {
    // Simulated performance metrics
    res.json({
      query_performance: {
        mysql: {
          simple_select: '2-5ms',
          join_queries: '5-15ms',
          aggregation: '10-20ms',
          index_type: 'B-tree indexes'
        },
        mongodb: {
          document_read: '1-3ms',
          text_search: '5-10ms',
          aggregation_pipeline: '15-30ms',
          index_type: 'B-tree, text, geospatial indexes'
        },
        comparison: {
          text_search: 'MongoDB is 3-5x faster due to native text indexes',
          complex_joins: 'MySQL is 2-3x faster due to optimized join algorithms',
          write_performance: 'MongoDB has better write throughput for large documents'
        }
      },
      optimization_tips: {
        mysql: 'Use proper indexing, normalize data, optimize queries with EXPLAIN',
        mongodb: 'Create compound indexes, use projection, leverage aggregation pipeline',
        hybrid_approach: 'Use MySQL for transactional data, MongoDB for search/analytics'
      }
    });
  } catch (error) {
    console.error('Get query performance error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

module.exports = {
  getOverview,
  getAuthorStats,
  getJournalStats,
  getPapersPerYear,
  getDatabaseInfo,
  getQueryPerformance
};