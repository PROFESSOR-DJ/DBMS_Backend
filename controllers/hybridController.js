const PaperModel = require('../models/mysql/paperModel');
const AuthorModel = require('../models/mysql/authorModel');
const PaperAuthorModel = require('../models/mysql/paperAuthorModel');
const PaperDocument = require('../models/mongodb/paperModel');

const paperDocument = new PaperDocument();

const getPaperDetailsHybrid = async (req, res) => {
  try {
    const { id } = req.params;

    // Get data from both databases in parallel
    const [mysqlData, mongoData] = await Promise.all([
      (async () => {
        const paper = await PaperModel.findById(id);
        if (paper) {
          const authors = await PaperAuthorModel.getAuthorsByPaper(id);
          paper.authors = authors.map(a => a.name);
        }
        return paper;
      })(),
      paperDocument.findById(id)
    ]);

    if (!mysqlData && !mongoData) {
      return res.status(404).json({ error: 'Paper not found in any database' });
    }

    res.json({
      paper_id: id,
      mysql_data: mysqlData || { note: 'Not found in MySQL' },
      mongodb_data: mongoData || { note: 'Not found in MongoDB' },
      data_consistency: mysqlData && mongoData ? 'Both databases have the paper' : 'Paper found in only one database',
      hybrid_analysis: {
        mysql_advantages: 'Normalized structure, referential integrity',
        mongodb_advantages: 'Rich document structure, full text available'
      }
    });
  } catch (error) {
    console.error('Get paper details hybrid error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const getAuthorNetwork = async (req, res) => {
  try {
    const { name } = req.params;

    // Get author's papers from MySQL (for relationships)
    const author = await AuthorModel.findByName(name);
    
    if (!author) {
      return res.status(404).json({ error: 'Author not found' });
    }

    const papers = await PaperAuthorModel.getPapersByAuthor(author.author_id);
    
    // Get co-authors from MongoDB (simplified approach)
    const authorPapers = await paperDocument.getByAuthor(name);
    const allAuthors = new Set();
    
    authorPapers.forEach(paper => {
      if (paper.authors) {
        paper.authors.forEach(a => allAuthors.add(a));
      }
    });

    // Remove the main author
    allAuthors.delete(name);

    res.json({
      author: {
        id: author.author_id,
        name: author.name,
        paper_count: papers.length
      },
      network: {
        co_authors: Array.from(allAuthors),
        total_co_authors: allAuthors.size,
        papers: papers.map(p => ({
          id: p.paper_id,
          title: p.title,
          year: p.year,
          journal: p.journal
        })),
        database_contributions: {
          mysql: 'Author identification and paper relationships',
          mongodb: 'Co-author discovery through document analysis',
          neo4j_future: 'Will provide graph traversal and centrality analysis'
        }
      }
    });
  } catch (error) {
    console.error('Get author network error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const getJournalAnalysis = async (req, res) => {
  try {
    const { journal } = req.params;

    // Get data from both databases
    const [mysqlPapers, mongoStats, topAuthors] = await Promise.all([
      PaperModel.getByJournal(journal),
      (async () => {
        const pipeline = [
          { $match: { journal } },
          { $group: {
            _id: null,
            total_papers: { $sum: 1 },
            avg_citations: { $avg: "$citation_count" },
            years: { $addToSet: "$year" },
            total_authors: { $addToSet: "$authors" }
          }},
          { $project: {
            total_papers: 1,
            avg_citations: { $round: ["$avg_citations", 2] },
            year_range: { $concat: [
              { $toString: { $min: "$years" } },
              " - ",
              { $toString: { $max: "$years" } }
            ]},
            unique_authors: { $size: { $reduce: {
              input: "$total_authors",
              initialValue: [],
              in: { $setUnion: ["$$value", "$$this"] }
            }}}
          }}
        ];
        const result = await paperDocument.collection.aggregate(pipeline).toArray();
        return result[0] || {};
      })(),
      (async () => {
        const pipeline = [
          { $match: { journal } },
          { $unwind: "$authors" },
          { $group: { _id: "$authors", count: { $sum: 1 } } },
          { $sort: { count: -1 } },
          { $limit: 5 }
        ];
        return paperDocument.collection.aggregate(pipeline).toArray();
      })()
    ]);

    res.json({
      journal,
      analysis: {
        mysql_data: {
          paper_count: mysqlPapers.length,
          sample_papers: mysqlPapers.slice(0, 5).map(p => p.title)
        },
        mongodb_analytics: mongoStats,
        top_contributors: topAuthors,
        hybrid_insights: {
          mysql_role: 'Provides structured paper listing with joins',
          mongodb_role: 'Provides analytical metrics and aggregations',
          combined_value: 'MySQL ensures data integrity, MongoDB enables complex analytics'
        }
      }
    });
  } catch (error) {
    console.error('Get journal analysis error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const searchHybrid = async (req, res) => {
  try {
    const { q } = req.query;

    if (!q) {
      return res.status(400).json({ error: 'Search query required' });
    }

    // Search in both databases in parallel
    const [mysqlResults, mongoResults] = await Promise.all([
      PaperModel.searchByTitle(q, 20),
      paperDocument.searchText(q, 20)
    ]);

    res.json({
      search_query: q,
      results: {
        mysql: {
          count: mysqlResults.length,
          papers: mysqlResults.map(p => p.title),
          search_method: 'LIKE operator on title field',
          performance: 'Good for exact matches, slower for complex text search'
        },
        mongodb: {
          count: mongoResults.length,
          papers: mongoResults.map(p => p.title),
          search_method: 'Text index search on title and abstract',
          performance: 'Excellent for full-text search with ranking'
        }
      },
      recommendation: {
        for_structured_queries: 'Use MySQL with specific filters',
        for_text_search: 'Use MongoDB text search',
        for_analytics: 'Use MongoDB aggregation or future PostgreSQL'
      }
    });
  } catch (error) {
    console.error('Search hybrid error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const syncMySQLToMongo = async (req, res) => {
  try {
    // This would sync data from MySQL to MongoDB
    // For now, return a simulation
    res.json({
      message: 'Data synchronization from MySQL to MongoDB',
      status: 'simulated_for_demo',
      process: {
        step1: 'Extract papers from MySQL',
        step2: 'Transform to document format',
        step3: 'Load into MongoDB',
        step4: 'Create/update indexes'
      },
      note: 'In production, this would be an actual ETL process'
    });
  } catch (error) {
    console.error('Sync MySQL to Mongo error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const syncMongoToMySQL = async (req, res) => {
  try {
    // This would sync data from MongoDB to MySQL
    res.json({
      message: 'Data synchronization from MongoDB to MySQL',
      status: 'simulated_for_demo',
      challenge: 'Denormalized MongoDB documents need to be normalized for MySQL',
      process: {
        step1: 'Extract documents from MongoDB',
        step2: 'Normalize into paper, author, paper_author tables',
        step3: 'Handle duplicates and relationships',
        step4: 'Maintain referential integrity'
      }
    });
  } catch (error) {
    console.error('Sync Mongo to MySQL error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

const getSyncStatus = async (req, res) => {
  try {
    // Get counts from both databases
    const [mysqlCount, mongoCount] = await Promise.all([
      PaperModel.count(),
      (async () => {
        const stats = await paperDocument.getStats();
        return stats.totalPapers || 0;
      })()
    ]);

    res.json({
      synchronization_status: {
        mysql_paper_count: mysqlCount,
        mongodb_paper_count: mongoCount,
        discrepancy: Math.abs(mysqlCount - mongoCount),
        sync_required: Math.abs(mysqlCount - mongoCount) > 0,
        last_sync: 'Never (simulation)',
        next_sync: 'Manual trigger required'
      },
      data_consistency: {
        goal: 'Keep both databases synchronized',
        challenge: 'Different data models require transformation',
        approach: 'ETL process with conflict resolution'
      }
    });
  } catch (error) {
    console.error('Get sync status error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

module.exports = {
  getPaperDetailsHybrid,
  getAuthorNetwork,
  getJournalAnalysis,
  searchHybrid,
  syncMySQLToMongo,
  syncMongoToMySQL,
  getSyncStatus
};