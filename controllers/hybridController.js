// hybridController handles backend requests that combine multiple databases.
const PaperModel       = require('../models/mysql/paperModel');
const AuthorModel      = require('../models/mysql/authorModel');
const PaperAuthorModel = require('../models/mysql/paperAuthorModel');
const PaperDocument    = require('../models/mongodb/paperModel');
const { AppError, classifyError, asyncHandler } = require('../utils/errorHandler');

const paperDocument = new PaperDocument();






const getPaperDetailsHybrid = asyncHandler(async (req, res) => {
  const { id } = req.params;

  const [mysqlData, mongoData] = await Promise.allSettled([
    (async () => {
      const paper = await PaperModel.findById(id);
      if (paper) {
        const authors  = await PaperAuthorModel.getAuthorsByPaper(id);
        paper.authors  = authors.map(a => a.name);
      }
      return paper;
    })(),
    paperDocument.findById(id),
  ]);

  const mysql = mysqlData.status === 'fulfilled' ? mysqlData.value : null;
  const mongo = mongoData.status === 'fulfilled' ? mongoData.value : null;

  const mysqlError = mysqlData.status === 'rejected'
    ? classifyError(mysqlData.reason).message : null;
  const mongoError = mongoData.status === 'rejected'
    ? classifyError(mongoData.reason).message : null;

  if (!mysql && !mongo) {
    throw new AppError(`Paper '${id}' not found in any database.`, 404, 'NOT_FOUND');
  }

  res.json({
    paper_id:      id,
    mysql_data:    mysql  || { note: 'Not found in MySQL',  error: mysqlError },
    mongodb_data:  mongo  || { note: 'Not found in MongoDB', error: mongoError },
    data_consistency: mysql && mongo
      ? 'Both databases have the paper.'
      : 'Paper found in only one database — sync may be required.',
    hybrid_analysis: {
      mysql_advantages:   'Normalised structure, referential integrity',
      mongodb_advantages: 'Rich document structure, full text available',
    },
  });
});






const getAuthorNetwork = asyncHandler(async (req, res) => {
  const { name } = req.params;

  let author;
  try {
    author = await AuthorModel.findByName(name);
  } catch (err) {
    throw classifyError(err);
  }

  if (!author) {
    throw new AppError(`Author '${name}' not found.`, 404, 'NOT_FOUND');
  }

  let papers = [];
  try {
    papers = await PaperAuthorModel.getPapersByAuthor(author.author_id);
  } catch (err) {
    console.warn('MySQL getPapersByAuthor warning:', classifyError(err).message);
  }

  let authorPapers = [];
  try {
    authorPapers = await paperDocument.getByAuthor(name);
  } catch (err) {
    console.warn('MongoDB getByAuthor warning:', classifyError(err).message);
  }

  const allAuthors = new Set();
  authorPapers.forEach(paper => {
    if (paper.authors) paper.authors.forEach(a => allAuthors.add(a));
  });
  allAuthors.delete(name);

  res.json({
    author: {
      id:          author.author_id,
      name:        author.author_name || author.name,
      paper_count: papers.length,
    },
    network: {
      co_authors:       Array.from(allAuthors),
      total_co_authors: allAuthors.size,
      papers:           papers.map(p => ({
        id:      p.paper_id,
        title:   p.title,
        year:    p.year || p.publish_year,
        journal: p.journal,
      })),
      database_contributions: {
        mysql:       'Author identification and paper-author relationships',
        mongodb:     'Co-author discovery through embedded document analysis',
        neo4j_future: 'Graph traversal and centrality analysis (planned)',
      },
    },
  });
});





const getJournalAnalysis = asyncHandler(async (req, res) => {
  const { journal } = req.params;

  const [mysqlPapersResult, mongoStatsResult, topAuthorsResult] = await Promise.allSettled([
    PaperModel.getByJournal(journal),

    paperDocument.collection.aggregate([
      { $match: { journal } },
      {
        $group: {
          _id:           null,
          total_papers:  { $sum: 1 },
          avg_citations: { $avg: '$citation_count' },
          years:         { $addToSet: '$year' },
          total_authors: { $addToSet: '$authors' },
        },
      },
      {
        $project: {
          total_papers:  1,
          avg_citations: { $round: ['$avg_citations', 2] },
          year_range: {
            $concat: [
              { $toString: { $min: '$years' } },
              ' - ',
              { $toString: { $max: '$years' } },
            ],
          },
          unique_authors: {
            $size: {
              $reduce: {
                input:        '$total_authors',
                initialValue: [],
                in:           { $setUnion: ['$$value', '$$this'] },
              },
            },
          },
        },
      },
    ]).toArray().then(r => r[0] || {}),

    paperDocument.collection.aggregate([
      { $match: { journal } },
      { $unwind: '$authors' },
      { $group: { _id: '$authors', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 5 },
    ]).toArray(),
  ]);

  const mysqlPapers  = mysqlPapersResult.status  === 'fulfilled' ? mysqlPapersResult.value  : [];
  const mongoStats   = mongoStatsResult.status   === 'fulfilled' ? mongoStatsResult.value   : {};
  const topAuthors   = topAuthorsResult.status   === 'fulfilled' ? topAuthorsResult.value   : [];

  const mysqlError  = mysqlPapersResult.status  === 'rejected'
    ? classifyError(mysqlPapersResult.reason).message  : null;
  const mongoError  = mongoStatsResult.status   === 'rejected'
    ? classifyError(mongoStatsResult.reason).message   : null;

  res.json({
    journal,
    analysis: {
      mysql_data: {
        paper_count:   mysqlPapers.length,
        sample_papers: mysqlPapers.slice(0, 5).map(p => p.title),
        ...(mysqlError && { warning: mysqlError }),
      },
      mongodb_analytics: {
        ...mongoStats,
        ...(mongoError && { warning: mongoError }),
      },
      top_contributors: topAuthors,
      hybrid_insights: {
        mysql_role:    'Provides structured paper listing with joins',
        mongodb_role:  'Provides analytical metrics and aggregations',
        combined_value: 'MySQL ensures data integrity, MongoDB enables complex analytics',
      },
    },
  });
});





const searchHybrid = asyncHandler(async (req, res) => {
  const { q } = req.query;

  if (!q || q.trim().length === 0) {
    throw new AppError('Search query parameter "q" is required.', 400, 'MISSING_PARAM');
  }

  const [mysqlResult, mongoResult] = await Promise.allSettled([
    PaperModel.searchByTitle(q, 20),
    paperDocument.searchText(q, 20),
  ]);

  const mysqlPapers = mysqlResult.status === 'fulfilled' ? mysqlResult.value : [];
  const mongoPapers = mongoResult.status === 'fulfilled' ? mongoResult.value : [];

  const mysqlError = mysqlResult.status === 'rejected'
    ? classifyError(mysqlResult.reason).message : null;
  const mongoError = mongoResult.status === 'rejected'
    ? classifyError(mongoResult.reason).message : null;

  res.json({
    search_query: q,
    results: {
      mysql: {
        count:          mysqlPapers.length,
        papers:         mysqlPapers.map(p => p.title),
        search_method:  'LIKE operator on title field',
        performance:    'Good for exact matches, slower for complex text search',
        ...(mysqlError && { warning: mysqlError }),
      },
      mongodb: {
        count:          mongoPapers.length,
        papers:         mongoPapers.map(p => p.title),
        search_method:  'Text index search on title and abstract',
        performance:    'Excellent for full-text search with ranking',
        ...(mongoError && { warning: mongoError }),
      },
    },
    recommendation: {
      for_structured_queries: 'Use MySQL with specific filters',
      for_text_search:        'Use MongoDB text search',
      for_analytics:          'Use MongoDB aggregation',
    },
  });
});




const syncMySQLToMongo = asyncHandler(async (req, res) => {
  res.json({
    message: 'Data synchronisation from MySQL to MongoDB',
    status:  'simulated_for_demo',
    process: {
      step1: 'Extract papers from MySQL',
      step2: 'Transform to document format',
      step3: 'Load into MongoDB',
      step4: 'Create/update indexes',
    },
    note: 'In production, this would be an ETL process.',
  });
});




const syncMongoToMySQL = asyncHandler(async (req, res) => {
  res.json({
    message:   'Data synchronisation from MongoDB to MySQL',
    status:    'simulated_for_demo',
    challenge: 'Denormalised MongoDB documents must be normalised for MySQL',
    process: {
      step1: 'Extract documents from MongoDB',
      step2: 'Normalise into paper, author, paper_authors tables',
      step3: 'Handle duplicates and relationships',
      step4: 'Maintain referential integrity',
    },
  });
});




const getSyncStatus = asyncHandler(async (req, res) => {
  const [mysqlCountResult, mongoCountResult] = await Promise.allSettled([
    PaperModel.count(),
    paperDocument.getStats().then(s => s.totalPapers || 0),
  ]);

  const mysqlCount = mysqlCountResult.status === 'fulfilled' ? mysqlCountResult.value : null;
  const mongoCount = mongoCountResult.status === 'fulfilled' ? mongoCountResult.value : null;

  const mysqlError = mysqlCountResult.status === 'rejected'
    ? classifyError(mysqlCountResult.reason).message : null;
  const mongoError = mongoCountResult.status === 'rejected'
    ? classifyError(mongoCountResult.reason).message : null;

  const discrepancy = (mysqlCount !== null && mongoCount !== null)
    ? Math.abs(mysqlCount - mongoCount) : null;

  res.json({
    synchronisation_status: {
      mysql_paper_count:  mysqlCount  ?? 'unavailable',
      mongodb_paper_count: mongoCount ?? 'unavailable',
      discrepancy:        discrepancy ?? 'cannot compare',
      sync_required:      discrepancy !== null ? discrepancy > 0 : 'unknown',
      last_sync:          'Never (simulation)',
      next_sync:          'Manual trigger required',
      ...(mysqlError && { mysql_warning: mysqlError }),
      ...(mongoError && { mongo_warning: mongoError }),
    },
    data_consistency: {
      goal:      'Keep both databases synchronised',
      challenge: 'Different data models require transformation',
      approach:  'ETL process with conflict resolution',
    },
  });
});

module.exports = {
  getPaperDetailsHybrid,
  getAuthorNetwork,
  getJournalAnalysis,
  searchHybrid,
  syncMySQLToMongo,
  syncMongoToMySQL,
  getSyncStatus,
};
