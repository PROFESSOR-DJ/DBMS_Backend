const PaperModel     = require('../models/mysql/paperModel');
const AuthorModel    = require('../models/mysql/authorModel');
const PaperDocument  = require('../models/mongodb/paperModel');
const { AppError, classifyError, asyncHandler } = require('../utils/errorHandler');

const paperDocument = new PaperDocument();

/**
 * GET /api/stats/overview
 * HYBRID: SQL for entity counts, MongoDB for analytics.
 * All queries run in parallel for performance.
 */
const getOverview = asyncHandler(async (req, res) => {
  const [mysqlStats, mongoStats, papersPerYear, topJournals, topAuthors] = await Promise.all([
    (async () => {
      try {
        const totalPapers  = await PaperModel.count();
        const totalAuthors = await AuthorModel.count();
        const yearStats    = await PaperModel.getYearStats();
        return { totalPapers, totalAuthors, yearStats };
      } catch (err) {
        const appErr = classifyError(err);
        console.error('MySQL stats error:', appErr.message);
        return { totalPapers: 0, totalAuthors: 0, yearStats: [], error: appErr.message };
      }
    })(),

    (async () => {
      try {
        return await paperDocument.getStats();
      } catch (err) {
        const appErr = classifyError(err);
        console.error('MongoDB stats error:', appErr.message);
        return {
          totalPapers: 0, uniqueJournalCount: 0,
          uniqueAuthorCount: 0, avgCitations: 0,
          error: appErr.message,
        };
      }
    })(),

    (async () => {
      try {
        return await paperDocument.getPapersPerYear();
      } catch (err) {
        console.error('MongoDB papersPerYear error:', classifyError(err).message);
        return [];
      }
    })(),

    (async () => {
      try {
        return await paperDocument.getTopJournals(10);
      } catch (err) {
        console.error('MongoDB topJournals error:', classifyError(err).message);
        return [];
      }
    })(),

    (async () => {
      try {
        return await paperDocument.getTopAuthors(10);
      } catch (err) {
        console.error('MongoDB topAuthors error:', classifyError(err).message);
        return [];
      }
    })(),
  ]);

  res.json({
    database_overview: {
      mysql: {
        total_papers:   mysqlStats.totalPapers,
        total_authors:  mysqlStats.totalAuthors,
        years_covered:  mysqlStats.yearStats?.length || 0,
        role:           'Normalised relational data with referential integrity',
        ...(mysqlStats.error && { warning: mysqlStats.error }),
      },
      mongodb: {
        total_papers:    mongoStats.totalPapers,
        unique_journals: mongoStats.uniqueJournalCount,
        unique_authors:  mongoStats.uniqueAuthorCount,
        avg_citations:   mongoStats.avgCitations,
        role:            'Document storage, full-text search, aggregation analytics',
        ...(mongoStats.error && { warning: mongoStats.error }),
      },
    },
    analytics: {
      papers_per_year: papersPerYear,
      top_journals:    topJournals,
      top_authors:     topAuthors,
    },
    optimisation: {
      parallel_execution:    'All queries run concurrently via Promise.all',
      sql_optimisation:      'COUNT on indexed primary keys',
      mongodb_optimisation:  'Aggregation pipelines with compound indexes',
    },
  });
});

/**
 * GET /api/stats/authors
 * SQL — normalised many-to-many relationship counts.
 */
const getAuthorStats = asyncHandler(async (req, res) => {
  const limit = Number(req.query.limit) || 50;

  if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
    throw new AppError('Limit must be an integer between 1 and 1000.', 400, 'INVALID_PARAM');
  }

  let authors;
  try {
    authors = await AuthorModel.getTopAuthors(limit);
  } catch (err) {
    throw classifyError(err);
  }

  res.json({
    authors,
    count:       authors.length,
    source:      'mysql',
    reason:      'SQL provides accurate many-to-many relationship counts.',
    optimisation: 'Heuristic: GROUP BY paper_author then JOIN to author table.',
    query_pattern: 'SELECT a.name, COUNT(pa.paper_id) FROM authors a LEFT JOIN paper_authors pa GROUP BY a.author_id',
  });
});

/**
 * GET /api/stats/journals
 * MongoDB — aggregation pipeline.
 */
const getJournalStats = asyncHandler(async (req, res) => {
  const limit = Number(req.query.limit) || 50;

  if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
    throw new AppError('Limit must be an integer between 1 and 1000.', 400, 'INVALID_PARAM');
  }

  let journals;
  try {
    journals = await paperDocument.getTopJournals(limit);
  } catch (err) {
    throw classifyError(err);
  }

  res.json({
    journals,
    count:        journals.length,
    source:       'mongodb',
    reason:       'MongoDB aggregation pipeline optimised for grouping.',
    optimisation: 'Single-pass: $group by journal → $sort → $limit',
    pipeline:     '[{$group:{_id:"$journal",count:{$sum:1}}},{$sort:{count:-1}},{$limit:N}]',
  });
});

/**
 * GET /api/stats/papers-per-year
 * MongoDB — indexed year aggregation.
 */
const getPapersPerYear = asyncHandler(async (req, res) => {
  let data;
  try {
    data = await paperDocument.getPapersPerYear();
  } catch (err) {
    throw classifyError(err);
  }

  res.json({
    papers_per_year: data,
    source:       'mongodb',
    reason:       'MongoDB year index enables fast grouping.',
    optimisation: 'Index-covered query on year field.',
    analysis:     'Shows publication trends over time.',
  });
});

/**
 * GET /api/stats/database-info
 */
const getDatabaseInfo = asyncHandler(async (req, res) => {
  res.json({
    hybrid_database_architecture: {
      mysql: {
        role:      'Core transactional data',
        schema:    'Normalised tables (papers, authors, paper_authors, journals, sources, users)',
        strengths: 'ACID compliance, complex joins, referential integrity',
        use_cases: [
          'User authentication and management',
          'Author-paper relationships (many-to-many)',
          'Entity integrity and constraints',
          'Transactional operations',
        ],
        stored_procedures: [
          'CreatePaperWithAuthors — atomic paper + authors insert with transaction',
          'GetTopAuthors — heuristic GROUP BY then JOIN optimisation',
          'SearchPapersByFilters — multi-filter paginated search',
          'GetJournalStats — journal-level aggregation',
          'DeleteAuthorSafe — guarded delete with feedback',
        ],
        triggers: [
          'trg_after_paper_insert — auto-insert paper_metrics row',
          'trg_after_paper_authors_insert — update author_count in paper_metrics',
          'trg_after_paper_authors_delete — update author_count in paper_metrics',
          'trg_before_author_delete — guard: block delete if author has linked papers',
          'trg_after_paper_delete — post-delete hook (audit log extension point)',
        ],
        optimisation: 'Heuristic query optimisation: GROUP BY before JOIN',
      },
      mongodb: {
        role:      'Research paper metadata',
        schema:    'Flexible documents with embedded arrays',
        strengths: 'Horizontal scaling, full-text search, flexible schema',
        use_cases: [
          'Full-text search on titles and abstracts',
          'Paper metadata browsing',
          'Keyword and abstract search',
          'Aggregation analytics',
        ],
        pipeline_stages_used: ['$match', '$group', '$project', '$sort', '$limit', '$unwind', '$facet'],
        optimisation:         'Compound indexes, aggregation pipelines, text indexes',
      },
      postgresql: {
        role:      'Analytics & reporting (future)',
        strengths: 'Complex analytics, window functions',
        use_cases: ['Trend analysis', 'Reporting dashboards'],
      },
      neo4j: {
        role:      'Collaboration & citation graph (future)',
        strengths: 'Relationship traversal, graph algorithms',
        use_cases: ['Collaboration networks', 'Citation analysis'],
      },
    },
    polyglot_persistence: {
      principle:   'Use the right database for the right job',
      data_flow:   'Single dataset → multiple optimised representations',
      consistency: 'Eventual consistency model with primary source (MongoDB)',
    },
  });
});

/**
 * GET /api/stats/query-performance
 */
const getQueryPerformance = asyncHandler(async (req, res) => {
  res.json({
    query_performance_comparison: {
      mysql: {
        simple_select:  '2–5ms (B-tree index lookup)',
        join_queries:   '5–15ms (optimised nested loop join)',
        aggregation:    '10–30ms (GROUP BY with index)',
        many_to_many:   '15–40ms (double JOIN with indexes)',
        optimisation:   'Heuristic: GROUP BY first, then JOIN to reduce intermediate results',
      },
      mongodb: {
        document_read:        '1–3ms (index lookup)',
        text_search:          '5–15ms (text index with scoring)',
        aggregation_pipeline: '10–25ms (indexed aggregation)',
        array_operations:     '3–8ms (embedded document access)',
        optimisation:         'Compound indexes, covered queries, aggregation pipeline stages',
      },
    },
    real_world_scenarios: {
      'Search papers by keyword': {
        winner:        'MongoDB',
        reason:        'Text indexes provide 3–5× faster full-text search',
        mongodb_time:  '8–15ms',
        mysql_time:    '30–50ms (LIKE operator)',
      },
      'Get author paper count': {
        winner:        'MySQL',
        reason:        'Normalised schema with JOIN optimisation',
        mysql_time:    '10–20ms (GROUP BY + JOIN)',
        mongodb_time:  '20–40ms (unwind + group)',
      },
      'Browse papers with pagination': {
        winner:        'MongoDB',
        reason:        'Document model with efficient skip/limit',
        mongodb_time:  '5–10ms',
        mysql_time:    '10–20ms',
      },
      'Multi-field aggregation': {
        winner:        'MongoDB',
        reason:        'Aggregation pipeline optimised for analytics',
        mongodb_time:  '15–30ms',
        mysql_time:    '25–50ms',
      },
    },
  });
});

module.exports = {
  getOverview,
  getAuthorStats,
  getJournalStats,
  getPapersPerYear,
  getDatabaseInfo,
  getQueryPerformance,
};