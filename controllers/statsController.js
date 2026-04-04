// statsController handles backend statistics and dashboard requests.
// Procedures used: GetTrendingPapers, GetAuthorImpact, GetIncompletePapers, GetActiveUsers
// Triggers active: trg_mark_important_paper, trg_update_last_login,
//                  trg_validate_paper, trg_update_journal_count

const PaperModel     = require('../models/mysql/paperModel');
const AuthorModel    = require('../models/mysql/authorModel');
const PaperDocument  = require('../models/mongodb/paperModel');
const { getMySQL }   = require('../config/database');
const { AppError, classifyError, asyncHandler } = require('../utils/errorHandler');

const paperDocument = new PaperDocument();

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/overview
// Combines MySQL + MongoDB stats in parallel.
// ─────────────────────────────────────────────────────────────────────────────
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

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/authors
// Calls stored procedure GetAuthorImpact (replaces old GetTopAuthors logic).
// GetAuthorImpact: returns author_id, author_name, total_papers for all authors
//                  ordered by total_papers DESC.
// ─────────────────────────────────────────────────────────────────────────────
const getAuthorStats = asyncHandler(async (req, res) => {
  const limit = Number(req.query.limit) || 50;

  if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
    throw new AppError('Limit must be an integer between 1 and 1000.', 400, 'INVALID_PARAM');
  }

  let authors;
  try {
    const pool = getMySQL();
    // GetAuthorImpact returns all authors ordered by total_papers DESC.
    // We slice to the requested limit in JS to avoid altering the procedure signature.
    const [rows] = await pool.execute('CALL GetAuthorImpact()');
    // MySQL returns the result set as rows[0] for a CALL with a SELECT.
    authors = (rows[0] || []).slice(0, limit);
  } catch (err) {
    throw classifyError(err);
  }

  res.json({
    authors,
    count:        authors.length,
    source:       'mysql',
    procedure:    'GetAuthorImpact',
    reason:       'Stored procedure aggregates paper counts per author ordered by impact.',
    query_pattern: 'CALL GetAuthorImpact()',
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/journals
// Uses MongoDB aggregation (unchanged — no SQL procedure for journals).
// ─────────────────────────────────────────────────────────────────────────────
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

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/papers-per-year
// MongoDB year-indexed aggregation (unchanged).
// ─────────────────────────────────────────────────────────────────────────────
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

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/trending
// Calls stored procedure GetTrendingPapers(p_year, p_limit).
// Returns papers published from p_year onwards, ordered by author_count DESC.
// ─────────────────────────────────────────────────────────────────────────────
const getTrendingPapers = asyncHandler(async (req, res) => {
  const year  = parseInt(req.query.year,  10) || new Date().getFullYear() - 4;
  const limit = parseInt(req.query.limit, 10) || 10;

  if (isNaN(year) || year < 1900 || year > new Date().getFullYear()) {
    throw new AppError('Invalid year parameter.', 400, 'INVALID_PARAM');
  }
  if (limit < 1 || limit > 100) {
    throw new AppError('Limit must be between 1 and 100.', 400, 'INVALID_PARAM');
  }

  let papers;
  try {
    const pool = getMySQL();
    const [rows] = await pool.execute('CALL GetTrendingPapers(?, ?)', [year, limit]);
    papers = rows[0] || [];
  } catch (err) {
    throw classifyError(err);
  }

  res.json({
    papers,
    count:     papers.length,
    from_year: year,
    limit,
    source:    'mysql',
    procedure: 'GetTrendingPapers',
    reason:    'Returns high-impact papers (by author_count) published from the given year.',
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/incomplete-papers
// Calls stored procedure GetIncompletePapers().
// Returns papers missing abstract, journal_id, or publish_year.
// ─────────────────────────────────────────────────────────────────────────────
const getIncompletePapers = asyncHandler(async (req, res) => {
  let papers;
  try {
    const pool = getMySQL();
    const [rows] = await pool.execute('CALL GetIncompletePapers()');
    papers = rows[0] || [];
  } catch (err) {
    throw classifyError(err);
  }

  res.json({
    papers,
    count:     papers.length,
    source:    'mysql',
    procedure: 'GetIncompletePapers',
    reason:    'Identifies papers missing abstract, journal, or publish year for data quality auditing.',
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/active-users
// Calls stored procedure GetActiveUsers().
// Returns users ordered by last_login DESC.
// last_login is automatically maintained by trigger trg_update_last_login.
// ─────────────────────────────────────────────────────────────────────────────
const getActiveUsers = asyncHandler(async (req, res) => {
  let users;
  try {
    const pool = getMySQL();
    const [rows] = await pool.execute('CALL GetActiveUsers()');
    users = rows[0] || [];
  } catch (err) {
    throw classifyError(err);
  }

  res.json({
    users,
    count:     users.length,
    source:    'mysql',
    procedure: 'GetActiveUsers',
    trigger:   'trg_update_last_login (BEFORE UPDATE on users — auto-sets last_login)',
    reason:    'Lists users by most recent activity; last_login kept current by trigger.',
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/important-papers
// Returns papers flagged is_important = TRUE by trigger trg_mark_important_paper.
// Trigger fires AFTER INSERT on paper_metrics when author_count >= 5.
// ─────────────────────────────────────────────────────────────────────────────
const getImportantPapers = asyncHandler(async (req, res) => {
  const limit  = parseInt(req.query.limit,  10) || 20;
  const offset = parseInt(req.query.offset, 10) || 0;

  if (limit < 1 || limit > 200) {
    throw new AppError('Limit must be between 1 and 200.', 400, 'INVALID_PARAM');
  }

  let papers;
  try {
    const pool = getMySQL();
    const [rows] = await pool.query(
      `SELECT p.paper_id, p.title, p.publish_year AS year,
              j.journal_name AS journal, pm.author_count
       FROM   papers p
       JOIN   paper_metrics pm ON pm.paper_id = p.paper_id
       LEFT JOIN journals j   ON j.journal_id = p.journal_id
       WHERE  p.is_important = TRUE
       ORDER  BY pm.author_count DESC, p.publish_year DESC
       LIMIT  ${limit} OFFSET ${offset}`,
    );
    papers = rows;
  } catch (err) {
    throw classifyError(err);
  }

  res.json({
    papers,
    count:    papers.length,
    limit,
    offset,
    source:   'mysql',
    trigger:  'trg_mark_important_paper (AFTER INSERT on paper_metrics — sets is_important when author_count >= 5)',
    reason:   'Papers auto-flagged as important based on author collaboration breadth.',
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/journal-popularity
// Returns journals with their paper_count maintained by trigger trg_update_journal_count.
// Trigger fires AFTER INSERT on papers — increments journals.paper_count.
// ─────────────────────────────────────────────────────────────────────────────
const getJournalPopularity = asyncHandler(async (req, res) => {
  const limit = parseInt(req.query.limit, 10) || 10;

  if (limit < 1 || limit > 500) {
    throw new AppError('Limit must be between 1 and 500.', 400, 'INVALID_PARAM');
  }

  let journals;
  try {
    const pool = getMySQL();
    const [rows] = await pool.query(
      `SELECT journal_name, paper_count
       FROM   journals
       ORDER  BY paper_count DESC
       LIMIT  ${limit}`,
    );
    journals = rows;
  } catch (err) {
    throw classifyError(err);
  }

  res.json({
    journals,
    count:   journals.length,
    source:  'mysql',
    trigger: 'trg_update_journal_count (AFTER INSERT on papers — increments journals.paper_count)',
    reason:  'paper_count is kept in sync automatically by trigger; no GROUP BY needed at query time.',
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/database-info
// Documents the full hybrid architecture including updated procedures/triggers.
// ─────────────────────────────────────────────────────────────────────────────
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
          'GetTrendingPapers(p_year, p_limit) — papers from year ordered by author_count DESC',
          'GetAuthorImpact()              — all authors with total paper counts ordered by impact',
          'GetIncompletePapers()          — papers missing abstract, journal, or publish_year',
          'GetActiveUsers()               — users ordered by last_login DESC',
        ],
        triggers: [
          'trg_mark_important_paper  — AFTER INSERT on paper_metrics: sets papers.is_important=TRUE when author_count >= 5',
          'trg_update_last_login     — BEFORE UPDATE on users: auto-sets last_login = CURRENT_TIMESTAMP',
          'trg_validate_paper        — BEFORE INSERT on papers: rejects titles shorter than 5 characters',
          'trg_update_journal_count  — AFTER INSERT on papers: increments journals.paper_count',
        ],
        schema_additions: [
          'papers.is_important BOOLEAN DEFAULT FALSE — flagged by trg_mark_important_paper',
          'journals.paper_count INT DEFAULT 0        — maintained by trg_update_journal_count',
        ],
        optimisation: 'Denormalised paper_count on journals avoids GROUP BY at read time',
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
      neo4j: {
        role:      'Collaboration & citation graph',
        strengths: 'Relationship traversal, graph algorithms',
        use_cases: ['Collaboration networks', 'Citation analysis', 'Author co-authorship'],
      },
    },
    polyglot_persistence: {
      principle:   'Use the right database for the right job',
      data_flow:   'Single dataset → multiple optimised representations',
      consistency: 'Eventual consistency model with primary source (MongoDB)',
    },
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/query-performance
// ─────────────────────────────────────────────────────────────────────────────
const getQueryPerformance = asyncHandler(async (req, res) => {
  res.json({
    query_performance_comparison: {
      mysql: {
        simple_select:      '2–5ms (B-tree index lookup)',
        join_queries:       '5–15ms (optimised nested loop join)',
        aggregation:        '10–30ms (GROUP BY with index)',
        stored_procedures:  '3–20ms (pre-compiled execution plans)',
        optimisation:       'trg_update_journal_count keeps paper_count denormalised — reads need no GROUP BY',
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
      'Trending papers since year X': {
        winner:       'MySQL (GetTrendingPapers)',
        reason:       'Pre-compiled stored procedure with metric join — faster than ad-hoc query',
        mysql_time:   '5–15ms',
      },
      'Author impact ranking': {
        winner:       'MySQL (GetAuthorImpact)',
        reason:       'Normalised many-to-many with GROUP BY; covers all authors in one pass',
        mysql_time:   '10–25ms',
      },
      'Journal paper count lookup': {
        winner:       'MySQL (denormalised column)',
        reason:       'trg_update_journal_count keeps count current — SELECT needs no aggregation',
        mysql_time:   '1–3ms',
      },
      'Search papers by keyword': {
        winner:       'MongoDB',
        reason:       'Text indexes provide 3–5× faster full-text search than LIKE',
        mongodb_time: '8–15ms',
        mysql_time:   '30–50ms (LIKE operator)',
      },
    },
  });
});

module.exports = {
  getOverview,
  getAuthorStats,
  getJournalStats,
  getPapersPerYear,
  getTrendingPapers,
  getIncompletePapers,
  getActiveUsers,
  getImportantPapers,
  getJournalPopularity,
  getDatabaseInfo,
  getQueryPerformance,
};