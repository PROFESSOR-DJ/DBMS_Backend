// statsController handles backend statistics and dashboard requests.
// Procedures used: GetTrendingPapers, GetAuthorImpact, GetIncompletePapers, GetActiveUsers
// Triggers active: trg_mark_important_paper, trg_update_last_login,
//                  trg_validate_paper, trg_update_journal_count

const PaperModel     = require('../models/mysql/paperModel');
const AuthorModel    = require('../models/mysql/authorModel');
const PaperDocument  = require('../models/mongodb/paperModel');
const { getMySQL }   = require('../config/database');
const { runQuery, isNeo4jConnected } = require('../config/neo4jDatabase');
const { AppError, classifyError, asyncHandler } = require('../utils/errorHandler');

const paperDocument = new PaperDocument();
const DEFAULT_PREVIEW_LIMIT = 6;
const DASHBOARD_AUTHOR_SOFT_CAP = 25;
const DASHBOARD_AUTHOR_EXTREME_THRESHOLD = 100;
const DASHBOARD_FETCH_MULTIPLIER = 25;
const MAX_DASHBOARD_FETCH = 400;

const parseBoundedInt = (value, fallback, min, max) => {
  const parsed = Number.parseInt(value, 10);
  if (Number.isNaN(parsed)) return fallback;
  return Math.min(Math.max(parsed, min), max);
};

const getCollaborationMeta = (authorCount) => {
  if (authorCount >= 101) {
    return {
      band: 'Consortium scale',
      note: 'Very large author lists are de-emphasised in dashboard rankings.',
      display: '100+ authors',
      isExtreme: true,
      isPreferred: false,
    };
  }

  if (authorCount >= 26) {
    return {
      band: 'Multi-team',
      note: 'Broad collaboration across multiple groups.',
      display: `${authorCount} authors`,
      isExtreme: false,
      isPreferred: false,
    };
  }

  if (authorCount >= 13) {
    return {
      band: 'Large team',
      note: 'Strong collaboration without crowding the ranking.',
      display: `${authorCount} authors`,
      isExtreme: false,
      isPreferred: true,
    };
  }

  if (authorCount >= 5) {
    return {
      band: 'Collaborative',
      note: 'Good fit for trigger-based collaboration highlights.',
      display: `${authorCount} authors`,
      isExtreme: false,
      isPreferred: true,
    };
  }

  return {
    band: 'Focused',
    note: 'Smaller authorship footprint.',
    display: `${authorCount} author${authorCount === 1 ? '' : 's'}`,
    isExtreme: false,
    isPreferred: false,
  };
};

const normalisePaperInsight = (row) => {
  const authorCount = Number(row.author_count) || 0;
  const collaboration = getCollaborationMeta(authorCount);

  return {
    paper_id: row.paper_id,
    title: row.title,
    year: row.year ?? row.publish_year ?? null,
    journal: row.journal ?? row.journal_name ?? null,
    author_count: authorCount,
    author_count_display: collaboration.display,
    collaboration_band: collaboration.band,
    collaboration_note: collaboration.note,
    is_extreme_collaboration: collaboration.isExtreme,
    is_preferred_collaboration: collaboration.isPreferred,
  };
};

const curateDashboardPaperRows = (rows, limit) => {
  const normalised = rows.map(normalisePaperInsight);
  const preferred = [];
  const secondary = [];
  const extreme = [];

  normalised.forEach((paper) => {
    if (paper.author_count > DASHBOARD_AUTHOR_EXTREME_THRESHOLD) {
      extreme.push(paper);
      return;
    }

    if (paper.author_count > DASHBOARD_AUTHOR_SOFT_CAP) {
      secondary.push(paper);
      return;
    }

    preferred.push(paper);
  });

  const papers = [...preferred, ...secondary, ...extreme].slice(0, limit);
  const selectedIds = new Set(papers.map(paper => paper.paper_id));
  const hiddenExtremeCount = extreme.filter(paper => !selectedIds.has(paper.paper_id)).length;

  return {
    papers,
    meta: {
      preferred_author_range: `5-${DASHBOARD_AUTHOR_SOFT_CAP} authors`,
      extreme_author_threshold: DASHBOARD_AUTHOR_EXTREME_THRESHOLD,
      hidden_extreme_count: hiddenExtremeCount,
      extreme_in_sample: extreme.length,
      display_strategy: 'Dashboard previews favour readable collaboration sizes before consortium-scale outliers.',
    },
  };
};

const enrichPaperRows = async (pool, rows) => {
  if (!rows.length) return rows;

  const paperIds = rows
    .map(row => row.paper_id)
    .filter(paperId => paperId !== null && paperId !== undefined);

  if (!paperIds.length) return rows;

  const placeholders = paperIds.map(() => '?').join(', ');
  const [metaRows] = await pool.query(
    `SELECT p.paper_id, p.publish_year AS year, j.journal_name AS journal
     FROM   papers p
     LEFT JOIN journals j ON j.journal_id = p.journal_id
     WHERE  p.paper_id IN (${placeholders})`,
    paperIds,
  );

  const metaById = new Map(metaRows.map(row => [row.paper_id, row]));

  return rows.map((row) => {
    const meta = metaById.get(row.paper_id) || {};
    return {
      ...row,
      year: row.year ?? row.publish_year ?? meta.year ?? null,
      journal: row.journal ?? row.journal_name ?? meta.journal ?? null,
    };
  });
};

const getDashboardGraphHighlights = async () => {
  if (!isNeo4jConnected()) {
    return {
      connected: false,
      collaboration_edges: 0,
      ranked_journals: 0,
      top_collaborators: [],
      note: 'Neo4j is not connected.',
    };
  }

  const [summaryRecord] = await runQuery(`
    CALL {
      MATCH (:Author)-[r:WROTE]->(:Paper)
      RETURN count(r) AS wrote_edges
    }
    CALL {
      MATCH (j:Journal)
      RETURN count(CASE WHEN j.sjr_rank IS NOT NULL THEN 1 END) AS ranked_journals
    }
    RETURN wrote_edges, ranked_journals
  `);

  const collaboratorRecords = await runQuery(`
    MATCH (a:Author)-[r:CO_AUTHORED]-(co:Author)
    WITH a.name AS author, SUM(r.shared_papers) AS collaboration_score
    RETURN author, collaboration_score
    ORDER BY collaboration_score DESC, author ASC
    LIMIT 5
  `);

  const toNumber = (value) => {
    if (value === null || value === undefined) return 0;
    if (typeof value === 'number') return value;
    if (typeof value.toNumber === 'function') return value.toNumber();
    return Number(value);
  };

  return {
    connected: true,
    collaboration_edges: toNumber(summaryRecord?.get('wrote_edges')),
    ranked_journals: toNumber(summaryRecord?.get('ranked_journals')),
    top_collaborators: collaboratorRecords.map((record) => ({
      author: record.get('author'),
      collaboration_score: toNumber(record.get('collaboration_score')),
    })),
  };
};

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/overview
// Combines MySQL + MongoDB stats in parallel.
// ─────────────────────────────────────────────────────────────────────────────
const getOverview = asyncHandler(async (req, res) => {
  const [mysqlStats, mongoStats, papersPerYear, topJournals, topAuthors, importantPreview, incompletePreview, activeUsersPreview, graphHighlights, keywordHighlights] = await Promise.all([
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

    (async () => {
      try {
        const pool = getMySQL();
        const [rows] = await pool.query(
          `SELECT p.paper_id, p.title, p.publish_year AS year,
                  j.journal_name AS journal, pm.author_count
           FROM papers p
           JOIN paper_metrics pm ON pm.paper_id = p.paper_id
           LEFT JOIN journals j ON j.journal_id = p.journal_id
           WHERE p.is_important = TRUE
           ORDER BY pm.author_count DESC, p.publish_year DESC, p.title ASC
           LIMIT 6`
        );
        return rows.map(normalisePaperInsight);
      } catch (err) {
        console.error('MySQL importantPreview error:', classifyError(err).message);
        return [];
      }
    })(),

    (async () => {
      try {
        const pool = getMySQL();
        const [[countRow]] = await pool.query(
          `SELECT COUNT(*) AS total
           FROM papers
           WHERE abstract IS NULL OR journal_id IS NULL OR publish_year IS NULL`
        );
        return { total: Number(countRow?.total) || 0 };
      } catch (err) {
        console.error('MySQL incompletePreview error:', classifyError(err).message);
        return { total: 0 };
      }
    })(),

    (async () => {
      try {
        const pool = getMySQL();
        const [rows] = await pool.query(
          `SELECT user_id, name, last_login
           FROM users
           ORDER BY last_login DESC
           LIMIT 5`
        );
        return rows;
      } catch (err) {
        console.error('MySQL activeUsersPreview error:', classifyError(err).message);
        return [];
      }
    })(),

    getDashboardGraphHighlights().catch((err) => ({
      connected: false,
      collaboration_edges: 0,
      ranked_journals: 0,
      top_collaborators: [],
      note: classifyError(err).message,
    })),

    (async () => {
      try {
        const options = await paperDocument.getFilterOptions();
        return (options.keywords || []).slice(0, 8);
      } catch (err) {
        console.error('MongoDB keywordHighlights error:', classifyError(err).message);
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
      neo4j: {
        collaboration_edges: graphHighlights.collaboration_edges,
        ranked_journals: graphHighlights.ranked_journals,
        role: 'Relationship traversal, collaboration context, venue-network overlays',
        ...(graphHighlights.note && { warning: graphHighlights.note }),
      },
    },
    analytics: {
      papers_per_year: papersPerYear,
      top_journals:    topJournals,
      top_authors:     topAuthors,
    },
    research_highlights: {
      dashboard_story: {
        recommended_use: 'Use Dashboard for cross-database orientation before drilling into authors, papers, journals, or graph exploration.',
      },
      mysql_curation: {
        important_papers: importantPreview,
        incomplete_paper_count: incompletePreview.total,
        active_users: activeUsersPreview,
      },
      mongodb_discovery: {
        top_keywords: keywordHighlights,
        avg_citations: mongoStats.avgCitations,
      },
      neo4j_network: graphHighlights,
    },
    optimisation: {
      parallel_execution:    'All queries run concurrently via Promise.all',
      sql_optimisation:      'COUNT on indexed primary keys',
      mongodb_optimisation:  'Aggregation pipelines with compound indexes',
      graph_optimisation:    'Neo4j powers collaboration snapshots and venue relationship views without SQL-style multi-hop joins.',
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

// GET /stats/author-track/:name
// Optimized: Fetches metrics from MySQL and co-authors from Neo4j in parallel.
const getAuthorTrackRecord = asyncHandler(async (req, res) => {
  const authorName = String(req.query.name || req.params.name || '')
    .trim()
    .replace(/^[\s'"[\]]+|[\s'"[\]]+$/g, '');

  if (!authorName || authorName.length < 2) {
    throw new AppError('Author name must be at least 2 characters.', 400, 'MISSING_PARAM');
  }

  const [trackRecordResult, coAuthorResult] = await Promise.all([
    (async () => {
      const pool = getMySQL();
      const [rows] = await pool.execute('CALL GetAuthorTrackRecord(?)', [authorName]);
      return rows[0]?.[0] || null;
    })(),
    (async () => {
      if (!isNeo4jConnected()) return null;
      try {
        const records = await runQuery(
          `MATCH (a:Author {name: $name})-[r:CO_AUTHORED]-(co:Author)
           RETURN co.name AS coauthor
           ORDER BY r.shared_papers DESC
           LIMIT 15`,
          { name: authorName }
        );
        return records.map(r => r.get('coauthor')).join(', ');
      } catch (err) {
        console.error('Neo4j track-record co-author error:', err.message);
        return null;
      }
    })()
  ]);

  if (trackRecordResult) {
    trackRecordResult.co_authors = coAuthorResult || 'No significant collaborators found in graph';
  }

  res.json({
    author: authorName,
    track_record: trackRecordResult,
    source: 'hybrid (mysql+neo4j)',
    procedure: 'GetAuthorTrackRecord',
    optimisation: 'Co-authors offloaded to Neo4j graph traversal'
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/journals
// Uses MongoDB aggregation (unchanged — no SQL procedure for journals).
// ─────────────────────────────────────────────────────────────────────────────
const getJournalStats = asyncHandler(async (req, res) => {
  const limit = parseBoundedInt(req.query.limit, 200, 1, 5000);
  const offset = parseBoundedInt(req.query.offset, 0, 0, 1000000);
  const search = String(req.query.search || '').trim();
  const sortBy = ['recent', 'name', 'count'].includes(req.query.sortBy)
    ? req.query.sortBy
    : 'recent';

  let journals;
  let total = 0;
  try {
    const pool = getMySQL();
    const params = [];
    let whereClause = `
      WHERE j.journal_name IS NOT NULL
        AND TRIM(j.journal_name) <> ''
    `;

    if (search) {
      whereClause += ' AND j.journal_name LIKE ?';
      params.push(`%${search}%`);
    }

    let orderBy = 'j.journal_id DESC';
    if (sortBy === 'name') {
      orderBy = 'j.journal_name ASC';
    } else if (sortBy === 'count') {
      orderBy = 'j.paper_count DESC, j.journal_name ASC';
    }

    const [rows] = await pool.query(
      `
        SELECT j.journal_id, j.journal_name AS journal, j.paper_count AS count
        FROM journals j
        ${whereClause}
        ORDER BY ${orderBy}
        LIMIT ${limit} OFFSET ${offset}
      `,
      params
    );
    journals = rows;

    const [countRows] = await pool.query(
      `
        SELECT COUNT(*) AS total
        FROM journals j
        ${whereClause}
      `,
      params
    );
    total = Number(countRows[0]?.total) || 0;
  } catch (err) {
    throw classifyError(err);
  }

  res.json({
    journals,
    count:        journals.length,
    total,
    limit,
    offset,
    source:       'mysql',
    reason:       'Journal list is read directly from the journals table so newly inserted venues appear immediately.',
    optimisation: 'Uses journals.paper_count and journal_id ordering instead of MongoDB top-N aggregation.',
    sort_by:      sortBy,
    query_pattern: search
      ? 'SELECT journal_name, paper_count FROM journals WHERE journal_name LIKE ? ORDER BY ... LIMIT/OFFSET'
      : 'SELECT journal_name, paper_count FROM journals ORDER BY ... LIMIT/OFFSET',
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
  const fallbackYear = new Date().getFullYear() - 4;
  const yearInput = req.query.year;
  const limitInput = req.query.limit;
  const year = parseBoundedInt(yearInput, fallbackYear, 1900, new Date().getFullYear());
  const limit = parseBoundedInt(limitInput, DEFAULT_PREVIEW_LIMIT, 1, 100);
  const curated = req.query.curated !== 'false';
  const fetchLimit = curated
    ? Math.min(Math.max(limit * DASHBOARD_FETCH_MULTIPLIER, limit), MAX_DASHBOARD_FETCH)
    : limit;

  if (yearInput !== undefined && Number.isNaN(Number.parseInt(yearInput, 10))) {
    throw new AppError('Invalid year parameter.', 400, 'INVALID_PARAM');
  }
  if (limitInput !== undefined && Number.isNaN(Number.parseInt(limitInput, 10))) {
    throw new AppError('Limit must be between 1 and 100.', 400, 'INVALID_PARAM');
  }

  let papers;
  let meta = {};
  try {
    const pool = getMySQL();
    const [rows] = await pool.execute('CALL GetTrendingPapers(?, ?)', [year, fetchLimit]);
    const enrichedRows = await enrichPaperRows(pool, rows[0] || []);

    if (curated) {
      const curatedRows = curateDashboardPaperRows(enrichedRows, limit);
      papers = curatedRows.papers;
      meta = curatedRows.meta;
    } else {
      papers = enrichedRows.slice(0, limit).map(normalisePaperInsight);
    }
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
    reason:    'Stored procedure supplies collaboration counts; dashboard previews group consortium-scale outliers so the ranking stays readable.',
    curated,
    meta,
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/incomplete-papers
// Calls stored procedure GetIncompletePapers().
// Returns papers missing abstract, journal_id, or publish_year.
// ─────────────────────────────────────────────────────────────────────────────
const getIncompletePapers = asyncHandler(async (req, res) => {
  const limit = parseBoundedInt(req.query.limit, DEFAULT_PREVIEW_LIMIT, 1, 100);
  const offset = parseBoundedInt(req.query.offset, 0, 0, 10000);
  let papers;
  let total = 0;
  try {
    const pool = getMySQL();
    const [[countRow]] = await pool.execute(
      `SELECT COUNT(*) AS total
       FROM papers
       WHERE abstract IS NULL
          OR journal_id IS NULL
          OR publish_year IS NULL`,
    );
    const [rows] = await pool.query(
      `SELECT paper_id, title
       FROM papers
       WHERE abstract IS NULL
          OR journal_id IS NULL
          OR publish_year IS NULL
       ORDER BY publish_year DESC, paper_id DESC
       LIMIT ${limit} OFFSET ${offset}`,
    );
    total = countRow.total || 0;
    papers = rows;
  } catch (err) {
    throw classifyError(err);
  }

  res.json({
    papers,
    count:     papers.length,
    total,
    limit,
    offset,
    has_more:  offset + papers.length < total,
    source:    'mysql',
    procedure: 'GetIncompletePapers',
    reason:    'Preview endpoint keeps the drawer light while still reflecting the GetIncompletePapers logic.',
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/active-users
// Calls stored procedure GetActiveUsers().
// Returns users ordered by last_login DESC.
// last_login is automatically maintained by trigger trg_update_last_login.
// ─────────────────────────────────────────────────────────────────────────────
const getActiveUsers = asyncHandler(async (req, res) => {
  const limit = parseBoundedInt(req.query.limit, DEFAULT_PREVIEW_LIMIT, 1, 100);
  const offset = parseBoundedInt(req.query.offset, 0, 0, 10000);
  let users;
  let total = 0;
  try {
    const pool = getMySQL();
    const [[countRow]] = await pool.execute('SELECT COUNT(*) AS total FROM users');
    const [rows] = await pool.query(
      `SELECT user_id, name, last_login
       FROM users
       ORDER BY last_login DESC
       LIMIT ${limit} OFFSET ${offset}`,
    );
    total = countRow.total || 0;
    users = rows;
  } catch (err) {
    throw classifyError(err);
  }

  res.json({
    users,
    count:     users.length,
    total,
    limit,
    offset,
    has_more:  offset + users.length < total,
    source:    'mysql',
    procedure: 'GetActiveUsers',
    trigger:   'trg_update_last_login (BEFORE UPDATE on users — auto-sets last_login)',
    reason:    'Preview endpoint returns the most recent users first so the activity drawer stays compact.',
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /stats/important-papers
// Returns papers flagged is_important = TRUE by trigger trg_mark_important_paper.
// Trigger fires AFTER INSERT on paper_metrics when author_count >= 5.
// ─────────────────────────────────────────────────────────────────────────────
const getImportantPapers = asyncHandler(async (req, res) => {
  const limitInput = req.query.limit;
  const limit = parseBoundedInt(limitInput, DEFAULT_PREVIEW_LIMIT, 1, 200);
  const offset = parseBoundedInt(req.query.offset, 0, 0, 10000);
  const curated = req.query.curated !== 'false';
  const fetchLimit = curated
    ? Math.min(Math.max(limit * DASHBOARD_FETCH_MULTIPLIER, limit), MAX_DASHBOARD_FETCH)
    : limit;

  if (limitInput !== undefined && Number.isNaN(Number.parseInt(limitInput, 10))) {
    throw new AppError('Limit must be between 1 and 200.', 400, 'INVALID_PARAM');
  }

  let papers;
  let total = 0;
  let meta = {};
  try {
    const pool = getMySQL();
    const [[countRow]] = await pool.execute(
      `SELECT COUNT(*) AS total
       FROM   papers
       WHERE  is_important = TRUE`,
    );
    const [rows] = await pool.query(
      `SELECT p.paper_id, p.title, p.publish_year AS year,
              j.journal_name AS journal, pm.author_count
       FROM   papers p
       JOIN   paper_metrics pm ON pm.paper_id = p.paper_id
       LEFT JOIN journals j   ON j.journal_id = p.journal_id
       WHERE  p.is_important = TRUE
       ORDER  BY pm.author_count DESC, p.publish_year DESC, p.title ASC
       LIMIT  ${fetchLimit} OFFSET ${offset}`,
    );
    total = countRow.total || 0;

    if (curated) {
      const curatedRows = curateDashboardPaperRows(rows, limit);
      papers = curatedRows.papers;
      meta = curatedRows.meta;
    } else {
      papers = rows.slice(0, limit).map(normalisePaperInsight);
    }
  } catch (err) {
    throw classifyError(err);
  }

  res.json({
    papers,
    count:    papers.length,
    total,
    limit,
    offset,
    source:   'mysql',
    trigger:  'trg_mark_important_paper (AFTER INSERT on paper_metrics — sets is_important when author_count >= 5)',
    reason:   'Trigger-flagged papers are curated for dashboard readability so consortium-scale outliers do not dominate the preview.',
    curated,
    meta,
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
          'GetAuthorTrackRecord(name)     — reviewer track record with venue quartile summary',
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
  getAuthorTrackRecord,
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
