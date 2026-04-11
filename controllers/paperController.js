// paperController handles backend paper CRUD and lookup requests.
// Trigger interactions:
//   trg_validate_paper        — BEFORE INSERT on papers: rejects title < 5 chars
//   trg_after_paper_insert    — AFTER INSERT on papers: creates paper_metrics row
//   trg_update_journal_count  — AFTER INSERT on papers: increments journals.paper_count
//   trg_mark_important_paper  — AFTER INSERT on paper_metrics: sets is_important when author_count >= 5
//   trg_after_paper_authors_insert/delete — sync paper_metrics.author_count
//   trg_before_author_delete  — guard against deleting linked authors (handled in authorController)
//
// CONSISTENCY NOTE:
//   trg_update_journal_count fires only on INSERT.  The paperModel.delete() method
//   therefore recomputes and writes back journals.paper_count after every deletion
//   so the Journals page and journal-popularity endpoint stay accurate.
//
//   Neo4j WROTE/PUBLISHED_IN relationship counts use DISTINCT in Cypher queries
//   (see neo4jController.js) so stale duplicate edges from past bulk-imports do
//   not inflate author or journal paper counts.

const PaperModel       = require('../models/mysql/paperModel');
const AuthorModel      = require('../models/mysql/authorModel');
const PaperAuthorModel = require('../models/mysql/paperAuthorModel');
const PaperDocument    = require('../models/mongodb/paperModel');
const DatabaseRouter   = require('../config/databaseRouter');
const { syncPaperToGraph, removePaperFromGraph } = require('../services/graphSyncService');
const { AppError, classifyError, asyncHandler } = require('../utils/errorHandler');

const paperDocument = new PaperDocument();

const getPaperIdentifier = (paper) => {
  if (!paper) return null;
  return String(paper.paper_id || paper._id || '').trim() || null;
};

const enrichPapersWithSqlFlags = async (papers) => {
  if (!Array.isArray(papers) || papers.length === 0) {
    return papers;
  }

  const flagsByPaperId = await PaperModel.getFlagsByPaperIds(
    papers.map(getPaperIdentifier)
  );

  return papers.map((paper) => {
    const paperId = getPaperIdentifier(paper);
    const sqlFlags = paperId ? flagsByPaperId.get(paperId) : null;

    if (!sqlFlags) {
      return paper;
    }

    return {
      ...paper,
      paper_id: paper.paper_id || paperId,
      is_important: sqlFlags.is_important,
      author_count: sqlFlags.author_count ?? paper.author_count,
    };
  });
};

const enrichSinglePaperWithSqlFlags = async (paper) => {
  if (!paper) {
    return paper;
  }

  const [enrichedPaper] = await enrichPapersWithSqlFlags([paper]);
  return enrichedPaper;
};

const isHighlyCollaborativeEnabled = (value) =>
  value === true || value === 'true' || value === '1' || value === 1;

const hasListFilters = ({ yearFrom, yearTo, journal, author, highlyCollaborative }) =>
  Boolean(yearFrom || yearTo || journal || author || highlyCollaborative);

const normalisePaperPayload = (paper = {}) => ({
  ...paper,
  paper_id: String(paper.paper_id || '').trim(),
  title: String(paper.title || '').trim(),
  abstract: paper.abstract ? String(paper.abstract).trim() : '',
  year: paper.year !== undefined && paper.year !== null && paper.year !== ''
    ? Number.parseInt(paper.year, 10)
    : null,
  doi: paper.doi ? String(paper.doi).trim() : '',
  journal: paper.journal ? String(paper.journal).trim() : '',
  source: paper.source ? String(paper.source).trim() : 'manual',
  authors: Array.isArray(paper.authors)
    ? paper.authors
        .map((author) => String(author || '').trim())
        .filter(Boolean)
        .filter((author, index, list) => list.indexOf(author) === index)
    : [],
  is_covid19: Boolean(paper.is_covid19),
  has_full_text: Boolean(paper.has_full_text),
});

const rollbackMongoPaperCreate = async (paperId) => {
  if (!paperId) return;

  try {
    await paperDocument.delete(paperId);
  } catch (error) {
    console.warn(`Mongo rollback warning for paper '${paperId}':`, classifyError(error).message);
  }
};

const rollbackSqlPaperCreate = async (createContext) => {
  if (!createContext?.paper_id) return;

  try {
    await PaperModel.compensateCreate(createContext);
  } catch (error) {
    console.warn(`MySQL rollback warning for paper '${createContext.paper_id}':`, classifyError(error).message);
  }
};

// ── GET ALL PAPERS ────────────────────────────────────────────────────────────
const getAllPapers = asyncHandler(async (req, res) => {
  const limit  = parseInt(req.query.limit, 10) || 20;
  const page   = parseInt(req.query.page,  10) || 1;
  const offset = (page - 1) * limit;
  const sortBy = req.query.sortBy || 'recent';
  const yearFrom = req.query.yearFrom ? parseInt(req.query.yearFrom, 10) : null;
  const yearTo = req.query.yearTo ? parseInt(req.query.yearTo, 10) : null;
  const journal = req.query.journal || null;
  const author = req.query.author || null;
  const highlyCollaborative = isHighlyCollaborativeEnabled(req.query.highlyCollaborative);

  if (hasListFilters({ yearFrom, yearTo, journal, author, highlyCollaborative })) {
    const result = await PaperModel.advancedSearch({
      query: null,
      yearFrom,
      yearTo,
      journal,
      author,
      limit,
      offset,
      sortBy,
      highlyCollaborative,
    });

    return res.json({
      papers: result.papers,
      pagination: {
        page,
        limit,
        total: result.total,
        pages: Math.ceil(result.total / limit),
      },
      source: 'mysql',
      reason: 'MySQL provides accurate server-side filtering for range, tags, and collaboration flags',
    });
  }

  const mongoPapers = await paperDocument.findAll(limit, offset, sortBy);
  const papers = await enrichPapersWithSqlFlags(mongoPapers);
  const stats  = await paperDocument.getStats();
  const total  = stats.totalPapers || 0;

  res.json({
    papers,
    pagination: { page, limit, total, pages: Math.ceil(total / limit) },
    source: 'mongodb',
    reason: 'MongoDB for flexible schema and fast retrieval of large-scale data',
  });
});

// ── GET PAPER BY ID ───────────────────────────────────────────────────────────
const getPaperById = asyncHandler(async (req, res) => {
  const { id } = req.params;

  const paper = await paperDocument.findById(id);
  if (!paper) {
    throw new AppError(`Paper with id '${id}' not found.`, 404, 'NOT_FOUND');
  }

  const enrichedPaper = await enrichSinglePaperWithSqlFlags(paper);

  res.json({
    paper: enrichedPaper,
    source:       'mongodb',
    enriched_with: 'sql_relationships',
    reason:       'MongoDB for document retrieval with SQL enrichment',
  });
});

// ── SEARCH PAPERS ─────────────────────────────────────────────────────────────
const searchPapers = asyncHandler(async (req, res) => {
  const {
    q, yearFrom, yearTo, journal, author,
    minCitations, keywords, abstract, doi,
    sortBy = 'relevance',
  } = req.query;

  const limit  = parseInt(req.query.limit, 10) || 20;
  const page   = parseInt(req.query.page,  10) || 1;
  const offset = (page - 1) * limit;
  const highlyCollaborative = isHighlyCollaborativeEnabled(req.query.highlyCollaborative);

  const searchParams = {
    query:        q,
    yearFrom:     yearFrom     ? parseInt(yearFrom)     : null,
    yearTo:       yearTo       ? parseInt(yearTo)       : null,
    journal,
    author,
    minCitations: minCitations ? parseInt(minCitations) : null,
    keywords,
    abstract,
    doi,
    limit,
    offset,
    sortBy,
    highlyCollaborative,
  };

  if (highlyCollaborative) {
    const result = await PaperModel.advancedSearch(searchParams);

    return res.json({
      papers: result.papers,
      count:  result.papers.length,
      total:  result.total,
      pagination: {
        page, limit, total: result.total,
        pages: Math.ceil(result.total / limit),
      },
      source: 'mysql',
      reason: 'MySQL search keeps trigger-backed collaboration filtering and totals consistent',
    });
  }

  const result = await paperDocument.advancedSearch(searchParams);
  const papers = await enrichPapersWithSqlFlags(result.papers);

  res.json({
    papers,
    count:  papers.length,
    total:  result.total,
    pagination: {
      page, limit, total: result.total,
      pages: Math.ceil(result.total / limit),
    },
    source: 'mongodb',
    reason: 'MongoDB text search optimised with indexed fields',
  });
});

const findSimilarPapers = asyncHandler(async (req, res) => {
  const { title, abstract } = req.query;
  const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 5, 1), 20);
  const searchText = [title, abstract].filter(Boolean).join(' ').trim();

  if (!searchText || searchText.length < 10) {
    throw new AppError('Provide title or abstract text with at least 10 characters.', 400, 'MISSING_PARAM');
  }

  const results = await paperDocument.getCollection()
    .find(
      { $text: { $search: searchText } },
      {
        projection: {
          title: 1,
          paper_id: 1,
          year: 1,
          journal: 1,
          authors: 1,
          score: { $meta: 'textScore' },
        },
      }
    )
    .sort({ score: { $meta: 'textScore' } })
    .limit(limit)
    .toArray();

  res.json({
    query: {
      title,
      abstract: abstract ? String(abstract).substring(0, 80) : undefined,
    },
    similar_papers: results,
    count: results.length,
    source: 'mongodb',
    note: 'Ranked by MongoDB textScore against title and abstract.',
  });
});

// ── GET PAPERS BY YEAR ────────────────────────────────────────────────────────
const getPapersByYear = asyncHandler(async (req, res) => {
  const { year } = req.params;
  const yearInt  = parseInt(year, 10);

  if (isNaN(yearInt)) {
    throw new AppError('Year must be a valid integer.', 400, 'INVALID_PARAM');
  }

  const mongoPapers = await paperDocument.getByYear(yearInt);
  const papers = await enrichPapersWithSqlFlags(mongoPapers);
  res.json({
    year, papers, count: papers.length,
    source: 'mongodb',
    reason: 'MongoDB year index provides fast filtering',
  });
});

// ── GET PAPERS BY JOURNAL ─────────────────────────────────────────────────────
const getPapersByJournal = asyncHandler(async (req, res) => {
  const { journal } = req.params;
  const mongoPapers = await paperDocument.getByJournal(journal);
  const papers = await enrichPapersWithSqlFlags(mongoPapers);
  res.json({
    journal, papers, count: papers.length,
    source: 'mongodb',
    reason: 'MongoDB journal index provides fast filtering',
  });
});

// ── GET PAPERS BY AUTHOR ──────────────────────────────────────────────────────
const getPapersByAuthor = asyncHandler(async (req, res) => {
  const { author } = req.params;
  const result = await paperDocument.advancedSearch({ author });
  const papers = await enrichPapersWithSqlFlags(result.papers || result);
  res.json({
    author, papers, count: papers.length,
    source: 'mongodb',
    reason: 'MongoDB for document details with author filter',
  });
});

// ── GET FILTER OPTIONS ────────────────────────────────────────────────────────
const getFilterOptions = asyncHandler(async (req, res) => {
  const filterOptions = await paperDocument.getFilterOptions();
  res.json({
    filters: filterOptions,
    source:  'mongodb',
    reason:  'MongoDB aggregation provides fast distinct value queries',
  });
});

// ── GET SUGGESTIONS ───────────────────────────────────────────────────────────
const getSuggestions = asyncHandler(async (req, res) => {
  const { q, type = 'all' } = req.query;

  if (!q || q.length < 2) return res.json({ suggestions: [] });

  const suggestions = await paperDocument.getSuggestions(q, type);
  res.json({
    suggestions, type,
    source: 'mongodb',
    reason: 'MongoDB regex search for autocomplete',
  });
});

// ── CREATE PAPER ──────────────────────────────────────────────────────────────
const createPaper = asyncHandler(async (req, res) => {
  const paper = normalisePaperPayload(req.body);

  if (!paper.paper_id || !paper.title) {
    throw new AppError('Fields paper_id and title are required.', 400, 'MISSING_FIELDS');
  }

  let mysqlCreate = null;
  try {
    mysqlCreate = await PaperModel.create(paper);
  } catch (mysqlErr) {
    throw classifyError(mysqlErr);
  }

  let mongoResult = { insertedId: null };
  try {
    mongoResult = await paperDocument.create(paper);
  } catch (mongoErr) {
    await rollbackSqlPaperCreate(mysqlCreate);
    throw classifyError(mongoErr);
  }

  try {
    await syncPaperToGraph(paper);
  } catch (neo4jErr) {
    await rollbackMongoPaperCreate(paper.paper_id);
    await rollbackSqlPaperCreate(mysqlCreate);
    throw new AppError(
      `Paper creation was rolled back because graph sync failed: ${neo4jErr.message}`,
      500,
      'NEO4J_SYNC_FAILED'
    );
  }

  res.status(201).json({
    message:           'Paper created successfully.',
    mongodb_insert_id: mongoResult.insertedId,
    mysql_insert_id:   mysqlCreate.result.insertId,
    paper,
    trigger_effects: {
      trg_validate_paper:       'Title length validated (>= 5 chars required)',
      trg_after_paper_insert:   'paper_metrics row auto-created',
      trg_update_journal_count: 'journals.paper_count incremented',
      trg_mark_important_paper: 'is_important set to TRUE if >= 5 authors linked',
    },
    graph_sync: 'Neo4j author, journal, paper, year, and source nodes updated',
  });
});

// ── UPDATE PAPER ──────────────────────────────────────────────────────────────
const updatePaper = asyncHandler(async (req, res) => {
  const { id }   = req.params;
  const updates  = req.body;

  if (!updates || Object.keys(updates).length === 0) {
    throw new AppError('No update fields provided.', 400, 'MISSING_FIELDS');
  }

  try {
    const result = await paperDocument.update(id, updates);
    if (result.matchedCount === 0) {
      throw new AppError(`Paper '${id}' not found in MongoDB.`, 404, 'NOT_FOUND');
    }
  } catch (err) {
    if (err instanceof AppError) throw err;
    throw classifyError(err);
  }

  try {
    await PaperModel.update(id, updates);
  } catch (mysqlErr) {
    console.warn('MySQL update warning:', classifyError(mysqlErr).message);
  }

  res.json({ message: 'Paper updated.', paper_id: id, updates });
});

// ── DELETE PAPER ──────────────────────────────────────────────────────────────
// Deletion order: MongoDB first, then MySQL (which recomputes journals.paper_count),
// then Neo4j cleanup.  Each step is attempted even if a previous one warns, so all
// three stores are kept in sync after a delete.
const deletePaper = asyncHandler(async (req, res) => {
  const { id } = req.params;

  // Step 1: MongoDB
  try {
    const result = await paperDocument.delete(id);
    if (result.deletedCount === 0) {
      throw new AppError(`Paper '${id}' not found in MongoDB.`, 404, 'NOT_FOUND');
    }
  } catch (err) {
    if (err instanceof AppError) throw err;
    throw classifyError(err);
  }

  // Step 2: MySQL  — paperModel.delete() also recomputes journals.paper_count
  try {
    await PaperModel.delete(id);
  } catch (mysqlErr) {
    const appErr = classifyError(mysqlErr);
    if (appErr.code === 'FK_REFERENCE_EXISTS') {
      console.warn(`MySQL delete FK warning for paper '${id}':`, appErr.message);
    } else {
      console.warn('MySQL delete warning:', appErr.message);
    }
  }

  // Step 3: Neo4j — removes Paper node and orphaned Author/Journal/Year/Source nodes
  try {
    await removePaperFromGraph(id);
  } catch (neo4jErr) {
    console.warn(`Neo4j delete warning for paper '${id}':`, neo4jErr.message);
  }

  res.json({ message: 'Paper deleted.', paper_id: id });
});

// ── BULK ADD PAPERS ───────────────────────────────────────────────────────────
const addPapersBulk = asyncHandler(async (req, res) => {
  const papers = req.body;

  if (!Array.isArray(papers) || papers.length === 0) {
    throw new AppError('Expected a non-empty array of papers.', 400, 'INVALID_BODY');
  }

  const results = { inserted: 0, skipped_duplicate: 0, failed: 0, errors: [] };

  for (const paper of papers) {
    try {
      await PaperModel.create(paper);
      results.inserted++;
    } catch (err) {
      const appErr = classifyError(err);
      if (appErr.code === 'DUPLICATE_ENTRY') {
        results.skipped_duplicate++;
      } else {
        results.failed++;
        results.errors.push({ paper_id: paper.paper_id, error: appErr.message, code: appErr.code });
      }
    }
  }

  const httpStatus = results.inserted > 0 ? 207 : 400;
  res.status(httpStatus).json({
    message:           'Bulk insert completed.',
    total_submitted:   papers.length,
    inserted:          results.inserted,
    skipped_duplicate: results.skipped_duplicate,
    failed:            results.failed,
    errors:            results.errors,
    trigger_note:      'trg_validate_paper, trg_after_paper_insert, trg_update_journal_count fired per paper.',
  });
});

module.exports = {
  getAllPapers,
  getPaperById,
  searchPapers,
  findSimilarPapers,
  getPapersByYear,
  getPapersByJournal,
  getPapersByAuthor,
  getFilterOptions,
  getSuggestions,
  createPaper,
  updatePaper,
  deletePaper,
  addPapersBulk,
};
