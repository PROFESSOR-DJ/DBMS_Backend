const PaperModel       = require('../models/mysql/paperModel');
const AuthorModel      = require('../models/mysql/authorModel');
const PaperAuthorModel = require('../models/mysql/paperAuthorModel');
const PaperDocument    = require('../models/mongodb/paperModel');
const DatabaseRouter   = require('../config/databaseRouter');
const { AppError, classifyError, asyncHandler, sendError } = require('../utils/errorHandler');

const paperDocument = new PaperDocument();

/**
 * Get all papers — MongoDB (flexible schema, fast retrieval)
 */
const getAllPapers = asyncHandler(async (req, res) => {
  const limit  = parseInt(req.query.limit, 10) || 20;
  const page   = parseInt(req.query.page,  10) || 1;
  const offset = (page - 1) * limit;
  const sortBy = req.query.sortBy || 'recent';

  const papers = await paperDocument.findAll(limit, offset, sortBy);
  const stats  = await paperDocument.getStats();
  const total  = stats.totalPapers || 0;

  res.json({
    papers,
    pagination: { page, limit, total, pages: Math.ceil(total / limit) },
    source: 'mongodb',
    reason: 'MongoDB for flexible schema and fast retrieval of large-scale data',
  });
});

/**
 * Get paper by ID — MongoDB primary, optional SQL enrichment
 */
const getPaperById = asyncHandler(async (req, res) => {
  const { id } = req.params;

  const paper = await paperDocument.findById(id);
  if (!paper) {
    throw new AppError(`Paper with id '${id}' not found.`, 404, 'NOT_FOUND');
  }

  res.json({
    paper,
    source: 'mongodb',
    enriched_with: 'sql_relationships',
    reason: 'MongoDB for document retrieval with SQL enrichment',
  });
});

/**
 * Search papers — MongoDB full-text search
 */
const searchPapers = asyncHandler(async (req, res) => {
  const {
    q, yearFrom, yearTo, journal, author,
    minCitations, keywords, abstract, doi,
    sortBy = 'relevance',
  } = req.query;

  const limit  = parseInt(req.query.limit, 10) || 20;
  const page   = parseInt(req.query.page,  10) || 1;
  const offset = (page - 1) * limit;

  const searchParams = {
    query: q,
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
  };

  const result = await paperDocument.advancedSearch(searchParams);

  res.json({
    papers: result.papers,
    count:  result.papers.length,
    total:  result.total,
    pagination: {
      page, limit, total: result.total,
      pages: Math.ceil(result.total / limit),
    },
    source: 'mongodb',
    reason: 'MongoDB text search optimised with indexed fields',
  });
});

/**
 * Get papers by year
 */
const getPapersByYear = asyncHandler(async (req, res) => {
  const { year } = req.params;
  const yearInt  = parseInt(year, 10);

  if (isNaN(yearInt)) {
    throw new AppError('Year must be a valid integer.', 400, 'INVALID_PARAM');
  }

  const papers = await paperDocument.getByYear(yearInt);

  res.json({
    year, papers, count: papers.length,
    source: 'mongodb',
    reason: 'MongoDB year index provides fast filtering',
  });
});

/**
 * Get papers by journal
 */
const getPapersByJournal = asyncHandler(async (req, res) => {
  const { journal } = req.params;

  const papers = await paperDocument.getByJournal(journal);

  res.json({
    journal, papers, count: papers.length,
    source: 'mongodb',
    reason: 'MongoDB journal index provides fast filtering',
  });
});

/**
 * Get papers by author — MongoDB
 */
const getPapersByAuthor = asyncHandler(async (req, res) => {
  const { author } = req.params;

  const result = await paperDocument.advancedSearch({ author });
  const papers = result.papers || result;

  res.json({
    author, papers, count: papers.length,
    source: 'mongodb',
    reason: 'MongoDB for document details with author filter',
  });
});

/**
 * Get filter options
 */
const getFilterOptions = asyncHandler(async (req, res) => {
  const filterOptions = await paperDocument.getFilterOptions();

  res.json({
    filters: filterOptions,
    source: 'mongodb',
    reason: 'MongoDB aggregation provides fast distinct value queries',
  });
});

/**
 * Get search suggestions
 */
const getSuggestions = asyncHandler(async (req, res) => {
  const { q, type = 'all' } = req.query;

  if (!q || q.length < 2) {
    return res.json({ suggestions: [] });
  }

  const suggestions = await paperDocument.getSuggestions(q, type);

  res.json({
    suggestions, type,
    source: 'mongodb',
    reason: 'MongoDB regex search for autocomplete',
  });
});

/**
 * Create paper — write to both databases
 */
const createPaper = asyncHandler(async (req, res) => {
  const paper = req.body;

  if (!paper.paper_id || !paper.title) {
    throw new AppError('Fields paper_id and title are required.', 400, 'MISSING_FIELDS');
  }

  // ── MongoDB insert ──
  let mongoResult = { insertedId: null };
  try {
    mongoResult = await paperDocument.create(paper);
  } catch (mongoErr) {
    // Re-throw as AppError with proper status
    throw classifyError(mongoErr);
  }

  // ── MySQL insert ──
  let mysqlResult = { insertId: null };
  try {
    mysqlResult = await PaperModel.create(paper);
  } catch (mysqlErr) {
    const appErr = classifyError(mysqlErr);
    // 409 Duplicate in MySQL after MongoDB succeeded — still a valid partial insert;
    // surface the conflict clearly.
    if (appErr.code === 'DUPLICATE_ENTRY') {
      return res.status(409).json({
        error:   appErr.message,
        code:    appErr.code,
        detail:  'Paper was inserted in MongoDB but already existed in MySQL.',
        mongodb_insert_id: mongoResult.insertedId,
      });
    }
    // Other MySQL errors: log and continue (MongoDB is primary)
    console.warn('MySQL paper insert warning:', appErr.message);
  }

  res.status(201).json({
    message: 'Paper created successfully.',
    mongodb_insert_id: mongoResult.insertedId,
    mysql_insert_id:   mysqlResult.insertId,
    paper,
    reason: 'MongoDB primary document store, MySQL for relational integrity',
  });
});

/**
 * Update paper — update both databases
 */
const updatePaper = asyncHandler(async (req, res) => {
  const { id }    = req.params;
  const updates   = req.body;

  if (!updates || Object.keys(updates).length === 0) {
    throw new AppError('No update fields provided.', 400, 'MISSING_FIELDS');
  }

  // ── MongoDB update ──
  try {
    const result = await paperDocument.update(id, updates);
    if (result.matchedCount === 0) {
      throw new AppError(`Paper '${id}' not found in MongoDB.`, 404, 'NOT_FOUND');
    }
  } catch (err) {
    if (err instanceof AppError) throw err;
    throw classifyError(err);
  }

  // ── MySQL update (non-fatal if it fails) ──
  try {
    await PaperModel.update(id, updates);
  } catch (mysqlErr) {
    console.warn('MySQL update warning:', classifyError(mysqlErr).message);
  }

  res.json({ message: 'Paper updated.', paper_id: id, updates });
});

/**
 * Delete paper — delete from both databases
 */
const deletePaper = asyncHandler(async (req, res) => {
  const { id } = req.params;

  // ── MongoDB delete ──
  try {
    const result = await paperDocument.delete(id);
    if (result.deletedCount === 0) {
      throw new AppError(`Paper '${id}' not found in MongoDB.`, 404, 'NOT_FOUND');
    }
  } catch (err) {
    if (err instanceof AppError) throw err;
    throw classifyError(err);
  }

  // ── MySQL delete (non-fatal) ──
  try {
    await PaperModel.delete(id);
  } catch (mysqlErr) {
    const appErr = classifyError(mysqlErr);
    // FK violation — child rows exist; warn but don't fail (MongoDB already deleted)
    if (appErr.code === 'FK_REFERENCE_EXISTS') {
      console.warn(`MySQL delete FK warning for paper '${id}':`, appErr.message);
    } else {
      console.warn('MySQL delete warning:', appErr.message);
    }
  }

  res.json({ message: 'Paper deleted.', paper_id: id });
});

/**
 * Bulk insert — MySQL with per-row error handling
 */
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

  const httpStatus = results.inserted > 0 ? 207 : 400;  // 207 Multi-Status for partial success
  res.status(httpStatus).json({
    message:          'Bulk insert completed.',
    total_submitted:  papers.length,
    inserted:         results.inserted,
    skipped_duplicate: results.skipped_duplicate,
    failed:           results.failed,
    errors:           results.errors,
    reason:           'MySQL for referential integrity on bulk operations',
  });
});

module.exports = {
  getAllPapers,
  getPaperById,
  searchPapers,
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