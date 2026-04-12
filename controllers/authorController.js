// authorController handles backend author CRUD and search requests.
// Trigger guard on delete: trg_before_author_delete (BEFORE DELETE on authors)
//   raises SQLSTATE 45000 with a descriptive message when the author has linked papers.
//   The old manual JS pre-check has been removed; the trigger is the single source of truth.

const AuthorModel = require('../models/mysql/authorModel');
const PaperDocument = require('../models/mongodb/paperModel');
const { runQuery, isNeo4jConnected } = require('../config/neo4jDatabase');
const { AppError, classifyError, asyncHandler } = require('../utils/errorHandler');
const { syncAuthorCreate, syncAuthorUpdate, syncAuthorDelete } = require('../services/authorSyncService');

const paperDocument = new PaperDocument();

// ── GET ALL AUTHORS ───────────────────────────────────────────────────────────
const getAllAuthors = asyncHandler(async (req, res) => {
  const limit = parseInt(req.query.limit, 10) || 100;
  const offset = parseInt(req.query.offset, 10) || 0;
  const sortBy = ['recent', 'name', 'papers'].includes(req.query.sortBy)
    ? req.query.sortBy
    : 'recent';

  if (isNaN(limit) || isNaN(offset) || limit < 1 || offset < 0) {
    throw new AppError('Invalid pagination parameters.', 400, 'INVALID_PARAM');
  }

  const authors = await AuthorModel.findAll(limit, offset, sortBy);
  res.json({ authors, count: authors.length, limit, offset, sortBy });
});

// ── SEARCH AUTHORS ────────────────────────────────────────────────────────────
const searchAuthors = asyncHandler(async (req, res) => {
  const { q } = req.query;
  if (!q || q.trim().length === 0) {
    throw new AppError('Query parameter "q" is required.', 400, 'MISSING_PARAM');
  }

  const authors = await AuthorModel.searchByName(q.trim());
  res.json({ authors, count: authors.length, query: q });
});

const getAuthorInsights = asyncHandler(async (req, res) => {
  const { id } = req.params;
  const author = await AuthorModel.findById(id);

  if (!author) {
    throw new AppError(`Author with id '${id}' not found.`, 404, 'NOT_FOUND');
  }

  const papers = await AuthorModel.getPapersByAuthor(author.author_name);

  const mysqlSummary = {
    author_id: author.author_id,
    author_name: author.author_name,
    paper_count: papers.length,
    latest_year: papers[0]?.publish_year || null,
    papers: papers.slice(0, 8),
  };

  const [mongoSummary, graphSummary] = await Promise.all([
    paperDocument.getAuthorInsights(author.author_name, 5).catch(() => ({
      paper_count: 0,
      total_citations: 0,
      avg_citations: 0,
      first_year: null,
      latest_year: null,
      top_keywords: [],
      top_journals: [],
      recent_papers: [],
      warning: 'MongoDB author insight lookup failed.',
    })),
    (async () => {
      if (!isNeo4jConnected()) {
        return {
          coauthors: [],
          collaboration_strength: 0,
          source: 'neo4j',
          warning: 'Neo4j is not connected.',
        };
      }

      const records = await runQuery(
        `
        MATCH (a:Author {name: $name})-[:WROTE]->(p:Paper)<-[:WROTE]-(co:Author)
        WHERE co.name <> $name
        RETURN co.name AS coauthor, COUNT(DISTINCT p) AS sharedPapers
        ORDER BY sharedPapers DESC, coauthor ASC
        LIMIT 8
        `,
        { name: author.author_name }
      );

      const coauthors = records.map((record) => ({
        name: record.get('coauthor'),
        shared_papers: typeof record.get('sharedPapers')?.toNumber === 'function'
          ? record.get('sharedPapers').toNumber()
          : Number(record.get('sharedPapers') || 0),
      }));

      return {
        coauthors,
        collaboration_strength: coauthors.reduce((sum, item) => sum + item.shared_papers, 0),
        source: 'neo4j',
      };
    })(),
  ]);

  res.json({
    author: {
      author_id: author.author_id,
      author_name: author.author_name,
      created_at: author.created_at || null,
    },
    mysql: mysqlSummary,
    mongodb: mongoSummary,
    neo4j: graphSummary,
    use_cases: {
      mysql: 'Structured author-paper list and edit-safe author identity.',
      mongodb: 'Discovery signals such as keywords, citations, and recent topical coverage.',
      neo4j: 'Collaboration context and strongest co-author links.',
    },
  });
});

// ── CREATE AUTHOR ─────────────────────────────────────────────────────────────
// Side-effects handled by triggers:
//   trg_after_paper_authors_insert — increments paper_metrics.author_count when linked.
//   trg_mark_important_paper       — sets papers.is_important=TRUE if count reaches 5.
const createAuthor = asyncHandler(async (req, res) => {
  const { name, paper_id } = req.body;

  if (!name || name.trim().length === 0) {
    throw new AppError('Author name is required.', 400, 'MISSING_FIELDS');
  }
  if (!paper_id) {
    throw new AppError('Linking to a paper (paper_id) is required for new authors.', 400, 'MISSING_FIELDS');
  }

  let result;
  try {
    result = await AuthorModel.create({ author_name: name.trim(), paper_id });
    
    // Sync to MongoDB and Neo4j
    await syncAuthorCreate(name.trim(), paper_id);
  } catch (err) {
    const appErr = classifyError(err);

    if (appErr.code === 'DUPLICATE_ENTRY') {
      throw new AppError(
        `Author '${name}' is already linked to paper '${paper_id}'.`,
        409,
        'DUPLICATE_ENTRY'
      );
    }
    if (appErr.code === 'FK_REFERENCE_NOT_FOUND') {
      throw new AppError(
        `Paper '${paper_id}' does not exist. Create the paper before linking an author.`,
        400,
        'FK_REFERENCE_NOT_FOUND'
      );
    }
    if (appErr.code === 'BUSINESS_RULE_VIOLATION') {
      throw new AppError(appErr.message, 409, 'BUSINESS_RULE_VIOLATION');
    }

    throw appErr;
  }

  res.status(201).json({
    message: 'Author created and linked to paper.',
    author_id: result.insertId,
    name: name.trim(),
    paper_id,
    note: 'trg_after_paper_authors_insert has updated paper_metrics.author_count. ' +
      'If author_count reached 5, trg_mark_important_paper flagged the paper as important.',
  });
});

// ── UPDATE AUTHOR ─────────────────────────────────────────────────────────────
const updateAuthor = asyncHandler(async (req, res) => {
  const { id } = req.params;
  const { name } = req.body;

  if (!name || name.trim().length === 0) {
    throw new AppError('Author name is required.', 400, 'MISSING_FIELDS');
  }

  const existing = await AuthorModel.findById(id);
  if (!existing) {
    throw new AppError(`Author with id '${id}' not found.`, 404, 'NOT_FOUND');
  }

  try {
    const oldName = existing.author_name;
    const newName = name.trim();
    
    await AuthorModel.update(id, { author_name: newName });
    
    // Sync to MongoDB and Neo4j
    await syncAuthorUpdate(oldName, newName);
  } catch (err) {
    const appErr = classifyError(err);
    if (appErr.code === 'DUPLICATE_ENTRY') {
      throw new AppError(
        `Another author named '${name}' already exists.`,
        409,
        'DUPLICATE_ENTRY'
      );
    }
    throw appErr;
  }

  res.json({ message: 'Author updated.', author_id: id, name: name.trim() });
});

// ── DELETE AUTHOR ─────────────────────────────────────────────────────────────
// The manual JS pre-check has been removed.
// trg_before_author_delete (BEFORE DELETE on authors) now owns the guard:
//   — counts paper_authors rows for this author_id
//   — raises SQLSTATE 45000 with a descriptive message if count > 0
//   — that error propagates as BUSINESS_RULE_VIOLATION (ER_SIGNAL_EXCEPTION / errno 1644)
const deleteAuthor = asyncHandler(async (req, res) => {
  const { id } = req.params;

  const existing = await AuthorModel.findById(id);
  if (!existing) {
    throw new AppError(`Author with id '${id}' not found.`, 404, 'NOT_FOUND');
  }

  try {
    await AuthorModel.delete(id);
    
    // Sync to MongoDB and Neo4j
    await syncAuthorDelete(existing.author_name);
  } catch (err) {
    const appErr = classifyError(err);

    // trg_before_author_delete raises SQLSTATE 45000 → ER_SIGNAL_EXCEPTION (1644)
    // → classifyError maps it to BUSINESS_RULE_VIOLATION
    if (appErr.code === 'BUSINESS_RULE_VIOLATION') {
      throw new AppError(appErr.message, 409, 'AUTHOR_HAS_PAPERS');
    }

    // Legacy FK path (kept for safety if trigger is ever disabled)
    if (appErr.code === 'FK_REFERENCE_EXISTS') {
      throw new AppError(
        `Author '${existing.author_name}' is still linked to papers. Remove the paper links first.`,
        409,
        'FK_REFERENCE_EXISTS'
      );
    }

    throw appErr;
  }

  res.json({ message: 'Author deleted.', author_id: id });
});

module.exports = {
  getAllAuthors,
  searchAuthors,
  getAuthorInsights,
  createAuthor,
  updateAuthor,
  deleteAuthor,
};
