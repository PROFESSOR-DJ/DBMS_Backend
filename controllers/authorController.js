// authorController handles backend author CRUD and search requests.
// Trigger guard on delete: trg_before_author_delete (BEFORE DELETE on authors)
//   raises SQLSTATE 45000 with a descriptive message when the author has linked papers.
//   The old manual JS pre-check has been removed; the trigger is the single source of truth.

const AuthorModel = require('../models/mysql/authorModel');
const { AppError, classifyError, asyncHandler } = require('../utils/errorHandler');

// ── GET ALL AUTHORS ───────────────────────────────────────────────────────────
const getAllAuthors = asyncHandler(async (req, res) => {
  const limit  = parseInt(req.query.limit,  10) || 100;
  const offset = parseInt(req.query.offset, 10) || 0;

  if (isNaN(limit) || isNaN(offset) || limit < 1 || offset < 0) {
    throw new AppError('Invalid pagination parameters.', 400, 'INVALID_PARAM');
  }

  const authors = await AuthorModel.findAll(limit, offset);
  res.json({ authors, count: authors.length, limit, offset });
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
    message:   'Author created and linked to paper.',
    author_id: result.insertId,
    name:      name.trim(),
    paper_id,
    note:      'trg_after_paper_authors_insert has updated paper_metrics.author_count. ' +
               'If author_count reached 5, trg_mark_important_paper flagged the paper as important.',
  });
});

// ── UPDATE AUTHOR ─────────────────────────────────────────────────────────────
const updateAuthor = asyncHandler(async (req, res) => {
  const { id }   = req.params;
  const { name } = req.body;

  if (!name || name.trim().length === 0) {
    throw new AppError('Author name is required.', 400, 'MISSING_FIELDS');
  }

  const existing = await AuthorModel.findById(id);
  if (!existing) {
    throw new AppError(`Author with id '${id}' not found.`, 404, 'NOT_FOUND');
  }

  try {
    await AuthorModel.update(id, { author_name: name.trim() });
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
  createAuthor,
  updateAuthor,
  deleteAuthor,
};