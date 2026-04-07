// journalController handles Scimago-style journal browsing and details.
const JournalModel = require('../models/mysql/journalModel');
const { AppError, asyncHandler } = require('../utils/errorHandler');

const parseBoolean = (value) => {
  if (value === undefined) return undefined;
  if (value === true || value === 'true' || value === '1') return true;
  if (value === false || value === 'false' || value === '0') return false;
  return undefined;
};

const searchJournals = asyncHandler(async (req, res) => {
  const q = req.query.q || '';
  const country = req.query.country;
  const quartile = req.query.quartile;
  const oa = parseBoolean(req.query.oa);
  const sortBy = req.query.sortBy || 'rank';
  const limit = Number.parseInt(req.query.limit || '20', 10);
  const offset = Number.parseInt(req.query.offset || '0', 10);

  if (!Number.isInteger(limit) || limit < 1 || limit > 200) {
    throw new AppError('limit must be an integer between 1 and 200.', 400, 'INVALID_PARAM');
  }
  if (!Number.isInteger(offset) || offset < 0) {
    throw new AppError('offset must be an integer >= 0.', 400, 'INVALID_PARAM');
  }

  const filters = { q, country, quartile, oa, sortBy, limit, offset };
  const [journals, total] = await Promise.all([
    JournalModel.searchJournals(filters),
    JournalModel.countSearch(filters),
  ]);

  res.json({
    filters: { q, country, quartile, oa, sortBy },
    pagination: { limit, offset, total },
    journals,
  });
});

const getJournalDetails = asyncHandler(async (req, res) => {
  const { id } = req.params;
  const data = await JournalModel.getJournalProfile(id);

  if (!data) {
    throw new AppError(`Journal '${id}' not found.`, 404, 'NOT_FOUND');
  }

  const { journal, papers, topAuthors } = data;
  const paperIds = papers.map((paper) => paper.paper_id);

  res.json({
    journal,
    connected_entities: {
      papers,
      paper_count: papers.length,
      top_authors: topAuthors,
      link_targets: {
        papers_page_cards: paperIds,
        authors_page_ids: topAuthors.map((author) => author.author_id),
        journal_search_key: journal.title,
      },
    },
  });
});

module.exports = {
  searchJournals,
  getJournalDetails,
};
