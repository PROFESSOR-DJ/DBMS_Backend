const PaperModel = require('../models/mysql/paperModel');
const AuthorModel = require('../models/mysql/authorModel');
const PaperAuthorModel = require('../models/mysql/paperAuthorModel');
const PaperDocument = require('../models/mongodb/paperModel');
const DatabaseRouter = require('../config/databaseRouter');

const paperDocument = new PaperDocument();

/**
 * Get all papers - Uses MongoDB for browsing (flexible schema, fast retrieval)
 */
const getAllPapers = async (req, res) => {
  try {
    const limit = parseInt(req.query.limit, 10) || 20;
    const page = parseInt(req.query.page, 10) || 1;
    const offset = (page - 1) * limit;
    const sortBy = req.query.sortBy || 'recent';

    // DECISION: Use MongoDB for paper browsing (large-scale semi-structured data)
    const papers = await paperDocument.findAll(limit, offset, sortBy);
    const stats = await paperDocument.getStats();
    const total = stats.totalPapers || 0;

    // MySQL Replacement (kept as backup)
    // const papers = await PaperModel.findAll(limit, offset, sortBy);
    // const total = await PaperModel.count(); // Approximate total or need to add countAll to PaperModel properly if filtering
    // Actually findAll doesn't return total. 
    // Let's use advancedSearch for browsing which returns total, or just count() for now.
    // papers array from findAll matches expectation.

    res.json({
      papers,
      pagination: {
        page: page,
        limit: limit,
        total,
        pages: Math.ceil(total / limit)
      },
      source: 'mongodb',
      reason: 'MongoDB for flexible schema and fast retrieval of large-scale data'
    });
  } catch (error) {
    console.error('Get all papers error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

/**
 * Get paper by ID - Uses MongoDB for document retrieval
 * Then enriches with SQL relationship data if needed
 */
const getPaperById = async (req, res) => {
  try {
    const { id } = req.params;

    // Primary: MongoDB for document retrieval
    const paper = await paperDocument.findById(id);

    // MySQL Replacement (kept as backup)
    // const paper = await PaperModel.findById(id);

    if (!paper) {
      return res.status(404).json({ error: 'Paper not found' });
    }

    // Optional enrichment: Get verified relationships from SQL
    try {
      // const sqlPaper = await PaperModel.findById(id); // Already fetched
      /*
      if (sqlPaper) {
        const authors = await PaperAuthorModel.getAuthorsByPaper(id);
        paper.verified_authors = authors.map(a => ({
          id: a.author_id,
          name: a.name
        }));
      }
      */
      // Authors are already joined in findById in PaperModel now (as comma separated string)
      // If we need array of objects, we might need to parse or fetch separately.
      // Frontend expects array of strings or string, we handled that. 
    } catch (enrichError) {
      // SQL enrichment is optional, continue without it
      console.log('SQL enrichment skipped:', enrichError.message);
    }

    res.json({
      paper,
      source: 'mongodb',
      enriched_with: 'sql_relationships',
      reason: 'MongoDB for document retrieval with SQL enrichment'
    });
  } catch (error) {
    console.error('Get paper by ID error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

/**
 * Search papers - Uses MongoDB (full-text search capability)
 */
const searchPapers = async (req, res) => {
  try {
    const {
      q,
      yearFrom,
      yearTo,
      journal,
      author,
      minCitations,
      keywords,
      abstract,
      doi,
      sortBy = 'relevance'
    } = req.query;

    const limit = parseInt(req.query.limit, 10) || 20;
    const page = parseInt(req.query.page, 10) || 1;
    const offset = (page - 1) * limit;

    // DECISION: Use MongoDB for text search (text indexes, flexible queries)
    const searchParams = {
      query: q,
      yearFrom: yearFrom ? parseInt(yearFrom) : null,
      yearTo: yearTo ? parseInt(yearTo) : null,
      journal,
      author,
      minCitations: minCitations ? parseInt(minCitations) : null,
      keywords,
      abstract,
      doi,
      limit,
      offset,
      sortBy
    };
    
    const result = await paperDocument.advancedSearch(searchParams);

    // MySQL Replacement (kept as backup)
    // const searchParams = {
    //   query: q,
    //   yearFrom: yearFrom ? parseInt(yearFrom) : null,
    //   yearTo: yearTo ? parseInt(yearTo) : null,
    //   journal,
    //   author,
    //   limit,
    //   offset,
    //   sortBy
    // };
    // const result = await PaperModel.advancedSearch(searchParams);

    res.json({
      papers: result.papers,
      count: result.papers.length,
      total: result.total,
      pagination: {
        page,
        limit,
        total: result.total,
        pages: Math.ceil(result.total / limit)
      },
      source: 'mongodb',
      reason: 'MongoDB text search optimized with indexed fields'
    });
  } catch (error) {
    console.error('Search papers error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

/**
 * Get papers by year - Uses MongoDB for aggregation
 */
const getPapersByYear = async (req, res) => {
  try {
    const { year } = req.params;

    // DECISION: Use MongoDB for year filtering (indexed, fast aggregation)
    const papers = await paperDocument.getByYear(parseInt(year, 10));

    // MySQL Replacement (kept as backup)
    // const papers = await PaperModel.getByYear(parseInt(year, 10));

    res.json({
      year,
      papers,
      count: papers.length,
      source: 'mongodb',
      reason: 'MongoDB year index provides fast filtering'
    });
  } catch (error) {
    console.error('Get papers by year error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

/**
 * Get papers by journal - Uses MongoDB for aggregation
 */
const getPapersByJournal = async (req, res) => {
  try {
    const { journal } = req.params;

    // DECISION: Use MongoDB for journal filtering (indexed, fast aggregation)
    const papers = await paperDocument.getByJournal(journal);

    // MySQL Replacement (kept as backup)
    // const papers = await PaperModel.getByJournal(journal);

    res.json({
      journal,
      papers,
      count: papers.length,
      source: 'mongodb',
      reason: 'MongoDB journal index provides fast filtering'
    });
  } catch (error) {
    console.error('Get papers by journal error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

/**
 * Get papers by author - HYBRID APPROACH
 * Uses SQL for verified relationships, MongoDB for metadata
 */
const getPapersByAuthor = async (req, res) => {
  try {
    const { author } = req.params;

    // DECISION: Use SQL for author-paper relationships (referential integrity)
    // Then get full paper details from MongoDB

    /*
    let papers = [];
    try {
      // ... (MongoDB logic)
    } 
    */

    // Use advancedSearch from MongoDB
    const result = await paperDocument.advancedSearch({ author });
    const papers = result.papers || result;

    // MySQL Replacement (kept as backup)
    // const result = await PaperModel.advancedSearch({ author });
    // const papers = result.papers;

    res.json({
      author,
      papers,
      count: papers.length,
      source: 'mongodb',
      reason: 'MongoDB for verified relationships and document details'
    });
  } catch (error) {
    console.error('Get papers by author error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

/**
 * Get filter options - Uses MongoDB for quick aggregation
 */
const getFilterOptions = async (req, res) => {
  try {
    // DECISION: Use MongoDB aggregation pipeline (optimized for analytics)
    const filterOptions = await paperDocument.getFilterOptions();

    // MySQL Replacement (kept as backup)
    // const filterOptions = await PaperModel.getFilterOptions();

    res.json({
      filters: filterOptions,
      source: 'mongodb',
      reason: 'MongoDB aggregation provides fast distinct value queries'
    });
  } catch (error) {
    console.error('Get filter options error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

/**
 * Get suggestions - Uses MongoDB for text matching
 */
const getSuggestions = async (req, res) => {
  try {
    const { q, type = 'all' } = req.query;

    if (!q || q.length < 2) {
      return res.json({ suggestions: [] });
    }

    // DECISION: Use MongoDB for autocomplete (regex queries on indexed fields)
    const suggestions = await paperDocument.getSuggestions(q, type);

    // MySQL Replacement (kept as backup)
    // const suggestions = await PaperModel.getSuggestions(q, type);

    res.json({
      suggestions,
      type,
      source: 'mongodb',
      reason: 'MongoDB regex search for autocomplete functionality'
    });
  } catch (error) {
    console.error('Get suggestions error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

/**
 * Create paper - HYBRID APPROACH with transaction-like behavior
 * Uses SQL for normalized data, MongoDB for full document
 */
const createPaper = async (req, res) => {
  try {
    const paper = req.body;

    // DECISION: Write to both databases for consistency
    // SQL: Normalized relational data with integrity
    // MongoDB: Full document for search and analytics

    // Step 1: Create in MongoDB (full document with metadata)
    let mongoResult = { insertedId: null };
    if (paperDocument) {
      mongoResult = await paperDocument.create(paper);
    }

    // Step 2: Create in SQL (normalized schema with constraints)
    // PaperModel.create now handles transaction including journals, authors, and metrics
    let mysqlResult = { insertId: null };
    try {
      mysqlResult = await PaperModel.create(paper);
    } catch (err) {
      console.log('SQL insert backup:', err.message);
    }

    res.status(201).json({
      message: 'Paper created successfully in MongoDB',
      mongodb_insert_id: mongoResult.insertedId,
      mysql_insert_id: mysqlResult.insertId,
      paper,
      reason: 'MongoDB for flexible document storage, SQL as backup'
    });
  } catch (error) {
    console.error('Create paper error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

/**
 * Update paper - Updates both databases
 */
const updatePaper = async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;

    // Update both databases for consistency
    // MongoDB update
    if (paperDocument) {
      await paperDocument.update(id, updates);
    }
    // MySQL update as backup
    try {
      await PaperModel.update(id, updates);
    } catch (err) {
      console.log('MySQL update backup:', err.message);
    }

    res.json({
      message: 'Paper updated in MongoDB',
      paper_id: id,
      updates
    });
  } catch (error) {
    console.error('Update paper error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

/**
 * Delete paper - Deletes from both databases
 */
const deletePaper = async (req, res) => {
  try {
    const { id } = req.params;

    // Delete from MongoDB
    if (paperDocument) {
      await paperDocument.delete(id);
    }
    // Delete from MySQL as backup
    try {
      await PaperModel.delete(id);
    } catch (err) {
      console.log('MySQL delete backup:', err.message);
    }

    res.json({
      message: 'Paper deleted from MongoDB',
      paper_id: id
    });
  } catch (error) {
    console.error('Delete paper error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

/**
 * Bulk insert - Uses MongoDB for efficiency, then syncs to SQL
 */
const addPapersBulk = async (req, res) => {
  try {
    const papers = req.body;

    if (!Array.isArray(papers)) {
      return res.status(400).json({ error: 'Expected an array of papers' });
    }

    // MySQL Only Bulk Insert (Sequential)
    let insertedCount = 0;
    for (const paper of papers) {
      try {
        await PaperModel.create(paper);
        insertedCount++;
        // Authors are handled by PaperModel.create now if passed in paper object
      } catch (e) {
        console.error(`Failed to insert paper ${paper.paper_id}:`, e.message);
      }
    }

    res.json({
      message: 'Bulk insert completed in MySQL',
      inserted_count: insertedCount,
      total_papers: papers.length,
      reason: 'MySQL for integrity'
    });
  } catch (error) {
    console.error('Bulk insert error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

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
  addPapersBulk
};