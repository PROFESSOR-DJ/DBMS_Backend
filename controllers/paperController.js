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

    res.json({
      papers,
      pagination: {
        page: page,
        limit: limit,
        total,
        pages: Math.ceil(total / limit)
      },
      source: 'mongodb',
      reason: 'Optimized for large-scale document retrieval'
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

    if (!paper) {
      return res.status(404).json({ error: 'Paper not found' });
    }

    // Optional enrichment: Get verified relationships from SQL
    try {
      const sqlPaper = await PaperModel.findById(id);
      if (sqlPaper) {
        const authors = await PaperAuthorModel.getAuthorsByPaper(id);
        paper.verified_authors = authors.map(a => ({
          id: a.author_id,
          name: a.name
        }));
      }
    } catch (enrichError) {
      // SQL enrichment is optional, continue without it
      console.log('SQL enrichment skipped:', enrichError.message);
    }

    res.json({
      paper,
      source: 'mongodb',
      enriched_with: 'mysql_relationships',
      reason: 'MongoDB for document, SQL for verified relationships'
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
      reason: 'Text search optimized with MongoDB text indexes',
      query_params: { q, yearFrom, yearTo, journal, author, minCitations, sortBy }
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

    // DECISION: Use MongoDB for journal filtering
    const papers = await paperDocument.getByJournal(journal);

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
    
    let papers = [];
    
    try {
      // Step 1: Get verified author from SQL
      const authorRecord = await AuthorModel.findByName(author);
      
      if (authorRecord) {
        // Step 2: Get paper IDs from SQL (normalized relationships)
        const sqlPapers = await PaperAuthorModel.getPapersByAuthor(authorRecord.author_id);
        const paperIds = sqlPapers.map(p => p.paper_id);
        
        // Step 3: Get full paper documents from MongoDB
        if (paperIds.length > 0) {
          const mongoPapers = await paperDocument.collection.find({
            paper_id: { $in: paperIds }
          }).toArray();
          
          papers = mongoPapers;
        }
      }
    } catch (sqlError) {
      console.log('SQL lookup failed, falling back to MongoDB:', sqlError.message);
      // Fallback: Direct MongoDB search
      papers = await paperDocument.getByAuthor(author);
    }

    res.json({
      author,
      papers,
      count: papers.length,
      source: 'hybrid',
      reason: 'SQL for verified relationships, MongoDB for document details'
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
    
    // Step 1: Create in SQL (normalized schema with constraints)
    const mysqlResult = await PaperModel.create(paper);
    
    // Step 2: Create author relationships in SQL
    if (paper.authors && Array.isArray(paper.authors)) {
      for (const authorName of paper.authors) {
        let author = await AuthorModel.findByName(authorName);
        if (!author) {
          const authorResult = await AuthorModel.create({ name: authorName });
          author = { author_id: authorResult.insertId, name: authorName };
        }
        await PaperAuthorModel.create(paper.paper_id, author.author_id);
      }
    }
    
    // Step 3: Create in MongoDB (full document with metadata)
    const mongoResult = await paperDocument.create(paper);

    res.status(201).json({
      message: 'Paper created successfully in hybrid architecture',
      mysql_insert_id: mysqlResult.insertId,
      mongo_insert_id: mongoResult.insertedId,
      paper,
      reason: 'SQL for integrity, MongoDB for search and analytics'
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
    await PaperModel.update(id, updates);
    await paperDocument.update(id, updates);

    res.json({
      message: 'Paper updated in both databases',
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
    
    await PaperModel.delete(id);
    await paperDocument.delete(id);
    
    res.json({
      message: 'Paper deleted from both databases',
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

    // DECISION: Bulk insert into MongoDB first (faster for large batches)
    const mongoResults = await paperDocument.collection.insertMany(papers);
    
    // Then sync to SQL asynchronously (don't block response)
    setImmediate(async () => {
      for (const paper of papers) {
        try {
          await PaperModel.create(paper);
          
          if (paper.authors) {
            for (const authorName of paper.authors) {
              let author = await AuthorModel.findByName(authorName);
              if (!author) {
                const authorResult = await AuthorModel.create({ name: authorName });
                author = { author_id: authorResult.insertId, name: authorName };
              }
              await PaperAuthorModel.create(paper.paper_id, author.author_id);
            }
          }
        } catch (syncError) {
          console.error(`Failed to sync paper ${paper.paper_id} to SQL:`, syncError.message);
        }
      }
    });

    res.json({
      message: 'Bulk insert completed in MongoDB, syncing to SQL in background',
      inserted_count: mongoResults.insertedCount,
      total_papers: papers.length,
      reason: 'MongoDB for fast bulk insert, SQL sync for integrity'
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